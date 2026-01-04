from __future__ import annotations

import io
import logging
import pandas as pd
import base64
import json
from datetime import timedelta
from bisect import bisect_left
from sqlalchemy import text
import time
import random
from Backend.celery import celery
from Backend.database import SessionLocal


logger = logging.getLogger(__name__)

# Safety cap: prevents pathological workloads (e.g., very long cycling sessions) from producing
# thousands of segment rows. This is not a "mile 10" hardcode; segments are still generated 1..N.
_MAX_SEGMENTS_PER_WORKOUT: int = 300

# Parse uploaded CSV bytes into a DataFrame for normalization and bulk inserts
def _parse_csv_bytes(data: bytes) -> pd.DataFrame:
    buffer = io.BytesIO(data)
    return pd.read_csv(buffer, dtype={"user_id": "string"})


@celery.task(name = "process_csv_upload")
def process_csv_upload(user_id: str, csv_bytes_b4: str) -> dict[str, int]:
    time.sleep(random.uniform(0.1, 0.5))  # Add a small random delay to help prevent exact simultaneous processing

    raw = base64.b64decode(csv_bytes_b4)
    df = _parse_csv_bytes(raw)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc = True, errors = "coerce")
    if "end_ts" in df.columns:
        df["end_ts"] = pd.to_datetime(df["end_ts"], utc = True, errors = "coerce")
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], utc = True, errors = "coerce")

    # Security + robustness:
    # - The authenticated user_id passed to this task is authoritative.
    # - CSVs may contain a different/old/malformed user_id (e.g., client-side bugs, casing issues).
    # To prevent cross-user writes AND avoid dropping all rows on mismatch, always stamp rows with the
    # authenticated user_id.
    if "user_id" not in df.columns:
        df["user_id"] = user_id
    else:
        try:
            # Log distinct CSV user_ids for debugging, but do not trust them.
            uniq = df["user_id"].dropna().astype(str).str.strip().unique().tolist()
            if uniq and (len(uniq) > 1 or (len(uniq) == 1 and uniq[0] != user_id)):
                logger.warning(
                    "process_csv_upload: overriding mismatched csv user_id(s)=%s with task user_id=%s",
                    uniq,
                    user_id,
                )
        except Exception:
            pass
        df["user_id"] = user_id

    if df.empty:
        logger.info("process_csv_upload: no rows for user_id=%s after filter; nothing to insert", user_id)
        return {"inserted": 0}

    # Drop/prune rows older than retention window (relative to the newest timestamp in the CSV)
    now_ts = df["timestamp"].max()
    if pd.isna(now_ts):
        # Allow delete-only uploads (may omit timestamp) for HealthKit mirroring.
        if "op" in df.columns and (df["op"].astype(str).str.lower() == "delete").any():
            now_ts = pd.Timestamp.utcnow()
        else:
            logger.info("process_csv_upload: all timestamps invalid for user_id=%s; nothing to insert", user_id)
            return {"inserted": 0}
    cutoff_ts = now_ts - pd.Timedelta(days=60)
    cutoff_dt = pd.Timestamp(cutoff_ts).to_pydatetime()
    if "op" in df.columns:
        # Never drop delete tombstones due to missing/old timestamps.
        df = df[(df["timestamp"] >= cutoff_ts) | (df["op"].astype(str).str.lower() == "delete")]
    else:
        df = df[df["timestamp"] >= cutoff_ts]

    # Split event/workout rows for separate main_health_events table. Remaining rows are raw metrics for main_health_metrics table
    df_events = df[df["metric_type"].str.startswith(("event_", "workout_"), na = False)].copy()
    df_metrics = df[~df["metric_type"].str.startswith(("event_", "workout_"), na = False)].copy()

    # Write to main_health_events and main_health_metrics table
    with SessionLocal() as session:
        def _py_dt(x):
            return pd.Timestamp(x).to_pydatetime()

        def _py_dt_or_none(x):
            if pd.isna(x):
                return None
            return pd.Timestamp(x).to_pydatetime()

        def _exec(stmt: str, params: dict, op: str) -> None:
            try:
                session.execute(text(stmt), params)
            except Exception:
                logger.exception("process_csv_upload: %s failed for user_id=%s", op, user_id)
                try:
                    session.rollback()
                except Exception:
                    logger.exception("process_csv_upload: rollback failed for user_id=%s", user_id)

        def _execmany(stmt: str, params: list[dict], op: str, delay: float) -> None:
            q = text(stmt)
            for attempt in range(3):
                try:
                    session.execute(q, params)
                    return
                except Exception as e:
                    msg = str(e).lower()
                    try:
                        session.rollback()
                    except Exception:
                        logger.exception("process_csv_upload: rollback failed for user_id=%s", user_id)
                        raise
                    transient = (
                        "deadlock detected" in msg or "could not serialize access" in msg or "serialization failure" in msg
                    )
                    if transient and attempt < 2:
                        time.sleep(delay * (attempt + 1) + random.random() * delay)
                        continue
                    logger.exception("process_csv_upload: %s failed for user_id=%s", op, user_id)
                    raise

        # Serialize same-user ingests to avoid races/deadlocks
        _exec(
            "SELECT pg_advisory_xact_lock(:lock_key, hashtext(:user_id))",
            {"lock_key": 42, "user_id": user_id},
            "advisory lock",
        )
        if cutoff_dt is not None:
            _exec(
                "DELETE FROM main_health_events WHERE user_id = :user_id AND timestamp < :cutoff",
                {"user_id": user_id, "cutoff": cutoff_dt},
                "prune main_health_events",
            )
            _exec(
                "DELETE FROM main_health_metrics WHERE user_id = :user_id AND timestamp < :cutoff",
                {"user_id": user_id, "cutoff": cutoff_dt},
                "prune main_health_metrics",
            )

        # Write to main_health_events table (mirror-only; require hk_uuid)
        deleted_event_uuids: list[str] = []
        workout_ids_deleted: set[str] = set()
        workout_ids_upserted: set[str] = set()
        if not df_events.empty:
            if "op" not in df_events.columns:
                df_events["op"] = "upsert"

            df_ev_del = df_events[df_events["op"].astype(str).str.lower().eq("delete")].copy()
            df_ev_up = df_events[~df_events["op"].astype(str).str.lower().eq("delete")].copy()

            # Fail fast: mirror mode requires hk_uuid for any upsert row.
            if not df_ev_up.empty:
                if "hk_uuid" not in df_ev_up.columns:
                    raise ValueError("Mirror ingest requires hk_uuid for main_health_events upserts")
                missing = df_ev_up["hk_uuid"].isna() | df_ev_up["hk_uuid"].astype(str).str.strip().eq("")
                if bool(missing.any()):
                    raise ValueError("Mirror ingest found main_health_events upsert rows missing hk_uuid")

            if not df_ev_del.empty and "hk_uuid" in df_ev_del.columns:
                deleted_event_uuids = (
                    df_ev_del["hk_uuid"].dropna().astype(str).str.strip().loc[lambda s: s != ""].unique().tolist()
                )
                if deleted_event_uuids:
                    # Track deleted workout ids so we can tombstone derived flags server-side.
                    for hu in deleted_event_uuids:
                        if "|" in hu:
                            wid = hu.split("|", 1)[0].strip()
                            if wid:
                                workout_ids_deleted.add(wid)
                    _exec(
                        """
                        UPDATE main_health_events
                        SET deleted_at = NOW()
                        WHERE user_id = :user_id AND hk_uuid = ANY(:uuids)
                        """,
                        {"user_id": user_id, "uuids": deleted_event_uuids},
                        "tombstone main_health_events deletes",
                    )

            # Upserts require metric_value + hk_uuid
            if not df_ev_up.empty:
                df_ev_up["metric_value"] = pd.to_numeric(df_ev_up["metric_value"], errors="coerce")
                df_ev_up = df_ev_up[pd.notna(df_ev_up["metric_value"])]

            params = []
            if not df_ev_up.empty:
                def _str_or_none(x):
                    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
                        return None
                    s = str(x).strip()
                    return s if s else None

                def _bool_or_none(x):
                    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
                        return None
                    if isinstance(x, bool):
                        return x
                    s = str(x).strip().lower()
                    if s in {"true", "1", "yes", "y"}:
                        return True
                    if s in {"false", "0", "no", "n"}:
                        return False
                    return None

                def _context_meta_event(tz, off, c, r, ci) -> dict:
                    meta: dict = {}
                    if tz is not None and pd.notna(tz) and str(tz).strip():
                        meta["tz_name"] = str(tz).strip()
                    if off is not None and pd.notna(off):
                        meta["utc_offset_min"] = int(float(off))
                    return meta

                def _merge_hk_metadata(existing_json: str | None, context: dict) -> str | None:
                    """Merge client-provided hk_metadata JSON with Voyant timezone context.

                    We store timezone here so downstream timestamp localization can use
                    hk_metadata['tz_name'] even when HK doesn't provide HKTimeZone.
                    """
                    if not context:
                        return existing_json
                    if not existing_json:
                        return json.dumps(context)
                    try:
                        parsed = json.loads(existing_json)
                        if isinstance(parsed, dict):
                            parsed.update(context)
                            return json.dumps(parsed)
                    except Exception:
                        # Fall through to wrapping the unparseable blob.
                        pass
                    wrapped = {"_raw_hk_metadata": existing_json}
                    wrapped.update(context)
                    return json.dumps(wrapped)

                for r in df_ev_up.to_dict(orient="records"):
                    hk_uuid = _str_or_none(r.get("hk_uuid"))
                    if not hk_uuid:
                        continue
                    # Track workout ids touched by base workout rows.
                    try:
                        if "|" in hk_uuid:
                            wid, suffix = hk_uuid.split("|", 1)
                            if suffix.startswith("workout_"):
                                wid = wid.strip()
                                if wid:
                                    workout_ids_upserted.add(wid)
                    except Exception:
                        pass
                    ts = r.get("timestamp")
                    etype = r.get("metric_type")
                    val = r.get("metric_value")
                    if ts is None or pd.isna(ts) or etype is None or pd.isna(etype) or val is None or pd.isna(val):
                        continue

                    # Attach timezone context (from GeoTimezoneHistoryService / TimezoneHistoryService)
                    # into hk_metadata so event timestamps can be localized consistently.
                    ctx = _context_meta_event(
                        r.get("timezone"),
                        r.get("utc_offset_min"),
                        r.get("place_country"),
                        r.get("place_region"),
                        r.get("place_city"),
                    )
                    hk_meta = _merge_hk_metadata(_str_or_none(r.get("hk_metadata")), ctx)
                    params.append(
                {
                    "user_id": user_id,
                            "hk_uuid": hk_uuid,
                    "timestamp": _py_dt(ts),
                            "end_ts": _py_dt_or_none(r.get("end_ts")) if "end_ts" in df_ev_up.columns else None,
                            "event_type": str(etype),
                    "value": float(val),
                            "unit": _str_or_none(r.get("unit")) if "unit" in df_ev_up.columns else None,
                            "source": _str_or_none(r.get("source")) if "source" in df_ev_up.columns else None,
                            "created_at": _py_dt_or_none(r.get("created_at")) if "created_at" in df_ev_up.columns else None,
                            "hk_source_bundle_id": _str_or_none(r.get("hk_source_bundle_id")) if "hk_source_bundle_id" in df_ev_up.columns else None,
                            "hk_source_name": _str_or_none(r.get("hk_source_name")) if "hk_source_name" in df_ev_up.columns else None,
                            "hk_source_version": _str_or_none(r.get("hk_source_version")) if "hk_source_version" in df_ev_up.columns else None,
                            "hk_metadata": hk_meta if "hk_metadata" in df_ev_up.columns else None,
                        }
                    )

            if params:
                _execmany(
                    """
                    INSERT INTO main_health_events
                        (user_id, hk_uuid, timestamp, end_ts, event_type, value, unit, source, created_at,
                         hk_source_bundle_id, hk_source_name, hk_source_version, hk_metadata, deleted_at)
                    VALUES
                        (:user_id, :hk_uuid, :timestamp, :end_ts, :event_type, :value, :unit, :source, :created_at,
                         :hk_source_bundle_id, :hk_source_name, :hk_source_version, CAST(:hk_metadata AS jsonb), NULL)
                    ON CONFLICT (user_id, hk_uuid, event_type) WHERE hk_uuid IS NOT NULL DO UPDATE
                    SET
                        timestamp = EXCLUDED.timestamp,
                        end_ts = COALESCE(EXCLUDED.end_ts, main_health_events.end_ts),
                        value = EXCLUDED.value,
                        unit = COALESCE(EXCLUDED.unit, main_health_events.unit),
                        source = COALESCE(EXCLUDED.source, main_health_events.source),
                        created_at = COALESCE(EXCLUDED.created_at, main_health_events.created_at),
                        hk_source_bundle_id = COALESCE(EXCLUDED.hk_source_bundle_id, main_health_events.hk_source_bundle_id),
                        hk_source_name = COALESCE(EXCLUDED.hk_source_name, main_health_events.hk_source_name),
                        hk_source_version = COALESCE(EXCLUDED.hk_source_version, main_health_events.hk_source_version),
                        hk_metadata = COALESCE(EXCLUDED.hk_metadata, main_health_events.hk_metadata),
                        deleted_at = NULL
                    """,
                    params,
                    "bulk upsert main_health_events (hk_uuid)",
                    delay=0.2,
                )

            # Keep derived workout flags only on `derived_workouts` (not in main_health_events) to avoid duplication.
            # Still clean up derived tables on workout deletes (best-effort).
            if workout_ids_deleted:
                # Remove any precomputed segments and workout rows for deleted workouts.
                _exec(
                    """
                    DELETE FROM derived_workout_segments
                    WHERE user_id = :user_id AND workout_uuid = ANY(:wids)
                    """,
                    {"user_id": user_id, "wids": sorted(list(workout_ids_deleted))},
                    "delete derived_workout_segments (workout deletes)",
                )
                _exec(
                    """
                    DELETE FROM derived_workouts
                    WHERE user_id = :user_id AND workout_uuid = ANY(:wids)
                    """,
                    {"user_id": user_id, "wids": sorted(list(workout_ids_deleted))},
                    "delete derived_workouts (workout deletes)",
                )
                # Tombstone any legacy derived rows that may exist from older versions.
                _exec(
                    """
                    UPDATE main_health_events
                    SET deleted_at = NOW()
                    WHERE user_id = :user_id
                      AND deleted_at IS NULL
                      AND hk_uuid = ANY(:uuids)
                    """,
                    {
                        "user_id": user_id,
                        "uuids": [
                            f"{wid}|event_hard_workout" for wid in workout_ids_deleted
                        ]
                        + [
                            f"{wid}|event_long_run_km" for wid in workout_ids_deleted
                        ],
                    },
                    "tombstone legacy derived workout flags (deletes)",
                )

        # Write to main_health_metrics table (HealthKit mirror-aware: raw samples + tombstone deletes)
        deleted_uuids: list[str] = []
        if not df_metrics.empty:
            if "op" not in df_metrics.columns:
                df_metrics["op"] = "upsert"

            df_del = df_metrics[df_metrics["op"].astype(str).str.lower().eq("delete")].copy()
            df_up = df_metrics[~df_metrics["op"].astype(str).str.lower().eq("delete")].copy()

            # Fail fast: mirror mode requires hk_uuid for any upsert row.
            if not df_up.empty:
                if "hk_uuid" not in df_up.columns:
                    raise ValueError("Mirror ingest requires hk_uuid for main_health_metrics upserts")
                missing = df_up["hk_uuid"].isna() | df_up["hk_uuid"].astype(str).str.strip().eq("")
                if bool(missing.any()):
                    raise ValueError("Mirror ingest found main_health_metrics upsert rows missing hk_uuid")

            # Tombstone deletes by hk_uuid (if present)
            if not df_del.empty and "hk_uuid" in df_del.columns:
                deleted_uuids = (
                    df_del["hk_uuid"].dropna().astype(str).str.strip().loc[lambda s: s != ""].unique().tolist()
                )
                if deleted_uuids:
                    _exec(
                        """
                        UPDATE main_health_metrics
                        SET deleted_at = NOW()
                        WHERE user_id = :user_id AND hk_uuid = ANY(:uuids)
                        """,
                        {"user_id": user_id, "uuids": deleted_uuids},
                        "tombstone main_health_metrics deletes",
                    )

            def _meta_json_m(tz, off, c, r, ci):
                meta: dict = {}
                if tz is not None and pd.notna(tz) and str(tz).strip():
                    meta["tz_name"] = str(tz).strip()
                if off is not None and pd.notna(off):
                    meta["utc_offset_min"] = int(float(off))
                return json.dumps(meta) if meta else None

            def _str_or_none(x):
                if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
                    return None
                s = str(x).strip()
                return s if s else None

            def _bool_or_none(x):
                if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
                    return None
                if isinstance(x, bool):
                    return x
                s = str(x).strip().lower()
                if s in {"true", "1", "yes", "y"}:
                    return True
                if s in {"false", "0", "no", "n"}:
                    return False
                return None

            params_m_uuid: list[dict] = []
            if not df_up.empty:
                df_up["metric_value"] = pd.to_numeric(df_up["metric_value"], errors="coerce")
                df_up = df_up[pd.notna(df_up["metric_value"])]

                for r in df_up.to_dict(orient="records"):
                    ts = r.get("timestamp")
                    mtype = r.get("metric_type")
                    val = r.get("metric_value")
                    if ts is None or pd.isna(ts) or mtype is None or pd.isna(mtype) or val is None or pd.isna(val):
                        continue

                    meta = _meta_json_m(
                        r.get("timezone"),
                        r.get("utc_offset_min"),
                        r.get("place_country"),
                        r.get("place_region"),
                        r.get("place_city"),
                    )

                    base = {
                    "user_id": user_id,
                    "timestamp": _py_dt(ts),
                        "end_ts": _py_dt_or_none(r.get("end_ts")) if "end_ts" in df_up.columns else None,
                        "metric_type": str(mtype),
                    "metric_value": float(val),
                        "unit": _str_or_none(r.get("unit")) if "unit" in df_up.columns else None,
                        "created_at": _py_dt_or_none(r.get("created_at")) if "created_at" in df_up.columns else None,
                    "meta": meta,
                        "hk_uuid": _str_or_none(r.get("hk_uuid")) if "hk_uuid" in df_up.columns else None,
                        "hk_source_bundle_id": _str_or_none(r.get("hk_source_bundle_id")) if "hk_source_bundle_id" in df_up.columns else None,
                        "hk_source_name": _str_or_none(r.get("hk_source_name")) if "hk_source_name" in df_up.columns else None,
                        "hk_source_version": _str_or_none(r.get("hk_source_version")) if "hk_source_version" in df_up.columns else None,
                        "hk_metadata": _str_or_none(r.get("hk_metadata")) if "hk_metadata" in df_up.columns else None,
                    }

                    if base["hk_uuid"]:
                        params_m_uuid.append(base)
                    else:
                        # Legacy rows are no longer supported; skip.
                        continue

            if params_m_uuid:
                _execmany(
                    """
                    INSERT INTO main_health_metrics
                        (user_id, hk_uuid, timestamp, end_ts, metric_type, metric_value, unit, created_at, meta,
                         hk_source_bundle_id, hk_source_name, hk_source_version, hk_metadata, deleted_at)
                    VALUES
                        (:user_id, :hk_uuid, :timestamp, :end_ts, :metric_type, :metric_value, :unit, :created_at, CAST(:meta AS jsonb),
                         :hk_source_bundle_id, :hk_source_name, :hk_source_version, CAST(:hk_metadata AS jsonb), NULL)
                    ON CONFLICT (user_id, hk_uuid) WHERE hk_uuid IS NOT NULL DO UPDATE
                    SET
                        timestamp = EXCLUDED.timestamp,
                        end_ts = COALESCE(EXCLUDED.end_ts, main_health_metrics.end_ts),
                        metric_type = EXCLUDED.metric_type,
                        metric_value = EXCLUDED.metric_value,
                        unit = COALESCE(EXCLUDED.unit, main_health_metrics.unit),
                        created_at = COALESCE(EXCLUDED.created_at, main_health_metrics.created_at),
                        meta = EXCLUDED.meta,
                        hk_source_bundle_id = COALESCE(EXCLUDED.hk_source_bundle_id, main_health_metrics.hk_source_bundle_id),
                        hk_source_name = COALESCE(EXCLUDED.hk_source_name, main_health_metrics.hk_source_name),
                        hk_source_version = COALESCE(EXCLUDED.hk_source_version, main_health_metrics.hk_source_version),
                        hk_metadata = COALESCE(EXCLUDED.hk_metadata, main_health_metrics.hk_metadata),
                        deleted_at = NULL
                    """,
                    params_m_uuid,
                    "bulk upsert main_health_metrics (hk_uuid)",
                    delay=0.25,
                )

            # No legacy upsert path.

        session.commit()  # Commit raw writes first so rollup failures never discard ingested data

        # Track metric window (if any) so we can recompute derived workout context for workouts
        # whose explanations might be affected by newly synced metrics.
        metrics_t_min = None
        metrics_t_max = None

        # Recompute hourly rollups (best-effort) for the affected time window from main_health_metrics table
        if not df_metrics.empty:
            t_min = pd.to_datetime(df_metrics["timestamp"].min())
            t_max = pd.to_datetime(df_metrics["timestamp"].max())
            if deleted_uuids:
                try:
                    res = session.execute(
                        text(
                            """
                            SELECT MIN(timestamp) AS tmin, MAX(timestamp) AS tmax
                            FROM main_health_metrics
                            WHERE user_id = :user_id AND hk_uuid = ANY(:uuids)
                            """
                        ),
                        {"user_id": user_id, "uuids": deleted_uuids},
                    ).mappings().first()
                    if res:
                        if res.get("tmin") is not None:
                            tmin_res = pd.to_datetime(res["tmin"], utc=True)
                            if pd.isna(t_min):
                                t_min = tmin_res
                            else:
                                t_min = min(t_min, tmin_res)
                        if res.get("tmax") is not None:
                            tmax_res = pd.to_datetime(res["tmax"], utc=True)
                            if pd.isna(t_max):
                                t_max = tmax_res
                            else:
                                t_max = max(t_max, tmax_res)
                except Exception:
                    logger.exception("process_csv_upload: failed to widen rollup window from deletes user_id=%s", user_id)
            if pd.notna(t_min) and pd.notna(t_max):
                metrics_t_min = pd.Timestamp(t_min).to_pydatetime()
                metrics_t_max = pd.Timestamp(t_max).to_pydatetime()
                t0 = pd.Timestamp(t_min).floor("H").to_pydatetime()
                t1 = (pd.Timestamp(t_max).floor("H") + pd.Timedelta(hours = 1)).to_pydatetime()
                _exec(
                    """
                    INSERT INTO derived_rollup_hourly
                        (user_id, bucket_ts, metric_type, avg_value, sum_value, min_value, max_value, n, hk_sources, meta)
                    SELECT
                        :user_id AS user_id,
                        date_trunc('hour', timestamp) AS bucket_ts,
                        metric_type,
                        AVG(CASE WHEN metric_type IN (
                            'heart_rate','resting_heart_rate','walking_hr_avg','hr_variability_sdnn',
                            'oxygen_saturation','walking_speed','vo2_max','body_mass','body_mass_index',
                            'blood_glucose','blood_pressure_systolic','blood_pressure_diastolic',
                            'respiratory_rate','body_temperature'
                        ) THEN metric_value END) AS avg_value,
                        SUM(CASE WHEN metric_type IN (
                            'steps','active_energy_burned','sleep_hours','active_time_minutes',
                            'distance_walking_running_km','distance_cycling_km','distance_swimming_km',
                            'dietary_water','mindfulness_minutes'
                        ) THEN metric_value END) AS sum_value,
                        MIN(metric_value) AS min_value,
                        MAX(metric_value) AS max_value,
                        COUNT(*) AS n
                        , COALESCE(
                            jsonb_agg(DISTINCT jsonb_build_object('name', hk_source_name, 'version', hk_source_version))
                              FILTER (WHERE hk_source_name IS NOT NULL),
                            '[]'::jsonb
                          ) AS hk_sources
                        , (ARRAY_AGG(meta ORDER BY timestamp DESC) FILTER (WHERE meta IS NOT NULL))[1] AS meta
                    FROM main_health_metrics
                    WHERE user_id = :user_id
                      AND deleted_at IS NULL
                      AND timestamp >= :t0 AND timestamp < :t1
                    GROUP BY 1,2,3
                    ON CONFLICT (user_id, metric_type, bucket_ts) DO UPDATE SET
                      avg_value = EXCLUDED.avg_value,
                      sum_value = EXCLUDED.sum_value,
                      min_value = EXCLUDED.min_value,
                      max_value = EXCLUDED.max_value,
                      n = EXCLUDED.n,
                      hk_sources = EXCLUDED.hk_sources,
                      meta = EXCLUDED.meta
                    """,
                    {"user_id": user_id, "t0": t0, "t1": t1},
                    "upsert derived_rollup_hourly",
                )
                session.commit()

                # Recompute daily rollups (best-effort) for the same affected window.
                d0 = pd.Timestamp(t_min).floor("D").to_pydatetime()
                d1 = (pd.Timestamp(t_max).floor("D") + pd.Timedelta(days=1)).to_pydatetime()
                _exec(
                    """
                    INSERT INTO derived_rollup_daily
                        (user_id, bucket_ts, metric_type, avg_value, sum_value, min_value, max_value, n, hk_sources, meta)
                    SELECT
                        :user_id AS user_id,
                        date_trunc('day', timestamp) AS bucket_ts,
                        metric_type,
                        AVG(CASE WHEN metric_type IN (
                            'heart_rate','resting_heart_rate','walking_hr_avg','hr_variability_sdnn',
                            'oxygen_saturation','walking_speed','vo2_max','body_mass','body_mass_index',
                            'blood_glucose','blood_pressure_systolic','blood_pressure_diastolic',
                            'respiratory_rate','body_temperature'
                        ) THEN metric_value END) AS avg_value,
                        SUM(CASE WHEN metric_type IN (
                            'steps','active_energy_burned','sleep_hours','active_time_minutes',
                            'distance_walking_running_km','distance_cycling_km','distance_swimming_km',
                            'dietary_water','mindfulness_minutes'
                        ) THEN metric_value END) AS sum_value,
                        MIN(metric_value) AS min_value,
                        MAX(metric_value) AS max_value,
                        COUNT(*) AS n
                        , COALESCE(
                            jsonb_agg(DISTINCT jsonb_build_object('name', hk_source_name, 'version', hk_source_version))
                              FILTER (WHERE hk_source_name IS NOT NULL),
                            '[]'::jsonb
                          ) AS hk_sources
                        , (ARRAY_AGG(meta ORDER BY timestamp DESC) FILTER (WHERE meta IS NOT NULL))[1] AS meta
                    FROM main_health_metrics
                    WHERE user_id = :user_id
                      AND deleted_at IS NULL
                      AND timestamp >= :t0 AND timestamp < :t1
                    GROUP BY 1,2,3
                    ON CONFLICT (user_id, metric_type, bucket_ts) DO UPDATE SET
                      avg_value = EXCLUDED.avg_value,
                      sum_value = EXCLUDED.sum_value,
                      min_value = EXCLUDED.min_value,
                      max_value = EXCLUDED.max_value,
                      n = EXCLUDED.n,
                      hk_sources = EXCLUDED.hk_sources,
                      meta = EXCLUDED.meta
                    """,
                    {"user_id": user_id, "t0": d0, "t1": d1},
                    "upsert derived_rollup_daily",
                )
                session.commit()
        # --- Derive derived_workout_segments (best-effort).
        # We persist segments in a dedicated table (not in main_health_events) to keep the event surface clean.
        try:
            target_wids: set[str] = set(workout_ids_upserted)
            # If metrics were updated, also recompute segments for workouts that overlap the affected window.
            if metrics_t_min is not None and metrics_t_max is not None:
                try:
                    res = session.execute(
                        text(
                            """
                            SELECT hk_uuid
                            FROM main_health_events
                            WHERE user_id = :user_id
                              AND deleted_at IS NULL
                              AND event_type = 'workout_duration_min'
                              AND timestamp >= :t0
                              AND timestamp < :t1
                            """
                        ),
                        {
                            "user_id": user_id,
                            "t0": metrics_t_min,
                            "t1": metrics_t_max + timedelta(hours=72),
                        },
                    ).mappings().all()
                    for rr in res:
                        hu = (rr.get("hk_uuid") or "").strip()
                        if "|" in hu:
                            wid = hu.split("|", 1)[0].strip()
                            if wid:
                                target_wids.add(wid)
                except Exception:
                    logger.exception("process_csv_upload: failed to expand workout set from metrics window user_id=%s", user_id)

            if target_wids:
                wids = sorted([w for w in target_wids if w])

                # Pull base workout rows for these workouts to get timestamp/source/duration/energy.
                base_uuids: list[str] = []
                for wid in wids:
                    base_uuids.extend(
                        [
                            f"{wid}|workout_distance_km",
                            f"{wid}|workout_duration_min",
                            f"{wid}|workout_energy_kcal",
                        ]
                    )

                # Some environments may not have all provenance columns on main_health_events.
                # Derivation should still work; we only select columns that exist.
                try:
                    cols = session.execute(
                        text(
                            """
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = 'main_health_events'
                            """
                        )
                    ).scalars().all()
                    he_cols = {str(c) for c in cols}
                except Exception:
                    he_cols = set()

                select_cols = ["hk_uuid", "event_type", "value", "source", "timestamp", "end_ts"]
                for c in (
                    "hk_source_bundle_id",
                    "hk_source_name",
                    "hk_source_version",
                    "hk_metadata",
                ):
                    if not he_cols or c in he_cols:
                        select_cols.append(c)

                base_rows = session.execute(
                    text(
                        f"""
                        SELECT
                          {", ".join(select_cols)}
                        FROM main_health_events
                        WHERE user_id = :user_id
                          AND hk_uuid = ANY(:uuids)
                          AND deleted_at IS NULL
                        """
                    ),
                    {"user_id": user_id, "uuids": base_uuids},
                ).mappings().all()

                workouts: dict[str, dict] = {}
                for r in base_rows:
                    hk_uuid = (r.get("hk_uuid") or "").strip()
                    if "|" not in hk_uuid:
                        continue
                    wid, _suffix = hk_uuid.split("|", 1)
                    wid = wid.strip()
                    if not wid:
                        continue
                    w = workouts.setdefault(
                        wid,
                        {
                            "timestamp": None,
                            "end_ts": None,
                            "source": None,
                            "dist_km": None,
                            "dur_min": None,
                            "kcal": None,
                            "hk_source_bundle_id": None,
                            "hk_source_name": None,
                            "hk_source_version": None,
                            "hk_metadata": None,
                        },
                    )
                    if w["timestamp"] is None and r.get("timestamp") is not None:
                        w["timestamp"] = r["timestamp"]
                    if w["end_ts"] is None and r.get("end_ts") is not None:
                        w["end_ts"] = r["end_ts"]
                    if w["source"] is None and r.get("source") is not None:
                        w["source"] = r["source"]
                    # Provenance: keep first non-null.
                    for k in (
                        "hk_source_bundle_id",
                        "hk_source_name",
                        "hk_source_version",
                        "hk_metadata",
                    ):
                        if w.get(k) is None and r.get(k) is not None:
                            w[k] = r.get(k)
                    et = r.get("event_type")
                    v = r.get("value")
                    if et == "workout_distance_km" and v is not None:
                        w["dist_km"] = float(v)
                    elif et == "workout_duration_min" and v is not None:
                        w["dur_min"] = float(v)
                    elif et == "workout_energy_kcal" and v is not None:
                        w["kcal"] = float(v)

                workouts = {wid: w for wid, w in workouts.items() if w.get("timestamp") is not None}
                if workouts:
                    # Normalize workout windows so we can derive splits/segments.
                    # Prefer explicit workout end_ts; fallback to duration_min when needed.
                    for _wid, _w in workouts.items():
                        if _w.get("end_ts") is None:
                            dur_min = _w.get("dur_min")
                            if dur_min is not None and _w.get("timestamp") is not None:
                                try:
                                    _w["end_ts"] = _w["timestamp"] + timedelta(minutes=float(dur_min))
                                except Exception:
                                    _w["end_ts"] = None

                    # Upsert derived_workouts (one row per workout uuid).
                    # This is the preferred surface for workout summary queries (avoids EAV pivoting).
                    dw_rows: list[dict] = []
                    for wid, w in workouts.items():
                        start_ts = w["timestamp"]
                        end_ts = w.get("end_ts")
                        dist_km = w.get("dist_km")
                        dur_min = w.get("dur_min")
                        kcal = w.get("kcal")
                        dw_rows.append(
                            {
                                "user_id": user_id,
                                "workout_uuid": wid,
                                "workout_type": w.get("source"),
                                "start_ts": start_ts,
                                "end_ts": end_ts,
                                "duration_min": float(dur_min) if dur_min is not None else None,
                                "distance_km": float(dist_km) if dist_km is not None else None,
                                "energy_kcal": float(kcal) if kcal is not None else None,
                                "hk_source_bundle_id": w.get("hk_source_bundle_id"),
                                "hk_sources": json.dumps(
                                    [
                                        {"name": w.get("hk_source_name"), "version": w.get("hk_source_version")}
                                    ]
                                )
                                if w.get("hk_source_name") is not None
                                else "[]",
                                "hk_metadata": json.dumps(w.get("hk_metadata")) if isinstance(w.get("hk_metadata"), (dict, list)) else w.get("hk_metadata"),
                            }
                        )
                    if dw_rows:
                        session.execute(
                            text(
                                """
                                INSERT INTO derived_workouts
                                    (user_id, workout_uuid, workout_type, start_ts, end_ts, duration_min, distance_km, energy_kcal,
                                     hk_source_bundle_id, hk_sources, hk_metadata,
                                     created_at, updated_at)
                                VALUES
                                    (:user_id, :workout_uuid, :workout_type, :start_ts, :end_ts, :duration_min, :distance_km, :energy_kcal,
                                     :hk_source_bundle_id,
                                     CAST(:hk_sources AS jsonb), CAST(:hk_metadata AS jsonb),
                                     NOW(), NOW())
                                ON CONFLICT (user_id, workout_uuid) DO UPDATE
                                SET
                                    workout_type = COALESCE(EXCLUDED.workout_type, derived_workouts.workout_type),
                                    start_ts = EXCLUDED.start_ts,
                                    end_ts = COALESCE(EXCLUDED.end_ts, derived_workouts.end_ts),
                                    duration_min = COALESCE(EXCLUDED.duration_min, derived_workouts.duration_min),
                                    distance_km = COALESCE(EXCLUDED.distance_km, derived_workouts.distance_km),
                                    energy_kcal = COALESCE(EXCLUDED.energy_kcal, derived_workouts.energy_kcal),
                                    hk_source_bundle_id = COALESCE(EXCLUDED.hk_source_bundle_id, derived_workouts.hk_source_bundle_id),
                                    hk_metadata = COALESCE(EXCLUDED.hk_metadata, derived_workouts.hk_metadata),
                                    hk_sources = COALESCE(EXCLUDED.hk_sources, derived_workouts.hk_sources),
                                    updated_at = NOW()
                                """
                            ),
                            dw_rows,
                        )

                    ts_list = [w["timestamp"] for w in workouts.values()]
                    t_min_w = min(ts_list)
                    t_max_w = max(ts_list)

                    # --- Raw metrics slab for segment derivation (batch query; then slice per workout).
                    # We only need a few raw metric types here.
                    dist_metric_types = ["distance_walking_running_km", "distance_cycling_km", "distance_swimming_km"]
                    raw_dist_rows = session.execute(
                        text(
                            """
                            SELECT metric_type, timestamp, end_ts, metric_value
                            FROM main_health_metrics
                            WHERE user_id = :user_id
                              AND deleted_at IS NULL
                              AND metric_type = ANY(:metric_types)
                              AND timestamp >= :t0
                              AND timestamp < :t1
                            ORDER BY metric_type, timestamp
                            """
                        ),
                        {
                            "user_id": user_id,
                            "metric_types": dist_metric_types,
                            "t0": t_min_w,
                            "t1": max([w.get("end_ts") or w["timestamp"] for w in workouts.values()]),
                        },
                    ).mappings().all()

                    raw_hr_rows = session.execute(
                        text(
                            """
                            SELECT timestamp, metric_value
                            FROM main_health_metrics
                            WHERE user_id = :user_id
                              AND deleted_at IS NULL
                              AND metric_type = 'heart_rate'
                              AND timestamp >= :t0
                              AND timestamp < :t1
                            ORDER BY timestamp
                            """
                        ),
                        {
                            "user_id": user_id,
                            "t0": t_min_w,
                            "t1": max([w.get("end_ts") or w["timestamp"] for w in workouts.values()]),
                        },
                    ).mappings().all()

                    # Build per-metric arrays for distance.
                    dist_times: dict[str, list] = {mt: [] for mt in dist_metric_types}
                    dist_end: dict[str, list] = {mt: [] for mt in dist_metric_types}
                    dist_vals: dict[str, list[float]] = {mt: [] for mt in dist_metric_types}
                    for rr in raw_dist_rows:
                        mt = rr.get("metric_type")
                        ts = rr.get("timestamp")
                        v = rr.get("metric_value")
                        if mt not in dist_times or ts is None or v is None:
                            continue
                        dist_times[mt].append(ts)
                        dist_end[mt].append(rr.get("end_ts"))
                        dist_vals[mt].append(float(v))

                    hr_times = [rr["timestamp"] for rr in raw_hr_rows if rr.get("timestamp") is not None and rr.get("metric_value") is not None]
                    hr_vals = [float(rr["metric_value"]) for rr in raw_hr_rows if rr.get("timestamp") is not None and rr.get("metric_value") is not None]

                    def _slice_dist(metric_type: str, start_ts, end_ts):
                        ts = dist_times.get(metric_type) or []
                        if not ts:
                            return []
                        lo = bisect_left(ts, start_ts)
                        hi = bisect_left(ts, end_ts)
                        out = []
                        for i in range(lo, hi):
                            out.append((ts[i], dist_end[metric_type][i], dist_vals[metric_type][i]))
                        return out

                    def _slice_hr(start_ts, end_ts):
                        if not hr_times:
                            return ([], [])
                        lo = bisect_left(hr_times, start_ts)
                        hi = bisect_left(hr_times, end_ts)
                        return (hr_times[lo:hi], hr_vals[lo:hi])

                    def _compute_segments(*, dist_samples, workout_start, seg_len_km: float):
                        """
                        Convert distance samples into contiguous segment time windows by assuming uniform speed
                        within each sample interval (when end_ts exists), and instantaneous distance otherwise.
                        Returns list of (segment_index_1based, seg_start_ts, seg_end_ts, distance_km).
                        Only returns FULL segments of length seg_len_km.
                        """
                        if seg_len_km <= 0:
                            return []
                        # Find a reasonable start: first sample timestamp (avoids counting pre-distance idle time).
                        if not dist_samples:
                            return []
                        seg_start = dist_samples[0][0] if dist_samples[0][0] is not None else workout_start
                        segs = []
                        carried = 0.0
                        idx = 1
                        cur_start = seg_start
                        for (s, e, dist) in dist_samples:
                            if s is None:
                                continue
                            if dist is None:
                                continue
                            d = float(dist)
                            if d <= 0:
                                continue
                            # Interval duration for proportional boundary placement.
                            if e is None or (hasattr(e, "timestamp") and hasattr(s, "timestamp") and e <= s):
                                e = s
                            dt_sec = float((e - s).total_seconds()) if e is not None else 0.0
                            # Consume this sample's distance into 1..N segments.
                            while idx <= _MAX_SEGMENTS_PER_WORKOUT and (carried + d) >= seg_len_km:
                                need = seg_len_km - carried
                                frac = (need / d) if d > 0 else 0.0
                                if dt_sec > 0:
                                    boundary = s + timedelta(seconds=dt_sec * frac)
                                else:
                                    boundary = s
                                segs.append((idx, cur_start, boundary, seg_len_km))
                                idx += 1
                                cur_start = boundary
                                d -= need
                                carried = 0.0
                                # Remaining portion of the interval starts at boundary.
                                s = boundary
                                if dt_sec > 0:
                                    dt_sec = float((e - s).total_seconds()) if e is not None else 0.0
                            carried += d
                            if idx > _MAX_SEGMENTS_PER_WORKOUT:
                                break
                        return segs

                    # Replace segments for these workouts (idempotent: delete then insert).
                    session.execute(
                        text(
                            """
                            DELETE FROM derived_workout_segments
                            WHERE user_id = :user_id AND workout_uuid = ANY(:wids)
                            """
                        ),
                        {"user_id": user_id, "wids": sorted(list(workouts.keys()))},
                    )

                    seg_rows: list[dict] = []
                    for wid, w in workouts.items():
                        ts0 = w["timestamp"]
                        end0 = w.get("end_ts") or ts0
                        workout_type = str(w.get("source") or "").strip().lower()
                        workout_dist_km = float(w.get("dist_km") or 0.0)

                        # Only generate segments for distance-based activities (and require non-trivial workout distance).
                        # This prevents background walking distance from accidentally producing "segments" for strength workouts.
                        distance_type_keywords = (
                            "run",
                            "walk",
                            "hike",
                            "cycle",
                            "bike",
                            "swim",
                            "row",
                            "ski",
                        )
                        is_distance_activity = any(k in workout_type for k in distance_type_keywords)
                        if (not is_distance_activity) or workout_dist_km < 1.0:
                            continue

                        # Choose the best distance stream inside the workout window.
                        candidates = []
                        for mt in dist_metric_types:
                            ds = _slice_dist(mt, ts0, end0)
                            total = sum([float(x[2]) for x in ds]) if ds else 0.0
                            candidates.append((total, mt, ds))
                        candidates.sort(key=lambda x: x[0], reverse=True)
                        best_total, _best_mt, best_ds = candidates[0] if candidates else (0.0, None, [])
                        if not best_ds or best_total < 1.0:
                            continue

                        hr_t, hr_v = _slice_hr(ts0, end0)

                        def _avg_hr_in_window(a, b) -> float | None:
                            if not hr_t:
                                return None
                            lo = bisect_left(hr_t, a)
                            hi = bisect_left(hr_t, b)
                            if hi <= lo:
                                return None
                            xs = hr_v[lo:hi]
                            return (sum(xs) / len(xs)) if xs else None

                        def _append_seg(*, unit: str, i: int, s, e, unit_km: float) -> None:
                            dur_min = max(0.0, (e - s).total_seconds() / 60.0)
                            start_off = max(0.0, (s - ts0).total_seconds() / 60.0)
                            end_off = max(0.0, (e - ts0).total_seconds() / 60.0)
                            # pace in seconds per unit (km or mi)
                            length_units = unit_km if unit == "km" else (unit_km / 1.609344)
                            pace = (dur_min * 60.0) / length_units if length_units > 0 else None
                            avg_hr = _avg_hr_in_window(s, e)
                            seg_rows.append(
                                {
                                    "user_id": user_id,
                                    "workout_uuid": wid,
                                    "workout_start_ts": ts0,
                                    "segment_unit": unit,
                                    "segment_index": int(i),
                                    "start_ts": s,
                                    "end_ts": e,
                                    "start_offset_min": float(start_off),
                                    "end_offset_min": float(end_off),
                                    "duration_min": float(dur_min),
                                    "pace_s_per_unit": float(pace) if pace is not None else None,
                                    "avg_hr_bpm": float(avg_hr) if avg_hr is not None else None,
                                }
                            )

                        # km segments (1.0 km)
                        for (i, s, e, dkm) in _compute_segments(dist_samples=best_ds, workout_start=ts0, seg_len_km=1.0):
                            _append_seg(unit="km", i=i, s=s, e=e, unit_km=dkm)
                        # mile segments (1.0 mi = 1.609344 km)
                        for (i, s, e, dkm) in _compute_segments(dist_samples=best_ds, workout_start=ts0, seg_len_km=1.609344):
                            _append_seg(unit="mi", i=i, s=s, e=e, unit_km=dkm)

                    if seg_rows:
                        session.execute(
                            text(
                                """
                                INSERT INTO derived_workout_segments
                                    (user_id, workout_uuid, workout_start_ts, segment_unit, segment_index,
                                     start_ts, end_ts, start_offset_min, end_offset_min, duration_min,
                                     pace_s_per_unit, avg_hr_bpm, created_at)
                                VALUES
                                    (:user_id, :workout_uuid, :workout_start_ts, :segment_unit, :segment_index,
                                     :start_ts, :end_ts, :start_offset_min, :end_offset_min, :duration_min,
                                     :pace_s_per_unit, :avg_hr_bpm, NOW())
                                ON CONFLICT (user_id, workout_uuid, segment_unit, segment_index) DO UPDATE
                                SET
                                    workout_start_ts = EXCLUDED.workout_start_ts,
                                    start_ts = EXCLUDED.start_ts,
                                    end_ts = EXCLUDED.end_ts,
                                    start_offset_min = EXCLUDED.start_offset_min,
                                    end_offset_min = EXCLUDED.end_offset_min,
                                    duration_min = EXCLUDED.duration_min,
                                    pace_s_per_unit = EXCLUDED.pace_s_per_unit,
                                    avg_hr_bpm = EXCLUDED.avg_hr_bpm
                                """
                            ),
                            seg_rows,
                        )

                session.commit()
        except Exception:
            logger.exception("process_csv_upload: derive derived_workout_segments failed user_id=%s", user_id)
            try:
                session.rollback()
            except Exception:
                logger.exception("process_csv_upload: rollback failed after derive derived_workout_segments user_id=%s", user_id)

    logger.info("process_csv_upload: done user_id=%s metrics=%s events=%s", user_id, len(df_metrics), len(df_events))
    return {"inserted": int(len(df_metrics) + len(df_events))}