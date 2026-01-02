from __future__ import annotations

import io
import logging
import pandas as pd
import base64
import json
from sqlalchemy import text
import time
import random
from Backend.celery import celery
from Backend.database import SessionLocal


logger = logging.getLogger(__name__)

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

        # Split event/workout rows for separate health_events table. Remaining rows are raw metrics for health_metrics table
    df_events = df[df["metric_type"].str.startswith(("event_", "workout_"), na = False)].copy()
    df_metrics = df[~df["metric_type"].str.startswith(("event_", "workout_"), na = False)].copy()

    # Write to health_events and health_metrics table
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
                "DELETE FROM health_events WHERE user_id = :user_id AND timestamp < :cutoff",
                {"user_id": user_id, "cutoff": cutoff_dt},
                "prune health_events",
            )
            _exec(
                "DELETE FROM health_metrics WHERE user_id = :user_id AND timestamp < :cutoff",
                {"user_id": user_id, "cutoff": cutoff_dt},
                "prune health_metrics",
            )

        # Write to health_events table (mirror-only; require hk_uuid)
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
                    raise ValueError("Mirror ingest requires hk_uuid for health_events upserts")
                missing = df_ev_up["hk_uuid"].isna() | df_ev_up["hk_uuid"].astype(str).str.strip().eq("")
                if bool(missing.any()):
                    raise ValueError("Mirror ingest found health_events upsert rows missing hk_uuid")

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
                        UPDATE health_events
                        SET deleted_at = NOW()
                        WHERE user_id = :user_id AND hk_uuid = ANY(:uuids)
                        """,
                        {"user_id": user_id, "uuids": deleted_event_uuids},
                        "tombstone health_events deletes",
                    )

            # Upserts require metric_value + hk_uuid
            if not df_ev_up.empty:
                df_ev_up["metric_value"] = pd.to_numeric(df_ev_up["metric_value"], errors="coerce")
                df_ev_up = df_ev_up[pd.notna(df_ev_up["metric_value"])]

            # Optional per-row timezone info from the CSV (used to display historical events in the tz they occurred in)
            tz_series = df_ev_up["timezone"] if "timezone" in df_ev_up.columns else None
            off_series = df_ev_up["utc_offset_min"] if "utc_offset_min" in df_ev_up.columns else None
            ctry_series = df_ev_up["place_country"] if "place_country" in df_ev_up.columns else None
            region_series = df_ev_up["place_region"] if "place_region" in df_ev_up.columns else None
            city_series = df_ev_up["place_city"] if "place_city" in df_ev_up.columns else None

            def _meta_json(tz, off, c, r, ci):
                meta: dict = {}
                if tz is not None and pd.notna(tz) and str(tz).strip():
                    meta["tz_name"] = str(tz).strip()
                if off is not None and pd.notna(off):
                    meta["utc_offset_min"] = int(float(off))
                place: dict = {}
                if c is not None and pd.notna(c) and str(c).strip():
                    place["country"] = str(c).strip()
                if r is not None and pd.notna(r) and str(r).strip():
                    place["region"] = str(r).strip()
                if ci is not None and pd.notna(ci) and str(ci).strip():
                    place["city"] = str(ci).strip()
                if place:
                    meta["place"] = place
                return json.dumps(meta) if meta else None

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
                    meta = _meta_json(
                        r.get("timezone"),
                        r.get("utc_offset_min"),
                        r.get("place_country"),
                        r.get("place_region"),
                        r.get("place_city"),
                    )
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
                            "meta": meta,
                            "hk_source_bundle_id": _str_or_none(r.get("hk_source_bundle_id")) if "hk_source_bundle_id" in df_ev_up.columns else None,
                            "hk_source_name": _str_or_none(r.get("hk_source_name")) if "hk_source_name" in df_ev_up.columns else None,
                            "hk_source_version": _str_or_none(r.get("hk_source_version")) if "hk_source_version" in df_ev_up.columns else None,
                            "hk_device": _str_or_none(r.get("hk_device")) if "hk_device" in df_ev_up.columns else None,
                            "hk_metadata": _str_or_none(r.get("hk_metadata")) if "hk_metadata" in df_ev_up.columns else None,
                            "hk_was_user_entered": _bool_or_none(r.get("hk_was_user_entered")) if "hk_was_user_entered" in df_ev_up.columns else None,
                        }
                    )

            if params:
                _execmany(
                    """
                    INSERT INTO health_events
                        (user_id, hk_uuid, timestamp, end_ts, event_type, value, unit, source, meta, created_at,
                         hk_source_bundle_id, hk_source_name, hk_source_version, hk_device, hk_metadata, hk_was_user_entered, deleted_at)
                    VALUES
                        (:user_id, :hk_uuid, :timestamp, :end_ts, :event_type, :value, :unit, :source, CAST(:meta AS jsonb), :created_at,
                         :hk_source_bundle_id, :hk_source_name, :hk_source_version, CAST(:hk_device AS jsonb), CAST(:hk_metadata AS jsonb), :hk_was_user_entered, NULL)
                    ON CONFLICT (user_id, hk_uuid, event_type) DO UPDATE
                    SET
                        timestamp = EXCLUDED.timestamp,
                        end_ts = COALESCE(EXCLUDED.end_ts, health_events.end_ts),
                        value = EXCLUDED.value,
                        unit = COALESCE(EXCLUDED.unit, health_events.unit),
                        source = COALESCE(EXCLUDED.source, health_events.source),
                        created_at = COALESCE(EXCLUDED.created_at, health_events.created_at),
                        meta = COALESCE(EXCLUDED.meta, health_events.meta),
                        hk_source_bundle_id = COALESCE(EXCLUDED.hk_source_bundle_id, health_events.hk_source_bundle_id),
                        hk_source_name = COALESCE(EXCLUDED.hk_source_name, health_events.hk_source_name),
                        hk_source_version = COALESCE(EXCLUDED.hk_source_version, health_events.hk_source_version),
                        hk_device = COALESCE(EXCLUDED.hk_device, health_events.hk_device),
                        hk_metadata = COALESCE(EXCLUDED.hk_metadata, health_events.hk_metadata),
                        hk_was_user_entered = COALESCE(EXCLUDED.hk_was_user_entered, health_events.hk_was_user_entered),
                        deleted_at = NULL
                    """,
                    params,
                    "bulk upsert health_events (hk_uuid)",
                    delay=0.2,
                )

            # Server-side derived workout flags to keep health_events faithful and consistent:
            # - event_long_run_km: when workout_distance_km >= 10.0
            # - event_hard_workout: when workout_energy_kcal >= 800 OR workout_duration_min >= 60
            # Derivations are keyed as hk_uuid "<workoutUUID>|event_*" and are tombstoned when conditions are not met.
            try:
                # Tombstone derived flags when any base workout row is deleted.
                if workout_ids_deleted:
                    derived_uuids: list[str] = []
                    for wid in workout_ids_deleted:
                        derived_uuids.append(f"{wid}|event_long_run_km")
                        derived_uuids.append(f"{wid}|event_hard_workout")
                    _exec(
                        """
                        UPDATE health_events
                        SET deleted_at = NOW()
                        WHERE user_id = :user_id AND hk_uuid = ANY(:uuids)
                        """,
                        {"user_id": user_id, "uuids": derived_uuids},
                        "tombstone derived workout flags (deletes)",
                    )

                # Recompute derived flags for workouts whose base rows were upserted.
                for wid in workout_ids_upserted:
                    base_uuids = [
                        f"{wid}|workout_distance_km",
                        f"{wid}|workout_duration_min",
                        f"{wid}|workout_energy_kcal",
                    ]
                    rows = session.execute(
                        text(
                            """
                            SELECT hk_uuid, event_type, value, unit, source, timestamp, meta,
                                   hk_source_bundle_id, hk_source_name, hk_source_version, hk_device, hk_metadata, hk_was_user_entered
                            FROM health_events
                            WHERE user_id = :user_id
                              AND hk_uuid = ANY(:uuids)
                              AND deleted_at IS NULL
                            """
                        ),
                        {"user_id": user_id, "uuids": base_uuids},
                    ).mappings().all()

                    # Default values if some rows missing.
                    dist_km = 0.0
                    dur_min = 0.0
                    kcal = 0.0
                    ts0 = None
                    src0 = None
                    meta0 = None
                    prov = {
                        "hk_source_bundle_id": None,
                        "hk_source_name": None,
                        "hk_source_version": None,
                        "hk_device": None,
                        "hk_metadata": None,
                        "hk_was_user_entered": None,
                    }
                    for rr in rows:
                        et = rr.get("event_type")
                        v = rr.get("value")
                        if et == "workout_distance_km" and v is not None:
                            dist_km = float(v)
                        elif et == "workout_duration_min" and v is not None:
                            dur_min = float(v)
                        elif et == "workout_energy_kcal" and v is not None:
                            kcal = float(v)
                        if ts0 is None:
                            ts0 = rr.get("timestamp")
                        if src0 is None:
                            src0 = rr.get("source")
                        if meta0 is None:
                            meta0 = rr.get("meta")
                        for k in prov.keys():
                            if prov[k] is None and rr.get(k) is not None:
                                prov[k] = rr.get(k)

                    if ts0 is None:
                        continue

                    def _upsert_flag(*, hk_uuid: str, event_type: str, value: float, unit: str) -> None:
                        hk_device = prov.get("hk_device")
                        hk_metadata = prov.get("hk_metadata")
                        hk_device_json = json.dumps(hk_device) if isinstance(hk_device, dict) else hk_device
                        hk_metadata_json = json.dumps(hk_metadata) if isinstance(hk_metadata, dict) else hk_metadata
                        session.execute(
                            text(
                                """
                                INSERT INTO health_events
                                    (user_id, hk_uuid, timestamp, end_ts, event_type, value, unit, source, meta, created_at,
                                     hk_source_bundle_id, hk_source_name, hk_source_version, hk_device, hk_metadata, hk_was_user_entered, deleted_at)
                                VALUES
                                    (:user_id, :hk_uuid, :timestamp, NULL, :event_type, :value, :unit, :source, CAST(:meta AS jsonb), NOW(),
                                     :hk_source_bundle_id, :hk_source_name, :hk_source_version, CAST(:hk_device AS jsonb), CAST(:hk_metadata AS jsonb), :hk_was_user_entered, NULL)
                                ON CONFLICT (user_id, hk_uuid, event_type) DO UPDATE
                                SET
                                    timestamp = EXCLUDED.timestamp,
                                    value = EXCLUDED.value,
                                    unit = EXCLUDED.unit,
                                    source = COALESCE(EXCLUDED.source, health_events.source),
                                    meta = COALESCE(EXCLUDED.meta, health_events.meta),
                                    hk_source_bundle_id = COALESCE(EXCLUDED.hk_source_bundle_id, health_events.hk_source_bundle_id),
                                    hk_source_name = COALESCE(EXCLUDED.hk_source_name, health_events.hk_source_name),
                                    hk_source_version = COALESCE(EXCLUDED.hk_source_version, health_events.hk_source_version),
                                    hk_device = COALESCE(EXCLUDED.hk_device, health_events.hk_device),
                                    hk_metadata = COALESCE(EXCLUDED.hk_metadata, health_events.hk_metadata),
                                    hk_was_user_entered = COALESCE(EXCLUDED.hk_was_user_entered, health_events.hk_was_user_entered),
                                    deleted_at = NULL
                                """
                            ),
                            {
                                "user_id": user_id,
                                "hk_uuid": hk_uuid,
                                "timestamp": ts0,
                                "event_type": event_type,
                                "value": float(value),
                                "unit": unit,
                                "source": src0,
                                "meta": json.dumps(meta0) if isinstance(meta0, dict) else None,
                                "hk_source_bundle_id": prov["hk_source_bundle_id"],
                                "hk_source_name": prov["hk_source_name"],
                                "hk_source_version": prov["hk_source_version"],
                                "hk_device": hk_device_json,
                                "hk_metadata": hk_metadata_json,
                                "hk_was_user_entered": prov["hk_was_user_entered"],
                            },
                        )

                    def _tombstone_flag(hk_uuid: str, event_type: str) -> None:
                        session.execute(
                            text(
                                """
                                UPDATE health_events
                                SET deleted_at = NOW()
                                WHERE user_id = :user_id AND hk_uuid = :hk_uuid AND event_type = :event_type
                                """
                            ),
                            {"user_id": user_id, "hk_uuid": hk_uuid, "event_type": event_type},
                        )

                    # Long run flag
                    hk_long = f"{wid}|event_long_run_km"
                    if dist_km >= 10.0:
                        _upsert_flag(hk_uuid=hk_long, event_type="event_long_run_km", value=dist_km, unit="km")
                    else:
                        _tombstone_flag(hk_long, "event_long_run_km")

                    # Hard workout flag
                    hk_hard = f"{wid}|event_hard_workout"
                    if kcal >= 800.0 or dur_min >= 60.0:
                        _upsert_flag(hk_uuid=hk_hard, event_type="event_hard_workout", value=1.0, unit="count")
                    else:
                        _tombstone_flag(hk_hard, "event_hard_workout")
            except Exception:
                logger.exception("process_csv_upload: derived workout flag recompute failed user_id=%s", user_id)

        # Write to health_metrics table (HealthKit mirror-aware: raw samples + tombstone deletes)
        deleted_uuids: list[str] = []
        if not df_metrics.empty:
            if "op" not in df_metrics.columns:
                df_metrics["op"] = "upsert"

            df_del = df_metrics[df_metrics["op"].astype(str).str.lower().eq("delete")].copy()
            df_up = df_metrics[~df_metrics["op"].astype(str).str.lower().eq("delete")].copy()

            # Fail fast: mirror mode requires hk_uuid for any upsert row.
            if not df_up.empty:
                if "hk_uuid" not in df_up.columns:
                    raise ValueError("Mirror ingest requires hk_uuid for health_metrics upserts")
                missing = df_up["hk_uuid"].isna() | df_up["hk_uuid"].astype(str).str.strip().eq("")
                if bool(missing.any()):
                    raise ValueError("Mirror ingest found health_metrics upsert rows missing hk_uuid")

            # Tombstone deletes by hk_uuid (if present)
            if not df_del.empty and "hk_uuid" in df_del.columns:
                deleted_uuids = (
                    df_del["hk_uuid"].dropna().astype(str).str.strip().loc[lambda s: s != ""].unique().tolist()
                )
                if deleted_uuids:
                    _exec(
                        """
                        UPDATE health_metrics
                        SET deleted_at = NOW()
                        WHERE user_id = :user_id AND hk_uuid = ANY(:uuids)
                        """,
                        {"user_id": user_id, "uuids": deleted_uuids},
                        "tombstone health_metrics deletes",
                    )

            def _meta_json_m(tz, off, c, r, ci):
                meta: dict = {}
                if tz is not None and pd.notna(tz) and str(tz).strip():
                    meta["tz_name"] = str(tz).strip()
                if off is not None and pd.notna(off):
                    meta["utc_offset_min"] = int(float(off))
                place: dict = {}
                if c is not None and pd.notna(c) and str(c).strip():
                    place["country"] = str(c).strip()
                if r is not None and pd.notna(r) and str(r).strip():
                    place["region"] = str(r).strip()
                if ci is not None and pd.notna(ci) and str(ci).strip():
                    place["city"] = str(ci).strip()
                if place:
                    meta["place"] = place
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
                        "source": _str_or_none(r.get("source")) if "source" in df_up.columns else None,
                        "created_at": _py_dt_or_none(r.get("created_at")) if "created_at" in df_up.columns else None,
                        "meta": meta,
                        "hk_uuid": _str_or_none(r.get("hk_uuid")) if "hk_uuid" in df_up.columns else None,
                        "hk_source_bundle_id": _str_or_none(r.get("hk_source_bundle_id")) if "hk_source_bundle_id" in df_up.columns else None,
                        "hk_source_name": _str_or_none(r.get("hk_source_name")) if "hk_source_name" in df_up.columns else None,
                        "hk_source_version": _str_or_none(r.get("hk_source_version")) if "hk_source_version" in df_up.columns else None,
                        "hk_device": _str_or_none(r.get("hk_device")) if "hk_device" in df_up.columns else None,
                        "hk_metadata": _str_or_none(r.get("hk_metadata")) if "hk_metadata" in df_up.columns else None,
                        "hk_was_user_entered": _bool_or_none(r.get("hk_was_user_entered")) if "hk_was_user_entered" in df_up.columns else None,
                    }

                    if base["hk_uuid"]:
                        params_m_uuid.append(base)
                    else:
                        # Legacy rows are no longer supported; skip.
                        continue

            if params_m_uuid:
                _execmany(
                    """
                    INSERT INTO health_metrics
                        (user_id, hk_uuid, timestamp, end_ts, metric_type, metric_value, unit, source, created_at, meta,
                         hk_source_bundle_id, hk_source_name, hk_source_version, hk_device, hk_metadata, hk_was_user_entered, deleted_at)
                    VALUES
                        (:user_id, :hk_uuid, :timestamp, :end_ts, :metric_type, :metric_value, :unit, :source, :created_at, CAST(:meta AS jsonb),
                         :hk_source_bundle_id, :hk_source_name, :hk_source_version, CAST(:hk_device AS jsonb), CAST(:hk_metadata AS jsonb), :hk_was_user_entered, NULL)
                    ON CONFLICT (user_id, hk_uuid) DO UPDATE
                    SET
                        timestamp = EXCLUDED.timestamp,
                        end_ts = COALESCE(EXCLUDED.end_ts, health_metrics.end_ts),
                        metric_type = EXCLUDED.metric_type,
                        metric_value = EXCLUDED.metric_value,
                        unit = COALESCE(EXCLUDED.unit, health_metrics.unit),
                        source = COALESCE(EXCLUDED.source, health_metrics.source),
                        created_at = COALESCE(EXCLUDED.created_at, health_metrics.created_at),
                        meta = COALESCE(EXCLUDED.meta, health_metrics.meta),
                        hk_source_bundle_id = COALESCE(EXCLUDED.hk_source_bundle_id, health_metrics.hk_source_bundle_id),
                        hk_source_name = COALESCE(EXCLUDED.hk_source_name, health_metrics.hk_source_name),
                        hk_source_version = COALESCE(EXCLUDED.hk_source_version, health_metrics.hk_source_version),
                        hk_device = COALESCE(EXCLUDED.hk_device, health_metrics.hk_device),
                        hk_metadata = COALESCE(EXCLUDED.hk_metadata, health_metrics.hk_metadata),
                        hk_was_user_entered = COALESCE(EXCLUDED.hk_was_user_entered, health_metrics.hk_was_user_entered),
                        deleted_at = NULL
                    """,
                    params_m_uuid,
                    "bulk upsert health_metrics (hk_uuid)",
                    delay=0.25,
                )

            # No legacy upsert path.

        session.commit()  # Commit raw writes first so rollup failures never discard ingested data

        # Recompute hourly rollups (best-effort) for the affected time window from health_metrics table
        if not df_metrics.empty:
            t_min = pd.to_datetime(df_metrics["timestamp"].min())
            t_max = pd.to_datetime(df_metrics["timestamp"].max())
            if deleted_uuids:
                try:
                    res = session.execute(
                        text(
                            """
                            SELECT MIN(timestamp) AS tmin, MAX(timestamp) AS tmax
                            FROM health_metrics
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
                t0 = pd.Timestamp(t_min).floor("H").to_pydatetime()
                t1 = (pd.Timestamp(t_max).floor("H") + pd.Timedelta(hours = 1)).to_pydatetime()
                _exec(
                    """
                    INSERT INTO health_rollup_hourly
                        (user_id, bucket_ts, metric_type, avg_value, sum_value, min_value, max_value, n, meta)
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
                        , (ARRAY_AGG(meta ORDER BY timestamp DESC) FILTER (WHERE meta IS NOT NULL))[1] AS meta
                    FROM health_metrics
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
                      meta = COALESCE(EXCLUDED.meta, health_rollup_hourly.meta)
                    """,
                    {"user_id": user_id, "t0": t0, "t1": t1},
                    "upsert health_rollup_hourly",
                )
                session.commit()

                # Recompute daily rollups (best-effort) for the same affected window.
                d0 = pd.Timestamp(t_min).floor("D").to_pydatetime()
                d1 = (pd.Timestamp(t_max).floor("D") + pd.Timedelta(days=1)).to_pydatetime()
                _exec(
                    """
                    INSERT INTO health_rollup_daily
                        (user_id, bucket_ts, metric_type, avg_value, sum_value, min_value, max_value, n, meta)
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
                        , (ARRAY_AGG(meta ORDER BY timestamp DESC) FILTER (WHERE meta IS NOT NULL))[1] AS meta
                    FROM health_metrics
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
                      meta = COALESCE(EXCLUDED.meta, health_rollup_daily.meta)
                    """,
                    {"user_id": user_id, "t0": d0, "t1": d1},
                    "upsert health_rollup_daily",
                )
                session.commit()
    logger.info("process_csv_upload: done user_id=%s metrics=%s events=%s", user_id, len(df_metrics), len(df_events))
    return {"inserted": int(len(df_metrics) + len(df_events))}