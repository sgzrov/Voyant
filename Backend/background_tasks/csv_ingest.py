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
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], utc = True, errors = "coerce")
    df = df[df["user_id"] == user_id]
    if df.empty:
        logger.info("process_csv_upload: no rows for user_id=%s after filter; nothing to insert", user_id)
        return {"inserted": 0}

    # Drop rows older than 60 days (relative to the newest timestamp in the CSV)
    now_ts = df["timestamp"].max()
    if pd.isna(now_ts):
        logger.info("process_csv_upload: all timestamps invalid for user_id=%s; nothing to insert", user_id)
        return {"inserted": 0}
    cutoff_ts = now_ts - pd.Timedelta(days = 60)
    cutoff_dt = pd.Timestamp(cutoff_ts).to_pydatetime()
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

        # Write to health_events table
        if not df_events.empty:
            df_events["metric_value"] = pd.to_numeric(df_events["metric_value"], errors = "coerce")
            df_events = df_events[pd.notna(df_events["metric_value"])]

            # Optional per-row timezone info from the CSV (used to display historical events in the tz they occurred in)
            tz_series = df_events["timezone"] if "timezone" in df_events.columns else None
            off_series = df_events["utc_offset_min"] if "utc_offset_min" in df_events.columns else None
            params = [
                {
                    "user_id": user_id,
                    "timestamp": _py_dt(ts),
                    "event_type": etype,
                    "value": float(val),
                    "unit": unit if unit is not None and pd.notna(unit) else None,
                    "source": source if source is not None and pd.notna(source) else None,
                    "meta": meta,
                }
                for ts, etype, val, unit, source, meta in zip(
                    df_events["timestamp"],
                    df_events["metric_type"],
                    df_events["metric_value"],
                    df_events["unit"] if "unit" in df_events.columns else [None] * len(df_events),
                    df_events["source"] if "source" in df_events.columns else [None] * len(df_events),
                    [
                        (
                            json.dumps(
                                {
                                    **(
                                        {"tz_name": str(tz)} if tz_series is not None and tz is not None and pd.notna(tz) and str(tz).strip() else {}
                                    ),
                                    **(
                                        {"utc_offset_min": int(float(off))}
                                        if off_series is not None and off is not None and pd.notna(off)
                                        else {}
                                    ),
                                }
                            )
                            if (
                                (tz_series is not None and tz is not None and pd.notna(tz) and str(tz).strip())
                                or (off_series is not None and off is not None and pd.notna(off))
                            )
                            else None
                        )
                        for tz, off in zip(
                            tz_series if tz_series is not None else [None] * len(df_events),
                            off_series if off_series is not None else [None] * len(df_events),
                        )
                    ],
                )
            ]
            if params:
                _execmany(
                    """
                    INSERT INTO health_events (user_id, timestamp, event_type, value, unit, source, meta)
                    VALUES (:user_id, :timestamp, :event_type, :value, :unit, :source, CAST(:meta AS jsonb))
                    ON CONFLICT (user_id, event_type, timestamp) DO UPDATE
                    SET
                        value = GREATEST(EXCLUDED.value, health_events.value),
                        unit = COALESCE(EXCLUDED.unit, health_events.unit),
                        source = COALESCE(EXCLUDED.source, health_events.source),
                        meta = COALESCE(EXCLUDED.meta, health_events.meta)
                    """,
                    params,
                    "bulk insert health_events",
                    delay = 0.2,
                )

        # Write to health_metrics table
        if not df_metrics.empty:
            df_metrics["metric_value"] = pd.to_numeric(df_metrics["metric_value"], errors = "coerce")
            df_metrics = df_metrics[pd.notna(df_metrics["metric_value"])]
            # Optional per-row timezone info from the CSV (used to display historical metrics in the tz they occurred in)
            tz_series_m = df_metrics["timezone"] if "timezone" in df_metrics.columns else None
            off_series_m = df_metrics["utc_offset_min"] if "utc_offset_min" in df_metrics.columns else None
            params_m = [
                {
                    "user_id": user_id,
                    "timestamp": _py_dt(ts),
                    "metric_type": mtype,
                    "metric_value": float(val),
                    "unit": unit if unit is not None and pd.notna(unit) else None,
                    "source": source if source is not None and pd.notna(source) else None,
                    "created_at": _py_dt_or_none(cat),
                    "meta": meta,
                }
                for ts, mtype, val, unit, source, cat, meta in zip(
                    df_metrics["timestamp"],
                    df_metrics["metric_type"],
                    df_metrics["metric_value"],
                    df_metrics["unit"] if "unit" in df_metrics.columns else [None] * len(df_metrics),
                    df_metrics["source"] if "source" in df_metrics.columns else [None] * len(df_metrics),
                    df_metrics["created_at"] if "created_at" in df_metrics.columns else [None] * len(df_metrics),
                    [
                        (
                            json.dumps(
                                {
                                    **(
                                        {"tz_name": str(tz)} if tz_series_m is not None and tz is not None and pd.notna(tz) and str(tz).strip() else {}
                                    ),
                                    **(
                                        {"utc_offset_min": int(float(off))}
                                        if off_series_m is not None and off is not None and pd.notna(off)
                                        else {}
                                    ),
                                }
                            )
                            if (
                                (tz_series_m is not None and tz is not None and pd.notna(tz) and str(tz).strip())
                                or (off_series_m is not None and off is not None and pd.notna(off))
                            )
                            else None
                        )
                        for tz, off in zip(
                            tz_series_m if tz_series_m is not None else [None] * len(df_metrics),
                            off_series_m if off_series_m is not None else [None] * len(df_metrics),
                        )
                    ],
                )
            ]
            if params_m:
                _execmany(
                    """
                    INSERT INTO health_metrics
                        (user_id, timestamp, metric_type, metric_value, unit, source, created_at, meta)
                    VALUES
                        (:user_id, :timestamp, :metric_type, :metric_value, :unit, :source, :created_at, CAST(:meta AS jsonb))
                    ON CONFLICT (user_id, metric_type, timestamp) DO UPDATE
                    SET
                        metric_value = EXCLUDED.metric_value,
                        unit = EXCLUDED.unit,
                        source = EXCLUDED.source,
                        created_at = COALESCE(EXCLUDED.created_at, health_metrics.created_at),
                        meta = COALESCE(EXCLUDED.meta, health_metrics.meta)
                    """,
                    params_m,
                    "bulk insert health_metrics",
                    delay = 0.25,
                )

        session.commit()  # Commit raw writes first so rollup failures never discard ingested data

        # Recompute hourly rollups (best-effort) for the affected time window from health_metrics table
        if not df_metrics.empty:
            t_min = pd.to_datetime(df_metrics["timestamp"].min())
            t_max = pd.to_datetime(df_metrics["timestamp"].max())
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
    logger.info("process_csv_upload: done user_id=%s metrics=%s events=%s", user_id, len(df_metrics), len(df_events))
    return {"inserted": int(len(df_metrics) + len(df_events))}