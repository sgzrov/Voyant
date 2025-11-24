from __future__ import annotations

import io
import logging
import pandas as pd
import base64
from typing import Dict
import math
from sqlalchemy import text
import time
import random
from Backend.celery import celery
from Backend.database import SessionLocal


logger = logging.getLogger(__name__)

# Parse uploaded CSV bytes into a DataFrame for normalization and bulk inserts
def _parse_csv_bytes(data: bytes) -> pd.DataFrame:
    buffer = io.BytesIO(data)
    return pd.read_csv(buffer, parse_dates = ["timestamp", "created_at"])


@celery.task(name = "process_csv_upload")
def process_csv_upload(user_id: str, csv_bytes_b4: str) -> Dict:
    logger.info("process_csv_upload: start user_id=%s bytes=%s", user_id, len(csv_bytes_b4) if isinstance(csv_bytes_b4, (bytes, bytearray)) else "unknown")
    raw = base64.b64decode(csv_bytes_b4)
    df = _parse_csv_bytes(raw)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc = True, errors = "coerce")
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], utc = True, errors = "coerce")
    df = df[df["user_id"] == user_id]
    if df.empty:
        logger.info("process_csv_upload: no rows for user_id=%s after filter; nothing to insert", user_id)
        return {"inserted": 0}

    # Determine user's timezone from CSV if provided; fall back to UTC
    tz_name = "UTC"
    try:
        if "timezone" in df.columns:
            # Use the most frequent non-null timezone in the file
            non_null_tz = df["timezone"].dropna()
            if not non_null_tz.empty:
                tz_name = str(non_null_tz.mode().iloc[0])
    except Exception:
        tz_name = "UTC"

    now_ts = df["timestamp"].max()
    # Retain only last 60 days to match frontend export window
    cutoff_ts = now_ts - pd.Timedelta(days = 60)
    df = df[df["timestamp"] >= cutoff_ts]

    df_events = df[df["metric_type"].str.startswith(("event_", "workout_"), na = False)].copy()  # Split event/workout rows for separate health_events table
    df_metrics = df[~df["metric_type"].str.startswith(("event_", "workout_"), na = False)].copy()  # Remaining rows are raw metrics for health_metrics

    # Perform cleanup and upserts for health_events and health_metrics tables
    with SessionLocal() as session:
        # Per-user transaction-scoped advisory lock to serialize writes and avoid deadlocks
        try:
            session.execute(
                text("SELECT pg_advisory_xact_lock(:lock_key, hashtext(:user_id))"),
                {"lock_key": 42, "user_id": user_id},
            )
        except Exception:
            logger.exception("Failed to acquire advisory lock for user_id=%s", user_id)
            # Proceed anyway; retry blocks below will still mitigate transient errors
        try:
            session.execute(
                text(
                    """
                    DELETE FROM health_events
                    WHERE user_id = :user_id AND timestamp < :cutoff
                    """
                ),
                {"user_id": user_id, "cutoff": cutoff_ts},
            )
        except Exception:
            logger.exception("Failed pruning health_events for user_id=%s", user_id)
        # Cleanup older health_metrics beyond cutoff
        try:
            session.execute(
                text(
                    """
                    DELETE FROM health_metrics
                    WHERE user_id = :user_id AND timestamp < :cutoff
                    """
                ),
                {"user_id": user_id, "cutoff": cutoff_ts},
            )
        except Exception:
            logger.exception("Failed pruning health_metrics for user_id=%s", user_id)
        # No health_summaries pruning in metrics-only mode

        # Bulk insert events/workouts into health_events table
        if not df_events.empty:
            params = [
                {
                    "user_id": user_id,
                    "timestamp": ts,
                    "event_type": etype,
                    "value": float(val),
                    "unit": unit if unit is not None else None,
                    "source": source if source is not None else None,
                }
                for ts, etype, val, unit, source in zip(
                    df_events["timestamp"],
                    df_events["metric_type"],
                    df_events["metric_value"],
                    df_events["unit"] if "unit" in df_events.columns else [None] * len(df_events),
                    df_events["source"] if "source" in df_events.columns else [None] * len(df_events),
                )
            ]
            if params:
                # Deadlock-resilient executemany
                for attempt in range(3):
                    try:
                        session.execute(
                            text(
                                """
                                INSERT INTO health_events (user_id, timestamp, event_type, value, unit, source)
                                VALUES (:user_id, :timestamp, :event_type, :value, :unit, :source)
                                ON CONFLICT (user_id, event_type, timestamp) DO UPDATE
                                SET
                                    value = GREATEST(EXCLUDED.value, health_events.value),
                                    unit = COALESCE(EXCLUDED.unit, health_events.unit),
                                    source = COALESCE(EXCLUDED.source, health_events.source)
                                """
                            ),
                            params,
                        )
                        break
                    except Exception as e:
                        msg = str(e).lower()
                        if "deadlock detected" in msg and attempt < 2:
                            try:
                                session.rollback()
                            except Exception:
                                pass
                            time.sleep(0.2 * (attempt + 1) + random.random() * 0.2)
                            continue
                        logger.exception("Failed to bulk insert health_events for user_id=%s", user_id)
                        try:
                            session.rollback()
                        except Exception:
                            logger.exception("Rollback failed after health_events error for user_id=%s", user_id)

        # Summaries/embeddings disabled
        # Bulk insert raw metrics into health_metrics
        if not df_metrics.empty:
            # Proactively clear the overlapping window to avoid duplicate buckets from re-exports
            try:
                t_min_existing = pd.to_datetime(df_metrics["timestamp"].min())
                t_max_existing = pd.to_datetime(df_metrics["timestamp"].max())
                if pd.notna(t_min_existing) and pd.notna(t_max_existing):
                    session.execute(
                        text(
                            """
                            DELETE FROM health_metrics
                            WHERE user_id = :user_id
                              AND timestamp >= :t0 AND timestamp < :t1
                            """
                        ),
                        {
                            "user_id": user_id,
                            "t0": pd.Timestamp(t_min_existing).to_pydatetime(),
                            "t1": (pd.Timestamp(t_max_existing).to_pydatetime()),
                        },
                    )
            except Exception:
                logger.exception("Failed to clear overlapping health_metrics window for user_id=%s", user_id)
            params_m = [
                {
                    "user_id": user_id,
                    "timestamp": ts,
                    "metric_type": mtype,
                    "metric_value": float(val),
                    "unit": unit if unit is not None else None,
                    "source": source if source is not None else None,
                    "created_at": cat if pd.notna(cat) else None,
                }
                for ts, mtype, val, unit, source, cat in zip(
                    df_metrics["timestamp"],
                    df_metrics["metric_type"],
                    df_metrics["metric_value"],
                    df_metrics["unit"] if "unit" in df_metrics.columns else [None] * len(df_metrics),
                    df_metrics["source"] if "source" in df_metrics.columns else [None] * len(df_metrics),
                    df_metrics["created_at"] if "created_at" in df_metrics.columns else [None] * len(df_metrics),
                )
            ]
            if params_m:
                for attempt in range(3):
                    try:
                        session.execute(
                            text(
                                """
                                INSERT INTO health_metrics
                                    (user_id, timestamp, metric_type, metric_value, unit, source, created_at)
                                VALUES
                                    (:user_id, :timestamp, :metric_type, :metric_value, :unit, :source, :created_at)
                                ON CONFLICT (user_id, metric_type, timestamp) DO UPDATE
                                SET
                                    metric_value = EXCLUDED.metric_value,
                                    unit = EXCLUDED.unit,
                                    source = EXCLUDED.source,
                                    created_at = COALESCE(EXCLUDED.created_at, health_metrics.created_at)
                                """
                            ),
                            params_m,
                        )
                        break
                    except Exception as e:
                        msg = str(e).lower()
                        if "deadlock detected" in msg and attempt < 2:
                            try:
                                session.rollback()
                            except Exception:
                                pass
                            time.sleep(0.25 * (attempt + 1) + random.random() * 0.25)
                            continue
                        logger.exception("Failed to bulk insert health_metrics for user_id=%s", user_id)
                        # Clear failed transaction so subsequent statements can proceed
                        try:
                            session.rollback()
                        except Exception:
                            logger.exception("Rollback failed after health_metrics error for user_id=%s", user_id)
        # Build/refresh rollups for the affected window (based on metrics timestamps)
        try:
            if not df_metrics.empty:
                t_min = pd.to_datetime(df_metrics["timestamp"].min())
                t_max = pd.to_datetime(df_metrics["timestamp"].max())
                # Guard against NaT
                if pd.notna(t_min) and pd.notna(t_max):
                    t0 = pd.Timestamp(t_min).floor("H").to_pydatetime()
                    t1 = (pd.Timestamp(t_max).floor("H") + pd.Timedelta(hours = 1)).to_pydatetime()
                    # Upsert HOURLY from raw metrics
                    session.execute(
                        text(
                            """
                            INSERT INTO health_rollup_hourly
                                (user_id, bucket_ts, metric_type, avg_value, sum_value, min_value, max_value, n)
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
                            FROM health_metrics
                            WHERE user_id = :user_id
                              AND timestamp >= :t0 AND timestamp < :t1
                            GROUP BY 1,2,3
                            ON CONFLICT (user_id, metric_type, bucket_ts) DO UPDATE SET
                              avg_value = EXCLUDED.avg_value,
                              sum_value = EXCLUDED.sum_value,
                              min_value = EXCLUDED.min_value,
                              max_value = EXCLUDED.max_value,
                              n = EXCLUDED.n
                            """
                        ),
                        {"user_id": user_id, "t0": t0, "t1": t1},
                    )
        except Exception:
            logger.exception("Failed to compute rollups for user_id=%s", user_id)
        session.commit()
    logger.info("process_csv_upload: done user_id=%s metrics=%s events=%s tz=%s", user_id, len(df_metrics), len(df_events), tz_name)
    return {"inserted": int(len(df_metrics) + len(df_events))}