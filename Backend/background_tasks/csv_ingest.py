from __future__ import annotations

import io
import logging
import pandas as pd
import base64
from typing import Dict, List
import math
from sqlalchemy import text
import time
import random
from collections import defaultdict

from Backend.celery import celery
from Backend.database import SessionLocal
from Backend.services.embeddings.embedder import Embedder
# Overview feature removed; no import

# Module logger
logger = logging.getLogger(__name__)


# Convert uploaded CSV into a pd DataFrame so we can compute summaries before embedding and saving to Postgres
def _parse_csv_bytes(data: bytes) -> pd.DataFrame:
    buffer = io.BytesIO(data)
    return pd.read_csv(buffer, parse_dates = ["timestamp", "created_at"])


def _summarize(df: pd.DataFrame) -> List[Dict]:
    if df.empty:
        return []

    now_ts = df["timestamp"].max()
    last14_start = now_ts - pd.Timedelta(days = 14)
    prev150_start = now_ts - pd.Timedelta(days = 164)
    df = df[df["timestamp"] >= prev150_start]

    df["date"] = df["timestamp"].dt.date
    df_week = df["timestamp"].dt.to_period("W").apply(lambda p: p.start_time.date())
    df = df.assign(week_start = df_week)

    SUM_METRICS = {
        "steps",
        "active_energy_burned",
        "dietary_water",
        "mindfulness_minutes",
        "sleep_hours",
        "active_time_minutes",
        "workout_distance_km",
        "workout_duration_min",
        "workout_energy_kcal",
    }
    # Units for embedding-friendly facts strings; keep labels concise and canonical
    UNIT_LABELS: Dict[str, str] = {
        "heart_rate": "bpm",
        "resting_heart_rate": "bpm",
        "walking_hr_avg": "bpm",
        "hr_variability_sdnn": "ms",
        "steps": "steps",
        "walking_speed": "m/s",
        "vo2_max": "ml/kg/min",
        "active_energy_burned": "kcal",
        "distance_walking_running_km": "km",
        "distance_cycling_km": "km",
        "distance_swimming_km": "km",
        "dietary_water": "L",
        "body_mass": "kg",
        "body_mass_index": "bmi",
        "blood_glucose": "mg/dL",
        "oxygen_saturation": "%",
        "blood_pressure_systolic": "mmHg",
        "blood_pressure_diastolic": "mmHg",
        "respiratory_rate": "breaths/min",
        "body_temperature": "°C",
        "mindfulness_minutes": "min",
        "sleep_hours": "hours",
        "active_time_minutes": "min",
        # Workout rollups for completeness
        "workout_distance_km": "km",
        "workout_duration_min": "min",
        "workout_energy_kcal": "kcal",
    }
    def _fmt(k: str, v: float) -> str:
        unit = UNIT_LABELS.get(k)
        if unit:
            return f"{k}={round(v, 2)} {unit}"
        return f"{k}={round(v, 2)}"

    # Build a per-metric dict, choosing to use sum or average based on metric type
    def pivot_metrics(frame: pd.DataFrame) -> Dict:
        grouped = frame.groupby("metric_type")["metric_value"]
        means = grouped.mean(numeric_only = True)
        sums = grouped.sum(numeric_only = True)

        metrics: Dict[str, float] = {}
        for metric in set(means.index).union(set(sums.index)):
            if metric in SUM_METRICS or str(metric).startswith("event_"):
                metrics[metric] = float(sums.get(metric, 0.0))
            else:
                metrics[metric] = float(means.get(metric, 0.0))
        return metrics

    rows: List[Dict] = []

    # Embedded DAILY summaries for the last 14 days
    df_last14 = df[df["timestamp"] >= last14_start]
    if not df_last14.empty:
        for day, frame in df_last14.groupby("date"):
            metrics = pivot_metrics(frame)
            # Compact, fact-centric string with explicit date, labels, and units for reliable retrieval
            text_summary = f"{day}: " + ", ".join(_fmt(k, v) for k, v in metrics.items())
            rows.append(
                {
                    "summary_type": "daily",
                    "start_date": day,
                    "end_date": day,
                    "summary_text": text_summary,
                    "metrics": metrics,
                }
            )

    # Embedded WEEKLY summaries for the previous 150 days (~21 weeks), after the last 14 days
    df_prev150 = df[(df["timestamp"] < last14_start) & (df["timestamp"] >= prev150_start)]
    if not df_prev150.empty:
        for week, frame in df_prev150.groupby("week_start"):
            end = max(frame["timestamp"]).date()
            metrics = pivot_metrics(frame)
            text_summary = f"{week}-{end}: " + ", ".join(_fmt(k, v) for k, v in metrics.items())
            rows.append(
                {
                    "summary_type": "weekly",
                    "start_date": week,
                    "end_date": end,
                    "summary_text": text_summary,
                    "metrics": metrics,
                }
            )

    start = min(df["timestamp"]).date()
    end = max(df["timestamp"]).date()
    metrics = pivot_metrics(df)
    text_summary = f"{start}–{end}: " + ", ".join(_fmt(k, v) for k, v in metrics.items())
    rows.append(
        {
            "summary_type": "global",
            "start_date": start,
            "end_date": end,
            "summary_text": text_summary,
            "metrics": metrics,
        }
    )
    return rows


@celery.task(name = "process_csv_upload")
def process_csv_upload(user_id: str, csv_bytes_b4: str) -> Dict:
    logger.info("process_csv_upload: start user_id=%s bytes=%s", user_id, len(csv_bytes_b4) if isinstance(csv_bytes_b4, (bytes, bytearray)) else "unknown")
    raw = base64.b64decode(csv_bytes_b4)
    df = _parse_csv_bytes(raw)
    # Normalize timestamps to tz-aware UTC once up-front
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc = True, errors = "coerce")
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], utc = True, errors = "coerce")
    df = df[df["user_id"] == user_id]
    if df.empty:
        logger.info("process_csv_upload: no rows for user_id=%s after filter; nothing to insert", user_id)
        return {"inserted": 0}

    now_ts = df["timestamp"].max()
    cutoff_ts = now_ts - pd.Timedelta(days = 164)
    df = df[df["timestamp"] >= cutoff_ts]

    df_events = df[df["metric_type"].str.startswith(("event_", "workout_"), na = False)].copy()  # Split event/workout rows for separate health_events table
    df_metrics = df[~df["metric_type"].str.startswith(("event_", "workout_"), na = False)].copy()  # Remaining rows are raw metrics for health_metrics
    summaries = _summarize(df)  # Create summaries (daily, weekly, global) for health_summaries table
    embedder = Embedder()

    # Perform cleanup for health_events and health_summaries tables
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
        try:
            session.execute(
                text(
                    """
                    DELETE FROM health_summaries
                    WHERE user_id = :user_id AND end_date < :cutoff_date
                    """
                ),
                {"user_id": user_id, "cutoff_date": cutoff_ts.date()},
            )
        except Exception:
            logger.exception("Failed pruning health_summaries for user_id=%s", user_id)

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

        # Bulk insert summaries into health_summaries table
        for s in summaries:
            vec = embedder.embed(s["summary_text"])  # type: ignore[arg-type]
            session.execute(
                text(
                    """
                    INSERT INTO health_summaries
                        (user_id, summary_type, start_date, end_date, summary_text, embedding, metrics)
                    VALUES
                        (:user_id, :summary_type, :start_date, :end_date, :summary_text, :embedding, CAST(:metrics AS JSONB))
                    """
                ),
                {
                    "user_id": user_id,
                    "summary_type": s["summary_type"],
                    "start_date": s["start_date"],
                    "end_date": s["end_date"],
                    "summary_text": s["summary_text"],
                    "embedding": vec,
                    "metrics": pd.Series(s["metrics"]).to_json(),
                },
            )
        # Bulk insert raw metrics into health_metrics
        if not df_metrics.empty:
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
        session.commit()
    # Compute rollups and upsert into rollup tables
    try:
        if not df_metrics.empty:
            dfm = df_metrics.copy()
            # Normalize oxygen saturation like '95%' → 95.0 and ensure numeric values for rollups
            if "unit" in dfm.columns:
                mask_o2 = dfm["metric_type"].isin(["oxygen_saturation", "blood_oxygen_saturation"])
                # Strip trailing percent sign if present in the value
                mask_pct = mask_o2 & dfm["metric_value"].astype(str).str.contains("%", na = False)
                dfm.loc[mask_pct, "metric_value"] = (
                    dfm.loc[mask_pct, "metric_value"].astype(str).str.replace("%", "", regex = False)
                )
            # Coerce to numeric (non-numeric → NaN so buckets with no numeric samples are skipped)
            dfm["metric_value"] = pd.to_numeric(dfm["metric_value"], errors = "coerce")
            # Metrics for which sum over time makes sense
            ADD_METRICS = {
                "steps",
                "active_energy_burned",
                "dietary_water",
                "mindfulness_minutes",
                "sleep_hours",
                "active_time_minutes",
            }
            # Hourly (last 14 days)
            t_cut_h = now_ts - pd.Timedelta(days = 14)
            dfh = dfm[dfm["timestamp"] >= t_cut_h].copy()
            if not dfh.empty:
                dfh.set_index("timestamp", inplace = True)
                parts_h = []
                removed_h = 0
                kept_h = 0
                for metric, g in dfh.groupby("metric_type"):
                    raw = g["metric_value"].resample("1H").agg(["mean", "sum", "min", "max", "count"])
                    before = len(raw)
                    agg = raw[raw["count"] > 0].dropna(how="all")
                    after = len(agg)
                    removed_h += max(0, before - after)
                    kept_h += max(0, after)
                    if not agg.empty:
                        agg = agg.reset_index().rename(columns={"timestamp": "bucket_ts"})
                        agg["metric_type"] = metric
                        parts_h.append(agg)
                if parts_h:
                    rh = pd.concat(parts_h, ignore_index = True)
                    with SessionLocal() as sh:
                        params = [
                            {
                                "user_id": user_id,
                                "bucket_ts": row["bucket_ts"].to_pydatetime(),
                                "metric_type": row["metric_type"],
                                "avg_value": float(row["mean"]) if pd.notna(row["mean"]) else None,
                                "sum_value": float(row["sum"]) if (pd.notna(row["sum"]) and row["metric_type"] in ADD_METRICS) else None,
                                "min_value": float(row["min"]) if pd.notna(row["min"]) else None,
                                "max_value": float(row["max"]) if pd.notna(row["max"]) else None,
                                "n": int(row["count"]) if pd.notna(row["count"]) else None,
                            }
                            for _, row in rh.iterrows()
                        ]
                        if params:
                            sh.execute(
                                text(
                                    """
                                    INSERT INTO health_rollup_hourly
                                      (user_id, bucket_ts, metric_type, avg_value, sum_value, min_value, max_value, n)
                                    VALUES
                                      (:user_id, :bucket_ts, :metric_type, :avg_value, :sum_value, :min_value, :max_value, :n)
                                    ON CONFLICT (user_id, metric_type, bucket_ts) DO UPDATE
                                    SET
                                      avg_value = COALESCE(EXCLUDED.avg_value, health_rollup_hourly.avg_value),
                                      sum_value = COALESCE(EXCLUDED.sum_value, health_rollup_hourly.sum_value),
                                      min_value = COALESCE(EXCLUDED.min_value, health_rollup_hourly.min_value),
                                      max_value = COALESCE(EXCLUDED.max_value, health_rollup_hourly.max_value),
                                      n = COALESCE(EXCLUDED.n, health_rollup_hourly.n)
                                    """
                                ),
                                params,
                            )
                            sh.commit()
            # Daily (all available in dfm)
            dfd = dfm.copy()
            dfd.set_index("timestamp", inplace = True)
            parts_d = []
            removed_d = 0
            kept_d = 0
            for metric, g in dfd.groupby("metric_type"):
                raw = g["metric_value"].resample("1D").agg(["mean", "sum", "min", "max", "count"])
                before = len(raw)
                agg = raw[raw["count"] > 0].dropna(how="all")
                after = len(agg)
                removed_d += max(0, before - after)
                kept_d += max(0, after)
                if not agg.empty:
                    agg = agg.reset_index().rename(columns={"timestamp": "bucket_ts"})
                    agg["metric_type"] = metric
                    parts_d.append(agg)
            if parts_d:
                rd = pd.concat(parts_d, ignore_index = True)
                with SessionLocal() as sd:
                    params = [
                        {
                            "user_id": user_id,
                            "bucket_ts": row["bucket_ts"].to_pydatetime(),
                            "metric_type": row["metric_type"],
                            "avg_value": float(row["mean"]) if pd.notna(row["mean"]) else None,
                            "sum_value": float(row["sum"]) if (pd.notna(row["sum"]) and row["metric_type"] in ADD_METRICS) else None,
                            "min_value": float(row["min"]) if pd.notna(row["min"]) else None,
                            "max_value": float(row["max"]) if pd.notna(row["max"]) else None,
                            "n": int(row["count"]) if pd.notna(row["count"]) else None,
                        }
                        for _, row in rd.iterrows()
                    ]
                    if params:
                        sd.execute(
                            text(
                                """
                                INSERT INTO health_rollup_daily
                                  (user_id, bucket_ts, metric_type, avg_value, sum_value, min_value, max_value, n)
                                VALUES
                                  (:user_id, :bucket_ts, :metric_type, :avg_value, :sum_value, :min_value, :max_value, :n)
                                ON CONFLICT (user_id, metric_type, bucket_ts) DO UPDATE
                                SET
                                  avg_value = COALESCE(EXCLUDED.avg_value, health_rollup_daily.avg_value),
                                  sum_value = COALESCE(EXCLUDED.sum_value, health_rollup_daily.sum_value),
                                  min_value = COALESCE(EXCLUDED.min_value, health_rollup_daily.min_value),
                                  max_value = COALESCE(EXCLUDED.max_value, health_rollup_daily.max_value),
                                  n = COALESCE(EXCLUDED.n, health_rollup_daily.n)
                                """
                            ),
                            params,
                        )
                        sd.commit()
    except Exception:
        logger.exception("Failed computing rollups for user_id=%s", user_id)
    # Derive health_sessions from workout events (one session per workout timestamp/type)
    try:
        df_w = df_events[df_events["metric_type"].str.startswith("workout_", na=False)].copy()
        if not df_w.empty:
            # Pivot metrics per (timestamp, source) group
            groups = df_w.groupby(["timestamp", "source"])
            with SessionLocal() as session2:
                created_sessions = 0
                created_slices = 0
                for (ts, src), g in groups:
                    # Aggregate values (take max per metric_type)
                    kv = g.pivot_table(index="metric_type", values="metric_value", aggfunc="max")["metric_value"].to_dict()
                    distance_km = float(kv.get("workout_distance_km", 0.0) or 0.0)
                    duration_min = float(kv.get("workout_duration_min", 0.0) or 0.0)
                    energy_kcal = float(kv.get("workout_energy_kcal", 0.0) or 0.0)
                    # Guard against negative/NaN durations
                    if not math.isfinite(duration_min) or duration_min < 0:
                        continue
                    avg_hr = None
                    # Normalize session type label from source (already like 'running', 'cycling', etc.)
                    session_type = str(src or "workout")
                    start_ts = pd.to_datetime(ts)
                    end_ts = start_ts + pd.to_timedelta(duration_min, unit="m")
                    # Insert/Upsert session and get id
                    row = session2.execute(
                        text(
                            """
                            INSERT INTO health_sessions
                                (user_id, session_type, source, external_id, start_ts, end_ts, duration_min, distance_km, energy_kcal, avg_hr)
                            VALUES
                                (:user_id, :session_type, :source, :external_id, :start_ts, :end_ts, :duration_min, :distance_km, :energy_kcal, :avg_hr)
                            ON CONFLICT (user_id, session_type, start_ts) DO UPDATE
                            SET
                                end_ts = EXCLUDED.end_ts,
                                duration_min = GREATEST(EXCLUDED.duration_min, health_sessions.duration_min),
                                distance_km = COALESCE(EXCLUDED.distance_km, health_sessions.distance_km),
                                energy_kcal = COALESCE(EXCLUDED.energy_kcal, health_sessions.energy_kcal),
                                avg_hr = COALESCE(EXCLUDED.avg_hr, health_sessions.avg_hr)
                            RETURNING id
                            """
                        ),
                        {
                            "user_id": user_id,
                            "session_type": session_type,
                            "source": "workout",
                            "external_id": None,
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                            "duration_min": duration_min,
                            "distance_km": distance_km if distance_km > 0 else None,
                            "energy_kcal": energy_kcal if energy_kcal > 0 else None,
                            "avg_hr": avg_hr,
                        },
                    ).fetchone()
                    if not row:
                        continue
                    session_id = row[0]
                    created_sessions += 1
                    # Upsert basic session features (drift metrics require fine-grain slices; set to NULL for now)
                    try:
                        flags = {}
                        try:
                            flags["fueling_gap"] = bool(duration_min is not None and duration_min >= 90 and (energy_kcal or 0.0) / max(duration_min, 1e-9) < 6.0)
                        except Exception:
                            flags["fueling_gap"] = None
                        session2.execute(
                            text(
                                """
                                INSERT INTO health_session_features
                                  (session_id, user_id, modality, duration_min, distance_km, avg_hr, pace_drift_pct, hr_drift_slope, decoupling_pct, time_to_fatigue_min, kcal, flags)
                                VALUES
                                  (:session_id, :user_id, :modality, :duration_min, :distance_km, :avg_hr, NULL, NULL, NULL, NULL, :kcal, CAST(:flags AS JSONB))
                                ON CONFLICT (session_id) DO UPDATE
                                SET
                                  user_id = EXCLUDED.user_id,
                                  modality = EXCLUDED.modality,
                                  duration_min = EXCLUDED.duration_min,
                                  distance_km = EXCLUDED.distance_km,
                                  avg_hr = COALESCE(EXCLUDED.avg_hr, health_session_features.avg_hr),
                                  kcal = COALESCE(EXCLUDED.kcal, health_session_features.kcal),
                                  flags = COALESCE(EXCLUDED.flags, health_session_features.flags)
                                """
                            ),
                            {
                                "session_id": session_id,
                                "user_id": user_id,
                                "modality": session_type,
                                "duration_min": duration_min if duration_min > 0 else None,
                                "distance_km": distance_km if distance_km > 0 else None,
                                "avg_hr": avg_hr,
                                "kcal": energy_kcal if energy_kcal > 0 else None,
                                "flags": pd.Series(flags).to_json(),
                            },
                        )
                    except Exception:
                        logger.exception("Failed upserting session features for session_id=%s", session_id)
                    # Create a single aggregate slice (km if distance present, else a time slice)
                    slice_type = "km" if distance_km > 0 else "min"
                    # Determine slice duration; ensure NOT NULL for partitioned table
                    if slice_type == "km":
                        slice_duration_min = duration_min if duration_min > 0 else 0.001
                    else:
                        if duration_min <= 0:
                            # No meaningful time slice to store
                            continue
                        slice_duration_min = duration_min
                    pace_s_per_km = None
                    speed_m_s = None
                    if distance_km > 0 and slice_duration_min > 0:
                        pace_s_per_km = (slice_duration_min * 60.0) / max(distance_km, 1e-9)
                        speed_m_s = (distance_km * 1000.0) / (slice_duration_min * 60.0)
                    slice_end_ts = start_ts + pd.to_timedelta(slice_duration_min, unit="m")
                    session2.execute(
                        text(
                            """
                            INSERT INTO health_session_slices
                                (session_id, slice_index, slice_type, start_ts, end_ts, distance_km, duration_min, pace_s_per_km, speed_m_s, avg_hr, kcal)
                            VALUES
                                (:session_id, :slice_index, :slice_type, :start_ts, :end_ts, :distance_km, :duration_min, :pace_s_per_km, :speed_m_s, :avg_hr, :kcal)
                            ON CONFLICT (session_id, slice_type, slice_index, start_ts) DO UPDATE
                            SET
                                end_ts = EXCLUDED.end_ts,
                                distance_km = COALESCE(EXCLUDED.distance_km, health_session_slices.distance_km),
                                duration_min = COALESCE(EXCLUDED.duration_min, health_session_slices.duration_min),
                                pace_s_per_km = COALESCE(EXCLUDED.pace_s_per_km, health_session_slices.pace_s_per_km),
                                speed_m_s = COALESCE(EXCLUDED.speed_m_s, health_session_slices.speed_m_s),
                                avg_hr = COALESCE(EXCLUDED.avg_hr, health_session_slices.avg_hr),
                                kcal = COALESCE(EXCLUDED.kcal, health_session_slices.kcal)
                            """
                        ),
                        {
                            "session_id": session_id,
                            "slice_index": 1,
                            "slice_type": slice_type,
                            "start_ts": start_ts,
                            "end_ts": slice_end_ts,
                            "distance_km": distance_km if distance_km > 0 else None,
                            "duration_min": slice_duration_min,
                            "pace_s_per_km": pace_s_per_km,
                            "speed_m_s": speed_m_s,
                            "avg_hr": avg_hr,
                            "kcal": energy_kcal if energy_kcal > 0 else None,
                        },
                    )
                    created_slices += 1
                session2.commit()
            logger.info("process_csv_upload: sessions created=%s slices_created=%s", created_sessions, created_slices)
    except Exception:
        logger.exception("Failed to derive health_sessions from workout events for user_id=%s", user_id)
    # Infer sessions from metrics when no explicit workout is present for that window
    try:
        # Build a pivot per timestamp for key metrics we can use for inference
        if not df_metrics.empty:
            key_metrics = [
                "active_time_minutes",
                "walking_speed",
                "steps",
                "active_energy_burned",
                "heart_rate",
                "distance_walking_running_km",
                "distance_cycling_km",
                "distance_swimming_km",
            ]
            df_k = df_metrics[df_metrics["metric_type"].isin(key_metrics)].copy()
            if not df_k.empty:
                # Convert metric rows into wide format per timestamp
                df_p = df_k.pivot_table(index="timestamp", columns="metric_type", values="metric_value", aggfunc="max")
                df_p = df_p.reset_index()
                # Fetch existing workout sessions for the user to avoid duplicating where a workout exists
                with SessionLocal() as s_chk:
                    res = s_chk.execute(
                        text(
                            """
                            SELECT start_ts, end_ts, session_type
                            FROM health_sessions
                            WHERE user_id = :user_id AND source = 'workout'
                            """
                        ),
                        {"user_id": user_id},
                    )
                    rows_chk = res.fetchall()
                    # Keep both start markers and full windows for overlap checks
                    existing_workout_starts = {(r[0], r[2]) for r in rows_chk}
                    existing_workout_windows = [(r[0], r[1]) for r in rows_chk]
                inferred_sessions = []
                inferred_slices = []
                for _, row in df_p.iterrows():
                    ts = pd.to_datetime(row["timestamp"])
                    # Coerce to finite values
                    raw_atm = row.get("active_time_minutes", 0)
                    atm = float(raw_atm) if raw_atm is not None else 0.0
                    if not math.isfinite(atm) or atm < 10.0:
                        continue  # require at least 10 min
                    def fv(x):
                        try:
                            v = float(x)
                            return v if math.isfinite(v) else 0.0
                        except Exception:
                            return 0.0
                    speed_walk = fv(row.get("walking_speed", 0))  # m/s
                    dwr_km = fv(row.get("distance_walking_running_km", 0))
                    dcy_km = fv(row.get("distance_cycling_km", 0))
                    dsw_km = fv(row.get("distance_swimming_km", 0))
                    # speed from distance per minute (km/min → m/s)
                    speed_from_dwr = ((dwr_km / atm) * (1000.0 / 60.0)) if atm > 0 and dwr_km > 0 else 0.0
                    speed = max(speed_walk, speed_from_dwr)
                    steps = fv(row.get("steps", 0))
                    kcal_v = fv(row.get("active_energy_burned", 0))
                    avg_hr_v = fv(row.get("heart_rate", 0))
                    avg_hr = avg_hr_v if avg_hr_v > 0 else None
                    # Classify modality using available distances first
                    session_type = None
                    if dcy_km > 0:
                        v_cyc = ((dcy_km / atm) * (1000.0 / 60.0)) if atm > 0 else 0.0  # m/s
                        if v_cyc >= 2.5:
                            session_type = "cycling"
                    if session_type is None and dsw_km > 0:
                        v_swim = ((dsw_km / atm) * (1000.0 / 60.0)) if atm > 0 else 0.0
                        if v_swim >= 0.3:
                            session_type = "swimming"
                    # Strength detection: low distance/steps with elevated HR or kcal intensity
                    if session_type is None:
                        total_distance_km = (dwr_km if dwr_km > 0 else 0.0) + (dcy_km if dcy_km > 0 else 0.0) + (dsw_km if dsw_km > 0 else 0.0)
                        kcal_per_min = (kcal_v / atm) if atm > 0 else 0.0
                        elevated_hr = avg_hr is not None and avg_hr >= 105.0
                        elevated_intensity = kcal_per_min >= 4.0
                        low_motion = total_distance_km <= 0.2 and steps < 800
                        if low_motion and (elevated_hr or elevated_intensity):
                            session_type = "strength"
                    # Speed/steps-based fallback
                    if session_type is None:
                        if speed >= 1.8:
                            session_type = "running"
                        elif speed >= 1.0 or steps >= 1200:
                            session_type = "walking"
                        else:
                            session_type = "other"
                    # Avoid creating inferred when a workout of same type starts at the same ts
                    if (ts, session_type) in existing_workout_starts:
                        continue
                    # Respect workouts as source of truth: skip if any workout overlaps this window
                    end_ts = ts + pd.to_timedelta(atm, unit="m")
                    try:
                        has_overlap = any((ws_start <= end_ts and ws_end >= ts) for (ws_start, ws_end) in existing_workout_windows)
                    except Exception:
                        has_overlap = False
                    if has_overlap:
                        continue
                    # Prefer actual distance metrics; otherwise approximate from walking speed
                    distance_km = None
                    for dv in (dwr_km, dcy_km, dsw_km):
                        if dv > 0:
                            distance_km = (distance_km or 0.0) + dv
                    if distance_km is None and speed > 0:
                        distance_km = (speed * atm * 60.0) / 1000.0
                    # Insert per-row to support RETURNING with psycopg2
                    with SessionLocal() as si:
                        row_id = si.execute(
                            text(
                                """
                                INSERT INTO health_sessions
                                    (user_id, session_type, source, external_id, start_ts, end_ts, duration_min, distance_km, energy_kcal, avg_hr)
                                VALUES
                                    (:user_id, :session_type, :source, :external_id, :start_ts, :end_ts, :duration_min, :distance_km, :energy_kcal, :avg_hr)
                                ON CONFLICT (user_id, session_type, start_ts) DO UPDATE
                                SET
                                    end_ts = EXCLUDED.end_ts,
                                    duration_min = GREATEST(EXCLUDED.duration_min, health_sessions.duration_min),
                                    distance_km = COALESCE(EXCLUDED.distance_km, health_sessions.distance_km),
                                    energy_kcal = COALESCE(EXCLUDED.energy_kcal, health_sessions.energy_kcal),
                                    avg_hr = COALESCE(EXCLUDED.avg_hr, health_sessions.avg_hr)
                                RETURNING id
                                """
                            ),
                            {
                                "user_id": user_id,
                                "session_type": session_type,
                                "source": "inferred",
                                "external_id": None,
                                "start_ts": ts,
                                "end_ts": end_ts,
                                "duration_min": atm,
                                "distance_km": distance_km,
                                "energy_kcal": kcal_v if kcal_v > 0 else None,
                                "avg_hr": avg_hr,
                            },
                        ).fetchone()
                        if not row_id:
                            continue
                        session_id = row_id[0]
                        # Upsert basic session features for inferred session
                        try:
                            flags = {}
                            try:
                                flags["fueling_gap"] = bool(atm is not None and atm >= 90 and (kcal_v or 0.0) / max(atm, 1e-9) < 6.0)
                            except Exception:
                                flags["fueling_gap"] = None
                            si.execute(
                                text(
                                    """
                                    INSERT INTO health_session_features
                                      (session_id, user_id, modality, duration_min, distance_km, avg_hr, pace_drift_pct, hr_drift_slope, decoupling_pct, time_to_fatigue_min, kcal, flags)
                                    VALUES
                                      (:session_id, :user_id, :modality, :duration_min, :distance_km, :avg_hr, NULL, NULL, NULL, NULL, :kcal, CAST(:flags AS JSONB))
                                    ON CONFLICT (session_id) DO UPDATE
                                    SET
                                      user_id = EXCLUDED.user_id,
                                      modality = EXCLUDED.modality,
                                      duration_min = EXCLUDED.duration_min,
                                      distance_km = EXCLUDED.distance_km,
                                      avg_hr = COALESCE(EXCLUDED.avg_hr, health_session_features.avg_hr),
                                      kcal = COALESCE(EXCLUDED.kcal, health_session_features.kcal),
                                      flags = COALESCE(EXCLUDED.flags, health_session_features.flags)
                                    """
                                ),
                                {
                                    "session_id": session_id,
                                    "user_id": user_id,
                                    "modality": session_type,
                                    "duration_min": float(atm) if atm and atm > 0 else None,
                                    "distance_km": float(distance_km) if distance_km and distance_km > 0 else None,
                                    "avg_hr": avg_hr,
                                    "kcal": float(kcal_v) if kcal_v and kcal_v > 0 else None,
                                    "flags": pd.Series(flags).to_json(),
                                },
                            )
                        except Exception:
                            logger.exception("Failed upserting session features for session_id=%s", session_id)
                        # Single aggregate slice (time-based if distance unknown)
                        slice_type = "km" if distance_km and distance_km > 0 else "min"
                        pace_s_per_km = None
                        speed_m_s = speed if speed > 0 else None
                        if distance_km and distance_km > 0:
                            pace_s_per_km = (atm * 60.0) / max(distance_km, 1e-9)
                        si.execute(
                            text(
                                """
                                INSERT INTO health_session_slices
                                    (session_id, slice_index, slice_type, start_ts, end_ts, distance_km, duration_min, pace_s_per_km, speed_m_s, avg_hr, kcal)
                                VALUES
                                    (:session_id, :slice_index, :slice_type, :start_ts, :end_ts, :distance_km, :duration_min, :pace_s_per_km, :speed_m_s, :avg_hr, :kcal)
                                ON CONFLICT (session_id, slice_type, slice_index, start_ts) DO UPDATE
                                SET
                                    end_ts = EXCLUDED.end_ts,
                                    distance_km = COALESCE(EXCLUDED.distance_km, health_session_slices.distance_km),
                                    duration_min = COALESCE(EXCLUDED.duration_min, health_session_slices.duration_min),
                                    pace_s_per_km = COALESCE(EXCLUDED.pace_s_per_km, health_session_slices.pace_s_per_km),
                                    speed_m_s = COALESCE(EXCLUDED.speed_m_s, health_session_slices.speed_m_s),
                                    avg_hr = COALESCE(EXCLUDED.avg_hr, health_session_slices.avg_hr),
                                    kcal = COALESCE(EXCLUDED.kcal, health_session_slices.kcal)
                                """
                            ),
                            {
                                "session_id": session_id,
                                "slice_index": 1,
                                "slice_type": slice_type,
                                "start_ts": ts,
                                "end_ts": end_ts,
                                "distance_km": distance_km if distance_km and distance_km > 0 else None,
                                "duration_min": atm,
                                "pace_s_per_km": pace_s_per_km,
                                "speed_m_s": speed_m_s,
                                "avg_hr": avg_hr,
                                "kcal": kcal_v if kcal_v > 0 else None,
                            },
                        )
                        si.commit()
                logger.info("process_csv_upload: inferred sessions created=%s", len(inferred_sessions) if inferred_sessions else 0)
    except Exception:
        logger.exception("Failed to infer sessions from metrics for user_id=%s", user_id)
    # Compute daily features for affected days
    try:
        affected_days = set()
        try:
            if not df.empty:
                affected_days.add(min(df["timestamp"]).date())
                affected_days.add(max(df["timestamp"]).date())
        except Exception:
            pass
        try:
            affected_days.add(pd.Timestamp.utcnow().date())
        except Exception:
            pass
        if affected_days:
            _upsert_daily_features(user_id, sorted(affected_days))
    except Exception:
        logger.exception("Failed computing daily features for user_id=%s", user_id)
    logger.info("process_csv_upload: done user_id=%s metrics=%s events=%s summaries=%s", user_id, len(df_metrics), len(df_events), len(summaries))
    return {"inserted": len(summaries)}


def _upsert_daily_features(user_id: str, days: List[pd.Timestamp]) -> None:
    """
    Compute per-day features from health_rollup_daily for the provided days and upsert into health_daily_features.
    """
    if not days:
        return
    day_min = min(days)
    day_max = max(days)
    with SessionLocal() as s:
        # Fetch rollups for prior 90 days window used for medians/MAD
        start_window = pd.to_datetime(day_min) - pd.Timedelta(days = 90)
        rows = s.execute(
            text(
                """
                SELECT bucket_ts::date AS day, metric_type, avg_value, sum_value, n
                FROM health_rollup_daily
                WHERE user_id = :user_id
                  AND bucket_ts::date >= :start_day
                  AND bucket_ts::date <= :end_day
                """
            ),
            {"user_id": user_id, "start_day": start_window.date(), "end_day": day_max},
        ).fetchall()
        by_day = defaultdict(dict)
        additive = {
            "steps",
            "active_energy_burned",
            "dietary_water",
            "mindfulness_minutes",
            "sleep_hours",
            "active_time_minutes",
        }
        for d, mtype, avg_v, sum_v, _ in rows:
            if mtype in additive:
                val = float(sum_v) if sum_v is not None else None
            else:
                val = float(avg_v) if avg_v is not None else None
            by_day[d][mtype] = val

        def gv(day, key):
            try:
                v = by_day.get(day, {}).get(key)
                return float(v) if v is not None and math.isfinite(float(v)) else None
            except Exception:
                return None

        def series_for(day, metric, back_days):
            vals = []
            for i in range(1, back_days + 1):
                d_prev = day - pd.Timedelta(days = i)
                v = gv(d_prev, metric)
                if v is not None:
                    vals.append(float(v))
            return vals

        def median(vals):
            if not vals:
                return None
            return float(pd.Series(vals).median())

        def mad(vals, med):
            if not vals:
                return None
            return float(pd.Series([abs(x - med) for x in vals]).median())

        for day in days:
            tv = {
                "hr_avg": gv(day, "heart_rate"),
                "rhr": gv(day, "resting_heart_rate"),
                "hrv_ms": gv(day, "hr_variability_sdnn"),
                "steps": gv(day, "steps"),
                "energy_kcal": gv(day, "active_energy_burned"),
                "sleep_hours": gv(day, "sleep_hours"),
                "active_min": gv(day, "active_time_minutes"),
            }
            keys = {
                "hr_avg": "heart_rate",
                "rhr": "resting_heart_rate",
                "hrv_ms": "hr_variability_sdnn",
                "steps": "steps",
                "energy_kcal": "active_energy_burned",
                "sleep_hours": "sleep_hours",
                "active_min": "active_time_minutes",
            }
            medians_obj = {}
            deltas_obj = {}
            zscores_obj = {}
            for label, mname in keys.items():
                vals30 = series_for(day, mname, 30)
                vals7 = vals30[:7]
                vals90 = series_for(day, mname, 90)
                m7 = median(vals7)
                m30 = median(vals30)
                m90 = median(vals90)
                medians_obj[f"{label}_7"] = m7
                medians_obj[f"{label}_30"] = m30
                medians_obj[f"{label}_90"] = m90
                today_v = tv.get(label)
                deltas_obj[f"{label}_vs_30"] = float(today_v - m30) if (today_v is not None and m30 is not None) else None
                if today_v is not None and m30 is not None and vals30:
                    mad30 = mad(vals30, m30)
                    z = float((today_v - m30) / (1.4826 * mad30)) if (mad30 is not None and mad30 > 0) else None
                else:
                    z = None
                zscores_obj[f"{label}_z"] = z

            flags_obj = {}
            try:
                flags_obj["low_sleep"] = bool(tv.get("sleep_hours") is not None and medians_obj.get("sleep_hours_30") is not None and tv["sleep_hours"] < (medians_obj["sleep_hours_30"] - 1.0))
            except Exception:
                flags_obj["low_sleep"] = None
            try:
                flags_obj["high_rhr"] = bool(tv.get("rhr") is not None and medians_obj.get("rhr_30") is not None and tv["rhr"] > (medians_obj["rhr_30"] + 3.0))
            except Exception:
                flags_obj["high_rhr"] = None
            try:
                flags_obj["low_hrv"] = bool(tv.get("hrv_ms") is not None and medians_obj.get("hrv_ms_30") is not None and tv["hrv_ms"] < (medians_obj["hrv_ms_30"] - 10.0))
            except Exception:
                flags_obj["low_hrv"] = None
            try:
                flags_obj["low_activity"] = bool(tv.get("steps") is not None and medians_obj.get("steps_30") is not None and tv["steps"] < (0.7 * medians_obj["steps_30"]))
            except Exception:
                flags_obj["low_activity"] = None
            try:
                flags_obj["strain_alert"] = bool(zscores_obj.get("energy_kcal_z") is not None and abs(zscores_obj["energy_kcal_z"]) >= 2.0)
            except Exception:
                flags_obj["strain_alert"] = None

            s.execute(
                text(
                    """
                    INSERT INTO health_daily_features (user_id, day, today_values, medians, deltas, zscores, flags)
                    VALUES (:user_id, :day, CAST(:tv AS JSONB), CAST(:med AS JSONB), CAST(:del AS JSONB), CAST(:zs AS JSONB), CAST(:fl AS JSONB))
                    ON CONFLICT (user_id, day) DO UPDATE
                    SET
                      today_values = EXCLUDED.today_values,
                      medians = EXCLUDED.medians,
                      deltas = EXCLUDED.deltas,
                      zscores = EXCLUDED.zscores,
                      flags = EXCLUDED.flags
                    """
                ),
                {
                    "user_id": user_id,
                    "day": day,
                    "tv": pd.Series(tv).to_json(),
                    "med": pd.Series(medians_obj).to_json(),
                    "del": pd.Series(deltas_obj).to_json(),
                    "zs": pd.Series(zscores_obj).to_json(),
                    "fl": pd.Series(flags_obj).to_json(),
                },
            )
        s.commit()


@celery.task(name = "refresh_daily_features")
def refresh_daily_features(user_id: str, days_back: int = 90) -> dict:
    """
    Nightly refresh: recompute health_daily_features for the last N days.
    Safe to run multiple times; uses upsert semantics.
    """
    try:
        end_day = pd.Timestamp.utcnow().date()
        start_day = (pd.Timestamp.utcnow() - pd.Timedelta(days = days_back)).date()
        days: List[pd.Timestamp] = []
        d = start_day
        while d <= end_day:
            days.append(d)
            d = (pd.Timestamp(d) + pd.Timedelta(days = 1)).date()
        _upsert_daily_features(user_id, days)
        return {"ok": True, "days": len(days)}
    except Exception:
        logger.exception("refresh_daily_features failed for user_id=%s", user_id)
        return {"ok": False}
