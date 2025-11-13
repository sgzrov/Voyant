from __future__ import annotations

import io
import logging
import pandas as pd
import base64
from typing import Dict, List
import math
from sqlalchemy import text

from Backend.celery import celery
from Backend.database import SessionLocal
from Backend.services.embeddings.embedder import Embedder

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
            text_summary = (
                f"Daily summary for {day}: "
                + ", ".join(f"{k}={round(v,2)}" for k, v in metrics.items())
            )
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
            text_summary = (
                f"Weekly summary {week}-{end}: "
                + ", ".join(f"{k}={round(v,2)}" for k, v in metrics.items())
            )
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
    text_summary = (
        f"Global summary {start}–{end}: "
        + ", ".join(f"{k} = {round(v,2)}" for k, v in metrics.items())
    )
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
                except Exception:
                    logger.exception("Failed to bulk insert health_events for user_id=%s", user_id)
                    # Clear failed transaction so subsequent statements can proceed
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
            try:
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
            except Exception:
                logger.exception("Failed to bulk insert health_metrics for user_id=%s", user_id)
                # Clear failed transaction so subsequent statements can proceed
                try:
                    session.rollback()
                except Exception:
                    logger.exception("Rollback failed after health_metrics error for user_id=%s", user_id)
        session.commit()
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
                                (user_id, session_type, source, external_id, activity_type, start_ts, end_ts, duration_min, distance_km, energy_kcal, avg_hr, notes)
                            VALUES
                                (:user_id, :session_type, :source, :external_id, :activity_type, :start_ts, :end_ts, :duration_min, :distance_km, :energy_kcal, :avg_hr, :notes)
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
                            "activity_type": None,
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                            "duration_min": duration_min,
                            "distance_km": distance_km if distance_km > 0 else None,
                            "energy_kcal": energy_kcal if energy_kcal > 0 else None,
                            "avg_hr": avg_hr,
                            "notes": None,
                        },
                    ).fetchone()
                    if not row:
                        continue
                    session_id = row[0]
                    created_sessions += 1
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
                            SELECT start_ts, session_type
                            FROM health_sessions
                            WHERE user_id = :user_id AND source = 'workout'
                            """
                        ),
                        {"user_id": user_id},
                    )
                    existing_workout_starts = {(r[0], r[1]) for r in res.fetchall()}
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
                    end_ts = ts + pd.to_timedelta(atm, unit="m")
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
                                    (user_id, session_type, source, external_id, activity_type, start_ts, end_ts, duration_min, distance_km, energy_kcal, avg_hr, notes)
                                VALUES
                                    (:user_id, :session_type, :source, :external_id, :activity_type, :start_ts, :end_ts, :duration_min, :distance_km, :energy_kcal, :avg_hr, :notes)
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
                                "activity_type": None,
                                "start_ts": ts,
                                "end_ts": end_ts,
                                "duration_min": atm,
                                "distance_km": distance_km,
                                "energy_kcal": kcal_v if kcal_v > 0 else None,
                                "avg_hr": avg_hr,
                                "notes": None,
                            },
                        ).fetchone()
                        if not row_id:
                            continue
                        session_id = row_id[0]
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
    logger.info("process_csv_upload: done user_id=%s metrics=%s events=%s summaries=%s", user_id, len(df_metrics), len(df_events), len(summaries))
    return {"inserted": len(summaries)}  # for monitoring


