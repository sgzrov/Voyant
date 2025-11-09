from __future__ import annotations

import io
import logging
import pandas as pd
import base64
from typing import Dict, List
from sqlalchemy import text

from Backend.celery import celery
from Backend.database import SessionLocal
from Backend.services.embeddings.embedder import Embedder

# Module logger
logger = logging.getLogger(__name__)


# Convert uploaded CSV into a pd DataFrame so we can compute summaries before embedding and saving to Postgres
def _parse_csv_bytes(data: bytes) -> pd.DataFrame:
    buffer = io.BytesIO(data)
    return pd.read_csv(buffer, parse_dates = ["timestamp", "created_at"], infer_datetime_format = True)


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
def process_csv_upload(user_id: str, csv_bytes_b64: str) -> Dict:

    raw = base64.b64decode(csv_bytes_b64)
    df = _parse_csv_bytes(raw)
    df = df[df["user_id"] == user_id]
    if df.empty:
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
                            """
                        ),
                        params,
                    )
                except Exception:
                    logger.exception("Failed to bulk insert health_events for user_id=%s", user_id)

        # Bulk insert summaries into health_summaries table
        for s in summaries:
            vec = embedder.embed(s["summary_text"])  # type: ignore[arg-type]
            session.execute(
                text(
                    """
                    INSERT INTO health_summaries
                        (user_id, summary_type, start_date, end_date, summary_text, embedding, metrics)
                    VALUES
                        (:user_id, :summary_type, :start_date, :end_date, :summary_text, :embedding, :metrics::jsonb)
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
        session.commit()

    return {"inserted": len(summaries)}  # for monitoring


