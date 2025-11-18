from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple
from datetime import datetime, timedelta, timezone
from sqlalchemy import text

from Backend.database import SessionLocal


def _robust_stats(values: List[float]) -> Tuple[float, float]:
    if not values:
        return 0.0, 1.0
    vs = sorted(v for v in values if math.isfinite(v))
    if not vs:
        return 0.0, 1.0
    n = len(vs)
    median = vs[n // 2] if n % 2 == 1 else 0.5 * (vs[n // 2 - 1] + vs[n // 2])
    abs_dev = [abs(v - median) for v in vs]
    mad = sorted(abs_dev)[n // 2] if n % 2 == 1 else 0.5 * (sorted(abs_dev)[n // 2 - 1] + sorted(abs_dev)[n // 2])
    scale = mad if mad > 1e-6 else (sum(abs_dev) / n if n > 0 else 1.0)
    if scale <= 1e-6:
        scale = 1.0
    return median, scale


def _safe_pct_delta(current: float, baseline: float) -> float:
    if not math.isfinite(current) or not math.isfinite(baseline) or abs(baseline) < 1e-9:
        return 0.0
    return 100.0 * (current - baseline) / abs(baseline)


def _fetch_daily_aggregates(session, user_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    res = session.execute(
        text(
            """
            SELECT
              date_trunc('day', timestamp) AS day,
              metric_type,
              AVG(metric_value) AS avg_value,
              SUM(metric_value) AS sum_value,
              COUNT(*) AS n
            FROM health_metrics
            WHERE user_id = :user_id
              AND timestamp >= :start_ts
              AND timestamp < :end_ts
            GROUP BY 1, 2
            ORDER BY 1 ASC
            """
        ),
        {"user_id": user_id, "start_ts": start_date, "end_ts": end_date},
    ).mappings().all()
    return [dict(r) for r in res]


def _series_to_latest_and_baseline(series: List[Tuple[datetime, float]], baseline_days: int = 90) -> Tuple[float, float]:
    if not series:
        return 0.0, 0.0
    latest = series[-1][1]
    cutoff = series[-1][0] - timedelta(days=baseline_days)
    baseline_vals = [v for (ts, v) in series if ts >= cutoff]
    if not baseline_vals:
        baseline_vals = [v for (_, v) in series]
    median, _ = _robust_stats(baseline_vals)
    return latest, median


def compute_overview(user_id: str) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    last_30 = now - timedelta(days=30)
    last_180 = now - timedelta(days=180)
    with SessionLocal() as session:
        # Aggregates for last 30/180 days
        daily_30 = _fetch_daily_aggregates(session, user_id, last_30, now)
        daily_180 = _fetch_daily_aggregates(session, user_id, last_180, now)
        # Organize per metric
        by_metric_30: Dict[str, List[Tuple[datetime, float, float]]] = {}
        for row in daily_30:
            ts = row["day"]
            metric = row["metric_type"]
            by_metric_30.setdefault(metric, []).append((ts, row["avg_value"], row["sum_value"]))
        by_metric_180: Dict[str, List[Tuple[datetime, float, float]]] = {}
        for row in daily_180:
            ts = row["day"]
            metric = row["metric_type"]
            by_metric_180.setdefault(metric, []).append((ts, row["avg_value"], row["sum_value"]))
        # Key metrics: choose sum metrics vs avg metrics
        sum_like = {
            "steps",
            "active_energy_burned",
            "workout_duration_min",
            "workout_distance_km",
            "sleep_hours",
            "mindfulness_minutes",
        }
        signals: List[Dict[str, Any]] = []
        for metric, series in by_metric_180.items():
            # prefer sum over avg where applicable
            series_sum = [(ts, s) for (ts, _, s) in series]
            series_avg = [(ts, a) for (ts, a, _) in series]
            if metric in sum_like:
                latest, baseline = _series_to_latest_and_baseline(series_sum)
                value = sum(v for (_, v) in series_sum[-30:])  # total over last 30 days
                baseline_series_vals = [v for (_, v) in series_sum[-90:]] or [v for (_, v) in series_sum]
                b_median, b_scale = _robust_stats(baseline_series_vals)
                delta_pct = _safe_pct_delta(value, 30.0 * b_median if b_median else value)
                effect = (value - 30.0 * b_median) / (b_scale if b_scale else 1.0)
                signals.append(
                    {
                        "metric": metric,
                        "kind": "sum",
                        "period": "last_30_days",
                        "value": round(value, 2),
                        "baseline_daily_median": round(b_median, 2),
                        "delta_pct": round(delta_pct, 2),
                        "effect_size": round(effect, 2),
                    }
                )
            else:
                latest, baseline = _series_to_latest_and_baseline(series_avg)
                values = [v for (_, v) in series_avg[-30:]]
                v_median, v_scale = _robust_stats(values)
                delta_pct = _safe_pct_delta(v_median, baseline)
                effect = (v_median - baseline) / (v_scale if v_scale else 1.0)
                signals.append(
                    {
                        "metric": metric,
                        "kind": "avg",
                        "period": "last_30_days",
                        "value": round(v_median, 2),
                        "baseline_daily_median": round(baseline, 2),
                        "delta_pct": round(delta_pct, 2),
                        "effect_size": round(effect, 2),
                    }
                )
        # Select top signals by absolute effect size
        signals.sort(key=lambda s: abs(s.get("effect_size", 0.0)), reverse=True)
        top_signals = signals[:8]
        # Basic anomalies: last 30 days against 90-day baseline
        anomalies: List[Dict[str, Any]] = []
        for metric, series in by_metric_180.items():
            series_avg = [(ts, a) for (ts, a, _) in series]
            baseline_vals = [v for (_, v) in series_avg[:-30]] or [v for (_, v) in series_avg]
            b_median, b_scale = _robust_stats(baseline_vals)
            for (ts, v) in series_avg[-30:]:
                if b_scale <= 1e-6:
                    continue
                z = (v - b_median) / b_scale
                if abs(z) >= 2.5:
                    anomalies.append(
                        {"metric": metric, "date": ts.date().isoformat(), "value": round(v, 2), "z": round(z, 2)}
                    )
        anomalies = sorted(anomalies, key=lambda a: abs(a["z"]), reverse=True)[:5]
        overview = {
            "period": {"start": (now - timedelta(days=30)).date().isoformat(), "end": now.date().isoformat()},
            "top_signals": top_signals,
            "anomalies": anomalies,
        }
        # Upsert cache
        session.execute(
            text(
                """
                INSERT INTO health_overview_cache (user_id, summary_json, summary_text, generated_at, inputs_version)
                VALUES (:user_id, CAST(:summary_json AS JSONB), :summary_text, NOW(), :inputs_version)
                ON CONFLICT (user_id) DO UPDATE
                SET summary_json = EXCLUDED.summary_json,
                    summary_text = EXCLUDED.summary_text,
                    generated_at = EXCLUDED.generated_at,
                    inputs_version = EXCLUDED.inputs_version
                """
            ),
            {
                "user_id": user_id,
                "summary_json": __import__("json").dumps(overview),
                "summary_text": None,
                "inputs_version": "v1",
            },
        )
        session.commit()
        return overview


def get_cached_overview(user_id: str) -> Dict[str, Any] | None:
    with SessionLocal() as session:
        row = session.execute(
            text(
                """
                SELECT summary_json, summary_text, generated_at
                FROM health_overview_cache
                WHERE user_id = :user_id
                """
            ),
            {"user_id": user_id},
        ).fetchone()
        if not row:
            return None
        return {"summary_json": row[0], "summary_text": row[1], "generated_at": row[2].isoformat() if row[2] else None}




