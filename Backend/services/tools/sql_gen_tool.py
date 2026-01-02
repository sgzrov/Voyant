import asyncio
import logging
import pathlib
from datetime import timezone
from sqlalchemy import text
from zoneinfo import ZoneInfo

from Backend.database import SessionLocal
from Backend.services.openai_compatible_client import get_async_openai_compatible_client
from Backend.services.sql_gen import _extract_sql_from_text, _sanitize_sql

logger = logging.getLogger(__name__)
_BACKEND_DIR = pathlib.Path(__file__).resolve().parents[2]


TOOL_SPEC = {
    "type": "function",
    "function": {
        "name": "fetch_health_context",
        "description": "Fetch SQL rows (exact numbers) for a health question. Call at most once.",
        "parameters": {
            "type": "object",
            "properties": {"question": {"type": "string"}},
            "required": ["question"],
        },
    },
}


# Generates SQL text via Gemini, sanitizes it, executes it, and returns db rows
async def execute_sql_gen_tool(*, user_id: str, question: str, tz_name: str) -> dict:
    sql_system_path = _BACKEND_DIR / "resources" / "sql_prompt.txt"

    sql_system_prompt = sql_system_path.read_text(encoding="utf-8")
    question_text = question

    logger.info("sql.gen.start: question='%s' model=%s", question, "gemini-2.5-flash-lite")

    client = get_async_openai_compatible_client("gemini")
    try:
        sql_resp = await client.chat.completions.create(
            model="gemini-2.5-flash-lite",
            messages=[
                {"role": "system", "content": sql_system_prompt},
                {"role": "user", "content": question_text},
            ],
            temperature=0,
        )
        sql_text = sql_resp.choices[0].message.content if sql_resp.choices else ""
    finally:
        try:
            await client.close()
        except Exception:
            pass
    if not isinstance(sql_text, str) or not sql_text.strip():
        logger.warning("sql.gen.empty: question='%s'", question)
        return {"sql": {"sql": None, "rows": [], "error": "no-sql"}}

    try:
        extracted = _extract_sql_from_text(sql_text)
        logger.info("sql.gen.sql.extracted:\n%s", extracted)
        safe_sql = _sanitize_sql(extracted)
        logger.info("sql.gen.sql.sanitized:\n%s", safe_sql)
    except Exception as e:
        logger.exception("sql.gen.error: question='%s' error=%s", question, str(e))
        try:
            logger.info("sql.gen.sql.raw:\n%s", sql_text)
        except Exception:
            pass
        return {"sql": {"sql": sql_text, "rows": [], "error": f"invalid-sql: {e}"}}

    loop = asyncio.get_running_loop()

    def execute_sql():
        with SessionLocal() as session:
            try:
                result = session.execute(text(safe_sql), {"user_id": user_id, "tz_name": tz_name}).mappings().all()
                rows = [dict(r) for r in result]

                # Post-processing: keep output travel-proof by formatting timestamps in the timezone active when recorded.
                try:
                    _rewrite_event_timestamps_inplace(session=session, user_id=user_id, rows=rows, request_tz=tz_name)
                except Exception:
                    pass
                try:
                    _rewrite_rollup_bucket_ts_inplace(session=session, user_id=user_id, rows=rows, request_tz=tz_name)
                except Exception:
                    pass

                if not rows:
                    logger.warning("sql.exec.empty: question='%s'\nsql:\n%s", question, safe_sql)
                return {"sql": safe_sql, "rows": rows}
            except Exception as e:
                logger.exception("sql.exec.error: question='%s' error=%s\nsql:\n%s", question, str(e), safe_sql)
                return {"sql": safe_sql, "rows": [], "error": str(e)}

    sql_out = await loop.run_in_executor(None, execute_sql)
    return {"sql": sql_out}


# Convert SQL UTC-default timestamps to user's current timezone
def localize_health_rows(rows: list[dict], tz: str) -> list[dict]:
    try:
        zone = ZoneInfo(tz)
    except Exception:
        zone = ZoneInfo("UTC")

    out: list[dict] = []
    for r in rows:
        rr = dict(r)
        # Localize any timestamp-like fields into the user's current timezone for display.
        # Note: workout timestamps may be further rewritten upstream using per-event timezone in health_events.meta.
        for key in ("timestamp", "start_ts", "end_ts", "bucket_ts", "workout_ts", "workout_timestamp"):
            if key in rr and rr[key]:
                dt = rr[key]
                try:
                    # If already formatted as a string upstream, leave as-is.
                    if isinstance(dt, str):
                        continue
                    if getattr(dt, "tzinfo", None) is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    rr[key] = dt.astimezone(zone).strftime("%Y-%m-%d %I:%M %p")
                except Exception:
                    pass
        for key in ("date", "day", "start_date", "end_date"):
            if key in rr and rr[key]:
                try:
                    d = rr[key]
                    rr[key] = d.isoformat() if hasattr(d, "isoformat") else str(d)
                except Exception:
                    pass
        out.append(rr)
    return out


def _rewrite_event_timestamps_inplace(*, session, user_id: str, rows: list[dict], request_tz: str) -> None:
    """Rewrite workout/event timestamps to the timezone active when the event occurred (from health_events.meta)."""
    candidate_keys = ("workout_ts", "workout_timestamp", "timestamp")

    ts_vals = []
    for rr in rows:
        for k in candidate_keys:
            v = rr.get(k)
            # Only consider actual datetime values
            if getattr(v, "tzinfo", None) is not None:
                ts_vals.append(v)
                break

    if not ts_vals:
        return

    uniq_ts = []
    seen = set()
    for dt in ts_vals:
        if dt not in seen:
            seen.add(dt)
            uniq_ts.append(dt)

    if not uniq_ts:
        return

    tz_map: dict[object, str | None] = {}
    for dt in uniq_ts:
        meta_row = session.execute(
            text(
                """
                SELECT meta
                FROM health_events
                WHERE user_id = :user_id
                  AND timestamp = :ts
                  AND event_type LIKE 'workout_%'
                LIMIT 1
                """
            ),
            {"user_id": user_id, "ts": dt},
        ).mappings().first()
        meta = meta_row.get("meta") if meta_row else None
        tzv = None
        if isinstance(meta, dict):
            tz_raw = meta.get("tz_name") or meta.get("timezone")
            if isinstance(tz_raw, str) and tz_raw.strip():
                tzv = tz_raw.strip()
        tz_map[dt] = tzv

    def _format_event_dt(dt):
        tzv = tz_map.get(dt)
        try:
            if tzv:
                return dt.astimezone(ZoneInfo(tzv)).strftime("%Y-%m-%d %I:%M %p"), tzv
        except Exception:
            pass
        # Fallback: use current request tz
        try:
            return dt.astimezone(ZoneInfo(request_tz)).strftime("%Y-%m-%d %I:%M %p"), None
        except Exception:
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %I:%M %p"), None

    for rr in rows:
        for k in candidate_keys:
            v = rr.get(k)
            if getattr(v, "tzinfo", None) is not None and v in tz_map:
                formatted, tzv = _format_event_dt(v)
                rr[k] = formatted
                # Optional: expose tz for the model to mention if helpful
                if tzv and "event_tz" not in rr:
                    rr["event_tz"] = tzv
                break


def _rewrite_rollup_bucket_ts_inplace(*, session, user_id: str, rows: list[dict], request_tz: str) -> None:
    """Rewrite rollup bucket_ts to the timezone active when the bucket occurred (from rollup meta)."""
    bucket_key = "bucket_ts"

    # Collect unique (bucket_ts, metric_type) pairs that look like datetimes
    pairs: list[tuple[object, str | None]] = []
    row_meta_tz: dict[tuple[object, str | None], str] = {}
    for rr in rows:
        dt = rr.get(bucket_key)
        if getattr(dt, "tzinfo", None) is None:
            continue
        mt = rr.get("metric_type")
        mtv = mt if isinstance(mt, str) and mt else None
        pairs.append((dt, mtv))

        # Prefer meta if the SQL row already included it (query explicitly selected meta)
        meta = rr.get("meta")
        if isinstance(meta, dict):
            tz_raw = meta.get("tz_name") or meta.get("timezone")
            if isinstance(tz_raw, str) and tz_raw.strip():
                row_meta_tz[(dt, mtv)] = tz_raw.strip()

    if not pairs:
        return

    uniq_pairs = []
    seen_pairs = set()
    for dt, mt in pairs:
        key = (dt, mt)
        if key not in seen_pairs:
            seen_pairs.add(key)
            uniq_pairs.append((dt, mt))

    if not uniq_pairs:
        return

    tz_map: dict[tuple[object, str | None], str | None] = {}
    for dt, mt in uniq_pairs:
        tzv = row_meta_tz.get((dt, mt))
        if tzv:
            tz_map[(dt, mt)] = tzv
            continue

        # Try hourly first; if not present, try daily (restored in mirror mode).
        meta_row = None
        if mt:
            meta_row = session.execute(
                text(
                    """
                    SELECT meta
                    FROM health_rollup_hourly
                    WHERE user_id = :user_id
                      AND bucket_ts = :ts
                      AND metric_type = :mt
                    LIMIT 1
                    """
                ),
                {"user_id": user_id, "ts": dt, "mt": mt},
            ).mappings().first()
            if not meta_row:
                meta_row = session.execute(
                    text(
                        """
                        SELECT meta
                        FROM health_rollup_daily
                        WHERE user_id = :user_id
                          AND bucket_ts = :ts
                          AND metric_type = :mt
                        LIMIT 1
                        """
                    ),
                    {"user_id": user_id, "ts": dt, "mt": mt},
                ).mappings().first()
        else:
            meta_row = session.execute(
                text(
                    """
                    SELECT meta
                    FROM health_rollup_hourly
                    WHERE user_id = :user_id
                      AND bucket_ts = :ts
                    LIMIT 1
                    """
                ),
                {"user_id": user_id, "ts": dt},
            ).mappings().first()
            if not meta_row:
                meta_row = session.execute(
                    text(
                        """
                        SELECT meta
                        FROM health_rollup_daily
                        WHERE user_id = :user_id
                          AND bucket_ts = :ts
                        LIMIT 1
                        """
                    ),
                    {"user_id": user_id, "ts": dt},
                ).mappings().first()

        meta = meta_row.get("meta") if meta_row else None
        if isinstance(meta, dict):
            tz_raw = meta.get("tz_name") or meta.get("timezone")
            if isinstance(tz_raw, str) and tz_raw.strip():
                tzv = tz_raw.strip()

        tz_map[(dt, mt)] = tzv

    def _format_bucket_dt(dt, mt):
        tzv = tz_map.get((dt, mt)) or tz_map.get((dt, None))
        try:
            if tzv:
                return dt.astimezone(ZoneInfo(tzv)).strftime("%Y-%m-%d %I:%M %p"), tzv
        except Exception:
            pass
        try:
            return dt.astimezone(ZoneInfo(request_tz)).strftime("%Y-%m-%d %I:%M %p"), None
        except Exception:
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %I:%M %p"), None

    for rr in rows:
        dt = rr.get(bucket_key)
        if getattr(dt, "tzinfo", None) is None:
            continue
        # If already formatted as string upstream, leave as-is.
        if isinstance(dt, str):
            continue
        mt = rr.get("metric_type")
        mtv = mt if isinstance(mt, str) and mt else None
        formatted, tzv = _format_bucket_dt(dt, mtv)
        rr[bucket_key] = formatted
        if tzv and "bucket_tz" not in rr:
            rr["bucket_tz"] = tzv

