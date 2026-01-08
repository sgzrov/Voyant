import asyncio
import logging
import pathlib
from datetime import timezone
from sqlalchemy import bindparam, text
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
        # Log as a single line to avoid multi-process interleaving under gunicorn.
        logger.info("sql.gen.sql.extracted: %s", extracted.replace("\n", "\\n"))
        safe_sql = _sanitize_sql(extracted)
        logger.info("sql.gen.sql.sanitized: %s", safe_sql.replace("\n", "\\n"))
    except Exception as e:
        logger.exception("sql.gen.error: question='%s' error=%s", question, str(e))
        try:
            logger.info("sql.gen.sql.raw: %s", str(sql_text).replace("\n", "\\n"))
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
                    _rewrite_workout_timestamps_inplace(session=session, user_id=user_id, rows=rows, request_tz=tz_name)
                except Exception:
                    pass
                try:
                    _rewrite_rollup_bucket_ts_inplace(session=session, user_id=user_id, rows=rows, request_tz=tz_name)
                except Exception:
                    pass
                try:
                    _rewrite_sleep_daily_timestamps_inplace(session=session, user_id=user_id, rows=rows, request_tz=tz_name)
                except Exception:
                    pass

                if not rows:
                    logger.warning("sql.exec.empty: question='%s' sql=%s", question, safe_sql.replace("\n", "\\n"))
                return {"sql": safe_sql, "rows": rows}
            except Exception as e:
                logger.exception(
                    "sql.exec.error: question='%s' error=%s sql=%s",
                    question,
                    str(e),
                    safe_sql.replace("\n", "\\n"),
                )
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
        # Note: workout timestamps may be further rewritten upstream using per-event timezone in main_health_events.hk_metadata (HKTimeZone).
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
    """Rewrite workout/event timestamps to the timezone active when the event occurred (from main_health_events.hk_metadata['HKTimeZone'])."""
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
                SELECT hk_metadata
                FROM main_health_events
                WHERE user_id = :user_id
                  AND timestamp = :ts
                  AND event_type LIKE 'workout_%'
                LIMIT 1
                """
            ),
            {"user_id": user_id, "ts": dt},
        ).mappings().first()
        meta = meta_row.get("hk_metadata") if meta_row else None
        tzv = None
        if isinstance(meta, dict):
            # HealthKit commonly stores timezone for workouts as HKTimeZone (IANA name).
            tz_raw = meta.get("HKTimeZone") or meta.get("tz_name") or meta.get("timezone")
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


def _rewrite_workout_timestamps_inplace(*, session, user_id: str, rows: list[dict], request_tz: str) -> None:
    """Rewrite derived workout timestamps (start_ts/end_ts/segment timestamps) to the timezone active when recorded.

    - For derived_workouts, timezone comes from derived_workouts.hk_metadata ("HKTimeZone" or injected "tz_name"/"timezone").
    - For derived_workout_segments, timezone comes from the parent workout (derived_workouts.hk_metadata) via workout_uuid.
    - Fallback: request_tz.
    """

    def _tz_from_meta(meta_obj) -> str | None:
        if isinstance(meta_obj, dict):
            tz_raw = meta_obj.get("HKTimeZone") or meta_obj.get("tz_name") or meta_obj.get("timezone")
            if isinstance(tz_raw, str) and tz_raw.strip():
                return tz_raw.strip()
        return None

    # Collect workout_uuids and/or candidate workout start_ts values we might use to look up hk_metadata.
    workout_uuids: set[str] = set()
    workout_start_ts_vals: set[object] = set()
    start_ts_vals: set[object] = set()

    for rr in rows:
        # Heuristic: segment rows have many per-segment columns; their start_ts is not the workout start_ts.
        is_segment = any(k in rr for k in ("segment_index", "segment_unit", "pace_s_per_unit", "start_offset_min", "end_offset_min"))
        wid = rr.get("workout_uuid")
        if isinstance(wid, str) and wid.strip():
            workout_uuids.add(wid.strip())
        wst = rr.get("workout_start_ts")
        if getattr(wst, "tzinfo", None) is not None:
            workout_start_ts_vals.add(wst)
        if not is_segment:
            st = rr.get("start_ts")
            if getattr(st, "tzinfo", None) is not None:
                start_ts_vals.add(st)

    # If we already have hk_metadata in-row for workouts, prefer it and avoid DB lookups.
    uuid_tz: dict[str, str | None] = {}
    start_tz: dict[object, str | None] = {}

    for rr in rows:
        meta = rr.get("hk_metadata")
        tzv = _tz_from_meta(meta)
        wid = rr.get("workout_uuid")
        if tzv and isinstance(wid, str) and wid.strip():
            uuid_tz.setdefault(wid.strip(), tzv)
        st = rr.get("start_ts")
        if tzv and getattr(st, "tzinfo", None) is not None:
            start_tz.setdefault(st, tzv)

    # Look up remaining workout_uuids in derived_workouts.
    missing_uuids = [u for u in workout_uuids if u not in uuid_tz]
    if missing_uuids:
        meta_rows = session.execute(
            text(
                """
                SELECT workout_uuid, hk_metadata
                FROM derived_workouts
                WHERE user_id = :user_id
                  AND workout_uuid IN :uuids
                """
            ).bindparams(bindparam("uuids", expanding=True)),
            {"user_id": user_id, "uuids": missing_uuids},
        ).mappings().all()
        for mr in meta_rows:
            wid = mr.get("workout_uuid")
            tzv = _tz_from_meta(mr.get("hk_metadata"))
            if isinstance(wid, str) and wid.strip():
                uuid_tz.setdefault(wid.strip(), tzv)

    # Look up remaining start_ts values in derived_workouts (covers queries that only returned start_ts).
    missing_start_ts = [dt for dt in start_ts_vals if dt not in start_tz]
    if missing_start_ts:
        meta_rows = session.execute(
            text(
                """
                SELECT start_ts, hk_metadata
                FROM derived_workouts
                WHERE user_id = :user_id
                  AND start_ts IN :start_ts_vals
                """
            ).bindparams(bindparam("start_ts_vals", expanding=True)),
            {"user_id": user_id, "start_ts_vals": missing_start_ts},
        ).mappings().all()
        for mr in meta_rows:
            st = mr.get("start_ts")
            if getattr(st, "tzinfo", None) is None:
                continue
            tzv = _tz_from_meta(mr.get("hk_metadata"))
            start_tz.setdefault(st, tzv)

    # Look up workout_start_ts values (covers segment-only result sets without workout_uuid).
    missing_workout_start_ts = [dt for dt in workout_start_ts_vals if dt not in start_tz]
    if missing_workout_start_ts:
        meta_rows = session.execute(
            text(
                """
                SELECT start_ts, hk_metadata
                FROM derived_workouts
                WHERE user_id = :user_id
                  AND start_ts IN :start_ts_vals
                """
            ).bindparams(bindparam("start_ts_vals", expanding=True)),
            {"user_id": user_id, "start_ts_vals": missing_workout_start_ts},
        ).mappings().all()
        for mr in meta_rows:
            st = mr.get("start_ts")
            if getattr(st, "tzinfo", None) is None:
                continue
            tzv = _tz_from_meta(mr.get("hk_metadata"))
            start_tz.setdefault(st, tzv)

    def _format_dt(dt, tzv: str | None):
        # Prefer per-row timezone when valid; fallback to request_tz.
        try:
            if tzv:
                return dt.astimezone(ZoneInfo(tzv)).strftime("%Y-%m-%d %I:%M %p"), tzv
        except Exception:
            pass
        try:
            return dt.astimezone(ZoneInfo(request_tz)).strftime("%Y-%m-%d %I:%M %p"), None
        except Exception:
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %I:%M %p"), None

    # Rewrite timestamps in-place.
    candidate_ts_keys = ("start_ts", "end_ts", "workout_start_ts")
    for rr in rows:
        is_segment = any(k in rr for k in ("segment_index", "segment_unit", "pace_s_per_unit", "start_offset_min", "end_offset_min"))
        tzv = _tz_from_meta(rr.get("hk_metadata"))
        if not tzv:
            wid = rr.get("workout_uuid")
            if isinstance(wid, str) and wid.strip():
                tzv = uuid_tz.get(wid.strip())
        if not tzv:
            wst = rr.get("workout_start_ts")
            if getattr(wst, "tzinfo", None) is not None:
                tzv = start_tz.get(wst)
        if not tzv and not is_segment:
            st = rr.get("start_ts")
            if getattr(st, "tzinfo", None) is not None:
                tzv = start_tz.get(st)

        for k in candidate_ts_keys:
            v = rr.get(k)
            if getattr(v, "tzinfo", None) is None:
                continue
            # If already formatted as a string upstream, leave as-is.
            if isinstance(v, str):
                continue
            formatted, tz_used = _format_dt(v, tzv)
            rr[k] = formatted
            # Optional: expose tz for the model to mention if helpful
            if tz_used and "event_tz" not in rr:
                rr["event_tz"] = tz_used


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
                    FROM derived_rollup_hourly
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
                        FROM derived_rollup_daily
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
                    FROM derived_rollup_hourly
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
                        FROM derived_rollup_daily
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


def _rewrite_sleep_daily_timestamps_inplace(*, session, user_id: str, rows: list[dict], request_tz: str) -> None:
    """Rewrite derived_sleep_daily timestamps to the timezone active when the sleep was recorded (from row meta tz_name)."""

    candidate_keys = ("sleep_start_ts", "sleep_end_ts")
    # Collect sleep_date keys we can use to fetch meta tz if the SQL row didn't include meta.
    sleep_dates: set[object] = set()
    for rr in rows:
        sd = rr.get("sleep_date")
        if sd is not None:
            sleep_dates.add(sd)

    tz_by_date: dict[object, str | None] = {}
    if sleep_dates:
        meta_rows = session.execute(
            text(
                """
                SELECT sleep_date, meta
                FROM derived_sleep_daily
                WHERE user_id = :user_id
                  AND sleep_date = ANY(:sleep_dates)
                """
            ),
            {"user_id": user_id, "sleep_dates": list(sleep_dates)},
        ).mappings().all()
        for mr in meta_rows:
            sd = mr.get("sleep_date")
            meta = mr.get("meta")
            tzv = None
            if isinstance(meta, dict):
                tz_raw = meta.get("tz_name") or meta.get("timezone")
                if isinstance(tz_raw, str) and tz_raw.strip():
                    tzv = tz_raw.strip()
            if sd is not None:
                tz_by_date[sd] = tzv

    def _format_dt(dt, tzv: str | None):
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
        tzv = None
        meta = rr.get("meta")
        if isinstance(meta, dict):
            tz_raw = meta.get("tz_name") or meta.get("timezone")
            if isinstance(tz_raw, str) and tz_raw.strip():
                tzv = tz_raw.strip()
        if not tzv:
            sd = rr.get("sleep_date")
            if sd is not None:
                tzv = tz_by_date.get(sd)

        for k in candidate_keys:
            v = rr.get(k)
            if getattr(v, "tzinfo", None) is None or isinstance(v, str):
                continue
            formatted, tz_used = _format_dt(v, tzv)
            rr[k] = formatted
            if tz_used and "event_tz" not in rr:
                rr["event_tz"] = tz_used

