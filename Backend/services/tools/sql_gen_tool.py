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

    logger.info("sql.gen.start: question='%s' model=%s", question, "gemini-2.5-flash")

    client = get_async_openai_compatible_client("gemini")
    try:
        sql_resp = await client.chat.completions.create(
            model="gemini-2.5-flash",
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
        safe_sql = _sanitize_sql(extracted)
    except Exception as e:
        logger.error("sql.gen.error: question='%s' error=%s", question, str(e))
        return {"sql": {"sql": sql_text, "rows": [], "error": f"invalid-sql: {e}"}}

    loop = asyncio.get_running_loop()

    def execute_sql():
        with SessionLocal() as session:
            try:
                result = session.execute(text(safe_sql), {"user_id": user_id, "tz_name": tz_name}).mappings().all()
                rows = [dict(r) for r in result]
                if not rows:
                    logger.warning("sql.exec.empty: question='%s'", question)
                return {"sql": safe_sql, "rows": rows}
            except Exception as e:
                logger.error("sql.exec.error: question='%s' error=%s", question, str(e))
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
        for key in ("timestamp", "start_ts", "end_ts"):
            if key in rr and rr[key]:
                dt = rr[key]
                try:
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


