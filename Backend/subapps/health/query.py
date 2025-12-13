import asyncio
import json
import logging
import os
import pathlib
import re
import time
import uuid
from datetime import timezone

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from openai import OpenAI
from zoneinfo import ZoneInfo

from Backend.auth import verify_clerk_jwt
from Backend.crud.chat import create_chat_message, get_chat_history, get_or_create_conversation, update_conversation_title
from Backend.database import SessionLocal
from Backend.services.openai_compatible_client import get_openai_compatible_client
from Backend.services.sql_gen import _extract_sql_from_text, _sanitize_sql
from sqlalchemy import text


router = APIRouter()
logger = logging.getLogger(__name__)

_BACKEND_DIR = pathlib.Path(__file__).resolve().parents[2]


DEFAULT_MODEL = {
    "openai": "gpt-5-mini",
    "grok": "grok-4-fast",
    "gemini": "gemini-2.5-flash",
    "anthropic": "claude-sonnet-4-5",
}

# Fast model for SQL generation and tool decisions (Gemini 2.5 Flash for all providers)
FAST_MODEL = "gemini-2.5-flash"


def _gemini_client() -> OpenAI:
    """Dedicated Gemini client for fast SQL generation and tool decisions."""
    return OpenAI(
        api_key=os.getenv("GEMINI_API_KEY"),
        base_url="https://generativelanguage.googleapis.com/v1beta/openai",
    )


def _openai_client() -> OpenAI:
    """Dedicated OpenAI client for reliable text generation (e.g., titles)."""
    return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def generate_chat_title_sync(user_message: str) -> str:
    """Generate a short, descriptive title for a chat based on the user's first message (synchronous)."""
    try:
        client = _openai_client()

        logger.info("chat.title.generating: message_preview='%s'", user_message[:100])

        title_prompt_path = _BACKEND_DIR / "resources" / "chat_title_prompt.txt"
        title_prompt = title_prompt_path.read_text(encoding="utf-8")

        response = client.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": f"{title_prompt}{user_message[:100]}"}],
        )

        content = response.choices[0].message.content
        if not content:
            logger.warning("chat.title.empty_response: API returned empty content")
            return "New Chat"

        title = content.strip()
        logger.info("chat.title.raw_response: '%s'", title)

        title = title.strip("\"'.:")
        title = re.sub(r"^(Title:|title:)\s*", "", title, flags=re.IGNORECASE)
        if len(title) > 60:
            title = title[:57] + "..."
        return title if title else "New Chat"
    except Exception as e:
        logger.exception("chat.title.error: Failed to generate chat title: %s", str(e))
        return "New Chat"


async def generate_chat_title(user_message: str) -> str:
    """Generate a short, descriptive title for a chat based on the user's first message."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, generate_chat_title_sync, user_message)


def _localize_rows(rows: list[dict], tz: str) -> list[dict]:
    """Convert known timestamp fields in SQL rows from UTC to the requested timezone and format for readability."""
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


def _json_dumps_safe(obj: object) -> str:
    def _default(o):
        try:
            if hasattr(o, "isoformat"):
                return o.isoformat()
        except Exception:
            pass
        return str(o)

    return json.dumps(obj, default=_default)


async def _fetch_health_context(user_id: str, question: str, tz_name: str):
    """Generate SQL using fast Gemini model with SQL-only prompt, then execute it."""
    sql_system_path = _BACKEND_DIR / "resources" / "sql_prompt.txt"
    sql_system = sql_system_path.read_text(encoding="utf-8")

    sql_prompt = f"Question: {question}\nReturn only SQL."
    logger.info("sql.gen.start: question='%s' model=%s", question, FAST_MODEL)

    fast_client = _gemini_client()
    t0_llm = time.perf_counter()
    sql_resp = fast_client.chat.completions.create(
        model=FAST_MODEL,
        messages=[
            {"role": "system", "content": sql_system},
            {"role": "user", "content": sql_prompt},
        ],
        temperature=0,
    )
    try:
        dt_llm = time.perf_counter() - t0_llm
        logger.info("sql.gen.llm: question='%s' llm_ms=%d", question, int(dt_llm * 1000))
    except Exception:
        pass

    sql_text = sql_resp.choices[0].message.content if sql_resp.choices else ""
    logger.info(
        "sql.gen.raw: question='%s' raw_output='%s'",
        question,
        sql_text[:500] + "..." if len(sql_text) > 500 else sql_text,
    )

    if not isinstance(sql_text, str) or not sql_text.strip():
        logger.warning("sql.gen.empty: question='%s'", question)
        return {"sql": {"sql": None, "rows": [], "error": "no-sql"}}

    try:
        extracted = _extract_sql_from_text(sql_text)
        logger.info(
            "sql.gen.extracted: question='%s' extracted='%s'",
            question,
            extracted[:500] + "..." if len(extracted) > 500 else extracted,
        )

        safe_sql = _sanitize_sql(extracted)
        logger.info(
            "sql.gen.final: question='%s' sql='%s'",
            question,
            safe_sql[:1000] + "..." if len(safe_sql) > 1000 else safe_sql,
        )
    except Exception as e:
        logger.error("sql.gen.error: question='%s' error=%s sql_text='%s'", question, str(e), sql_text[:500])
        return {"sql": {"sql": sql_text, "rows": [], "error": f"invalid-sql: {e}"}}

    loop = asyncio.get_running_loop()

    def execute_sql():
        with SessionLocal() as session:
            try:
                try:
                    lowered = safe_sql.lower()
                    sources = []
                    if "health_rollup_daily" in lowered:
                        sources.append("health_rollup_daily")
                    if "health_rollup_hourly" in lowered:
                        sources.append("health_rollup_hourly")
                    if "health_metrics" in lowered:
                        sources.append("health_metrics")
                    if "health_events" in lowered:
                        sources.append("health_events")
                    logger.info(
                        "sql.exec.start: question='%s' user_id=%s tables=%s",
                        question,
                        user_id,
                        ",".join(sources) if sources else "unknown",
                    )
                except Exception:
                    logger.info("sql.exec.start: question='%s' user_id=%s tables=unknown", question, user_id)

                t0_exec = time.time()
                result = (
                    session.execute(text(safe_sql), {"user_id": user_id, "tz_name": tz_name}).mappings().all()
                )
                rows = [dict(r) for r in result]
                dt_ms = int((time.time() - t0_exec) * 1000)

                if rows:
                    logger.info(
                        "sql.exec.result: question='%s' row_count=%d db_ms=%d sample_first_row=%s",
                        question,
                        len(rows),
                        dt_ms,
                        str(rows[0])[:300] if rows else "none",
                    )
                    if len(rows) > 1:
                        logger.info("sql.exec.result: question='%s' sample_last_row=%s", question, str(rows[-1])[:300])
                else:
                    logger.warning(
                        "sql.exec.empty: question='%s' db_ms=%d sql='%s'",
                        question,
                        dt_ms,
                        safe_sql[:500],
                    )

                return {"sql": safe_sql, "rows": rows}
            except Exception as e:
                logger.error("sql.exec.error: question='%s' error=%s sql='%s'", question, str(e), safe_sql[:500])
                return {"sql": safe_sql, "rows": [], "error": str(e)}

    sql_task = loop.run_in_executor(None, execute_sql)
    sql_out = await sql_task
    row_count = len(sql_out.get("rows", [])) if isinstance(sql_out, dict) else 0
    logger.info("sql.gen.done: question='%s' row_count=%d", question, row_count)
    return {"sql": sql_out}


@router.post("/health/query/stream")
async def query_stream(payload: dict, request: Request):
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    user_tz = request.headers.get("x-user-tz") or "UTC"

    question = payload.get("question")
    conversation_id = payload.get("conversation_id")
    provider = payload.get("provider")
    if not isinstance(provider, str) or not provider.strip():
        raise HTTPException(status_code=400, detail="Missing provider")
    provider = provider.lower()

    answer_model = payload.get("model") or DEFAULT_MODEL.get(provider) or ""
    if not answer_model:
        raise HTTPException(status_code=400, detail=f"No default model for provider: {provider}")

    if conversation_id is not None and isinstance(conversation_id, str) and conversation_id.strip() == "":
        raise HTTPException(status_code=400, detail="conversation_id cannot be empty string")
    if not conversation_id:
        conversation_id = str(uuid.uuid4())

    if not isinstance(question, str) or not question.strip():
        raise HTTPException(status_code=400, detail="Missing question")

    try:
        client = get_openai_compatible_client(provider)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    async def generator():
        prompt_path = _BACKEND_DIR / "resources" / "chat_prompt.txt"
        system = prompt_path.read_text(encoding="utf-8")

        session = None
        history_msgs = []
        try:
            session = SessionLocal()
            try:
                prior = get_chat_history(session, conversation_id, user_id)
                for m in prior:
                    role = "assistant" if m.role == "assistant" else "user"
                    if isinstance(m.content, str) and m.content.strip():
                        history_msgs.append({"role": role, "content": m.content})
            except Exception:
                history_msgs = []
            try:
                create_chat_message(session, conversation_id, user_id, "user", question)
            except Exception:
                pass
        except Exception:
            session = None
            history_msgs = []

        tools = [
            {
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
        ]

        messages = [{"role": "system", "content": system}]
        messages.extend(history_msgs)
        messages.append({"role": "user", "content": question})

        try:
            logger.info("health.query: conv=%s provider=%s answer_model=%s", conversation_id, provider, answer_model)
        except Exception:
            pass

        generated_title = None
        is_new_conversation = len(history_msgs) == 0
        if is_new_conversation and session:
            try:
                conv = get_or_create_conversation(session, conversation_id, user_id)
                if conv and not conv.title:
                    generated_title = await generate_chat_title(question)
                    update_conversation_title(session, conversation_id, user_id, generated_title)
                    logger.info("chat.title_generated: conv=%s title='%s'", conversation_id, generated_title)
            except Exception as title_err:
                logger.warning("chat.title_error: conv=%s err=%s", conversation_id, str(title_err))

        try:
            initial_payload = {"conversation_id": conversation_id, "content": "", "done": False}
            if generated_title:
                initial_payload["title"] = generated_title
            yield f"data: {json.dumps(initial_payload)}\n\n"
        except Exception:
            pass

        try:
            try:
                t0_stream = time.perf_counter()
                logger.info(
                    "stream.start: conv=%s decision_model=%s answer_model=%s",
                    conversation_id,
                    FAST_MODEL,
                    answer_model,
                )
            except Exception:
                t0_stream = time.perf_counter()

            fast_client = _gemini_client()
            stream = fast_client.chat.completions.create(
                model=FAST_MODEL,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                stream=True,
                temperature=0,
            )

            full_response = ""
            streamed_chars = 0
            tool_calls = None
            assistant_content = ""
            finish_reason = None

            sql_task = None
            try:
                if question:
                    sql_task = asyncio.create_task(_fetch_health_context(user_id, question, user_tz))
            except Exception:
                sql_task = None

            for chunk in stream:
                choice = chunk.choices[0]
                delta = getattr(choice, "delta", None)
                if getattr(choice, "finish_reason", None):
                    finish_reason = choice.finish_reason

                if delta is not None:
                    content = getattr(delta, "content", None)
                    if isinstance(content, str) and content:
                        assistant_content += content
                        full_response += content
                        streamed_chars += len(content)
                        yield f"data: {json.dumps({'content': content, 'done': False})}\n\n"

                    if hasattr(delta, "tool_calls") and delta.tool_calls:
                        if tool_calls is None:
                            tool_calls = []
                        for tc_delta in delta.tool_calls:
                            idx = getattr(tc_delta, "index", None)
                            if idx is not None and idx < len(tool_calls):
                                existing = tool_calls[idx]
                                if hasattr(tc_delta, "function") and tc_delta.function:
                                    if not hasattr(existing, "function"):
                                        from openai.types.chat import Function

                                        existing.function = Function(name="", arguments="")
                                    if hasattr(tc_delta.function, "name") and tc_delta.function.name:
                                        existing.function.name = tc_delta.function.name
                                    if hasattr(tc_delta.function, "arguments") and tc_delta.function.arguments:
                                        existing.function.arguments = (existing.function.arguments or "") + tc_delta.function.arguments
                            else:
                                tool_calls.append(tc_delta)

                text_piece = getattr(choice, "text", None)
                if isinstance(text_piece, str) and text_piece:
                    assistant_content += text_piece
                    full_response += text_piece
                    streamed_chars += len(text_piece)
                    yield f"data: {json.dumps({'content': text_piece, 'done': False})}\n\n"

            if finish_reason == "tool_calls" and tool_calls:
                try:
                    tool_call = tool_calls[0]
                    try:
                        arguments_json = getattr(getattr(tool_call, "function", None), "arguments", None) or "{}"
                    except Exception:
                        arguments_json = "{}"
                    try:
                        args = json.loads(arguments_json) if isinstance(arguments_json, str) else {}
                    except Exception:
                        args = {}
                    model_question = args.get("question")
                    if not isinstance(model_question, str) or not model_question.strip():
                        model_question = question

                    t0 = time.perf_counter()
                    try:
                        trunc_q = model_question[:200] + ("...(truncated)" if len(model_question) > 200 else "")
                        logger.info("tool.fetch_context.start: conv=%s question='%s'", conversation_id, trunc_q)
                    except Exception:
                        pass

                    if sql_task and not sql_task.done():
                        ctx = await sql_task
                    elif sql_task:
                        ctx = await sql_task
                    else:
                        ctx = {"sql": {"sql": None, "rows": [], "error": "no-pre-started-sql-task"}}

                    try:
                        if isinstance(ctx, dict) and isinstance(ctx.get("sql"), dict):
                            rows = ctx["sql"].get("rows")
                            if isinstance(rows, list):
                                ctx["sql"]["rows"] = _localize_rows(rows, user_tz)
                    except Exception:
                        pass

                    try:
                        dt = time.perf_counter() - t0
                        num_rows = None
                        try:
                            num_rows = len(ctx.get("sql", {}).get("rows", [])) if isinstance(ctx, dict) else None
                        except Exception:
                            num_rows = None
                        logger.info("tool.fetch_context.done: conv=%s dt=%.3fs rows=%s", conversation_id, dt, num_rows)
                    except Exception:
                        pass

                    messages.append({"role": "assistant", "content": assistant_content, "tool_calls": tool_calls})
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": _json_dumps_safe(ctx),
                        }
                    )

                    try:
                        logger.info("tool.injected: conv=%s tool_call_id=%s", conversation_id, getattr(tool_call, "id", None))
                    except Exception:
                        pass

                    stream2 = client.chat.completions.create(model=answer_model, messages=messages, stream=True)
                    streamed_chars = len(full_response)
                    for chunk in stream2:
                        choice = chunk.choices[0]
                        delta = getattr(choice, "delta", None)
                        if delta is not None:
                            content = getattr(delta, "content", None)
                            if isinstance(content, str) and content:
                                full_response += content
                                streamed_chars += len(content)
                                yield f"data: {json.dumps({'content': content, 'done': False})}\n\n"
                        text_piece = getattr(choice, "text", None)
                        if isinstance(text_piece, str) and text_piece:
                            full_response += text_piece
                            streamed_chars += len(text_piece)
                            yield f"data: {json.dumps({'content': text_piece, 'done': False})}\n\n"
                except Exception as e:
                    messages.append({"role": "assistant", "content": f"Tool error: {str(e)}"})
                    try:
                        logger.exception("tool.error: conv=%s err=%s", conversation_id, str(e))
                    except Exception:
                        pass
                    yield f"data: {json.dumps({'content': f'I encountered an error fetching your data: {str(e)}', 'done': False})}\n\n"
                    full_response = f"I encountered an error fetching your data: {str(e)}"
            else:
                if sql_task and not sql_task.done():
                    sql_task.cancel()
                    try:
                        await sql_task
                    except asyncio.CancelledError:
                        pass

                full_response = assistant_content or ""
                if streamed_chars == 0 and full_response:
                    streamed_chars = len(full_response)
                    yield f"data: {json.dumps({'content': full_response, 'done': False})}\n\n"

            try:
                dt_stream = time.perf_counter() - t0_stream
                logger.info("stream.done: conv=%s chars=%d model_ms=%d", conversation_id, streamed_chars, int(dt_stream * 1000))
            except Exception:
                pass

        except Exception as e:
            yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"
            try:
                logger.exception("stream.error: conv=%s err=%s", conversation_id, str(e))
            except Exception:
                pass
            if session:
                try:
                    session.close()
                except Exception:
                    pass
            return

        try:
            if session and full_response.strip():
                create_chat_message(session, conversation_id, user_id, "assistant", full_response.strip())
                try:
                    logger.info("chat.persisted: conv=%s chars=%d", conversation_id, len(full_response.strip()))
                except Exception:
                    pass
        except Exception:
            pass
        if session:
            try:
                session.close()
            except Exception:
                pass

        yield f"data: {json.dumps({'content': '', 'done': True})}\n\n"

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


