import base64
import json
import asyncio
import os
import pathlib
import logging
import uuid
from typing import Optional
from fastapi import APIRouter, UploadFile, File, HTTPException, Request
from fastapi.responses import StreamingResponse
from openai import OpenAI
from sqlalchemy import func, text
from datetime import timezone
from zoneinfo import ZoneInfo
import time

from Backend.auth import verify_clerk_jwt
from Backend.background_tasks.csv_ingest import process_csv_upload
from Backend.services.sql_gen import _extract_sql_from_text, _sanitize_sql
from Backend.database import SessionLocal
from Backend.crud.chat import get_chat_history, create_chat_message
from Backend.models.chat_data_model import ChatsDB


async def _fetch_health_context(user_id: str, question: str, client: OpenAI, system_prompt: str, model: str):
    """Generate SQL using chat prompt, then execute it."""
    # Ask the model to generate SQL for this question
    sql_prompt = f"Question: {question}\nReturn only SQL."
    logger.info("sql.gen.start: question='%s' model=%s", question, model)

    sql_resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": sql_prompt},
        ]
    )
    sql_text = sql_resp.choices[0].message.content if sql_resp.choices else ""
    logger.info("sql.gen.raw: question='%s' raw_output='%s'", question, sql_text[:500] + "..." if len(sql_text) > 500 else sql_text)

    if not isinstance(sql_text, str) or not sql_text.strip():
        logger.warning("sql.gen.empty: question='%s'", question)
        return {"sql": {"sql": None, "rows": [], "error": "no-sql"}}

    # Extract and sanitize SQL
    try:
        extracted = _extract_sql_from_text(sql_text)
        logger.info("sql.gen.extracted: question='%s' extracted='%s'", question, extracted[:500] + "..." if len(extracted) > 500 else extracted)

        safe_sql = _sanitize_sql(extracted)
        logger.info("sql.gen.final: question='%s' sql='%s'", question, safe_sql[:1000] + "..." if len(safe_sql) > 1000 else safe_sql)
    except Exception as e:
        logger.error("sql.gen.error: question='%s' error=%s sql_text='%s'", question, str(e), sql_text[:500])
        return {"sql": {"sql": sql_text, "rows": [], "error": f"invalid-sql: {e}"}}

    # Execute SQL
    loop = asyncio.get_running_loop()
    def execute_sql():
        with SessionLocal() as session:
            try:
                logger.info("sql.exec.start: question='%s' user_id=%s", question, user_id)
                result = session.execute(text(safe_sql), {"user_id": user_id}).mappings().all()
                rows = [dict(r) for r in result]

                # Log sample of rows for debugging
                if rows:
                    logger.info("sql.exec.result: question='%s' row_count=%d sample_first_row=%s",
                              question, len(rows), str(rows[0])[:300] if rows else "none")
                    if len(rows) > 1:
                        logger.info("sql.exec.result: question='%s' sample_last_row=%s",
                                  question, str(rows[-1])[:300])
                else:
                    logger.warning("sql.exec.empty: question='%s' sql='%s'", question, safe_sql[:500])

                return {"sql": safe_sql, "rows": rows}
            except Exception as e:
                logger.error("sql.exec.error: question='%s' error=%s sql='%s'", question, str(e), safe_sql[:500])
                return {"sql": safe_sql, "rows": [], "error": str(e)}

    sql_task = loop.run_in_executor(None, execute_sql)
    sql_out = await sql_task
    row_count = len(sql_out.get("rows", [])) if isinstance(sql_out, dict) else 0
    logger.info("sql.gen.done: question='%s' row_count=%d", question, row_count)
    return {"sql": sql_out}


router = APIRouter()

logger = logging.getLogger(__name__)
# Ensure module logs are visible when running under gunicorn/uvicorn (which may keep root at WARNING)
if not logger.handlers:
    try:
        log_level_name = os.getenv("VOYANT_LOG_LEVEL", "INFO").upper()
        log_level = getattr(logging, log_level_name, logging.INFO)
    except Exception:
        log_level = logging.INFO
    logger.setLevel(log_level)
    _handler = logging.StreamHandler()
    _handler.setLevel(log_level)
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(_handler)
    # Also ensure other module loggers propagate to a handler (root)
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.setLevel(log_level)
        root_handler = logging.StreamHandler()
        root_handler.setLevel(log_level)
        root_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        root_logger.addHandler(root_handler)

def _openai_compatible_client(provider: str) -> OpenAI:
    if not isinstance(provider, str) or not provider.strip():
        raise HTTPException(status_code = 400, detail = "Missing provider")
    p = provider.lower()
    if p == "openai":
        return OpenAI(api_key = os.getenv("OPENAI_API_KEY"))
    if p == "grok":
        return OpenAI(api_key = os.getenv("GROK_API_KEY"), base_url = "https://api.x.ai/v1")
    if p == "gemini":
        return OpenAI(api_key = os.getenv("GEMINI_API_KEY"), base_url = "https://generativelanguage.googleapis.com/v1beta/openai")
    if p == "anthropic":
        return OpenAI(api_key = os.getenv("ANTHROPIC_API_KEY"), base_url = "https://api.anthropic.com/v1")
    raise HTTPException(status_code = 400, detail = f"Unsupported provider: {p}")

DEFAULT_MODEL = {
    "openai": "gpt-5-mini",
    "grok": "grok-4-fast",
    "gemini": "gemini-2.5-flash",
    "anthropic": "claude-sonnet-4-5",
}

def generate_conversation_id(existing_conversation_id: Optional[str] = None) -> str:
    if existing_conversation_id:
        return existing_conversation_id
    return str(uuid.uuid4())


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
                    # Ensure timezone-aware UTC then convert
                    if getattr(dt, "tzinfo", None) is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    rr[key] = dt.astimezone(zone).strftime("%Y-%m-%d %I:%M %p")
                except Exception:
                    # Leave original on failure
                    pass
        # Convert date-only fields to ISO strings to ensure JSON serializable
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


@router.post("/health/upload-csv")
async def upload_csv(file: UploadFile = File(...), request: Request = None):
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    content = await file.read()
    try:
        logger.info("upload_csv: user_id=%s filename=%s bytes=%s content_type=%s",
                    user_id, getattr(file, 'filename', None), len(content), getattr(file, 'content_type', None))
        b64 = base64.b64encode(content).decode("utf-8")
        task = process_csv_upload.delay(user_id, b64)
        logger.info("upload_csv: enqueued task_id=%s", task.id)
        return {"task_id": task.id}
    except Exception as e:
        logger.exception("upload_csv: failed to enqueue task for user_id=%s", user_id)
        raise HTTPException(status_code=500, detail=f"Failed to enqueue CSV ingest: {e}")


@router.get("/health/task-status/{task_id}")
async def task_status(task_id: str):
    res = process_csv_upload.AsyncResult(task_id)
    return {"id": task_id, "state": res.state, "result": res.result if res.ready() else None}


@router.post("/health/query/stream")
async def query_stream(payload: dict, request: Request):
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    user_tz = request.headers.get("x-user-tz") or "UTC"
    question = payload.get("question")
    conversation_id = payload.get("conversation_id")
    provider = payload.get("provider")
    if not isinstance(provider, str) or not provider.strip():
        raise HTTPException(status_code = 400, detail = "Missing provider")
    provider = provider.lower()
    answer_model = payload.get("model") or DEFAULT_MODEL.get(provider) or ""
    if not answer_model:
        raise HTTPException(status_code = 400, detail = f"No default model for provider: {provider}")
    if conversation_id is not None and isinstance(conversation_id, str) and conversation_id.strip() == "":
        raise HTTPException(status_code = 400, detail = "conversation_id cannot be empty string")
    if not conversation_id:
        conversation_id = str(uuid.uuid4())
    if not isinstance(question, str) or not question.strip():
        raise HTTPException(status_code = 400, detail = "Missing question")

    # No intent shortcuts; model decides whether to call tools

    async def generator():
        client = _openai_compatible_client(provider)
        prompt_path = pathlib.Path(__file__).resolve().parents[1] / "resources" / "chat_prompt.txt"
        system = prompt_path.read_text(encoding = "utf-8")

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

        tools = [{
            "type": "function",
            "function": {
                "name": "fetch_health_context",
                "description": "Fetch SQL rows (exact numbers) for a health question. Call at most once.",
                "parameters": {
                    "type": "object",
                    "properties": {"question": {"type": "string"}},
                    "required": ["question"]
                }
            }
        }]

        messages = [{"role": "system", "content": system}]
        messages.extend(history_msgs)
        messages.append({"role": "user", "content": question})

        try:
            logger.info("health.query: conv=%s provider=%s answer_model=%s", conversation_id, provider, answer_model)
        except Exception:
            pass

        try:
            yield f"data: {json.dumps({'conversation_id': conversation_id, 'content': '', 'done': False})}\n\n"
        except Exception:
            pass

        # Stream with tools - model decides if tool is needed
        try:
            try:
                logger.info("stream.start: conv=%s provider=%s model=%s", conversation_id, provider, answer_model)
            except Exception:
                pass
            stream = client.chat.completions.create(
                model = answer_model,
                messages = messages,
                tools = tools,
                tool_choice = "auto",
                stream = True
            )
            full_response = ""
            streamed_chars = 0
            tool_calls = None
            assistant_content = ""
            finish_reason = None

            # Start SQL generation in parallel (optimistic - we'll use it if tool is called)
            sql_task = None
            if question:  # Only if we have a question to work with
                sql_task = asyncio.create_task(
                    _fetch_health_context(user_id, question, client, system, answer_model)
                )

            # Stream and collect tool calls if any
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

                    # Accumulate tool calls
                    if hasattr(delta, "tool_calls") and delta.tool_calls:
                        if tool_calls is None:
                            tool_calls = []
                        for tc_delta in delta.tool_calls:
                            idx = getattr(tc_delta, "index", None)
                            if idx is not None and idx < len(tool_calls):
                                # Update existing
                                existing = tool_calls[idx]
                                if hasattr(tc_delta, "function") and tc_delta.function:
                                    if not hasattr(existing, "function"):
                                        from openai.types.chat import ChatCompletionMessageToolCall, Function
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

            # If tool was called, use pre-generated SQL (or generate if not ready)
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

                    # Use pre-started SQL task (testing - no fallback)
                    if sql_task and not sql_task.done():
                        # SQL generation already in progress, wait for it
                        ctx = await sql_task
                    elif sql_task:
                        # Task already completed, use it
                        ctx = await sql_task
                    else:
                        # No SQL task - return error (testing only)
                        ctx = {"sql": {"sql": None, "rows": [], "error": "no-pre-started-sql-task"}}
                    # Localize SQL rows to the user's timezone for clearer model grounding
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
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": _json_dumps_safe(ctx)
                    })
                    try:
                        logger.info("tool.injected: conv=%s tool_call_id=%s", conversation_id, getattr(tool_call, "id", None))
                    except Exception:
                        pass

                    # Stream final answer with tool results
                    stream2 = client.chat.completions.create(
                        model = answer_model,
                        messages = messages,
                        stream = True
                    )
                    # Continue streaming (don't reset full_response - keep the intro text)
                    # full_response already has intro text, now add final answer
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
                    messages.append({
                        "role": "assistant",
                        "content": f"Tool error: {str(e)}"
                    })
                    try:
                        logger.exception("tool.error: conv=%s err=%s", conversation_id, str(e))
                    except Exception:
                        pass
                    # Stream error message
                    yield f"data: {json.dumps({'content': f'I encountered an error fetching your data: {str(e)}', 'done': False})}\n\n"
                    full_response = f"I encountered an error fetching your data: {str(e)}"
            else:
                # No tool call - cancel the pre-started SQL task if it's still running
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
                logger.info("stream.done: conv=%s chars=%d", conversation_id, streamed_chars)
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

        # Persist assistant response if captured
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

    return StreamingResponse(generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})


@router.get("/chat/retrieve-chat-sessions/")
def get_chat_sessions(request: Request):
    user = verify_clerk_jwt(request)
    user_id = user['sub']
    db_session = SessionLocal()
    try:
        subquery = db_session.query(
            ChatsDB.conversation_id,
            func.max(ChatsDB.timestamp).label('last_message_at')
        ).filter(ChatsDB.user_id == user_id).group_by(ChatsDB.conversation_id).subquery()

        latest_messages = db_session.query(ChatsDB).join(
            subquery,
            (ChatsDB.conversation_id == subquery.c.conversation_id) &
            (ChatsDB.timestamp == subquery.c.last_message_at)
        ).filter(ChatsDB.user_id == user_id).all()

        sessions_data = [
            {
                "conversation_id": msg.conversation_id,
                "last_active_date": msg.timestamp.isoformat() if msg.timestamp else None
            }
            for msg in latest_messages
        ]
        return {"sessions": sessions_data}
    finally:
        db_session.close()


@router.get("/chat/all-messages/{conversation_id}")
def get_all_chat_messages(conversation_id: str, request: Request):
    user = verify_clerk_jwt(request)
    user_id = user['sub']
    db_session = SessionLocal()
    try:
        messages = get_chat_history(db_session, conversation_id, user_id)
        return [
            {
                "id": m.id,
                "role": m.role,
                "content": m.content,
                "timestamp": m.timestamp.isoformat() if m.timestamp else None
            }
            for m in messages
        ]
    finally:
        db_session.close()


@router.post("/chat/add-message/")
def add_chat_message(conversation_id: Optional[str] = None, role: str = '', content: str = '', request: Request = None):
    if conversation_id is not None and conversation_id.strip() == '':
        raise HTTPException(status_code = 400, detail = "conversation_id cannot be empty string")

    original_conversation_id = conversation_id
    conversation_id = generate_conversation_id(conversation_id)
    is_new_conversation = original_conversation_id is None

    user = verify_clerk_jwt(request)
    user_id = user['sub']
    db_session = SessionLocal()
    try:
        msg = create_chat_message(db_session, conversation_id, user_id, role, content)
        response = {
            "id": msg.id,
            "conversation_id": conversation_id,
            "user_id": msg.user_id,
            "role": msg.role,
            "content": msg.content,
            "timestamp": msg.timestamp.isoformat() if msg.timestamp else None
        }
        if is_new_conversation:
            response["new_conversation"] = True
        return response
    finally:
        db_session.close()

