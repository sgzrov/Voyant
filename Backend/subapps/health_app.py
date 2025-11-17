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
from sqlalchemy import func
from datetime import timezone
from zoneinfo import ZoneInfo
import time

from Backend.auth import verify_clerk_jwt
from Backend.background_tasks.csv_ingest import process_csv_upload
from Backend.services.ai.vector import vector_search
from Backend.services.ai.sql_gen import execute_generated_sql
from Backend.database import SessionLocal
from Backend.crud.chat import get_chat_history, create_chat_message
from Backend.models.chat_data_model import ChatsDB


async def _fetch_health_context(user_id: str, question: str):
    loop = asyncio.get_running_loop()
    vec_task = loop.run_in_executor(None, vector_search, user_id, question)
    sql_task = loop.run_in_executor(None, execute_generated_sql, user_id, question)
    vec_res, sql_res = await asyncio.gather(vec_task, sql_task)
    return {"sql": sql_res, "vector": vec_res}


router = APIRouter()

logger = logging.getLogger(__name__)

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
        out.append(rr)
    return out


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
    # Use same model for decision unless explicitly overridden
    decision_model = payload.get("decision_model") or answer_model
    if conversation_id is not None and isinstance(conversation_id, str) and conversation_id.strip() == "":
        raise HTTPException(status_code = 400, detail = "conversation_id cannot be empty string")
    if not conversation_id:
        conversation_id = str(uuid.uuid4())
    if not isinstance(question, str) or not question.strip():
        raise HTTPException(status_code = 400, detail = "Missing question")

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
                "description": "Fetch both SQL rows (exact numbers) and vector summaries (context) for a health question. Call at most once.",
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
            logger.info("health.query: conv=%s provider=%s answer_model=%s decision_model=%s", conversation_id, provider, answer_model, decision_model)
        except Exception:
            pass

        try:
            yield f"data: {json.dumps({'conversation_id': conversation_id, 'content': '', 'done': False})}\n\n"
        except Exception:
            pass

        try:
            logger.info("tool.decision.start: model=%s", decision_model)
        except Exception:
            pass
        decide = client.chat.completions.create(
            model = decision_model,
            messages = messages,
            tools = tools,
            tool_choice = "auto"
        )
        choice = decide.choices[0]
        tool_calls = getattr(choice.message, "tool_calls", None)
        try:
            if tool_calls:
                summaries = []
                try:
                    for tc in tool_calls:
                        fname = getattr(getattr(tc, "function", None), "name", None)
                        fargs = getattr(getattr(tc, "function", None), "arguments", None)
                        if isinstance(fargs, str) and len(fargs) > 200:
                            fargs = fargs[:200] + "...(truncated)"
                        summaries.append({"name": fname, "arguments": fargs})
                except Exception:
                    summaries = ["unavailable"]
                logger.info("tool.decision.result: tool_calls=%s", summaries)
            else:
                logger.info("tool.decision.result: no_tool_calls")
        except Exception:
            pass

        if tool_calls:
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
                ctx = await _fetch_health_context(user_id, model_question)
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
                    num_ctx = None
                    try:
                        num_rows = len(ctx.get("sql", {}).get("rows", [])) if isinstance(ctx, dict) else None
                    except Exception:
                        num_rows = None
                    try:
                        num_ctx = len(ctx.get("vector", {}).get("semantic_contexts", [])) if isinstance(ctx, dict) else None
                    except Exception:
                        num_ctx = None
                    logger.info("tool.fetch_context.done: conv=%s dt=%.3fs rows=%s contexts=%s", conversation_id, dt, num_rows, num_ctx)
                except Exception:
                    pass
                messages.append({"role": "assistant", "tool_calls": tool_calls})
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(ctx)
                })
                try:
                    logger.info("tool.injected: conv=%s tool_call_id=%s", conversation_id, getattr(tool_call, "id", None))
                except Exception:
                    pass
            except Exception as e:
                messages.append({
                    "role": "assistant",
                    "content": f"Tool error: {str(e)}"
                })
                try:
                    logger.exception("tool.error: conv=%s err=%s", conversation_id, str(e))
                except Exception:
                    pass

        try:
            try:
                logger.info("stream.start: conv=%s provider=%s model=%s", conversation_id, provider, answer_model)
            except Exception:
                pass
            stream = client.chat.completions.create(
                model = answer_model,
                messages = messages,
                stream = True
            )
            full_response = ""
            streamed_chars = 0
            for chunk in stream:
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

