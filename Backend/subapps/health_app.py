import base64
import json
import asyncio
import os
import pathlib
import logging
import uuid
import hashlib
from typing import Optional, Dict
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from fastapi import APIRouter, UploadFile, File, HTTPException, Request, Header
from fastapi.responses import StreamingResponse
from openai import OpenAI
from sqlalchemy import func, text, Column, String, Integer, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from zoneinfo import ZoneInfo
import time
import re

from Backend.auth import verify_clerk_jwt
from Backend.background_tasks.csv_ingest import process_csv_upload
from Backend.services.sql_gen import _extract_sql_from_text, _sanitize_sql
from Backend.database import SessionLocal, Base
from Backend.crud.chat import get_chat_history, create_chat_message
from Backend.models.chat_data_model import ChatsDB

# Upload tracking model
class HealthUploadTracking(Base):
    __tablename__ = 'health_upload_tracking'
    
    id = Column(String(64), primary_key=True)  # SHA256 hash of content
    user_id = Column(String(100), nullable=False)
    task_id = Column(String(100), nullable=True)
    file_size = Column(Integer, nullable=False)
    file_name = Column(String(255), nullable=True)
    status = Column(String(50), nullable=False, default='pending')
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    row_count = Column(Integer, nullable=True)
    idempotency_key = Column(String(64), nullable=True)
    request_count = Column(Integer, nullable=False, default=1)


async def _fetch_health_context(user_id: str, question: str, client: OpenAI, system_prompt: str, model: str, tz_name: str):
    """Generate SQL using chat prompt, then execute it."""
    # Ask the model to generate SQL for this question
    sql_prompt = f"Question: {question}\nReturn only SQL."
    logger.info("sql.gen.start: question='%s' model=%s", question, model)

    t0_llm = time.perf_counter()
    sql_resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": sql_prompt},
        ]
    )
    try:
        dt_llm = time.perf_counter() - t0_llm
        logger.info("sql.gen.llm: question='%s' llm_ms=%d", question, int(dt_llm * 1000))
    except Exception:
        pass
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
                # Debug which tables are referenced by the query
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
                    logger.info("sql.exec.start: question='%s' user_id=%s tables=%s", question, user_id, ",".join(sources) if sources else "unknown")
                except Exception:
                    logger.info("sql.exec.start: question='%s' user_id=%s tables=unknown", question, user_id)
                t0_exec = time.time()
                # Bind only user_id and tz_name; date windows are rewritten inside SQL
                result = session.execute(text(safe_sql), {"user_id": user_id, "tz_name": tz_name}).mappings().all()
                rows = [dict(r) for r in result]
                dt_ms = int((time.time() - t0_exec) * 1000)

                # Log sample of rows for debugging
                if rows:
                    logger.info("sql.exec.result: question='%s' row_count=%d db_ms=%d sample_first_row=%s",
                              question, len(rows), dt_ms, str(rows[0])[:300] if rows else "none")
                    if len(rows) > 1:
                        logger.info("sql.exec.result: question='%s' sample_last_row=%s",
                                  question, str(rows[-1])[:300])
                else:
                    logger.warning("sql.exec.empty: question='%s' db_ms=%d sql='%s'", question, dt_ms, safe_sql[:500])

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

# Simple in-memory rate limiter for upload endpoints
class UploadRateLimiter:
    def __init__(self, max_requests_per_minute: int = 5):
        self.max_requests = max_requests_per_minute
        self.requests: Dict[str, list] = defaultdict(list)
        self.lock = asyncio.Lock()
    
    async def check_rate_limit(self, user_id: str) -> bool:
        """Check if user has exceeded rate limit. Returns True if allowed."""
        async with self.lock:
            now = datetime.utcnow()
            minute_ago = now - timedelta(minutes=1)
            
            # Clean old requests
            self.requests[user_id] = [
                req_time for req_time in self.requests[user_id]
                if req_time > minute_ago
            ]
            
            # Check limit
            if len(self.requests[user_id]) >= self.max_requests:
                return False
            
            # Record new request
            self.requests[user_id].append(now)
            return True
    
    async def get_wait_time(self, user_id: str) -> int:
        """Get seconds until next request is allowed."""
        async with self.lock:
            if not self.requests[user_id]:
                return 0
            
            oldest_request = min(self.requests[user_id])
            wait_until = oldest_request + timedelta(minutes=1)
            wait_seconds = max(0, int((wait_until - datetime.utcnow()).total_seconds()))
            return wait_seconds

upload_rate_limiter = UploadRateLimiter(max_requests_per_minute=10)

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
async def upload_csv(
    file: UploadFile = File(...), 
    request: Request = None,
    x_idempotency_key: Optional[str] = Header(None)
):
    """Upload CSV with deduplication and idempotency support.
    
    Uses content hash to detect duplicate uploads and prevent reprocessing.
    Supports optional idempotency key for client-side duplicate prevention.
    Includes rate limiting to prevent abuse.
    """
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    
    # Check rate limit
    if not await upload_rate_limiter.check_rate_limit(user_id):
        wait_time = await upload_rate_limiter.get_wait_time(user_id)
        logger.warning("upload_csv: rate limit exceeded for user_id=%s, wait=%ds", user_id, wait_time)
        raise HTTPException(
            status_code=429,
            detail=f"Too many upload requests. Please wait {wait_time} seconds before trying again."
        )
    
    content = await file.read()
    
    # Generate content hash for deduplication
    content_hash = hashlib.sha256(content).hexdigest()
    file_size = len(content)
    file_name = getattr(file, 'filename', 'health.csv')
    
    logger.info("upload_csv: user_id=%s filename=%s bytes=%d content_hash=%s idempotency_key=%s",
                user_id, file_name, file_size, content_hash[:8], x_idempotency_key)
    
    # Use database session for atomic operations
    with SessionLocal() as session:
        try:
            # Check for existing upload with same content hash
            existing = session.query(HealthUploadTracking).filter(
                HealthUploadTracking.user_id == user_id,
                HealthUploadTracking.id == content_hash
            ).first()
            
            if existing:
                # Content already uploaded
                now = datetime.utcnow()
                
                # Update request count
                existing.request_count = (existing.request_count or 0) + 1
                existing.updated_at = now
                
                # Check if still processing (less than 5 minutes old and not failed)
                if existing.status in ['pending', 'processing']:
                    time_since_creation = (now - existing.created_at).total_seconds()
                    if time_since_creation < 300:  # 5 minutes timeout
                        logger.info("upload_csv: duplicate detected, still processing task_id=%s age=%ds",
                                   existing.task_id, time_since_creation)
                        session.commit()
                        return {
                            "task_id": existing.task_id,
                            "status": "processing",
                            "message": "Upload already in progress"
                        }
                    else:
                        # Timeout - mark as failed and reprocess
                        logger.warning("upload_csv: task_id=%s timed out after %ds, reprocessing",
                                      existing.task_id, time_since_creation)
                        existing.status = 'timeout'
                        existing.error_message = f"Timed out after {time_since_creation}s"
                
                elif existing.status == 'completed':
                    # Already successfully processed
                    logger.info("upload_csv: duplicate detected, already completed task_id=%s",
                               existing.task_id)
                    session.commit()
                    return {
                        "task_id": existing.task_id,
                        "status": "completed",
                        "message": "Data already uploaded and processed"
                    }
                
                # Failed or timed out - allow reprocessing
                logger.info("upload_csv: reprocessing failed/timeout upload, previous task_id=%s status=%s",
                           existing.task_id, existing.status)
                
                # Enqueue new task
                b64 = base64.b64encode(content).decode("utf-8")
                task = process_csv_upload.delay(user_id, b64)
                
                # Update existing record with new task
                existing.task_id = task.id
                existing.status = 'pending'
                existing.updated_at = now
                existing.error_message = None
                existing.idempotency_key = x_idempotency_key
                
                session.commit()
                logger.info("upload_csv: reprocessing with new task_id=%s", task.id)
                return {"task_id": task.id, "status": "reprocessing"}
                
            else:
                # New upload - create tracking record
                b64 = base64.b64encode(content).decode("utf-8")
                task = process_csv_upload.delay(user_id, b64)
                
                tracking = HealthUploadTracking(
                    id=content_hash,
                    user_id=user_id,
                    task_id=task.id,
                    file_size=file_size,
                    file_name=file_name,
                    status='pending',
                    idempotency_key=x_idempotency_key,
                    request_count=1
                )
                session.add(tracking)
                session.commit()
                
                logger.info("upload_csv: new upload enqueued task_id=%s content_hash=%s",
                           task.id, content_hash[:8])
                return {"task_id": task.id, "status": "new"}
                
        except Exception as e:
            session.rollback()
            logger.exception("upload_csv: database error for user_id=%s", user_id)
            # Fall back to direct upload without tracking on DB error
            try:
                b64 = base64.b64encode(content).decode("utf-8")
                task = process_csv_upload.delay(user_id, b64)
                logger.warning("upload_csv: fallback mode, enqueued task_id=%s", task.id)
                return {"task_id": task.id, "status": "fallback"}
            except Exception as task_error:
                logger.exception("upload_csv: failed to enqueue task for user_id=%s", user_id)
                raise HTTPException(status_code=500, detail=f"Failed to enqueue CSV ingest: {task_error}")


@router.get("/health/task-status/{task_id}")
async def task_status(task_id: str, request: Request = None):
    """Get task status with upload tracking integration."""
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    
    # Get Celery task status
    res = process_csv_upload.AsyncResult(task_id)
    celery_state = res.state
    
    # Update tracking record if task completed
    with SessionLocal() as session:
        tracking = session.query(HealthUploadTracking).filter(
            HealthUploadTracking.task_id == task_id,
            HealthUploadTracking.user_id == user_id
        ).first()
        
        if tracking:
            if celery_state == 'SUCCESS' and tracking.status != 'completed':
                tracking.status = 'completed'
                tracking.completed_at = datetime.utcnow()
                tracking.updated_at = datetime.utcnow()
                if res.result and isinstance(res.result, dict):
                    tracking.row_count = res.result.get('row_count')
                session.commit()
                logger.info("task_status: marked upload as completed task_id=%s", task_id)
                
            elif celery_state == 'FAILURE' and tracking.status not in ['failed', 'completed']:
                tracking.status = 'failed'
                tracking.error_message = str(res.info) if res.info else 'Unknown error'
                tracking.updated_at = datetime.utcnow()
                session.commit()
                logger.warning("task_status: marked upload as failed task_id=%s error=%s",
                             task_id, tracking.error_message)
            
            elif celery_state == 'PENDING' and tracking.status == 'pending':
                # Check for timeout
                time_since_creation = (datetime.utcnow() - tracking.created_at).total_seconds()
                if time_since_creation > 300:  # 5 minutes
                    tracking.status = 'timeout'
                    tracking.error_message = f"Task timed out after {time_since_creation}s"
                    tracking.updated_at = datetime.utcnow()
                    session.commit()
                    logger.warning("task_status: task timeout detected task_id=%s age=%ds",
                                 task_id, time_since_creation)
                    return {
                        "id": task_id,
                        "state": "TIMEOUT",
                        "result": None,
                        "message": "Task timed out"
                    }
            
            elif celery_state in ['STARTED', 'RETRY'] and tracking.status == 'pending':
                tracking.status = 'processing'
                tracking.updated_at = datetime.utcnow()
                session.commit()
    
    return {
        "id": task_id,
        "state": celery_state,
        "result": res.result if res.ready() else None
    }


@router.get("/health/upload-history")
async def get_upload_history(request: Request, limit: int = 10):
    """Get recent upload history for the user."""
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    
    with SessionLocal() as session:
        uploads = session.query(HealthUploadTracking).filter(
            HealthUploadTracking.user_id == user_id
        ).order_by(
            HealthUploadTracking.created_at.desc()
        ).limit(limit).all()
        
        history = []
        for upload in uploads:
            history.append({
                "content_hash": upload.id[:8],  # First 8 chars of hash
                "task_id": upload.task_id,
                "file_name": upload.file_name,
                "file_size": upload.file_size,
                "status": upload.status,
                "created_at": upload.created_at.isoformat() if upload.created_at else None,
                "completed_at": upload.completed_at.isoformat() if upload.completed_at else None,
                "row_count": upload.row_count,
                "request_count": upload.request_count,
                "error_message": upload.error_message
            })
        
        return {"uploads": history}


@router.delete("/health/cleanup-uploads")
async def cleanup_old_uploads(request: Request, days_old: int = 7):
    """Clean up old upload tracking records."""
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    
    if days_old < 1:
        raise HTTPException(status_code=400, detail="days_old must be at least 1")
    
    cutoff_date = datetime.utcnow() - timedelta(days=days_old)
    
    with SessionLocal() as session:
        # Delete old completed or failed uploads
        deleted_count = session.query(HealthUploadTracking).filter(
            HealthUploadTracking.user_id == user_id,
            HealthUploadTracking.created_at < cutoff_date,
            HealthUploadTracking.status.in_(['completed', 'failed', 'timeout'])
        ).delete()
        
        session.commit()
        
        logger.info("cleanup_uploads: user_id=%s deleted=%d older_than=%s",
                   user_id, deleted_count, cutoff_date.isoformat())
        
        return {
            "deleted": deleted_count,
            "cutoff_date": cutoff_date.isoformat()
        }


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
                t0_stream = time.perf_counter()
                logger.info("stream.start: conv=%s provider=%s model=%s", conversation_id, provider, answer_model)
            except Exception:
                t0_stream = time.perf_counter()
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
                    _fetch_health_context(user_id, question, client, system, answer_model, user_tz)
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

