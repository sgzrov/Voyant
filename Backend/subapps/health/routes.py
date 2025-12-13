import uuid
import base64
import hashlib
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Optional

from fastapi import APIRouter, File, Header, HTTPException, Request, UploadFile

from Backend.auth import verify_clerk_jwt
from Backend.background_tasks.csv_ingest import process_csv_upload
from Backend.database import SessionLocal
from Backend.models.health_upload_tracking_model import HealthUploadTracking
from Backend.services.chat_stream import DEFAULT_MODEL, build_agent_stream_response
from Backend.services.tools.health_sql_tool import TOOL_SPEC, execute_health_sql_tool, localize_health_rows


router = APIRouter()
logger = logging.getLogger(__name__)


# Simple in-memory rate limiter for upload endpoints
class UploadRateLimiter:
    def __init__(self, max_requests_per_minute: int = 5):
        self.max_requests = max_requests_per_minute
        self.requests: Dict[str, list] = defaultdict(list)

    async def check_rate_limit(self, user_id: str) -> bool:
        """Check if user has exceeded rate limit. Returns True if allowed."""
        now = datetime.utcnow()
        minute_ago = now - timedelta(minutes=1)

        # Clean old requests
        self.requests[user_id] = [req_time for req_time in self.requests[user_id] if req_time > minute_ago]

        if len(self.requests[user_id]) >= self.max_requests:
            return False

        self.requests[user_id].append(now)
        return True

    async def get_wait_time(self, user_id: str) -> int:
        """Get seconds until next request is allowed."""
        if not self.requests[user_id]:
            return 0
        oldest_request = min(self.requests[user_id])
        wait_until = oldest_request + timedelta(minutes=1)
        return max(0, int((wait_until - datetime.utcnow()).total_seconds()))


upload_rate_limiter = UploadRateLimiter(max_requests_per_minute=10)


@router.post("/health/query/stream")
async def health_query_stream(payload: dict, request: Request):
    """
    SSE chat streaming endpoint with an optional health-SQL tool call.
    """
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
        async def _prefetch():
            ctx = await execute_health_sql_tool(user_id=user_id, question=question, tz_name=user_tz)
            try:
                if isinstance(ctx, dict) and isinstance(ctx.get("sql"), dict):
                    rows = ctx["sql"].get("rows")
                    if isinstance(rows, list):
                        ctx["sql"]["rows"] = localize_health_rows(rows, user_tz)
            except Exception:
                pass
            return ctx

        async def _health_tool_handler(args: dict):
            q = args.get("question")
            if not isinstance(q, str) or not q.strip():
                q = question
            ctx = await execute_health_sql_tool(user_id=user_id, question=q, tz_name=user_tz)
            try:
                if isinstance(ctx, dict) and isinstance(ctx.get("sql"), dict):
                    rows = ctx["sql"].get("rows")
                    if isinstance(rows, list):
                        ctx["sql"]["rows"] = localize_health_rows(rows, user_tz)
            except Exception:
                pass
            return ctx

        return build_agent_stream_response(
            user_id=user_id,
            conversation_id=conversation_id,
            question=question,
            provider=provider,
            answer_model=answer_model,
            tools=[TOOL_SPEC],
            tool_handlers={"fetch_health_context": _health_tool_handler},
            tool_prefetch=_prefetch,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/health/upload-csv")
async def upload_csv(
    file: UploadFile = File(...),
    request: Request = None,
    x_idempotency_key: Optional[str] = Header(None),
):
    """Upload CSV with deduplication and idempotency support."""
    user = verify_clerk_jwt(request)
    user_id = user["sub"]

    if not await upload_rate_limiter.check_rate_limit(user_id):
        wait_time = await upload_rate_limiter.get_wait_time(user_id)
        logger.warning("upload_csv: rate limit exceeded for user_id=%s, wait=%ds", user_id, wait_time)
        raise HTTPException(
            status_code=429,
            detail=f"Too many upload requests. Please wait {wait_time} seconds before trying again.",
        )

    content = await file.read()

    content_hash = hashlib.sha256(content).hexdigest()
    file_size = len(content)
    file_name = getattr(file, "filename", "health.csv")

    logger.info(
        "upload_csv: user_id=%s filename=%s bytes=%d content_hash=%s idempotency_key=%s",
        user_id,
        file_name,
        file_size,
        content_hash[:8],
        x_idempotency_key,
    )

    with SessionLocal() as session:
        try:
            existing = (
                session.query(HealthUploadTracking)
                .filter(HealthUploadTracking.user_id == user_id, HealthUploadTracking.id == content_hash)
                .first()
            )

            if existing:
                now = datetime.utcnow()
                existing.request_count = (existing.request_count or 0) + 1
                existing.updated_at = now

                if existing.status in ["pending", "processing"]:
                    time_since_creation = (now - existing.created_at).total_seconds()
                    if time_since_creation < 300:
                        logger.info(
                            "upload_csv: duplicate detected, still processing task_id=%s age=%ds",
                            existing.task_id,
                            time_since_creation,
                        )
                        session.commit()
                        return {"task_id": existing.task_id, "status": "processing", "message": "Upload already in progress"}

                    logger.warning(
                        "upload_csv: task_id=%s timed out after %ds, reprocessing",
                        existing.task_id,
                        time_since_creation,
                    )
                    existing.status = "timeout"
                    existing.error_message = f"Timed out after {time_since_creation}s"

                elif existing.status == "completed":
                    logger.info("upload_csv: duplicate detected, already completed task_id=%s", existing.task_id)
                    session.commit()
                    return {"task_id": existing.task_id, "status": "completed", "message": "Data already uploaded and processed"}

                logger.info(
                    "upload_csv: reprocessing failed/timeout upload, previous task_id=%s status=%s",
                    existing.task_id,
                    existing.status,
                )

                b64 = base64.b64encode(content).decode("utf-8")
                task = process_csv_upload.delay(user_id, b64)

                existing.task_id = task.id
                existing.status = "pending"
                existing.updated_at = now
                existing.error_message = None
                existing.idempotency_key = x_idempotency_key

                session.commit()
                logger.info("upload_csv: reprocessing with new task_id=%s", task.id)
                return {"task_id": task.id, "status": "reprocessing"}

            # New upload
            b64 = base64.b64encode(content).decode("utf-8")
            task = process_csv_upload.delay(user_id, b64)

            tracking = HealthUploadTracking(
                id=content_hash,
                user_id=user_id,
                task_id=task.id,
                file_size=file_size,
                file_name=file_name,
                status="pending",
                idempotency_key=x_idempotency_key,
                request_count=1,
            )
            session.add(tracking)
            try:
                session.commit()
                logger.info("upload_csv: new upload enqueued task_id=%s content_hash=%s", task.id, content_hash[:8])
                return {"task_id": task.id, "status": "new"}
            except Exception as commit_error:
                session.rollback()
                if "duplicate key" in str(commit_error).lower():
                    existing = (
                        session.query(HealthUploadTracking)
                        .filter(HealthUploadTracking.user_id == user_id, HealthUploadTracking.id == content_hash)
                        .first()
                    )
                    if existing:
                        existing.request_count = (existing.request_count or 0) + 1
                        session.commit()
                        logger.info("upload_csv: race condition handled, using existing task_id=%s", existing.task_id)
                        return {
                            "task_id": existing.task_id,
                            "status": "processing",
                            "message": "Upload already in progress (race condition handled)",
                        }
                raise commit_error

        except Exception as e:
            session.rollback()
            logger.exception("upload_csv: database error for user_id=%s", user_id)
            try:
                b64 = base64.b64encode(content).decode("utf-8")
                task = process_csv_upload.delay(user_id, b64)
                logger.warning("upload_csv: fallback mode, enqueued task_id=%s", task.id)
                return {"task_id": task.id, "status": "fallback"}
            except Exception as task_error:
                logger.exception("upload_csv: failed to enqueue task for user_id=%s", user_id)
                raise HTTPException(status_code=500, detail=f"Failed to enqueue CSV ingest: {task_error}") from e


@router.get("/health/task-status/{task_id}")
async def task_status(task_id: str, request: Request = None):
    """Get task status with upload tracking integration."""
    user = verify_clerk_jwt(request)
    user_id = user["sub"]

    res = process_csv_upload.AsyncResult(task_id)
    celery_state = res.state

    with SessionLocal() as session:
        tracking = (
            session.query(HealthUploadTracking)
            .filter(HealthUploadTracking.task_id == task_id, HealthUploadTracking.user_id == user_id)
            .first()
        )

        if tracking:
            if celery_state == "SUCCESS" and tracking.status != "completed":
                tracking.status = "completed"
                tracking.completed_at = datetime.utcnow()
                tracking.updated_at = datetime.utcnow()
                if res.result and isinstance(res.result, dict):
                    # Back-compat: older code expects "row_count"
                    tracking.row_count = res.result.get("row_count") or res.result.get("inserted")
                session.commit()
                logger.info("task_status: marked upload as completed task_id=%s", task_id)

            elif celery_state == "FAILURE" and tracking.status not in ["failed", "completed"]:
                tracking.status = "failed"
                tracking.error_message = str(res.info) if res.info else "Unknown error"
                tracking.updated_at = datetime.utcnow()
                session.commit()
                logger.warning("task_status: marked upload as failed task_id=%s error=%s", task_id, tracking.error_message)

            elif celery_state == "PENDING" and tracking.status == "pending":
                time_since_creation = (datetime.utcnow() - tracking.created_at).total_seconds()
                if time_since_creation > 300:
                    tracking.status = "timeout"
                    tracking.error_message = f"Task timed out after {time_since_creation}s"
                    tracking.updated_at = datetime.utcnow()
                    session.commit()
                    logger.warning("task_status: task timeout detected task_id=%s age=%ds", task_id, time_since_creation)
                    return {"id": task_id, "state": "TIMEOUT", "result": None, "message": "Task timed out"}

            elif celery_state in ["STARTED", "RETRY"] and tracking.status == "pending":
                tracking.status = "processing"
                tracking.updated_at = datetime.utcnow()
                session.commit()

    return {"id": task_id, "state": celery_state, "result": res.result if res.ready() else None}


@router.get("/health/upload-history")
async def get_upload_history(request: Request, limit: int = 10):
    """Get recent upload history for the user."""
    user = verify_clerk_jwt(request)
    user_id = user["sub"]

    with SessionLocal() as session:
        uploads = (
            session.query(HealthUploadTracking)
            .filter(HealthUploadTracking.user_id == user_id)
            .order_by(HealthUploadTracking.created_at.desc())
            .limit(limit)
            .all()
        )

        history = []
        for upload in uploads:
            history.append(
                {
                    "content_hash": upload.id[:8],
                    "task_id": upload.task_id,
                    "file_name": upload.file_name,
                    "file_size": upload.file_size,
                    "status": upload.status,
                    "created_at": upload.created_at.isoformat() if upload.created_at else None,
                    "completed_at": upload.completed_at.isoformat() if upload.completed_at else None,
                    "row_count": upload.row_count,
                    "request_count": upload.request_count,
                    "error_message": upload.error_message,
                }
            )

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
        deleted_count = (
            session.query(HealthUploadTracking)
            .filter(
                HealthUploadTracking.user_id == user_id,
                HealthUploadTracking.created_at < cutoff_date,
                HealthUploadTracking.status.in_(["completed", "failed", "timeout"]),
            )
            .delete()
        )
        session.commit()

        logger.info(
            "cleanup_uploads: user_id=%s deleted=%d older_than=%s",
            user_id,
            deleted_count,
            cutoff_date.isoformat(),
        )

        return {"deleted": deleted_count, "cutoff_date": cutoff_date.isoformat()}


