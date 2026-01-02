from __future__ import annotations

import base64
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from Backend.crud import health_upload_tracking as tracking_crud
from Backend.models.health_upload_tracking_model import HealthUploadTracking
from Backend.background_tasks.csv_ingest import process_csv_upload
from Backend.rate_limiters.upload_rate_limiter import get_upload_rate_limiter


logger = logging.getLogger(__name__)

# Default service limits (kept in code to avoid env-based complexity).
DEFAULT_PROCESSING_TIMEOUT_SECONDS = 300
DEFAULT_MAX_UPLOAD_BYTES = 200 * 1024 * 1024  # 200MB (single-shot 60d mirror seed can be large)


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass(frozen=True)
class UploadCsvResult:
    task_id: str
    status: str
    message: Optional[str] = None


# Validates, rate-limits, tracks idempotency by content hash, and enqueues CSV ingest tasks
class HealthUploadService:
    def __init__(
        self,
        db: Session,
        *,
        processing_timeout_seconds: int = DEFAULT_PROCESSING_TIMEOUT_SECONDS,
        max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES,
    ):
        self.db = db
        self.limiter = get_upload_rate_limiter()
        self.processing_timeout_seconds = int(processing_timeout_seconds)
        self.max_upload_bytes = int(max_upload_bytes)

    def _enqueue_ingest_task(self, *, user_id: str, csv_b64: str) -> str:
        task = process_csv_upload.delay(user_id, csv_b64)
        return task.id

    def _get_ingest_task_state(self, task_id: str) -> tuple[str, Optional[Any]]:
        res = process_csv_upload.AsyncResult(task_id)
        state = res.state
        result = res.result if res.ready() else None
        return state, result

    # Applies per-user rate limiting; raises 429 when the user must wait before uploading again
    def enforce_rate_limit(self, user_id: str) -> None:
        decision = self.limiter.check(user_id)
        if decision.allowed:
            return
        raise HTTPException(
            status_code=429,
            detail=f"Too many upload requests. Please wait {decision.wait_seconds} seconds before trying again.",
        )

    # Validates file size, coalesces delta uploads, writes tracking row, and enqueues ingest work
    def enqueue_csv_bytes(
        self,
        *,
        user_id: str,
        content: bytes,
        file_name: str,
        upload_mode: Optional[str] = None,
        seed_batch_id: Optional[str] = None,
        seed_chunk_index: Optional[int] = None,
        seed_chunk_total: Optional[int] = None,
    ) -> UploadCsvResult:
        if len(content) > self.max_upload_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"Upload too large. Max size is {self.max_upload_bytes} bytes.",
            )

        content_hash = hashlib.sha256(content).hexdigest()
        now = _utcnow_naive()

        existing = tracking_crud.get_by_user_and_hash(self.db, user_id, content_hash)
        if existing:
            existing.request_count = (existing.request_count or 0) + 1
            existing.updated_at = now
            existing.upload_mode = (upload_mode or existing.upload_mode)
            existing.seed_batch_id = seed_batch_id or existing.seed_batch_id
            existing.seed_chunk_index = seed_chunk_index or existing.seed_chunk_index
            existing.seed_chunk_total = seed_chunk_total or existing.seed_chunk_total

            if existing.status in ["pending", "processing"]:
                age_s = (now - existing.created_at).total_seconds() if existing.created_at else 0
                if age_s < self.processing_timeout_seconds:
                    self.db.commit()
                    return UploadCsvResult(
                        task_id=existing.task_id,
                        status="processing",
                        message="Upload already in progress",
                    )
                existing.status = "timeout"
                existing.error_message = f"Timed out after {age_s}s"

            elif existing.status == "completed":
                self.db.commit()
                return UploadCsvResult(
                    task_id=existing.task_id,
                    status="completed",
                    message="Data already uploaded and processed",
                )

            # Reprocess failed/timeout/completed? (Completed already returned above)
            self.enforce_rate_limit(user_id)
            b64 = base64.b64encode(content).decode("utf-8")
            task_id = self._enqueue_ingest_task(user_id=user_id, csv_b64=b64)
            existing.task_id = task_id
            existing.status = "pending"
            existing.updated_at = now
            existing.error_message = None
            self.db.commit()
            return UploadCsvResult(task_id=task_id, status="reprocessing")

        # New upload
        # For frequent delta uploads, coalesce while there is an in-flight ingest for this user.
        # This avoids enqueueing lots of overlapping work when HealthKit fires many observer events.
        if (upload_mode or "").strip().lower() == "delta":
            inflight = (
                self.db.query(HealthUploadTracking)
                .filter(
                    HealthUploadTracking.user_id == user_id,
                    HealthUploadTracking.status.in_(["pending", "processing"]),
                )
                .order_by(HealthUploadTracking.created_at.desc())
                .first()
            )
            if inflight and inflight.task_id:
                # If the client isn't polling task status, tracking.status may be stale.
                # Double-check Celery state and only coalesce if the task is truly still running.
                try:
                    st, _ = self._get_ingest_task_state(inflight.task_id)
                except Exception:
                    st = None

                if st in {"SUCCESS", "FAILURE", "REVOKED"}:
                    # Mark tracking terminal based on Celery so delta uploads can enqueue new work.
                    if st == "SUCCESS":
                        inflight.status = "completed"
                        inflight.completed_at = _utcnow_naive()
                    else:
                        inflight.status = "failed"
                        inflight.error_message = f"Task state={st}"
                    inflight.updated_at = now
                    try:
                        self.db.commit()
                    except Exception:
                        self.db.rollback()
                else:
                    inflight.request_count = (inflight.request_count or 0) + 1
                    inflight.updated_at = now
                    try:
                        self.db.commit()
                    except Exception:
                        self.db.rollback()
                    return UploadCsvResult(
                        task_id=inflight.task_id,
                        status="processing",
                        message="Delta coalesced: ingest already in progress",
                    )

        self.enforce_rate_limit(user_id)
        b64 = base64.b64encode(content).decode("utf-8")
        task_id = self._enqueue_ingest_task(user_id=user_id, csv_b64=b64)
        tracking = HealthUploadTracking(
            id=content_hash,
            user_id=user_id,
            task_id=task_id,
            file_size=len(content),
            file_name=file_name,
            upload_mode=(upload_mode or None),
            seed_batch_id=(seed_batch_id or None),
            seed_chunk_index=seed_chunk_index,
            seed_chunk_total=seed_chunk_total,
            status="pending",
            request_count=1,
        )
        self.db.add(tracking)
        try:
            self.db.commit()
            return UploadCsvResult(task_id=task_id, status="new")
        except IntegrityError:
            self.db.rollback()
            # Race condition: someone else inserted same (user_id, hash).
            existing = tracking_crud.get_by_user_and_hash(self.db, user_id, content_hash)
            if existing:
                existing.request_count = (existing.request_count or 0) + 1
                self.db.commit()
                return UploadCsvResult(
                    task_id=existing.task_id,
                    status="processing",
                    message="Upload already in progress (race condition handled)",
                )
            raise

    # Returns Celery status for a task and reconciles persistent tracking status with task state
    def get_task_status(self, *, user_id: str, task_id: str) -> dict[str, Any]:
        celery_state, celery_result = self._get_ingest_task_state(task_id)

        tracking = tracking_crud.get_by_user_and_task_id(self.db, user_id, task_id)
        if tracking:
            if celery_state == "SUCCESS" and tracking.status != "completed":
                tracking.status = "completed"
                tracking.completed_at = _utcnow_naive()
                tracking.updated_at = _utcnow_naive()
                if isinstance(celery_result, dict):
                    tracking.row_count = celery_result.get("row_count") or celery_result.get("inserted")
                self.db.commit()

            elif celery_state == "FAILURE" and tracking.status not in ["failed", "completed"]:
                tracking.status = "failed"
                tracking.error_message = str(celery_result) if celery_result else "Unknown error"
                tracking.updated_at = _utcnow_naive()
                self.db.commit()

            elif celery_state == "PENDING" and tracking.status == "pending":
                age_s = (_utcnow_naive() - tracking.created_at).total_seconds() if tracking.created_at else 0
                if age_s > self.processing_timeout_seconds:
                    tracking.status = "timeout"
                    tracking.error_message = f"Task timed out after {age_s}s"
                    tracking.updated_at = _utcnow_naive()
                    self.db.commit()
                    return {"id": task_id, "state": "TIMEOUT", "result": None, "message": "Task timed out"}

            elif celery_state in ["STARTED", "RETRY"] and tracking.status == "pending":
                tracking.status = "processing"
                tracking.updated_at = _utcnow_naive()
                self.db.commit()

        return {"id": task_id, "state": celery_state, "result": celery_result}

    def get_seed_status(self, *, user_id: str, batch_id: Optional[str] = None, limit: int = 200) -> dict[str, Any]:
        bid = batch_id or tracking_crud.get_latest_seed_batch_id(self.db, user_id=user_id)
        if not bid:
            return {"batch_id": None, "chunks": [], "summary": {"total": 0, "completed": 0, "failed": 0, "processing": 0}}

        rows = tracking_crud.list_seed_batch(self.db, user_id=user_id, batch_id=bid, limit=limit)
        chunks = []
        completed = failed = processing = 0
        total = 0
        for r in rows:
            if r.seed_chunk_total and r.seed_chunk_total > total:
                total = int(r.seed_chunk_total)
            st = (r.status or "").lower()
            if st == "completed":
                completed += 1
            elif st in {"failed", "timeout"}:
                failed += 1
            elif st in {"pending", "processing"}:
                processing += 1

            chunks.append(
                {
                    "chunk_index": r.seed_chunk_index,
                    "chunk_total": r.seed_chunk_total,
                    "task_id": r.task_id,
                    "status": r.status,
                    "file_size": r.file_size,
                    "row_count": r.row_count,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                    "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                    "error_message": r.error_message,
                }
            )

        # If chunk_total isn't present (older rows), fall back to count.
        if total == 0:
            total = len({c.get("chunk_index") for c in chunks if c.get("chunk_index") is not None}) or len(chunks)

        return {
            "batch_id": bid,
            "summary": {"total": total, "completed": completed, "failed": failed, "processing": processing},
            "chunks": chunks,
        }