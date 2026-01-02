import logging
from typing import Optional

from fastapi import APIRouter, Depends, File, Header, Query, Request, UploadFile
from sqlalchemy.orm import Session

from Backend.auth import verify_clerk_jwt
from Backend.database import get_db
from Backend.services.health_upload_service import HealthUploadService


router = APIRouter()
logger = logging.getLogger(__name__)


# Upload a CSV file with SHA-256 deduplication
@router.post("/health/upload-csv")
def upload_csv(
    file: UploadFile = File(...),
    request: Request = None,  # kept for backwards-compat with existing clients/middleware
    x_upload_mode: Optional[str] = Header(None),
    x_seed_batch_id: Optional[str] = Header(None),
    x_seed_chunk_index: Optional[int] = Header(None),
    x_seed_chunk_total: Optional[int] = Header(None),
    db: Session = Depends(get_db),
):
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    svc = HealthUploadService(db)
    content = file.file.read()
    file_name = getattr(file, "filename", "health.csv")
    result = svc.enqueue_csv_bytes(
        user_id=user_id,
        content=content,
        file_name=file_name,
        upload_mode=x_upload_mode,
        seed_batch_id=x_seed_batch_id,
        seed_chunk_index=x_seed_chunk_index,
        seed_chunk_total=x_seed_chunk_total,
    )
    payload = {"task_id": result.task_id, "status": result.status}
    if result.message:
        payload["message"] = result.message
    return payload


# Gets task status with upload tracking integration
@router.get("/health/task-status/{task_id}")
def task_status(
    task_id: str,
    request: Request = None,  # kept for backwards-compat
    db: Session = Depends(get_db),
):
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    svc = HealthUploadService(db)
    return svc.get_task_status(user_id=user_id, task_id=task_id)


@router.get("/health/seed-status")
def seed_status(
    request: Request = None,  # kept for backwards-compat
    batch_id: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=500),
    db: Session = Depends(get_db),
):
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    svc = HealthUploadService(db)
    return svc.get_seed_status(user_id=user_id, batch_id=batch_id, limit=limit)