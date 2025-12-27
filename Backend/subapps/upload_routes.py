import logging
from typing import Optional

from fastapi import APIRouter, Depends, File, Header, Request, UploadFile
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