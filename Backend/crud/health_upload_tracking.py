from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from Backend.models.health_upload_tracking_model import HealthUploadTracking


# Get upload tracking row by (user_id, content_hash)
def get_by_user_and_hash(db: Session, user_id: str, content_hash: str) -> Optional[HealthUploadTracking]:
    stmt = select(HealthUploadTracking).where(
        HealthUploadTracking.user_id == user_id,
        HealthUploadTracking.id == content_hash,
    )
    return db.execute(stmt).scalar_one_or_none()


# Get upload tracking row by (user_id, task_id)
def get_by_user_and_task_id(db: Session, user_id: str, task_id: str) -> Optional[HealthUploadTracking]:
    stmt = select(HealthUploadTracking).where(
        HealthUploadTracking.user_id == user_id,
        HealthUploadTracking.task_id == task_id,
    )
    return db.execute(stmt).scalar_one_or_none()