from sqlalchemy import Column, DateTime, Integer, String, Text, func

from Backend.database import Base


# Stores CSV uploads for tracking/deduplication purposes
class HealthUploadTracking(Base):
    __tablename__ = "health_upload_tracking"

    id = Column(String(64), primary_key=True)  # SHA256 hash of content
    user_id = Column(String(100), primary_key=True)
    task_id = Column(String(100), nullable=True)
    file_size = Column(Integer, nullable=False)
    file_name = Column(String(255), nullable=True)
    upload_mode = Column(String(32), nullable=True)  # e.g. "seed" / "delta"
    seed_batch_id = Column(String(64), nullable=True)
    seed_chunk_index = Column(Integer, nullable=True)
    seed_chunk_total = Column(Integer, nullable=True)
    status = Column(String(50), nullable=False, default="pending")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    row_count = Column(Integer, nullable=True)
    request_count = Column(Integer, nullable=False, default=1)