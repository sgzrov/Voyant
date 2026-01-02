"""Add seed batch tracking columns to health_upload_tracking

Revision ID: aa01bb02cc03
Revises: 9c0d1e2f3a4b
Create Date: 2026-01-02
"""

from alembic import op
import sqlalchemy as sa


revision = "aa01bb02cc03"
down_revision = "9c0d1e2f3a4b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add columns (idempotent).
    op.add_column("health_upload_tracking", sa.Column("upload_mode", sa.String(length=32), nullable=True))
    op.add_column("health_upload_tracking", sa.Column("seed_batch_id", sa.String(length=64), nullable=True))
    op.add_column("health_upload_tracking", sa.Column("seed_chunk_index", sa.Integer(), nullable=True))
    op.add_column("health_upload_tracking", sa.Column("seed_chunk_total", sa.Integer(), nullable=True))

    # Helpful index for batch lookup.
    try:
        op.create_index(
            "ix_health_upload_tracking_seed_batch",
            "health_upload_tracking",
            ["user_id", "upload_mode", "seed_batch_id", "created_at"],
            unique=False,
        )
    except Exception:
        pass


def downgrade() -> None:
    try:
        op.drop_index("ix_health_upload_tracking_seed_batch", table_name="health_upload_tracking")
    except Exception:
        pass

    for col in ("seed_chunk_total", "seed_chunk_index", "seed_batch_id", "upload_mode"):
        try:
            op.drop_column("health_upload_tracking", col)
        except Exception:
            pass


