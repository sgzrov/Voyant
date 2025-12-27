"""Drop unused upload idempotency key column

Revision ID: 3c4d5e6f7a8b
Revises: 2c3d4e5f6a7b
Create Date: 2025-12-26

"""

from alembic import op
import sqlalchemy as sa


revision = "3c4d5e6f7a8b"
down_revision = "2c3d4e5f6a7b"
branch_labels = None
depends_on = None


def upgrade():
    # Drop index first, then column.
    try:
        op.drop_index("ix_health_upload_tracking_idempotency_key", table_name="health_upload_tracking")
    except Exception:
        pass
    try:
        op.drop_column("health_upload_tracking", "idempotency_key")
    except Exception:
        pass


def downgrade():
    # Recreate the column + index (best-effort).
    try:
        op.add_column("health_upload_tracking", sa.Column("idempotency_key", sa.String(length=64), nullable=True))
    except Exception:
        pass
    try:
        op.create_index(
            "ix_health_upload_tracking_idempotency_key",
            "health_upload_tracking",
            ["idempotency_key"],
            unique=False,
        )
    except Exception:
        pass


