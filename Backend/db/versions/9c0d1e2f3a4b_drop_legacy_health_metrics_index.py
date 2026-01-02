"""drop legacy (hk_uuid IS NULL) unique index for health_metrics

Revision ID: 9c0d1e2f3a4b
Revises: 8a2b3c4d5e6f
Create Date: 2026-01-02
"""

from alembic import op


revision = "9c0d1e2f3a4b"
down_revision = "8a2b3c4d5e6f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ux_health_metrics_legacy_user_type_ts;")


def downgrade() -> None:
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_health_metrics_legacy_user_type_ts
        ON health_metrics (user_id, metric_type, timestamp)
        WHERE hk_uuid IS NULL;
        """
    )


