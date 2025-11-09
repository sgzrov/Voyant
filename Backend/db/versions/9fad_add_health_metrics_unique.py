"""add unique constraint on health_metrics (user_id, metric_type, timestamp)

Revision ID: 9fad
Revises: 9fac
Create Date: 2025-11-08
"""

from alembic import op
import sqlalchemy as sa


revision = '9fad'
down_revision = '9fac'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'uq_metrics_user_type_ts'
          ) THEN
            ALTER TABLE health_metrics
            ADD CONSTRAINT uq_metrics_user_type_ts UNIQUE (user_id, metric_type, timestamp);
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE IF EXISTS health_metrics
        DROP CONSTRAINT IF EXISTS uq_metrics_user_type_ts;
        """
    )



