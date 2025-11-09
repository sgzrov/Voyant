"""add health_metrics table for raw time-series

Revision ID: 9fac
Revises: 9fab
Create Date: 2025-11-08
"""

from alembic import op
import sqlalchemy as sa


revision = '9fac'
down_revision = '9fab'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_metrics (
          id SERIAL PRIMARY KEY,
          user_id TEXT NOT NULL,
          timestamp TIMESTAMPTZ NOT NULL,
          metric_type TEXT NOT NULL,
          metric_value DOUBLE PRECISION NOT NULL,
          unit TEXT,
          source TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_metrics_user_ts ON health_metrics(user_id, timestamp);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_metrics_user_type_ts ON health_metrics(user_id, metric_type, timestamp);")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_metrics_user_type_ts;")
    op.execute("DROP INDEX IF EXISTS idx_metrics_user_ts;")
    op.execute("DROP TABLE IF EXISTS health_metrics;")



