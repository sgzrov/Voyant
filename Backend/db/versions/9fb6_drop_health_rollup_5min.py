"""drop health_rollup_5min table (no longer used)

Revision ID: 9fb6
Revises: 9fb5
Create Date: 2025-11-20
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9fb6'
down_revision = '9fb5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop index first (if it exists), then drop the table
    op.execute("DROP INDEX IF EXISTS idx_health_rollup_5min_user_type_ts;")
    op.execute("DROP TABLE IF EXISTS health_rollup_5min;")


def downgrade() -> None:
    # Recreate the 5-minute rollup table and index on rollback
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_rollup_5min (
          user_id TEXT NOT NULL,
          bucket_ts TIMESTAMPTZ NOT NULL,
          metric_type TEXT NOT NULL,
          avg_value DOUBLE PRECISION,
          sum_value DOUBLE PRECISION,
          min_value DOUBLE PRECISION,
          max_value DOUBLE PRECISION,
          n BIGINT,
          PRIMARY KEY (user_id, metric_type, bucket_ts)
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_health_rollup_5min_user_type_ts ON health_rollup_5min(user_id, metric_type, bucket_ts);")


