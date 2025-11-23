"""create hourly and daily health rollup tables

Revision ID: e2b1c4d5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2025-11-23

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e2b1c4d5f6a7"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Hourly rollups
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_rollup_hourly (
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
    # Daily rollups
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_rollup_daily (
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

    # Indexes optimized for newest-first reads and index-only scans (PostgreSQL)
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rolluph_user_metric_ts_desc
        ON health_rollup_hourly (user_id, metric_type, bucket_ts DESC)
        INCLUDE (avg_value, sum_value, min_value, max_value, n);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rollupd_user_metric_ts_desc
        ON health_rollup_daily (user_id, metric_type, bucket_ts DESC)
        INCLUDE (avg_value, sum_value, min_value, max_value, n);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_rollupd_user_metric_ts_desc;")
    op.execute("DROP INDEX IF EXISTS idx_rolluph_user_metric_ts_desc;")
    op.execute("DROP TABLE IF EXISTS health_rollup_daily;")
    op.execute("DROP TABLE IF EXISTS health_rollup_hourly;")


