"""add DESC composite indexes for recent-first scans

Revision ID: a1b2c3d4e5f6
Revises: c610270318a2
Create Date: 2025-11-19 00:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "c610270318a2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create indexes to accelerate newest-first reads with LIMIT
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_metrics_user_metric_ts_desc
        ON health_metrics (user_id, metric_type, timestamp DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_events_user_type_ts_desc
        ON health_events (user_id, event_type, timestamp DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rollup5_user_metric_ts_desc
        ON health_rollup_5min (user_id, metric_type, bucket_ts DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rolluph_user_metric_ts_desc
        ON health_rollup_hourly (user_id, metric_type, bucket_ts DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rollupd_user_metric_ts_desc
        ON health_rollup_daily (user_id, metric_type, bucket_ts DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sessions_user_start_desc
        ON health_sessions (user_id, start_ts DESC);
        """
    )


def downgrade() -> None:
    # Drop indexes if they exist
    op.execute("DROP INDEX IF EXISTS idx_sessions_user_start_desc;")
    op.execute("DROP INDEX IF EXISTS idx_rollupd_user_metric_ts_desc;")
    op.execute("DROP INDEX IF EXISTS idx_rolluph_user_metric_ts_desc;")
    op.execute("DROP INDEX IF EXISTS idx_rollup5_user_metric_ts_desc;")
    op.execute("DROP INDEX IF EXISTS idx_events_user_type_ts_desc;")
    op.execute("DROP INDEX IF EXISTS idx_metrics_user_metric_ts_desc;")


