"""drop health_rollup_daily table and related index

Revision ID: ee12ab34cd56
Revises: d1e2f3g4h5i6
Create Date: 2025-11-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "ee12ab34cd56"
down_revision: Union[str, Sequence[str], None] = "d1e2f3g4h5i6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop index first (if it exists), then drop the table.
    op.execute("DROP INDEX IF EXISTS idx_rollupd_user_metric_ts_desc;")
    op.execute("DROP TABLE IF EXISTS health_rollup_daily;")


def downgrade() -> None:
    # Recreate the table and index to allow rollback.
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
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rollupd_user_metric_ts_desc
        ON health_rollup_daily (user_id, metric_type, bucket_ts DESC)
        INCLUDE (avg_value, sum_value, min_value, max_value, n);
        """
    )


