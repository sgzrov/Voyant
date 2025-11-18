"""add health rollup tables (5min, hourly, daily)

Revision ID: 9fb4
Revises: 9fb3
Create Date: 2025-11-17
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9fb4'
down_revision = '9fb3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    for tbl in ("health_rollup_5min", "health_rollup_hourly", "health_rollup_daily"):
        op.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
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
        op.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_user_type_ts ON {tbl}(user_id, metric_type, bucket_ts);")


def downgrade() -> None:
    for tbl in ("health_rollup_5min", "health_rollup_hourly", "health_rollup_daily"):
        op.execute(f"DROP INDEX IF EXISTS idx_{tbl}_user_type_ts;")
        op.execute(f"DROP TABLE IF EXISTS {tbl};")




