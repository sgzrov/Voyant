"""healthkit mirror: add hk identity/tombstones to health_events; restore health_rollup_daily

Revision ID: 8a2b3c4d5e6f
Revises: 7c9d1e2f3a4b
Create Date: 2026-01-02
"""

from alembic import op


revision = "8a2b3c4d5e6f"
down_revision = "7c9d1e2f3a4b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- health_events: make it mirror-friendly (uuid identity + provenance + tombstones)
    op.execute(
        """
        ALTER TABLE health_events
          ADD COLUMN IF NOT EXISTS hk_uuid TEXT,
          ADD COLUMN IF NOT EXISTS end_ts TIMESTAMPTZ,
          ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ,
          ADD COLUMN IF NOT EXISTS hk_source_bundle_id TEXT,
          ADD COLUMN IF NOT EXISTS hk_source_name TEXT,
          ADD COLUMN IF NOT EXISTS hk_source_version TEXT,
          ADD COLUMN IF NOT EXISTS hk_device JSONB,
          ADD COLUMN IF NOT EXISTS hk_metadata JSONB,
          ADD COLUMN IF NOT EXISTS hk_was_user_entered BOOLEAN;
        """
    )

    # Enforce identity for mirrored rows.
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_health_events_user_hk_uuid_event_type
        ON health_events (user_id, hk_uuid, event_type)
        WHERE hk_uuid IS NOT NULL;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_health_events_user_deleted_at
        ON health_events (user_id, deleted_at)
        WHERE deleted_at IS NOT NULL;
        """
    )

    # --- health_rollup_daily: restore (some envs dropped it); include meta like hourly
    # Also ensure health_rollup_hourly exists (some environments previously dropped rollups).
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
          meta JSONB,
          PRIMARY KEY (user_id, metric_type, bucket_ts)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rolluph_user_metric_ts_desc
        ON health_rollup_hourly (user_id, metric_type, bucket_ts DESC)
        INCLUDE (avg_value, sum_value, min_value, max_value, n, meta);
        """
    )

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
          meta JSONB,
          PRIMARY KEY (user_id, metric_type, bucket_ts)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rollupd_user_metric_ts_desc
        ON health_rollup_daily (user_id, metric_type, bucket_ts DESC)
        INCLUDE (avg_value, sum_value, min_value, max_value, n, meta);
        """
    )


def downgrade() -> None:
    # Rollup daily can be dropped on rollback.
    op.execute("DROP INDEX IF EXISTS idx_rollupd_user_metric_ts_desc;")
    op.execute("DROP TABLE IF EXISTS health_rollup_daily;")

    op.execute("DROP INDEX IF EXISTS idx_health_events_user_deleted_at;")
    op.execute("DROP INDEX IF EXISTS ux_health_events_user_hk_uuid_event_type;")

    op.execute(
        """
        ALTER TABLE health_events
          DROP COLUMN IF EXISTS hk_was_user_entered,
          DROP COLUMN IF EXISTS hk_metadata,
          DROP COLUMN IF EXISTS hk_device,
          DROP COLUMN IF EXISTS hk_source_version,
          DROP COLUMN IF EXISTS hk_source_name,
          DROP COLUMN IF EXISTS hk_source_bundle_id,
          DROP COLUMN IF EXISTS deleted_at,
          DROP COLUMN IF EXISTS end_ts,
          DROP COLUMN IF EXISTS hk_uuid;
        """
    )


