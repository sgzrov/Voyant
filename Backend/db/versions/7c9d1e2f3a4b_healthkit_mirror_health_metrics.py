"""healthkit mirror: identity + provenance + tombstones for health_metrics

Revision ID: 7c9d1e2f3a4b
Revises: 4f1a2b3c4d5e
Create Date: 2026-01-02
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "7c9d1e2f3a4b"
down_revision = "4f1a2b3c4d5e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Allow multiple samples at the same timestamp by switching identity to HKObject UUID.
    # Note: We keep the existing `id SERIAL` PK for simplicity; uniqueness is enforced with a composite unique index.
    op.execute("ALTER TABLE health_metrics DROP CONSTRAINT IF EXISTS uq_metrics_user_type_ts;")

    op.execute(
        """
        ALTER TABLE health_metrics
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

    # Unique identity per user for raw sample mirroring.
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_health_metrics_user_hk_uuid
        ON health_metrics (user_id, hk_uuid)
        WHERE hk_uuid IS NOT NULL;
        """
    )

    # Backwards-compat: legacy uploads without hk_uuid were previously deduped by (user_id, metric_type, timestamp).
    # Keep that behavior only for legacy rows where hk_uuid IS NULL so raw mirroring can store multiple samples per ts.
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_health_metrics_legacy_user_type_ts
        ON health_metrics (user_id, metric_type, timestamp)
        WHERE hk_uuid IS NULL;
        """
    )

    # Helpful indexes for newest-first reads and rollups while excluding tombstones.
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_health_metrics_user_type_ts_desc
        ON health_metrics (user_id, metric_type, timestamp DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_health_metrics_user_deleted_at
        ON health_metrics (user_id, deleted_at)
        WHERE deleted_at IS NOT NULL;
        """
    )


def downgrade() -> None:
    # Best-effort rollback. Note: data written with hk_uuid identity may violate the old uniqueness constraint.
    op.execute("DROP INDEX IF EXISTS idx_health_metrics_user_deleted_at;")
    op.execute("DROP INDEX IF EXISTS idx_health_metrics_user_type_ts_desc;")
    op.execute("DROP INDEX IF EXISTS ux_health_metrics_legacy_user_type_ts;")
    op.execute("DROP INDEX IF EXISTS ux_health_metrics_user_hk_uuid;")

    op.execute(
        """
        ALTER TABLE health_metrics
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

    # Restore old uniqueness (may fail if duplicates exist).
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


