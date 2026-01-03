"""Drop hk_source_name and hk_source_version columns from health rollup tables

Now that health_rollup_hourly/daily have hk_sources JSONB, we no longer keep the
denormalized hk_source_name/hk_source_version columns on rollups.

Revision ID: b0c1d2e3f4a5
Revises: a9b8c7d6e5f4
Create Date: 2026-01-03
"""

from alembic import op


revision = "b0c1d2e3f4a5"
down_revision = "a9b8c7d6e5f4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'health_rollup_hourly'
          ) THEN
            IF EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='health_rollup_hourly' AND column_name='hk_source_name'
            ) THEN
              ALTER TABLE health_rollup_hourly DROP COLUMN hk_source_name;
            END IF;
            IF EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='health_rollup_hourly' AND column_name='hk_source_version'
            ) THEN
              ALTER TABLE health_rollup_hourly DROP COLUMN hk_source_version;
            END IF;
          END IF;
        END$$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'health_rollup_daily'
          ) THEN
            IF EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='health_rollup_daily' AND column_name='hk_source_name'
            ) THEN
              ALTER TABLE health_rollup_daily DROP COLUMN hk_source_name;
            END IF;
            IF EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='health_rollup_daily' AND column_name='hk_source_version'
            ) THEN
              ALTER TABLE health_rollup_daily DROP COLUMN hk_source_version;
            END IF;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    # Best-effort rollback: re-add columns (data is not backfilled).
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'health_rollup_hourly'
          ) THEN
            ALTER TABLE health_rollup_hourly ADD COLUMN IF NOT EXISTS hk_source_name TEXT;
            ALTER TABLE health_rollup_hourly ADD COLUMN IF NOT EXISTS hk_source_version TEXT;
          END IF;
        END$$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'health_rollup_daily'
          ) THEN
            ALTER TABLE health_rollup_daily ADD COLUMN IF NOT EXISTS hk_source_name TEXT;
            ALTER TABLE health_rollup_daily ADD COLUMN IF NOT EXISTS hk_source_version TEXT;
          END IF;
        END$$;
        """
    )


