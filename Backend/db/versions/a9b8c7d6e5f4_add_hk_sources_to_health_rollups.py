"""Add hk_sources JSONB column to health rollup tables (hourly/daily)

Revision ID: a9b8c7d6e5f4
Revises: f123456789ab
Create Date: 2026-01-03
"""

from alembic import op


revision = "a9b8c7d6e5f4"
down_revision = "f123456789ab"
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
            ALTER TABLE health_rollup_hourly ADD COLUMN IF NOT EXISTS hk_sources JSONB;
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
            ALTER TABLE health_rollup_daily ADD COLUMN IF NOT EXISTS hk_sources JSONB;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'health_rollup_daily'
          ) THEN
            ALTER TABLE health_rollup_daily DROP COLUMN IF EXISTS hk_sources;
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
            WHERE table_schema = 'public' AND table_name = 'health_rollup_hourly'
          ) THEN
            ALTER TABLE health_rollup_hourly DROP COLUMN IF EXISTS hk_sources;
          END IF;
        END$$;
        """
    )


