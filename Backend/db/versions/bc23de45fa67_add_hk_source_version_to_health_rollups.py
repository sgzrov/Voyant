"""Add hk_source_version column to health rollup tables (hourly/daily)

Revision ID: bc23de45fa67
Revises: ab12cd34ef56
Create Date: 2026-01-02
"""

from alembic import op


revision = "bc23de45fa67"
down_revision = "ab12cd34ef56"
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
            ALTER TABLE health_rollup_daily ADD COLUMN IF NOT EXISTS hk_source_version TEXT;
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
            ALTER TABLE health_rollup_daily DROP COLUMN IF EXISTS hk_source_version;
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
            ALTER TABLE health_rollup_hourly DROP COLUMN IF EXISTS hk_source_version;
          END IF;
        END$$;
        """
    )


