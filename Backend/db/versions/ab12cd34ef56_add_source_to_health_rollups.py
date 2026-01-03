"""Add source column to health rollup tables (hourly/daily)

Revision ID: ab12cd34ef56
Revises: aa01bb02cc03
Create Date: 2026-01-02
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "ab12cd34ef56"
down_revision = "aa01bb02cc03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rollups may have been created in different migrations across environments.
    # Add column idempotently.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'health_rollup_hourly'
          ) THEN
            ALTER TABLE health_rollup_hourly ADD COLUMN IF NOT EXISTS source TEXT;
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
            ALTER TABLE health_rollup_daily ADD COLUMN IF NOT EXISTS source TEXT;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    # Best-effort rollback.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'health_rollup_daily'
          ) THEN
            ALTER TABLE health_rollup_daily DROP COLUMN IF EXISTS source;
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
            ALTER TABLE health_rollup_hourly DROP COLUMN IF EXISTS source;
          END IF;
        END$$;
        """
    )


