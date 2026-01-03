"""Drop meta column from health_events

Revision ID: cd34ef56ab78
Revises: bc23de45fa67
Create Date: 2026-01-02
"""

from alembic import op


revision = "cd34ef56ab78"
down_revision = "bc23de45fa67"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop column idempotently; some environments may not have the table/column.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'health_events'
              AND column_name = 'meta'
          ) THEN
            ALTER TABLE health_events DROP COLUMN meta;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    # Best-effort rollback: re-add column (data is lost).
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'health_events'
          ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'health_events'
              AND column_name = 'meta'
          ) THEN
            ALTER TABLE health_events ADD COLUMN meta JSONB;
          END IF;
        END$$;
        """
    )


