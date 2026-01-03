"""Drop hk_was_user_entered columns (redundant with hk_metadata)

Revision ID: ef56ab78cd90
Revises: de45fa67bc89
Create Date: 2026-01-02
"""

from alembic import op


revision = "ef56ab78cd90"
down_revision = "de45fa67bc89"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='health_metrics' AND column_name='hk_was_user_entered'
          ) THEN
            ALTER TABLE health_metrics DROP COLUMN hk_was_user_entered;
          END IF;
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='health_events' AND column_name='hk_was_user_entered'
          ) THEN
            ALTER TABLE health_events DROP COLUMN hk_was_user_entered;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    # Best-effort rollback: re-add columns (data is lost).
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name='health_metrics'
          ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='health_metrics' AND column_name='hk_was_user_entered'
          ) THEN
            ALTER TABLE health_metrics ADD COLUMN hk_was_user_entered BOOLEAN;
          END IF;
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name='health_events'
          ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='health_events' AND column_name='hk_was_user_entered'
          ) THEN
            ALTER TABLE health_events ADD COLUMN hk_was_user_entered BOOLEAN;
          END IF;
        END$$;
        """
    )


