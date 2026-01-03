"""Drop hk_device columns entirely (rely on hk_source_name)

Revision ID: f123456789ab
Revises: ef56ab78cd90
Create Date: 2026-01-02
"""

from alembic import op


revision = "f123456789ab"
down_revision = "ef56ab78cd90"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='health_metrics' AND column_name='hk_device'
          ) THEN
            ALTER TABLE health_metrics DROP COLUMN hk_device;
          END IF;
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='health_events' AND column_name='hk_device'
          ) THEN
            ALTER TABLE health_events DROP COLUMN hk_device;
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
            WHERE table_schema='public' AND table_name='health_metrics'
          ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='health_metrics' AND column_name='hk_device'
          ) THEN
            ALTER TABLE health_metrics ADD COLUMN hk_device JSONB;
          END IF;
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name='health_events'
          ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='health_events' AND column_name='hk_device'
          ) THEN
            ALTER TABLE health_events ADD COLUMN hk_device JSONB;
          END IF;
        END$$;
        """
    )


