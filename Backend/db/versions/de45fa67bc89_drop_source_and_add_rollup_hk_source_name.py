"""Drop legacy source columns and standardize on hk_source_name

- Drop health_metrics.source
- Drop health_rollup_hourly.source / health_rollup_daily.source
- Add hk_source_name to rollup tables (hourly/daily)

Revision ID: de45fa67bc89
Revises: cd34ef56ab78
Create Date: 2026-01-02
"""

from alembic import op


revision = "de45fa67bc89"
down_revision = "cd34ef56ab78"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # health_metrics: drop legacy `source` (redundant with hk_source_name).
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='health_metrics' AND column_name='source'
          ) THEN
            ALTER TABLE health_metrics DROP COLUMN source;
          END IF;
        END$$;
        """
    )

    # rollups: drop legacy `source`, add hk_source_name.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name='health_rollup_hourly'
          ) THEN
            IF EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='health_rollup_hourly' AND column_name='source'
            ) THEN
              ALTER TABLE health_rollup_hourly DROP COLUMN source;
            END IF;
            ALTER TABLE health_rollup_hourly ADD COLUMN IF NOT EXISTS hk_source_name TEXT;
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
            WHERE table_schema='public' AND table_name='health_rollup_daily'
          ) THEN
            IF EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='health_rollup_daily' AND column_name='source'
            ) THEN
              ALTER TABLE health_rollup_daily DROP COLUMN source;
            END IF;
            ALTER TABLE health_rollup_daily ADD COLUMN IF NOT EXISTS hk_source_name TEXT;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    # Best-effort rollback: re-add legacy columns (data is lost / not backfilled).
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name='health_metrics'
          ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='health_metrics' AND column_name='source'
          ) THEN
            ALTER TABLE health_metrics ADD COLUMN source TEXT;
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
            WHERE table_schema='public' AND table_name='health_rollup_hourly'
          ) THEN
            IF NOT EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='health_rollup_hourly' AND column_name='source'
            ) THEN
              ALTER TABLE health_rollup_hourly ADD COLUMN source TEXT;
            END IF;
            IF EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='health_rollup_hourly' AND column_name='hk_source_name'
            ) THEN
              ALTER TABLE health_rollup_hourly DROP COLUMN hk_source_name;
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
            WHERE table_schema='public' AND table_name='health_rollup_daily'
          ) THEN
            IF NOT EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='health_rollup_daily' AND column_name='source'
            ) THEN
              ALTER TABLE health_rollup_daily ADD COLUMN source TEXT;
            END IF;
            IF EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='health_rollup_daily' AND column_name='hk_source_name'
            ) THEN
              ALTER TABLE health_rollup_daily DROP COLUMN hk_source_name;
            END IF;
          END IF;
        END$$;
        """
    )


