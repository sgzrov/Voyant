"""Add unit column to derived rollup tables.

Rollups should expose units as a dedicated column (like main_health_metrics.unit),
and rollup meta should not carry units.
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "f0e1d2c3b4a5"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add unit to new derived_* names (and legacy health_rollup_* for safety).
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_rollup_hourly')
             AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='derived_rollup_hourly' AND column_name='unit')
          THEN
            ALTER TABLE derived_rollup_hourly ADD COLUMN unit TEXT;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_rollup_daily')
             AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='derived_rollup_daily' AND column_name='unit')
          THEN
            ALTER TABLE derived_rollup_daily ADD COLUMN unit TEXT;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_rollup_hourly')
             AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='health_rollup_hourly' AND column_name='unit')
          THEN
            ALTER TABLE health_rollup_hourly ADD COLUMN unit TEXT;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_rollup_daily')
             AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='health_rollup_daily' AND column_name='unit')
          THEN
            ALTER TABLE health_rollup_daily ADD COLUMN unit TEXT;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_rollup_hourly')
             AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='derived_rollup_hourly' AND column_name='unit')
          THEN
            ALTER TABLE derived_rollup_hourly DROP COLUMN unit;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_rollup_daily')
             AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='derived_rollup_daily' AND column_name='unit')
          THEN
            ALTER TABLE derived_rollup_daily DROP COLUMN unit;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_rollup_hourly')
             AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='health_rollup_hourly' AND column_name='unit')
          THEN
            ALTER TABLE health_rollup_hourly DROP COLUMN unit;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_rollup_daily')
             AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='health_rollup_daily' AND column_name='unit')
          THEN
            ALTER TABLE health_rollup_daily DROP COLUMN unit;
          END IF;
        END$$;
        """
    )


