"""Rename health tables to main_* and derived_* naming

Revision ID: k0l1m2n3o4p5
Revises: j9k0l1m2n3o4
Create Date: 2026-01-04
"""

from alembic import op


revision = "k0l1m2n3o4p5"
down_revision = "j9k0l1m2n3o4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Core table renames (guarded so migration is idempotent-ish across environments).
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_events')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='main_health_events')
          THEN
            ALTER TABLE health_events RENAME TO main_health_events;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_metrics')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='main_health_metrics')
          THEN
            ALTER TABLE health_metrics RENAME TO main_health_metrics;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_rollup_daily')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_rollup_daily')
          THEN
            ALTER TABLE health_rollup_daily RENAME TO derived_rollup_daily;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_rollup_hourly')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_rollup_hourly')
          THEN
            ALTER TABLE health_rollup_hourly RENAME TO derived_rollup_hourly;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='all_workouts')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_workouts')
          THEN
            ALTER TABLE all_workouts RENAME TO derived_workouts;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='distance_workout_segments')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_workout_segments')
          THEN
            ALTER TABLE distance_workout_segments RENAME TO derived_workout_segments;
          END IF;
        END$$;
        """
    )

    # Index renames (best-effort).
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_all_workouts_user_start_desc')
             AND NOT EXISTS (SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_derived_workouts_user_start_desc')
          THEN
            ALTER INDEX idx_all_workouts_user_start_desc RENAME TO idx_derived_workouts_user_start_desc;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    # Reverse index rename first.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_derived_workouts_user_start_desc')
             AND NOT EXISTS (SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_all_workouts_user_start_desc')
          THEN
            ALTER INDEX idx_derived_workouts_user_start_desc RENAME TO idx_all_workouts_user_start_desc;
          END IF;
        END$$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_workout_segments')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='distance_workout_segments')
          THEN
            ALTER TABLE derived_workout_segments RENAME TO distance_workout_segments;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_workouts')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='all_workouts')
          THEN
            ALTER TABLE derived_workouts RENAME TO all_workouts;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_rollup_hourly')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_rollup_hourly')
          THEN
            ALTER TABLE derived_rollup_hourly RENAME TO health_rollup_hourly;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_rollup_daily')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_rollup_daily')
          THEN
            ALTER TABLE derived_rollup_daily RENAME TO health_rollup_daily;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='main_health_metrics')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_metrics')
          THEN
            ALTER TABLE main_health_metrics RENAME TO health_metrics;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='main_health_events')
             AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='health_events')
          THEN
            ALTER TABLE main_health_events RENAME TO health_events;
          END IF;
        END$$;
        """
    )


