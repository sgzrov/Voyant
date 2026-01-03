"""Drop hard/long workout flags from workouts

Revision ID: f5g6h7i8j9k0
Revises: e3f4g5h6i7j8
Create Date: 2026-01-03
"""

from alembic import op


revision = "f5g6h7i8j9k0"
down_revision = "e3f4g5h6i7j8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop index first because it included the soon-to-be-dropped columns.
    op.execute("DROP INDEX IF EXISTS idx_workouts_user_start_desc;")

    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'workouts'
          ) THEN
            IF EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='workouts' AND column_name='is_hard_workout'
            ) THEN
              ALTER TABLE workouts DROP COLUMN is_hard_workout;
            END IF;
            IF EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='workouts' AND column_name='is_long_run'
            ) THEN
              ALTER TABLE workouts DROP COLUMN is_long_run;
            END IF;
          END IF;
        END$$;
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workouts_user_start_desc
        ON workouts (user_id, start_ts DESC)
        INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_workouts_user_start_desc;")

    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'workouts'
          ) THEN
            IF NOT EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='workouts' AND column_name='is_hard_workout'
            ) THEN
              ALTER TABLE workouts ADD COLUMN is_hard_workout BOOLEAN;
            END IF;
            IF NOT EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name='workouts' AND column_name='is_long_run'
            ) THEN
              ALTER TABLE workouts ADD COLUMN is_long_run BOOLEAN;
            END IF;
          END IF;
        END$$;
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workouts_user_start_desc
        ON workouts (user_id, start_ts DESC)
        INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal, is_hard_workout, is_long_run);
        """
    )


