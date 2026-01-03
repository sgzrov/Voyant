"""Add distance_workouts table (one row per workout) for fast structured queries

Revision ID: d2e3f4g5h6i7
Revises: c1d2e3f4g5h6
Create Date: 2026-01-03
"""

from alembic import op


revision = "d2e3f4g5h6i7"
down_revision = "c1d2e3f4g5h6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS distance_workouts (
          user_id TEXT NOT NULL,
          workout_uuid TEXT NOT NULL,            -- HKWorkout UUID
          workout_type TEXT,                     -- e.g., running/strength/etc (from HealthKit workout type label)
          start_ts TIMESTAMPTZ NOT NULL,
          end_ts TIMESTAMPTZ,
          duration_min DOUBLE PRECISION,
          distance_km DOUBLE PRECISION,
          energy_kcal DOUBLE PRECISION,
          is_hard_workout BOOLEAN,
          is_long_run BOOLEAN,
          features JSONB,                        -- optional extensibility (cadence/elevation/temp/etc when available)
          created_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ DEFAULT NOW(),
          PRIMARY KEY (user_id, workout_uuid)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_distance_workouts_user_start_desc
        ON distance_workouts (user_id, start_ts DESC)
        INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal, is_hard_workout, is_long_run);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_distance_workouts_user_start_desc;")
    op.execute("DROP TABLE IF EXISTS distance_workouts;")


