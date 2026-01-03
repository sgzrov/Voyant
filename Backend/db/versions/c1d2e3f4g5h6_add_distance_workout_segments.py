"""Add distance_workout_segments table for per-workout km/mile splits

Revision ID: c1d2e3f4g5h6
Revises: b0c1d2e3f4a5
Create Date: 2026-01-03
"""

from alembic import op


revision = "c1d2e3f4g5h6"
down_revision = "b0c1d2e3f4a5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS distance_workout_segments (
          user_id TEXT NOT NULL,
          workout_uuid TEXT NOT NULL,
          workout_start_ts TIMESTAMPTZ NOT NULL,
          segment_unit TEXT NOT NULL,           -- 'km' | 'mi'
          segment_index INTEGER NOT NULL,       -- 1..N
          start_ts TIMESTAMPTZ NOT NULL,
          end_ts TIMESTAMPTZ NOT NULL,
          start_offset_min DOUBLE PRECISION NOT NULL,
          end_offset_min DOUBLE PRECISION NOT NULL,
          duration_min DOUBLE PRECISION NOT NULL,
          pace_s_per_unit DOUBLE PRECISION,
          avg_hr_bpm DOUBLE PRECISION,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          PRIMARY KEY (user_id, workout_uuid, segment_unit, segment_index)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_distance_workout_segments_user_workout
        ON distance_workout_segments (user_id, workout_uuid, segment_unit, segment_index);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_distance_workout_segments_user_start_desc
        ON distance_workout_segments (user_id, workout_start_ts DESC)
        INCLUDE (segment_unit, segment_index, start_ts, end_ts, duration_min, pace_s_per_unit, avg_hr_bpm);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_distance_workout_segments_user_start_desc;")
    op.execute("DROP INDEX IF EXISTS idx_distance_workout_segments_user_workout;")
    op.execute("DROP TABLE IF EXISTS distance_workout_segments;")


