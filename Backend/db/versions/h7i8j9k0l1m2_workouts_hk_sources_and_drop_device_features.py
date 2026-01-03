"""Workouts: add hk_sources; drop hk_device, hk_was_user_entered, and features

Revision ID: h7i8j9k0l1m2
Revises: g6h7i8j9k0l1
Create Date: 2026-01-03
"""

from alembic import op


revision = "h7i8j9k0l1m2"
down_revision = "g6h7i8j9k0l1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add hk_sources array (like rollups).
    op.execute("ALTER TABLE workouts ADD COLUMN IF NOT EXISTS hk_sources JSONB;")

    # Best-effort backfill hk_sources from per-workout source name/version.
    op.execute(
        """
        UPDATE workouts
        SET hk_sources = COALESCE(
          hk_sources,
          CASE
            WHEN hk_source_name IS NOT NULL
              THEN jsonb_build_array(jsonb_build_object('name', hk_source_name, 'version', hk_source_version))
            ELSE '[]'::jsonb
          END
        )
        WHERE hk_sources IS NULL;
        """
    )

    # Drop and rebuild index (we've been changing included columns over time).
    op.execute("DROP INDEX IF EXISTS idx_workouts_user_start_desc;")

    # Drop no-longer-used columns.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='workouts' AND column_name='hk_device'
          ) THEN
            ALTER TABLE workouts DROP COLUMN hk_device;
          END IF;
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='workouts' AND column_name='hk_was_user_entered'
          ) THEN
            ALTER TABLE workouts DROP COLUMN hk_was_user_entered;
          END IF;
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='workouts' AND column_name='features'
          ) THEN
            ALTER TABLE workouts DROP COLUMN features;
          END IF;
        END$$;
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workouts_user_start_desc
        ON workouts (user_id, start_ts DESC)
        INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal, hk_source_name, hk_source_version, hk_sources);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_workouts_user_start_desc;")

    op.execute("ALTER TABLE workouts ADD COLUMN IF NOT EXISTS features JSONB;")
    op.execute("ALTER TABLE workouts ADD COLUMN IF NOT EXISTS hk_device JSONB;")
    op.execute("ALTER TABLE workouts ADD COLUMN IF NOT EXISTS hk_was_user_entered BOOLEAN;")
    op.execute("ALTER TABLE workouts DROP COLUMN IF EXISTS hk_sources;")

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workouts_user_start_desc
        ON workouts (user_id, start_ts DESC)
        INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal, hk_source_name, hk_source_version);
        """
    )


