"""Add HealthKit provenance columns to workouts (split out of features JSONB)

Revision ID: g6h7i8j9k0l1
Revises: f5g6h7i8j9k0
Create Date: 2026-01-03
"""

from alembic import op


revision = "g6h7i8j9k0l1"
down_revision = "f5g6h7i8j9k0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE workouts
          ADD COLUMN IF NOT EXISTS hk_source_bundle_id TEXT,
          ADD COLUMN IF NOT EXISTS hk_source_name TEXT,
          ADD COLUMN IF NOT EXISTS hk_source_version TEXT,
          ADD COLUMN IF NOT EXISTS hk_device JSONB,
          ADD COLUMN IF NOT EXISTS hk_metadata JSONB,
          ADD COLUMN IF NOT EXISTS hk_was_user_entered BOOLEAN;
        """
    )

    # Best-effort backfill from features JSONB (older rows stored provenance there).
    op.execute(
        """
        UPDATE workouts
        SET
          hk_source_bundle_id = COALESCE(hk_source_bundle_id, features->>'hk_source_bundle_id'),
          hk_source_name = COALESCE(hk_source_name, features->>'hk_source_name'),
          hk_source_version = COALESCE(hk_source_version, features->>'hk_source_version'),
          hk_was_user_entered = COALESCE(hk_was_user_entered, (features->>'hk_was_user_entered')::boolean),
          hk_device = COALESCE(hk_device, features->'hk_device'),
          hk_metadata = COALESCE(hk_metadata, features->'hk_metadata')
        WHERE features IS NOT NULL;
        """
    )

    # Rebuild the workouts index to include source columns for common selects.
    op.execute("DROP INDEX IF EXISTS idx_workouts_user_start_desc;")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workouts_user_start_desc
        ON workouts (user_id, start_ts DESC)
        INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal, hk_source_name, hk_source_version);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_workouts_user_start_desc;")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workouts_user_start_desc
        ON workouts (user_id, start_ts DESC)
        INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal);
        """
    )
    op.execute(
        """
        ALTER TABLE workouts
          DROP COLUMN IF EXISTS hk_was_user_entered,
          DROP COLUMN IF EXISTS hk_metadata,
          DROP COLUMN IF EXISTS hk_device,
          DROP COLUMN IF EXISTS hk_source_version,
          DROP COLUMN IF EXISTS hk_source_name,
          DROP COLUMN IF EXISTS hk_source_bundle_id;
        """
    )


