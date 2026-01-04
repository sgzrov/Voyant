"""Drop hk_source_name and hk_source_version from workouts (use hk_sources JSONB instead)

Revision ID: i8j9k0l1m2n3
Revises: h7i8j9k0l1m2
Create Date: 2026-01-03
"""

from alembic import op


revision = "i8j9k0l1m2n3"
down_revision = "h7i8j9k0l1m2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_workouts_user_start_desc;")
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='workouts' AND column_name='hk_source_name'
          ) THEN
            ALTER TABLE workouts DROP COLUMN hk_source_name;
          END IF;
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='workouts' AND column_name='hk_source_version'
          ) THEN
            ALTER TABLE workouts DROP COLUMN hk_source_version;
          END IF;
        END$$;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workouts_user_start_desc
        ON workouts (user_id, start_ts DESC)
        INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal, hk_source_bundle_id, hk_sources);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_workouts_user_start_desc;")
    op.execute("ALTER TABLE workouts ADD COLUMN IF NOT EXISTS hk_source_name TEXT;")
    op.execute("ALTER TABLE workouts ADD COLUMN IF NOT EXISTS hk_source_version TEXT;")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workouts_user_start_desc
        ON workouts (user_id, start_ts DESC)
        INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal, hk_source_bundle_id, hk_source_name, hk_source_version, hk_sources);
        """
    )


