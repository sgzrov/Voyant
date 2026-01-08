"""Drop hk_source_bundle_id everywhere.

We no longer store HealthKit source bundle ids; provenance should come from hk_sources / hk_metadata where relevant.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f7"
down_revision: Union[str, Sequence[str], None] = "f3a4b5c6d7e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Workouts index may INCLUDE hk_source_bundle_id in older schemas; drop and recreate without it.
    op.execute("DROP INDEX IF EXISTS idx_derived_workouts_user_start_desc;")
    op.execute("DROP INDEX IF EXISTS idx_all_workouts_user_start_desc;")
    op.execute("DROP INDEX IF EXISTS idx_workouts_user_start_desc;")

    op.execute(
        """
        DO $$
        BEGIN
          -- Main mirrored tables
          IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='main_health_metrics' AND column_name='hk_source_bundle_id') THEN
            ALTER TABLE main_health_metrics DROP COLUMN hk_source_bundle_id;
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='health_metrics' AND column_name='hk_source_bundle_id') THEN
            ALTER TABLE health_metrics DROP COLUMN hk_source_bundle_id;
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='main_health_events' AND column_name='hk_source_bundle_id') THEN
            ALTER TABLE main_health_events DROP COLUMN hk_source_bundle_id;
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='health_events' AND column_name='hk_source_bundle_id') THEN
            ALTER TABLE health_events DROP COLUMN hk_source_bundle_id;
          END IF;

          -- Derived tables
          IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='derived_sleep_segments' AND column_name='hk_source_bundle_id') THEN
            ALTER TABLE derived_sleep_segments DROP COLUMN hk_source_bundle_id;
          END IF;

          IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='derived_workouts' AND column_name='hk_source_bundle_id') THEN
            ALTER TABLE derived_workouts DROP COLUMN hk_source_bundle_id;
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='all_workouts' AND column_name='hk_source_bundle_id') THEN
            ALTER TABLE all_workouts DROP COLUMN hk_source_bundle_id;
          END IF;
          IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='workouts' AND column_name='hk_source_bundle_id') THEN
            ALTER TABLE workouts DROP COLUMN hk_source_bundle_id;
          END IF;
        END$$;
        """
    )

    # Recreate workouts index on whichever table exists.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='derived_workouts') THEN
            CREATE INDEX IF NOT EXISTS idx_derived_workouts_user_start_desc
            ON derived_workouts (user_id, start_ts DESC)
            INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal, hk_sources, hk_metadata);
          ELSIF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='all_workouts') THEN
            CREATE INDEX IF NOT EXISTS idx_all_workouts_user_start_desc
            ON all_workouts (user_id, start_ts DESC)
            INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal, hk_sources, hk_metadata);
          ELSIF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='workouts') THEN
            CREATE INDEX IF NOT EXISTS idx_workouts_user_start_desc
            ON workouts (user_id, start_ts DESC)
            INCLUDE (workout_type, end_ts, duration_min, distance_km, energy_kcal, hk_sources, hk_metadata);
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    # Best-effort: we don't restore bundle ids.
    pass


