"""Drop hk_metadata from derived_sleep_segments.

We keep sleep-specific timezone/context in `meta` and provenance in hk_source_* columns.
"""

from alembic import op


revision = "f6a7b8c9d0e1"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Best-effort: column may not exist in fresh setups after migration edits.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='derived_sleep_segments'
              AND column_name='hk_metadata'
          ) THEN
            ALTER TABLE derived_sleep_segments DROP COLUMN hk_metadata;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    # Best-effort rollback.
    op.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='derived_sleep_segments'
              AND column_name='hk_metadata'
          ) THEN
            ALTER TABLE derived_sleep_segments ADD COLUMN hk_metadata JSONB;
          END IF;
        END$$;
        """
    )


