"""Add hk_sources JSONB to derived_sleep_segments (and backfill).

Keeps provenance consistent with other derived tables: hk_sources is a JSONB array like:
  [{"name":"Stephan’s Apple Watch","version":"11.6.1"}]
"""

from alembic import op


revision = "a7b8c9d0e1f2"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add column (best-effort if it already exists).
    op.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='derived_sleep_segments'
              AND column_name='hk_sources'
          ) THEN
            ALTER TABLE derived_sleep_segments ADD COLUMN hk_sources JSONB;
          END IF;
        END$$;
        """
    )

    # Backfill from hk_source_name/version where possible.
    op.execute(
        """
        UPDATE derived_sleep_segments
        SET hk_sources = CASE
          WHEN hk_source_name IS NOT NULL
            THEN jsonb_build_array(jsonb_build_object('name', hk_source_name, 'version', hk_source_version))
          ELSE '[]'::jsonb
        END
        WHERE hk_sources IS NULL;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='derived_sleep_segments'
              AND column_name='hk_sources'
          ) THEN
            ALTER TABLE derived_sleep_segments DROP COLUMN hk_sources;
          END IF;
        END$$;
        """
    )


