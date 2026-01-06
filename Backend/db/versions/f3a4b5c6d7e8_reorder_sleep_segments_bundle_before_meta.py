"""Rebuild derived_sleep_segments to place hk_source_bundle_id before meta.

Postgres cannot reorder columns in-place. We recreate the table and copy data.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "f3a4b5c6d7e8"
down_revision: Union[str, Sequence[str], None] = "e2f3a4b5c6d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          -- If a previous attempt already renamed the table, continue from that state.
          IF EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'derived_sleep_segments'
          ) THEN
            -- If __old exists, don't overwrite it (would lose data). Bail.
            IF EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = 'public' AND table_name = 'derived_sleep_segments__old'
            ) THEN
              RAISE EXCEPTION 'derived_sleep_segments__old already exists; previous migration attempt likely failed mid-way. Fix state before retry.';
            END IF;
            ALTER TABLE derived_sleep_segments RENAME TO derived_sleep_segments__old;
          ELSIF NOT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'derived_sleep_segments__old'
          ) THEN
            -- Nothing to do (neither table exists).
            RETURN;
          END IF;

          -- At this point, __old must exist. Rename PK constraint/index so we can reuse the canonical names.
          IF EXISTS (SELECT 1 FROM pg_class WHERE relname='pk_derived_sleep_segments') THEN
            ALTER INDEX pk_derived_sleep_segments RENAME TO pk_derived_sleep_segments__old;
          END IF;
          IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname='pk_derived_sleep_segments') THEN
            ALTER TABLE derived_sleep_segments__old RENAME CONSTRAINT pk_derived_sleep_segments TO pk_derived_sleep_segments__old;
          END IF;

          -- Rename secondary indexes if present so we can recreate them cleanly.
          IF EXISTS (SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_derived_sleep_segments_user_date') THEN
            ALTER INDEX idx_derived_sleep_segments_user_date RENAME TO idx_derived_sleep_segments_user_date__old;
          END IF;
          IF EXISTS (SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_derived_sleep_segments_user_start_desc') THEN
            ALTER INDEX idx_derived_sleep_segments_user_start_desc RENAME TO idx_derived_sleep_segments_user_start_desc__old;
          END IF;
          IF EXISTS (SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_derived_sleep_segments_user_date_stage') THEN
            ALTER INDEX idx_derived_sleep_segments_user_date_stage RENAME TO idx_derived_sleep_segments_user_date_stage__old;
          END IF;

          -- Recreate table with desired column order.
          CREATE TABLE derived_sleep_segments (
            user_id TEXT NOT NULL,
            hk_uuid TEXT NOT NULL,
            sleep_date DATE NOT NULL,
            stage TEXT NOT NULL,
            segment_start_ts TIMESTAMPTZ NOT NULL,
            segment_end_ts TIMESTAMPTZ NOT NULL,
            minutes DOUBLE PRECISION NOT NULL,
            hk_source_bundle_id TEXT,
            meta JSONB,
            hk_sources JSONB,
            CONSTRAINT pk_derived_sleep_segments PRIMARY KEY (user_id, hk_uuid)
          );

          CREATE INDEX IF NOT EXISTS idx_derived_sleep_segments_user_date
          ON derived_sleep_segments (user_id, sleep_date);

          CREATE INDEX IF NOT EXISTS idx_derived_sleep_segments_user_start_desc
          ON derived_sleep_segments (user_id, segment_start_ts);

          CREATE INDEX IF NOT EXISTS idx_derived_sleep_segments_user_date_stage
          ON derived_sleep_segments (user_id, sleep_date, stage);

          INSERT INTO derived_sleep_segments (
            user_id, hk_uuid, sleep_date, stage, segment_start_ts, segment_end_ts, minutes,
            hk_source_bundle_id, meta, hk_sources
          )
          SELECT
            user_id, hk_uuid, sleep_date, stage, segment_start_ts, segment_end_ts, minutes,
            hk_source_bundle_id, meta, hk_sources
          FROM derived_sleep_segments__old;

          DROP TABLE derived_sleep_segments__old;
        END$$;
        """
    )


def downgrade() -> None:
    # Best-effort no-op: reverting physical column order is not required for correctness.
    pass


