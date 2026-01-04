"""Recreate sleep tables so meta physically precedes hk_sources.

Postgres cannot reorder columns in-place. This migration recreates:
- derived_sleep_daily
- derived_sleep_segments

…copying data over and recreating PK/indexes so the on-disk column order matches
the project convention: meta then hk_sources.
"""

from alembic import op


revision = "b8c9d0e1f2a3"
down_revision = "a7b8c9d0e1f2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name='derived_sleep_daily'
          ) THEN
            -- Rename old table out of the way.
            IF EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema='public' AND table_name='derived_sleep_daily__old'
            ) THEN
              DROP TABLE derived_sleep_daily__old;
            END IF;
            ALTER TABLE derived_sleep_daily RENAME TO derived_sleep_daily__old;

            -- Constraint/index names are global within a schema; rename the old ones so we can reuse the canonical names.
            IF EXISTS (
              SELECT 1 FROM pg_constraint WHERE conname = 'pk_derived_sleep_daily'
            ) THEN
              ALTER TABLE derived_sleep_daily__old RENAME CONSTRAINT pk_derived_sleep_daily TO pk_derived_sleep_daily__old;
            END IF;
            IF EXISTS (
              SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_derived_sleep_daily_user_date_desc'
            ) THEN
              ALTER INDEX idx_derived_sleep_daily_user_date_desc RENAME TO idx_derived_sleep_daily_user_date_desc__old;
            END IF;

            -- Recreate with desired column order (CURRENT schema: no in_bed/asleep_unspecified).
            CREATE TABLE derived_sleep_daily (
              user_id TEXT NOT NULL,
              sleep_date DATE NOT NULL,
              sleep_start_ts TIMESTAMPTZ,
              sleep_end_ts TIMESTAMPTZ,
              asleep_minutes DOUBLE PRECISION,
              rem_minutes DOUBLE PRECISION,
              core_minutes DOUBLE PRECISION,
              deep_minutes DOUBLE PRECISION,
              awake_minutes DOUBLE PRECISION,
              meta JSONB,
              hk_sources JSONB,
              CONSTRAINT pk_derived_sleep_daily PRIMARY KEY (user_id, sleep_date)
            );

            -- Recreate index.
            CREATE INDEX IF NOT EXISTS idx_derived_sleep_daily_user_date_desc
            ON derived_sleep_daily (user_id, sleep_date);

            -- Copy data (handle older envs that might still have legacy columns gracefully).
            INSERT INTO derived_sleep_daily (
              user_id, sleep_date, sleep_start_ts, sleep_end_ts,
              asleep_minutes, rem_minutes, core_minutes, deep_minutes, awake_minutes,
              meta, hk_sources
            )
            SELECT
              user_id, sleep_date, sleep_start_ts, sleep_end_ts,
              asleep_minutes, rem_minutes, core_minutes, deep_minutes, awake_minutes,
              meta, hk_sources
            FROM derived_sleep_daily__old;

            DROP TABLE derived_sleep_daily__old;
          END IF;
        END$$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name='derived_sleep_segments'
          ) THEN
            IF EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema='public' AND table_name='derived_sleep_segments__old'
            ) THEN
              DROP TABLE derived_sleep_segments__old;
            END IF;
            ALTER TABLE derived_sleep_segments RENAME TO derived_sleep_segments__old;

            -- Rename old constraint/indexes to avoid name collisions.
            IF EXISTS (
              SELECT 1 FROM pg_constraint WHERE conname = 'pk_derived_sleep_segments'
            ) THEN
              ALTER TABLE derived_sleep_segments__old RENAME CONSTRAINT pk_derived_sleep_segments TO pk_derived_sleep_segments__old;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_derived_sleep_segments_user_date') THEN
              ALTER INDEX idx_derived_sleep_segments_user_date RENAME TO idx_derived_sleep_segments_user_date__old;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_derived_sleep_segments_user_start_desc') THEN
              ALTER INDEX idx_derived_sleep_segments_user_start_desc RENAME TO idx_derived_sleep_segments_user_start_desc__old;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_class WHERE relkind='i' AND relname='idx_derived_sleep_segments_user_date_stage') THEN
              ALTER INDEX idx_derived_sleep_segments_user_date_stage RENAME TO idx_derived_sleep_segments_user_date_stage__old;
            END IF;

            -- Recreate with desired column order and CURRENT schema (no hk_metadata).
            CREATE TABLE derived_sleep_segments (
              user_id TEXT NOT NULL,
              hk_uuid TEXT NOT NULL,
              sleep_date DATE NOT NULL,
              stage TEXT NOT NULL,
              segment_start_ts TIMESTAMPTZ NOT NULL,
              segment_end_ts TIMESTAMPTZ NOT NULL,
              minutes DOUBLE PRECISION NOT NULL,
              hk_source_bundle_id TEXT,
              hk_source_name TEXT,
              hk_source_version TEXT,
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
              hk_source_bundle_id, hk_source_name, hk_source_version,
              meta, hk_sources
            )
            SELECT
              user_id, hk_uuid, sleep_date, stage, segment_start_ts, segment_end_ts, minutes,
              hk_source_bundle_id, hk_source_name, hk_source_version,
              meta, hk_sources
            FROM derived_sleep_segments__old;

            DROP TABLE derived_sleep_segments__old;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    # Best-effort no-op: reverting physical column order is not required for correctness.
    pass


