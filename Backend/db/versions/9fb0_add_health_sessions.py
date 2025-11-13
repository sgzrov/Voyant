"""
add health_sessions and health_session_slices tables; ensure unique index on health_events

Revision ID: 9fb0
Revises: 9fab
Create Date: 2025-11-13
"""

from alembic import op
import sqlalchemy as sa


revision = "9fb0"
down_revision = "9fab"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # health_sessions: canonical session rows (HKWorkout or inferred)
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_sessions (
          id SERIAL PRIMARY KEY,
          user_id TEXT NOT NULL,
          session_type TEXT NOT NULL,           -- running, walking, cycling, swimming, hiking, strength, yoga, other
          source TEXT NOT NULL,                 -- 'workout' | 'inferred'
          external_id TEXT,                     -- HKWorkout UUID (string) when available
          activity_type TEXT,                   -- raw HK identifier when available
          start_ts TIMESTAMPTZ NOT NULL,
          end_ts TIMESTAMPTZ NOT NULL,
          duration_min DOUBLE PRECISION NOT NULL,
          distance_km DOUBLE PRECISION,
          energy_kcal DOUBLE PRECISION,
          avg_hr DOUBLE PRECISION,
          notes TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_health_sessions_user_start ON health_sessions(user_id, start_ts);")
    # Unique key for idempotent upsert when external_id is absent
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_health_sessions_user_type_start
        ON health_sessions(user_id, session_type, start_ts);
        """
    )
    # Provide a dedupe key if desired by external_id
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_health_sessions_user_source_external
        ON health_sessions(user_id, source, external_id)
        WHERE external_id IS NOT NULL;
        """
    )

    # health_session_slices: km/time splits per session
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_session_slices (
          id SERIAL,
          session_id INTEGER NOT NULL REFERENCES health_sessions(id) ON DELETE CASCADE,
          slice_index INTEGER NOT NULL,           -- km index or time index
          slice_type TEXT NOT NULL,               -- 'km' | 'min' | '5min' etc.
          start_ts TIMESTAMPTZ NOT NULL,
          end_ts TIMESTAMPTZ NOT NULL,
          distance_km DOUBLE PRECISION,
          duration_min DOUBLE PRECISION NOT NULL,
          pace_s_per_km DOUBLE PRECISION,         -- optional, for distance-based slices
          speed_m_s DOUBLE PRECISION,
          avg_hr DOUBLE PRECISION,
          kcal DOUBLE PRECISION,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
        PARTITION BY RANGE (start_ts);
        """
    )
    # Unique across partitions must include the partition key (start_ts)
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_session_slices_unique
        ON health_session_slices(session_id, slice_type, slice_index, start_ts);
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_session_slices_session ON health_session_slices(session_id);")
    # Default partition to catch all rows; monthly partitions can be added later without downtime
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_session_slices_default
        PARTITION OF health_session_slices DEFAULT;
        """
    )

    # Ensure unique index on health_events to support upsert policy
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_events_user_type_ts
        ON health_events (user_id, event_type, timestamp);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ux_health_sessions_user_type_start;")
    op.execute("DROP INDEX IF EXISTS ux_events_user_type_ts;")
    op.execute("DROP TABLE IF EXISTS health_session_slices_default;")
    op.execute("DROP INDEX IF EXISTS idx_session_slices_session;")
    op.execute("DROP INDEX IF EXISTS ux_session_slices_unique;")
    op.execute("DROP TABLE IF EXISTS health_session_slices;")
    op.execute("DROP INDEX IF EXISTS ux_health_sessions_user_source_external;")
    op.execute("DROP INDEX IF EXISTS idx_health_sessions_user_start;")
    op.execute("DROP TABLE IF EXISTS health_sessions;")


