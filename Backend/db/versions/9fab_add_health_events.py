"""add health_events table for workouts/events

Revision ID: 9fab
Revises: 9f9a
Create Date: 2025-11-08
"""

from alembic import op
import sqlalchemy as sa


revision = '9fab'
down_revision = '9f9a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_events (
          id SERIAL PRIMARY KEY,
          user_id TEXT NOT NULL,
          timestamp TIMESTAMPTZ NOT NULL,
          event_type TEXT NOT NULL,
          value DOUBLE PRECISION NOT NULL,
          unit TEXT,
          source TEXT,
          meta JSONB,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_events_user_ts ON health_events(user_id, timestamp);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_events_user_type_ts ON health_events(user_id, event_type, timestamp);")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_events_user_type_ts;")
    op.execute("DROP INDEX IF EXISTS idx_events_user_ts;")
    op.execute("DROP TABLE IF EXISTS health_events;")


