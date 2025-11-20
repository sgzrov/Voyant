"""
add feature tables for daily and session diagnostics

Revision ID: 9fb7
Revises: 9fb6
Create Date: 2025-11-20
"""
from alembic import op
import sqlalchemy as sa


revision = "9fb7"
down_revision = "9fb6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # health_daily_features
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_daily_features (
          user_id TEXT NOT NULL,
          day DATE NOT NULL,
          today_values JSONB,
          medians JSONB,
          deltas JSONB,
          zscores JSONB,
          flags JSONB,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          PRIMARY KEY (user_id, day)
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_daily_features_user_day ON health_daily_features(user_id, day DESC);")

    # health_session_features
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_session_features (
          session_id INTEGER PRIMARY KEY REFERENCES health_sessions(id) ON DELETE CASCADE,
          user_id TEXT NOT NULL,
          modality TEXT NOT NULL,
          duration_min DOUBLE PRECISION,
          distance_km DOUBLE PRECISION,
          avg_hr DOUBLE PRECISION,
          pace_drift_pct DOUBLE PRECISION,
          hr_drift_slope DOUBLE PRECISION,
          decoupling_pct DOUBLE PRECISION,
          time_to_fatigue_min DOUBLE PRECISION,
          kcal DOUBLE PRECISION,
          flags JSONB,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_session_features_user ON health_session_features(user_id);")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_session_features_user;")
    op.execute("DROP TABLE IF EXISTS health_session_features;")
    op.execute("DROP INDEX IF EXISTS idx_daily_features_user_day;")
    op.execute("DROP TABLE IF EXISTS health_daily_features;")


