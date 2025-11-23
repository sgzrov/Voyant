"""drop health_sessions and health_session_slices tables

Revision ID: d1e2f3g4h5i6
Revises: e2b1c4d5f6a7
Create Date: 2025-11-23
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "d1e2f3g4h5i6"
down_revision: Union[str, Sequence[str], None] = "e2b1c4d5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop dependent indexes first if they exist, then tables
    op.execute("DROP TABLE IF EXISTS health_session_slices;")
    op.execute("DROP TABLE IF EXISTS health_sessions;")


def downgrade() -> None:
    # Recreate minimal schemas on downgrade (without partitions/features)
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_sessions (
          id SERIAL PRIMARY KEY,
          user_id TEXT NOT NULL,
          session_type TEXT NOT NULL,
          source TEXT,
          external_id TEXT,
          start_ts TIMESTAMPTZ NOT NULL,
          end_ts TIMESTAMPTZ,
          duration_min DOUBLE PRECISION,
          distance_km DOUBLE PRECISION,
          energy_kcal DOUBLE PRECISION,
          avg_hr DOUBLE PRECISION
        );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_session_slices (
          session_id INTEGER NOT NULL,
          slice_index INTEGER NOT NULL,
          slice_type TEXT NOT NULL,
          start_ts TIMESTAMPTZ NOT NULL,
          end_ts TIMESTAMPTZ NOT NULL,
          distance_km DOUBLE PRECISION,
          duration_min DOUBLE PRECISION,
          pace_s_per_km DOUBLE PRECISION,
          speed_m_s DOUBLE PRECISION,
          avg_hr DOUBLE PRECISION,
          kcal DOUBLE PRECISION,
          PRIMARY KEY (session_id, slice_type, slice_index, start_ts)
        );
        """
    )

