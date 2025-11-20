"""drop health_overview_cache table (no longer used)

Revision ID: 9fb5
Revises: a1b2c3d4e5f6
Create Date: 2025-11-19
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9fb5'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop index first (if it exists), then drop the table
    op.execute("DROP INDEX IF EXISTS idx_overview_generated_at;")
    op.execute("DROP TABLE IF EXISTS health_overview_cache;")


def downgrade() -> None:
    # Recreate table and index if we roll back
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_overview_cache (
          user_id TEXT PRIMARY KEY,
          summary_json JSONB NOT NULL,
          summary_text TEXT,
          generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          inputs_version TEXT
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_overview_generated_at ON health_overview_cache(generated_at);")


