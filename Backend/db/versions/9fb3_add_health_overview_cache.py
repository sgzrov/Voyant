"""add health_overview_cache table

Revision ID: 9fb3
Revises: c610270318a2
Create Date: 2025-11-17
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9fb3'
down_revision = 'c610270318a2'
branch_labels = None
depends_on = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_overview_generated_at;")
    op.execute("DROP TABLE IF EXISTS health_overview_cache;")




