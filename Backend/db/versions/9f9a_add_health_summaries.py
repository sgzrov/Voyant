"""add health_summaries table with pgvector

Revision ID: 9f9a
Revises: 9091e2221e96
Create Date: 2025-11-07
"""

from alembic import op
import sqlalchemy as sa


revision = '9f9a'
down_revision = '9091e2221e96'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS health_summaries (
          id SERIAL PRIMARY KEY,
          user_id TEXT NOT NULL,
          summary_type TEXT NOT NULL,
          start_date DATE NOT NULL,
          end_date DATE NOT NULL,
          summary_text TEXT NOT NULL,
          embedding VECTOR(1536),
          metrics JSONB,
          created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
    )
    op.create_index('idx_user', 'health_summaries', ['user_id'])
    op.execute("CREATE INDEX IF NOT EXISTS idx_vector ON health_summaries USING ivfflat (embedding vector_cosine_ops);")


def downgrade() -> None:
    op.drop_index('idx_user', table_name='health_summaries')
    op.drop_table('health_summaries')


