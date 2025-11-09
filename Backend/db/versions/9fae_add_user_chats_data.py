"""create user_chats_data table

Revision ID: 9fae
Revises: 9fad
Create Date: 2025-11-09
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9fae'
down_revision = '9fad'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Use IF NOT EXISTS for idempotency across environments
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS user_chats_data (
            id SERIAL PRIMARY KEY,
            conversation_id VARCHAR(64) NOT NULL,
            user_id VARCHAR(64) NOT NULL,
            role VARCHAR(16) NOT NULL,
            content TEXT NOT NULL,
            timestamp TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_chats_conversation ON user_chats_data(conversation_id);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_chats_user ON user_chats_data(user_id);")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_chats_user;")
    op.execute("DROP INDEX IF EXISTS idx_chats_conversation;")
    op.execute("DROP TABLE IF EXISTS user_chats_data;")


