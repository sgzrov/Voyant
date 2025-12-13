"""add chat_conversations table for storing titles

Revision ID: 9fb8
Revises: 9fb7
Create Date: 2025-11-27

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '9fb8'
down_revision = '9fb7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'chat_conversations',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('conversation_id', sa.String(64), nullable=False),
        sa.Column('user_id', sa.String(64), nullable=False),
        sa.Column('title', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_chat_conversations_conversation_id', 'chat_conversations', ['conversation_id'], unique=True)
    op.create_index('ix_chat_conversations_user_id', 'chat_conversations', ['user_id'], unique=False)


def downgrade():
    op.drop_index('ix_chat_conversations_user_id', table_name='chat_conversations')
    op.drop_index('ix_chat_conversations_conversation_id', table_name='chat_conversations')
    op.drop_table('chat_conversations')

