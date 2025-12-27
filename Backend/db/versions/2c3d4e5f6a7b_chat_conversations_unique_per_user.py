"""Make chat_conversations unique per user

Revision ID: 2c3d4e5f6a7b
Revises: 1a2b3c4d5e6f
Create Date: 2025-12-26

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "2c3d4e5f6a7b"
down_revision = "1a2b3c4d5e6f"
branch_labels = None
depends_on = None


def upgrade():
    # Previously: conversation_id was globally unique via a unique index.
    # Change to per-user uniqueness: UNIQUE(user_id, conversation_id).
    op.drop_index("ix_chat_conversations_conversation_id", table_name="chat_conversations")
    op.create_index(
        "ix_chat_conversations_conversation_id",
        "chat_conversations",
        ["conversation_id"],
        unique=False,
    )
    op.create_index(
        "ux_chat_conversations_user_id_conversation_id",
        "chat_conversations",
        ["user_id", "conversation_id"],
        unique=True,
    )


def downgrade():
    op.drop_index("ux_chat_conversations_user_id_conversation_id", table_name="chat_conversations")
    op.drop_index("ix_chat_conversations_conversation_id", table_name="chat_conversations")
    op.create_index(
        "ix_chat_conversations_conversation_id",
        "chat_conversations",
        ["conversation_id"],
        unique=True,
    )


