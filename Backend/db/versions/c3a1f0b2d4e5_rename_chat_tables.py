"""Rename chat tables to chat_sessions and chat_messages.

Renames:
- chat_conversations -> chat_sessions
- user_chats_data -> chat_messages

Also recreates/aligns indexes to match SQLAlchemy's default naming and the updated models.
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "c3a1f0b2d4e5"
down_revision = "l6m7n8o9p0q1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ---- chat_conversations -> chat_sessions ----
    # Drop existing indexes (best-effort) so we can recreate them with new names after the rename.
    op.execute("DROP INDEX IF EXISTS ux_chat_conversations_user_id_conversation_id;")
    op.execute("DROP INDEX IF EXISTS ix_chat_conversations_conversation_id;")
    op.execute("DROP INDEX IF EXISTS ix_chat_conversations_user_id;")

    op.rename_table("chat_conversations", "chat_sessions")

    # Recreate indexes under the new table name.
    op.create_index("ix_chat_sessions_conversation_id", "chat_sessions", ["conversation_id"], unique=False)
    op.create_index("ix_chat_sessions_user_id", "chat_sessions", ["user_id"], unique=False)
    op.create_index(
        "ux_chat_sessions_user_id_conversation_id",
        "chat_sessions",
        ["user_id", "conversation_id"],
        unique=True,
    )

    # ---- user_chats_data -> chat_messages ----
    # Prior migration created these with raw SQL, so drop best-effort.
    op.execute("DROP INDEX IF EXISTS idx_chats_conversation;")
    op.execute("DROP INDEX IF EXISTS idx_chats_user;")

    op.rename_table("user_chats_data", "chat_messages")

    # Align with SQLAlchemy index=True defaults on the updated model.
    op.create_index("ix_chat_messages_conversation_id", "chat_messages", ["conversation_id"], unique=False)
    op.create_index("ix_chat_messages_user_id", "chat_messages", ["user_id"], unique=False)


def downgrade() -> None:
    # ---- chat_messages -> user_chats_data ----
    op.execute("DROP INDEX IF EXISTS ix_chat_messages_user_id;")
    op.execute("DROP INDEX IF EXISTS ix_chat_messages_conversation_id;")

    op.rename_table("chat_messages", "user_chats_data")

    op.execute("CREATE INDEX IF NOT EXISTS idx_chats_conversation ON user_chats_data(conversation_id);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_chats_user ON user_chats_data(user_id);")

    # ---- chat_sessions -> chat_conversations ----
    op.execute("DROP INDEX IF EXISTS ux_chat_sessions_user_id_conversation_id;")
    op.execute("DROP INDEX IF EXISTS ix_chat_sessions_user_id;")
    op.execute("DROP INDEX IF EXISTS ix_chat_sessions_conversation_id;")

    op.rename_table("chat_sessions", "chat_conversations")

    op.create_index("ix_chat_conversations_conversation_id", "chat_conversations", ["conversation_id"], unique=False)
    op.create_index("ix_chat_conversations_user_id", "chat_conversations", ["user_id"], unique=False)
    op.create_index(
        "ux_chat_conversations_user_id_conversation_id",
        "chat_conversations",
        ["user_id", "conversation_id"],
        unique=True,
    )


