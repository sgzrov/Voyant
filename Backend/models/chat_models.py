from sqlalchemy import Column, DateTime, Index, Integer, String, Text, func

from Backend.database import Base


# Stores conversation-level metadata (e.g. title)
class ChatConversation(Base):
    __tablename__ = "chat_conversations"

    # DB-level uniqueness is per-user: (user_id, conversation_id)
    __table_args__ = (
        Index("ux_chat_conversations_user_id_conversation_id", "user_id", "conversation_id", unique=True),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(String(64), index=True, nullable=False)
    user_id = Column(String(64), index=True, nullable=False)
    title = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# Stores individual chat messages
class ChatData(Base):
    __tablename__ = "user_chats_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(String(64), index=True, nullable=False)
    user_id = Column(String(64), index=True, nullable=False)
    role = Column(String(16), nullable=False)
    content = Column(Text, nullable=False)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


