from sqlalchemy.exc import IntegrityError

from Backend.models.chat_data_model import ChatData
from Backend.models.chat_conversation_model import ChatConversation

# Create a new chat message
def create_chat_message(session, conversation_id, user_id, role, content):
    msg = ChatData(
        conversation_id = conversation_id,
        user_id = user_id,
        role = role,
        content = content
    )
    session.add(msg)
    session.commit()
    session.refresh(msg)
    return msg

# Get chat history for a conversation
def get_chat_history(session, conversation_id, user_id):
    return (
        session.query(ChatData)
        .filter_by(conversation_id=conversation_id, user_id=user_id)
        .order_by(ChatData.timestamp, ChatData.id)
        .all()
    )

# Get existing conversation or create a new one
def get_or_create_conversation(session, conversation_id, user_id):
    conv = session.query(ChatConversation).filter_by(
        conversation_id=conversation_id,
        user_id=user_id
    ).first()

    if not conv:
        conv = ChatConversation(conversation_id=conversation_id, user_id=user_id, title=None)
        session.add(conv)
        try:
            session.commit()
            session.refresh(conv)
        except IntegrityError:  # Another request likely created it concurrently OR the conversation_id already exists (potentially for another user). Roll back and re-query safely.
            session.rollback()
            existing = session.query(ChatConversation).filter_by(conversation_id=conversation_id).first()
            if existing and existing.user_id == user_id:
                return existing
            return None

    return conv

# Update the title of a conversation
def update_conversation_title(session, conversation_id, user_id, title):
    conv = session.query(ChatConversation).filter_by(conversation_id=conversation_id, user_id=user_id).first()
    if not conv:
        conv = get_or_create_conversation(session, conversation_id, user_id)
        if not conv:
            return None
    conv.title = title
    session.commit()
    session.refresh(conv)
    return conv

# Get the title of a conversation, or None if not set
def get_conversation_title(session, conversation_id, user_id):
    conv = session.query(ChatConversation).filter_by(
        conversation_id=conversation_id,
        user_id=user_id
    ).first()
    return conv.title if conv else None

# Get all conversation titles for a user as a dict
def get_all_conversation_titles(session, user_id):
    conversations = session.query(ChatConversation).filter_by(user_id=user_id).all()
    return {conv.conversation_id: conv.title for conv in conversations}


