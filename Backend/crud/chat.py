from sqlalchemy import func
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
    session.flush()
    try:
        session.refresh(msg)
    except Exception:
        pass
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
        # Use a nested transaction so an IntegrityError here doesn't blow away the caller's transaction.
        try:
            with session.begin_nested():
                session.add(conv)
                session.flush()
                try:
                    session.refresh(conv)
                except Exception:
                    pass
        except IntegrityError:
            # Another request likely created it concurrently for the same user_id.
            existing = session.query(ChatConversation).filter_by(conversation_id=conversation_id, user_id=user_id).first()
            return existing

    return conv


# Update the title of a conversation
def update_conversation_title(session, conversation_id, user_id, title):
    conv = session.query(ChatConversation).filter_by(conversation_id=conversation_id, user_id=user_id).first()
    if not conv:
        conv = get_or_create_conversation(session, conversation_id, user_id)
        if not conv:
            return None
    conv.title = title
    session.flush()
    try:
        session.refresh(conv)
    except Exception:
        pass
    return conv


# Get chat sessions for a user with last-active timestamps + titles
def get_chat_sessions(session, user_id):
    subquery = (
        session.query(
            ChatData.conversation_id,
            func.max(ChatData.timestamp).label("last_message_at"),
        )
        .filter(ChatData.user_id == user_id)
        .group_by(ChatData.conversation_id)
        .subquery()
    )

    latest_messages = (
        session.query(ChatData)
        .join(
            subquery,
            (ChatData.conversation_id == subquery.c.conversation_id) & (ChatData.timestamp == subquery.c.last_message_at),
        )
        .filter(ChatData.user_id == user_id)
        .all()
    )

    conversations = session.query(ChatConversation).filter_by(user_id=user_id).all()
    titles = {conv.conversation_id: conv.title for conv in conversations}
    return [
        {
            "conversation_id": msg.conversation_id,
            "title": titles.get(msg.conversation_id),
            "last_active_date": msg.timestamp.isoformat() if msg.timestamp else None,
        }
        for msg in latest_messages
    ]


