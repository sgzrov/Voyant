import logging
import uuid
from typing import Optional
from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import func

from Backend.Database.db import SessionLocal
from Backend.Database.chat_repository import get_chat_history, create_chat_message
from Backend.Database.chat_models import ChatsDB
from Backend.auth import verify_clerk_jwt

router = APIRouter(prefix="/chat", tags=["chat"])

logger = logging.getLogger(__name__)

# Generate a conversation id, or use an existing one if provided
def generate_conversation_id(existing_conversation_id: Optional[str] = None) -> str:
    if existing_conversation_id:
        return existing_conversation_id
    return str(uuid.uuid4())

# Retrieve all chat sessions for a user
@router.get("/retrieve-chat-sessions/")
def get_chat_sessions(request: Request):
    user = verify_clerk_jwt(request)
    user_id = user['sub']
    db_session = SessionLocal()
    try:
        # Get unique conversation IDs and their last message timestamps
        # Get the latest message for each conversation
        subquery = db_session.query(
            ChatsDB.conversation_id,
            func.max(ChatsDB.timestamp).label('last_message_at')
        ).filter(ChatsDB.user_id == user_id).group_by(ChatsDB.conversation_id).subquery()

        # Get the actual messages with the latest timestamps
        latest_messages = db_session.query(ChatsDB).join(
            subquery,
            (ChatsDB.conversation_id == subquery.c.conversation_id) &
            (ChatsDB.timestamp == subquery.c.last_message_at)
        ).filter(ChatsDB.user_id == user_id).all()

        sessions_data = [
            {
                "conversation_id": msg.conversation_id,
                "last_active_date": msg.timestamp.isoformat() if msg.timestamp else None
            }
            for msg in latest_messages
        ]
        logger.info(f"[DEBUG] Returning {len(sessions_data)} sessions for user {user_id}")
        for session_data in sessions_data:
            logger.info(f"[DEBUG] Session {session_data['conversation_id']}: last_active_date = {session_data['last_active_date']}")
        return {"sessions": sessions_data}
    finally:
        db_session.close()

# Retrieve the full chat history for a given conversation and user
@router.get("/all-messages/{conversation_id}")
def get_all_chat_messages(conversation_id: str, request: Request):
    user = verify_clerk_jwt(request)
    user_id = user['sub']
    db_session = SessionLocal()
    try:
        messages = get_chat_history(db_session, conversation_id, user_id)
        return [
            {
                "id": m.id,
                "role": m.role,
                "content": m.content,
                "timestamp": m.timestamp.isoformat() if m.timestamp else None
            }
            for m in messages
        ]
    finally:
        db_session.close()

# Add a message for a given conversation and user
@router.post("/add-message/")
def add_chat_message(conversation_id: Optional[str] = None, role: str = '', content: str = '', request: Request = None):
    if conversation_id is not None and conversation_id.strip() == '':
        raise HTTPException(status_code = 400, detail = "conversation_id cannot be empty string")

    original_conversation_id = conversation_id
    conversation_id = generate_conversation_id(conversation_id)
    is_new_conversation = original_conversation_id is None

    user = verify_clerk_jwt(request)
    user_id = user['sub']
    db_session = SessionLocal()
    try:
        msg = create_chat_message(db_session, conversation_id, user_id, role, content)
        response = {
            "id": msg.id,
            "conversation_id": conversation_id,
            "user_id": msg.user_id,
            "role": msg.role,
            "content": msg.content,
            "timestamp": msg.timestamp.isoformat() if msg.timestamp else None
        }
        if is_new_conversation:
            response["new_conversation"] = True
        return response
    finally:
        db_session.close()