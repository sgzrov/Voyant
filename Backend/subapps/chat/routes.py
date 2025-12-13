import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import func

from Backend.auth import verify_clerk_jwt
from Backend.crud.chat import create_chat_message, get_all_conversation_titles, get_chat_history
from Backend.database import SessionLocal
from Backend.models.chat_data_model import ChatData
from Backend.subapps.health.routes import health_query_stream


router = APIRouter()


def _generate_conversation_id(existing_conversation_id: Optional[str] = None) -> str:
    if existing_conversation_id:
        return existing_conversation_id
    return str(uuid.uuid4())


@router.get("/chat/retrieve-chat-sessions/")
def get_chat_sessions(request: Request):
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    db_session = SessionLocal()
    try:
        subquery = (
            db_session.query(
                ChatData.conversation_id,
                func.max(ChatData.timestamp).label("last_message_at"),
            )
            .filter(ChatData.user_id == user_id)
            .group_by(ChatData.conversation_id)
            .subquery()
        )

        latest_messages = (
            db_session.query(ChatData)
            .join(
                subquery,
                (ChatData.conversation_id == subquery.c.conversation_id)
                & (ChatData.timestamp == subquery.c.last_message_at),
            )
            .filter(ChatData.user_id == user_id)
            .all()
        )

        titles = get_all_conversation_titles(db_session, user_id)

        sessions_data = [
            {
                "conversation_id": msg.conversation_id,
                "title": titles.get(msg.conversation_id),
                "last_active_date": msg.timestamp.isoformat() if msg.timestamp else None,
            }
            for msg in latest_messages
        ]
        return {"sessions": sessions_data}
    finally:
        db_session.close()


@router.get("/chat/all-messages/{conversation_id}")
def get_all_chat_messages(conversation_id: str, request: Request):
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    db_session = SessionLocal()
    try:
        messages = get_chat_history(db_session, conversation_id, user_id)
        return [
            {
                "id": m.id,
                "role": m.role,
                "content": m.content,
                "timestamp": m.timestamp.isoformat() if m.timestamp else None,
            }
            for m in messages
        ]
    finally:
        db_session.close()


@router.post("/chat/add-message/")
def add_chat_message(
    conversation_id: Optional[str] = None,
    role: str = "",
    content: str = "",
    request: Request = None,
):
    if conversation_id is not None and conversation_id.strip() == "":
        raise HTTPException(status_code=400, detail="conversation_id cannot be empty string")

    original_conversation_id = conversation_id
    conversation_id = _generate_conversation_id(conversation_id)
    is_new_conversation = original_conversation_id is None

    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    db_session = SessionLocal()
    try:
        msg = create_chat_message(db_session, conversation_id, user_id, role, content)
        response = {
            "id": msg.id,
            "conversation_id": conversation_id,
            "user_id": msg.user_id,
            "role": msg.role,
            "content": msg.content,
            "timestamp": msg.timestamp.isoformat() if msg.timestamp else None,
        }
        if is_new_conversation:
            response["new_conversation"] = True
        return response
    finally:
        db_session.close()


@router.post("/chat/stream")
async def chat_stream(payload: dict, request: Request):
    """Preferred route (alias) for streaming chat with optional tool calls."""
    return await health_query_stream(payload, request)


