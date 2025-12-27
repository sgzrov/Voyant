from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from Backend.database import get_db
from Backend.auth import verify_clerk_jwt
from Backend.schemas.chat import ChatMessageOut, ChatRequest, ChatSessionsOut
from Backend.services.chat_service import ChatService


router = APIRouter()


def _get_user_tz(request: Request) -> str:
    return request.headers.get("x-user-tz") or "UTC"


# Streams a chat conversation with an optional health-SQL tool call
@router.post("/chat/tool-sql/stream")
async def chat_tool_sql_stream(
    payload: ChatRequest,
    request: Request,
    user_tz: str = Depends(_get_user_tz),
    db: Session = Depends(get_db),
):
    svc = ChatService(db)
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    return await svc.stream_tool_sql(payload=payload, user_id=user_id, user_tz=user_tz)


# Retrieves all chat sessions for a user
@router.get("/chat/retrieve-chat-sessions/")
def retrieve_chat_sessions(
    request: Request,
    db: Session = Depends(get_db),
) -> ChatSessionsOut:
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    svc = ChatService(db)
    return svc.list_sessions(user_id=user_id)


# Retrieves all messages for a specific conversation
@router.get("/chat/all-messages/{conversation_id}")
def get_all_chat_messages(
    conversation_id: str,
    request: Request,
    db: Session = Depends(get_db),
) -> list[ChatMessageOut]:
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    svc = ChatService(db)
    return svc.list_messages(conversation_id=conversation_id, user_id=user_id)