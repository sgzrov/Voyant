from typing import List, Optional
from pydantic import BaseModel, ConfigDict


# Request body for chat streaming endpoints (question + optional conversation/provider/model selectors)
class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    question: str
    conversation_id: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None


# Single chat message returned from history endpoints
class ChatMessageOut(BaseModel):
    id: int
    role: str
    content: str
    timestamp: Optional[str] = None


# Chat session summary row (conversation id + optional title + last active timestamp)
class ChatSessionOut(BaseModel):
    conversation_id: str
    title: Optional[str] = None
    last_active_date: Optional[str] = None


# Response payload for listing chat sessions
class ChatSessionsOut(BaseModel):
    sessions: List[ChatSessionOut]

