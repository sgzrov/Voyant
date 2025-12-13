from typing import Optional
from pydantic import BaseModel

# Chat request schema
class ChatRequest(BaseModel):
    user_input: str
    conversation_id: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None


