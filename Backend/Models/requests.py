from typing import Optional
from pydantic import BaseModel

class ChatRequest(BaseModel):
    user_input: str
    conversation_id: Optional[str] = None
