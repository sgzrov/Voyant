from typing import Optional
from pydantic import BaseModel

class ChatWithCIRequest(BaseModel):
    s3_url: str
    user_input: str
    conversation_id: Optional[str] = None
