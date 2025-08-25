from typing import Optional
from pydantic import BaseModel

class StudySummaryRequest(BaseModel):
    text: str
    study_id: Optional[str] = None

class SimpleChatRequest(BaseModel):
    user_input: str
    conversation_id: Optional[str] = None

class ChatWithRAGRequest(BaseModel):
    s3_url: str
    user_input: str
    conversation_id: Optional[str] = None

class StudyOutcomeRequest(BaseModel):
    s3_url: str
    text: str
    study_id: Optional[str] = None
