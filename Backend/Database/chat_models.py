from sqlalchemy import Column, Integer, String, Text, DateTime, func

from .db import Base

class ChatsDB(Base):
    __tablename__ = 'user_chats_data'
    id = Column(Integer, primary_key = True, autoincrement = True)
    conversation_id = Column(String(64), index = True, nullable = False)
    user_id = Column(String(64), index = True, nullable = False)
    role = Column(String(16), nullable = False)
    content = Column(Text, nullable = False)
    timestamp = Column(DateTime(timezone = True), server_default = func.now())
