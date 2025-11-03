from .chat_models import ChatsDB

from .db import Base, engine

Base.metadata.create_all(
    bind = engine,
    tables = [ChatsDB.__table__]
)