from __future__ import annotations

import uuid
from typing import Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from Backend.crud.chat import get_chat_history, get_chat_sessions
from Backend.schemas.chat import ChatMessageOut, ChatRequest, ChatSessionsOut
from Backend.services.chat_stream import DEFAULT_MODEL, build_agent_stream_response
from Backend.services.tools.sql_gen_tool import execute_sql_gen_tool, localize_health_rows, TOOL_SPEC


class ChatService:
    # Initializes the service with a DB session used by CRUD helpers and streaming.
    def __init__(self, db: Session):
        self.db = db

    # Ensures we have a conversation id
    @staticmethod
    def _generate_conversation_id(existing_conversation_id: Optional[str] = None) -> str:
        if existing_conversation_id:
            return existing_conversation_id
        return str(uuid.uuid4())

    # Validates input, wires tool handlers, then delegates SSE streaming to `build_agent_stream_response()`.
    async def stream_tool_sql(self, *, payload: ChatRequest, user_id: str, user_tz: str):
        question = payload.question
        conversation_id = payload.conversation_id
        provider = payload.provider

        if not isinstance(provider, str) or not provider.strip():
            raise HTTPException(status_code=400, detail="Missing provider")
        provider = provider.strip().lower()

        answer_model = payload.model or DEFAULT_MODEL.get(provider) or ""
        if not answer_model:
            raise HTTPException(status_code=400, detail=f"No default model for provider: {provider}")

        if conversation_id is not None and isinstance(conversation_id, str) and conversation_id.strip() == "":
            raise HTTPException(status_code=400, detail="conversation_id cannot be empty string")
        if not conversation_id:
            conversation_id = self._generate_conversation_id(conversation_id)

        if not isinstance(question, str) or not question.strip():
            raise HTTPException(status_code=400, detail="Missing question")

        def _localize_ctx_inplace(ctx: object) -> object:
            try:
                if isinstance(ctx, dict) and isinstance(ctx.get("sql"), dict):
                    rows = ctx["sql"].get("rows")
                    if isinstance(rows, list):
                        ctx["sql"]["rows"] = localize_health_rows(rows, user_tz)
            except Exception:
                pass
            return ctx

        async def _prefetch():
            ctx = await execute_sql_gen_tool(user_id=user_id, question=question, tz_name=user_tz)
            return _localize_ctx_inplace(ctx)

        async def _health_tool_handler(args: dict):
            q = args.get("question")
            if not isinstance(q, str) or not q.strip():
                q = question
            ctx = await execute_sql_gen_tool(user_id=user_id, question=q, tz_name=user_tz)
            return _localize_ctx_inplace(ctx)

        return build_agent_stream_response(
            user_id=user_id,
            conversation_id=conversation_id,
            question=question,
            provider=provider,
            answer_model=answer_model,
            tools=[TOOL_SPEC],
            tool_handlers={"fetch_health_context": _health_tool_handler},
            tool_prefetch=_prefetch,
            db_session=self.db,
        )

    def list_sessions(self, *, user_id: str) -> ChatSessionsOut:
        sessions = get_chat_sessions(self.db, user_id)
        return ChatSessionsOut(sessions=sessions)

    def list_messages(self, *, conversation_id: str, user_id: str) -> list[ChatMessageOut]:
        messages = get_chat_history(self.db, conversation_id, user_id)
        return [
            ChatMessageOut(
                id=m.id,
                role=m.role,
                content=m.content,
                timestamp=m.timestamp.isoformat() if getattr(m, "timestamp", None) else None,
            )
            for m in messages
        ]