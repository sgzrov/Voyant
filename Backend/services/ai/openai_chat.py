import os
import logging
import openai
from typing import Optional, List, Any, Generator
from dataclasses import dataclass

from Backend.crud.chat import create_chat_message, get_chat_history

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = True


@dataclass
class Message:
    role: str
    content: str


class Chat:
    def __init__(self, api_key: str, prompt_path: str, model: str = "gpt-5-mini") -> None:
        self.api_key = api_key
        self.model = model
        self.client = openai.OpenAI(api_key = api_key)

        with open(prompt_path, "r", encoding="utf-8") as f:
            self.prompt = f.read()

    def _append_user_message(self, conversation_id: str, user_id: str, user_message: str, session) -> None:
        if not conversation_id or not user_message.strip():
            return
        if session is None:
            raise ValueError("A database session must be provided.")
        create_chat_message(session, conversation_id, user_id, "user", user_message.strip())

    def _append_assistant_response(self, conversation_id: str, user_id: str, full_response: str, session) -> None:
        if not conversation_id or not full_response.strip():
            return
        if session is None:
            raise ValueError("A database session must be provided.")
        create_chat_message(session, conversation_id, user_id, "assistant", full_response.strip())

    def get_conversation_messages(self, conversation_id: str, user_id: str, session) -> List[Message]:
        db_history = get_chat_history(session, conversation_id, user_id)
        messages = []
        for m in db_history:
            messages.append(Message(role=m.role, content=m.content))
        return messages

    def _openai_compatible_client(self, provider: str | None):
        provider_l = (provider or "openai").lower()
        if provider_l == "openai":
            return openai.OpenAI(api_key = os.getenv("OPENAI_API_KEY"))
        if provider_l == "grok":
            return openai.OpenAI(api_key = os.getenv("GROK_API_KEY"), base_url = "https://api.x.ai/v1")
        if provider_l == "gemini":
            return openai.OpenAI(api_key = os.getenv("GEMINI_API_KEY"), base_url = "https://generativelanguage.googleapis.com/v1beta/openai")
        if provider_l == "anthropic":
            return openai.OpenAI(api_key = os.getenv("ANTHROPIC_API_KEY"), base_url = "https://api.anthropic.com/v1")
        raise ValueError(f"Unsupported provider: {provider_l}")

    def chat_stream(self, user_id: str,
                    prompt: Optional[str] = None, provider: Optional[str] = None,
                    model_override: Optional[str] = None, conversation_id: Optional[str] = None,
                    session = None) -> Generator[Any, None, None]:
        instructions = prompt if prompt is not None else self.prompt

        messages: list[dict[str, str]] = []
        if instructions:
            messages.append({"role": "system", "content": instructions})

        if conversation_id and user_id and session is not None:
            for msg in self.get_conversation_messages(conversation_id, user_id, session):
                role = "assistant" if msg.role == "assistant" else "user"
                messages.append({"role": role, "content": msg.content})

        selected_model = model_override or self.model
        prov = (provider or "openai").lower()
        try:
            client = self._openai_compatible_client(prov)
            stream = client.chat.completions.create(
                model = selected_model,
                messages = messages,
                stream = True,
            )
            for chunk in stream:
                try:
                    choice = chunk.choices[0]
                    delta = getattr(choice, "delta", None)
                    if delta is not None:
                        content = getattr(delta, "content", None)
                        if isinstance(content, str) and content:
                            yield content
                    text_piece = getattr(choice, "text", None)
                    if isinstance(text_piece, str) and text_piece:
                        yield text_piece
                except Exception:
                    pass
            return
        except Exception as e:
            logger.error(f"Unified chat_stream error for provider {prov}: {e}")
            raise


