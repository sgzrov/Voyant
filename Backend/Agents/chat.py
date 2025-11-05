import logging
import openai
from typing import Optional, List, Any, Generator
from dataclasses import dataclass

from Backend.Database.chat_repository import create_chat_message, get_chat_history

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

        try:
            with open(prompt_path, "r", encoding = "utf-8") as f:
                self.prompt = f.read()
        except Exception as e:
            logger.error(f"Error reading prompt file: {e}")
            raise

    # Save a user message to the database for conversation history
    def _append_user_message(self, conversation_id: str, user_id: str, user_message: str, session) -> None:
        if not conversation_id or not user_message.strip():
            return
        if session is None:
            raise ValueError("A database session must be provided.")
        create_chat_message(session, conversation_id, user_id, "user", user_message.strip())
        logger.info(f"[CONV] User message appended to DB ({conversation_id}, {user_id}): {user_message.strip()}")

    # Save an assistant response to the database for conversation history
    def _append_assistant_response(self, conversation_id: str, user_id: str, full_response: str, session) -> None:
        if not conversation_id or not full_response.strip():
            return
        if session is None:
            raise ValueError("A database session must be provided.")
        create_chat_message(session, conversation_id, user_id, "assistant", full_response.strip())
        logger.info(f"[CONV] Assistant response appended to DB ({conversation_id}, {user_id}): {full_response.strip()}")

    # Build a formatted string for conversation history for LLM context (user: ..., assistant: ...)
    def _build_conversation_context_string(self, conversation_id: Optional[str], user_id: Optional[str], session) -> str:
        if not conversation_id or not user_id:
            return ""

        db_history = get_chat_history(session, conversation_id, user_id)
        conversation_context = ""
        for message in db_history:
            role = "User" if message.role == "user" else "Assistant"
            conversation_context += f"{role}: {message.content}\n"
        logger.info(f"[CONV] Context for LLM ({conversation_id}, {user_id}):\n{conversation_context.strip()}")
        return conversation_context.strip()

    # Retrieve conversation history as a list of Message objects
    def get_conversation_messages(self, conversation_id: str, user_id: str, session) -> List[Message]:
        db_history = get_chat_history(session, conversation_id, user_id)
        messages = []
        for m in db_history:
            msg = Message(role = m.role, content = m.content)
            messages.append(msg)
        return messages

    def chat_stream(self, user_input: str, user_id: str,
                    prompt: Optional[str] = None, conversation_id: Optional[str] = None,
                    session = None) -> Generator[Any, None, None]:
        instructions = prompt if prompt is not None else self.prompt

        # Build messages: system prompt + prior conversation + current user message
        messages: list[dict[str, str]] = []
        if instructions:
            messages.append({"role": "system", "content": instructions})

        if conversation_id and user_id and session is not None:
            for msg in self.get_conversation_messages(conversation_id, user_id, session):
                # Only include supported roles
                role = "assistant" if msg.role == "assistant" else "user"
                messages.append({"role": role, "content": msg.content})

        try:
            # Stream via the Responses API with GPT-5 params
            with self.client.responses.stream(
                model = self.model,
                input = messages,
                text = {"verbosity": "medium"},
                reasoning = {"effort": "minimal"},
            ) as stream:
                for event in stream:
                    yield event
                _ = stream.get_final_response()
        except openai.APIError as e:
            logger.error(f"OpenAI API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in chat_stream: {e}")
            raise


