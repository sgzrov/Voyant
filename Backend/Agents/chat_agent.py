import logging
import os
import json
import openai
from typing import BinaryIO, Optional, List, Any, Generator
from dataclasses import dataclass

from Backend.Database.chat_repository import create_chat_message, get_chat_history
from Backend.rag_pipeline import RAGPipeline

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = True

@dataclass
class Message:
    role: str
    content: str

class ChatAgent:
    def __init__(self, api_key: str, prompt_path: str, model: str = "gpt-5") -> None:
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

    # Generate a streaming simple chat response (without OpenAI tools)
    def simple_chat(self, user_input: str, user_id: str, prompt: Optional[str] = None,
                    conversation_id: Optional[str] = None, session = None) -> Generator[Any, None, None]:
        conversation_context = self._build_conversation_context_string(conversation_id, user_id, session)
        instructions = prompt if prompt is not None else self.prompt

        try:
            response = self.client.responses.create(
                model = self.model,
                input = f"{instructions}\nConversation:\n{conversation_context}\nUser: {user_input}",
                stream = True
            )
            for chunk in response:
                yield chunk
        except openai.APIError as e:
            logger.error(f"OpenAI API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in simple_chat: {e}")
            raise

    # Generate a streaming chat response using RAG (no code interpreter)
    def chat_with_rag(self, file_obj: BinaryIO, user_input: str, user_id: str,
                      prompt: Optional[str] = None, conversation_id: Optional[str] = None,
                      filename: str = "user_health_data.csv", session = None) -> Generator[Any, None, None]:

        conversation_context = self._build_conversation_context_string(conversation_id, user_id, session)
        instructions = prompt if prompt is not None else self.prompt

        try:
            db_url = os.getenv("DATABASE_URL") # Get database URL from environment for pgvector

            # Create RAG pipeline instance with pgvector support
            # Note: pgvector should be installed in your PostgreSQL database for optimal performance
            rag_pipeline = RAGPipeline(self.api_key, db_url=db_url)

            # Step 1: Ingest (if provided) and find relevant data scoped to user/conversation
            logger.info(f"[RAG] Processing question: {user_input}")
            relevant_data = rag_pipeline.process_question_with_rag(
                user_question = user_input,
                user_id = user_id,
                csv_file = file_obj,
                dataset_id = conversation_id
            )

            # Step 2: Create prompt with focused data
            data_context = f"""
            Relevant Health Data (JSON format):
            {json.dumps(relevant_data, indent=2)}
            """

            full_prompt = f"{instructions}\nConversation:\n{conversation_context}\n{data_context}\nUser: {user_input}"

            # Step 3: Send to LLM directly (no CI needed)
            logger.info(f"[RAG] Sending {len(relevant_data)} relevant rows to LLM")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": full_prompt}],
                stream=True
            )

            # Step 4: Stream response back
            for chunk in response:
                yield chunk

        except openai.APIError as e:
            logger.error(f"OpenAI API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in RAG chat: {e}")
            raise
