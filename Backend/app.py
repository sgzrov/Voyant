import os
import logging
import json
import uuid
from typing import Any, Optional, Callable
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv

from Backend.Agents.chat import Chat


from Backend.auth import verify_clerk_jwt
from Backend.Database.db import SessionLocal
from Backend.Models.requests import ChatRequest

from Backend.Database import *

from Backend.Routers.chat_router import router as chat_router


logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger('Backend.Agents.chat').setLevel(logging.DEBUG)

load_dotenv()

app = FastAPI()

app.include_router(chat_router)



api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable not set.")

PROMPT_DIR = os.path.join(os.path.dirname(__file__), "Prompts")
PROMPT_PATHS = {
    "chat_prompt": os.path.join(PROMPT_DIR, "chat_prompt.txt"),
}

chat_agent = Chat(api_key, prompt_path = PROMPT_PATHS["chat_prompt"])

# Generate a conversation id, or use an existing one if provided
def generate_conversation_id(existing_conversation_id: Optional[str] = None) -> str:
    if existing_conversation_id:
        return existing_conversation_id
    return str(uuid.uuid4())

def setup_conversation_history(conversation_id: Optional[str],
                               user_input: str,
                               user_id: str,
                               session,
                               chat_agent) -> tuple[Optional[Callable[[str], None]], Optional[Callable[[str], None]], Optional[str]]:
    original_conversation_id = conversation_id
    conversation_id = generate_conversation_id(conversation_id)
    if original_conversation_id is None:
        logger.info(f"[CONV] Created new conversation_id: {conversation_id}")

    # Save user message to database immediately
    chat_agent._append_user_message(conversation_id, user_id, user_input, session = session)

    # Create callback function for saving assistant response. This will be called when the AI response is complete
    def save_conversation(full_response: str) -> None:
        chat_agent._append_assistant_response(conversation_id, user_id, full_response, session = session)

    # Return None for partial callback since we only save final responses
    return save_conversation, None, conversation_id

def extract_text_from_chunk(chunk: Any, full_response: str = "") -> str:
    # Support Chat Completions streaming (choices[0].delta.content)
    try:
        choices = getattr(chunk, 'choices', None)
        if choices and len(choices) > 0:
            choice = choices[0]
            delta = getattr(choice, 'delta', None)
            if delta is not None:
                content = getattr(delta, 'content', None)
                if isinstance(content, str):
                    return content or ""
            # Some SDKs may expose incremental text under choice.text
            text_piece = getattr(choice, 'text', None)
            if isinstance(text_piece, str):
                return text_piece or ""
    except Exception:
        pass

    # Fallback: previous Responses API formats
    if hasattr(chunk, 'type'):
        if chunk.type == 'text_delta':
            if hasattr(chunk, 'delta') and chunk.delta and hasattr(chunk.delta, 'text'):
                return chunk.delta.text or ""

        elif chunk.type == 'response.output_text.delta':
            if hasattr(chunk, 'delta') and chunk.delta:
                return chunk.delta or ""

        elif chunk.type == 'response.output_text.done':
            if hasattr(chunk, 'text') and chunk.text:
                remaining_text = chunk.text[len(full_response):]
                return remaining_text or ""
    return ""

def process_streaming_response(response: Any, conversation_callback: Optional[Callable[[str], None]] = None, partial_callback: Optional[Callable[[str], None]] = None) -> Any:
    full_response = ""
    for chunk in response:
        text = extract_text_from_chunk(chunk, full_response)
        if text:
            full_response += text
            yield f"data: {json.dumps({'content': text, 'done': False})}\n\n"

    if conversation_callback and full_response.strip():
        try:
            conversation_callback(full_response.strip())
            logger.info(f"[DEBUG] process_streaming_response: conversation_callback successful")
        except Exception as e:
            logger.error(f"[DEBUG] process_streaming_response: conversation_callback failed: {e}")
    else:
        logger.warning(f"[DEBUG] process_streaming_response: No callback or empty response - callback: {conversation_callback is not None}")
    yield f"data: {json.dumps({'content': '', 'done': True})}\n\n"

def create_streaming_response(generator_func: Callable, **kwargs) -> StreamingResponse:
    return StreamingResponse(
        generator_func(**kwargs),
        media_type = "text/plain",
        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
        }
    )

@app.post("/chat/stream/")
async def chat_stream(request: ChatRequest, req: Request):
    user = verify_clerk_jwt(req)
    user_id = user['sub']
    print(f"[DEBUG] /chat/stream/ called with conversation_id={request.conversation_id}, user_id={user_id}")

    async def generate_stream():
        session = SessionLocal()
        try:
            user_input_str = request.user_input
            save_conversation, save_partial_conversation, new_conversation_id = setup_conversation_history(request.conversation_id, user_input_str, user_id, session, chat_agent)

            # Always determine the conversation_id that will be used for this request
            conversation_id = new_conversation_id or request.conversation_id

            # Emit the conversation_id upfront so the client can persist and reuse it
            try:
                yield f"data: {json.dumps({'conversation_id': conversation_id, 'content': '', 'done': False})}\n\n"
            except Exception as e:
                logger.error(f"[DEBUG] Failed to emit initial conversation_id event: {e}")

            try:
                response = chat_agent.chat_stream(user_input_str, user_id, conversation_id = conversation_id, session = session)
                for event in process_streaming_response(response, save_conversation, save_partial_conversation):
                    yield event
            except Exception as e:
                logger.error(f"chat stream error: {e}")
                yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"
        finally:
            session.close()

    return create_streaming_response(generate_stream)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host = "0.0.0.0", port = 8000)
