import os
import logging
import json
from fastapi import FastAPI, HTTPException, Depends, Request
from dotenv import load_dotenv

from Backend.Agents.chat_agent import ChatAgent


from Backend.auth import verify_clerk_jwt

from Backend.Database.db import SessionLocal
from Backend.Database.s3_storage import S3Storage

from Backend.Utils.streaming_utils import process_streaming_response, create_streaming_response
from Backend.Utils.conversation_utils import setup_conversation_history


from Backend.Models.requests import ChatWithCIRequest

from Backend.Database import *

from Backend.Routers.file_router import router as file_router
from Backend.Routers.chat_router import router as chat_router


logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger('chat_agent').setLevel(logging.DEBUG)

load_dotenv()

app = FastAPI()

app.include_router(file_router)
app.include_router(chat_router)


try:
    s3_storage = S3Storage()
except ValueError as e:
    logger.warning(f"S3Storage initialization failed: {e}")
    s3_storage = None

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable not set.")

PROMPT_DIR = os.path.join(os.path.dirname(__file__), "Prompts")
PROMPT_PATHS = {
    "chat": os.path.join(PROMPT_DIR, "ChatWithCodeInterpreterPrompt.txt"),
}

chat_agent = ChatAgent(api_key, prompt_path = PROMPT_PATHS["chat"])

@app.post("/chat-with-ci/")
async def chat_with_ci(request: ChatWithCIRequest, req: Request):
    user = verify_clerk_jwt(req)
    user_id = user['sub']
    print(f"[DEBUG] /chat-with-ci/ called with conversation_id={request.conversation_id}, user_id={user_id}")
    if s3_storage is None:
        logger.error("S3 storage is None")
        raise HTTPException(status_code = 503, detail = "Tigris storage not configured")

    async def generate_stream():
        session = SessionLocal()
        try:
            file_obj = s3_storage.download_file_from_url(request.s3_url)
            logger.info(f"[chat-with-ci] File downloaded from S3: {request.s3_url}")

            user_input_str = request.user_input
            save_conversation, save_partial_conversation, new_conversation_id = setup_conversation_history(request.conversation_id, user_input_str, user_id, session, chat_agent)

            try:
                conversation_id = new_conversation_id or request.conversation_id
                response = chat_agent.chat_with_ci(file_obj, user_input_str, user_id, conversation_id = conversation_id, session = session)
                for event in process_streaming_response(response, save_conversation, save_partial_conversation):
                    yield event
            except Exception as e:
                logger.error(f"CI health analysis error: {e}")
                yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"
        finally:
            session.close()

    return create_streaming_response(generate_stream)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host = "0.0.0.0", port = 8000)
