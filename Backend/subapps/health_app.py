import base64
import json
import asyncio
import os
import pathlib
import logging
import uuid
from typing import Optional, Any, Callable
from fastapi import APIRouter, UploadFile, File, HTTPException, Request
from fastapi.responses import StreamingResponse
from openai import OpenAI
from sqlalchemy import func

from Backend.auth import verify_clerk_jwt
from Backend.background_tasks.csv_ingest import process_csv_upload
from Backend.services.ai.vector import vector_search
from Backend.services.ai.sql_gen import execute_generated_sql
from Backend.database import SessionLocal
from Backend.crud.chat import get_chat_history, create_chat_message
from Backend.models.chat_data_model import ChatsDB
from Backend.services.ai.openai_chat import Chat
from Backend.schemas.chat import ChatRequest


async def _fetch_health_context(user_id: str, question: str):
    loop = asyncio.get_running_loop()
    vec_task = loop.run_in_executor(None, vector_search, user_id, question)
    sql_task = loop.run_in_executor(None, execute_generated_sql, user_id, question)
    vec_res, sql_res = await asyncio.gather(vec_task, sql_task)
    return {"sql": sql_res, "vector": vec_res}


router = APIRouter()

logger = logging.getLogger(__name__)


PROMPT_PATH = (pathlib.Path(__file__).resolve().parents[1] / "resources" / "chat_prompt.txt")
api_key = os.getenv("OPENAI_API_KEY") or ""
chat_agent = Chat(api_key, prompt_path=str(PROMPT_PATH))


def generate_conversation_id(existing_conversation_id: Optional[str] = None) -> str:
    if existing_conversation_id:
        return existing_conversation_id
    return str(uuid.uuid4())

def setup_conversation_history(conversation_id: Optional[str],
                               user_input: str,
                               user_id: str,
                               session,
                               chat_agent) -> tuple[Optional[Callable[[str], None]], Optional[Callable[[str], None]], Optional[str]]:
    conversation_id = generate_conversation_id(conversation_id)
    chat_agent._append_user_message(conversation_id, user_id, user_input, session=session)
    def save_conversation(full_response: str) -> None:
        chat_agent._append_assistant_response(conversation_id, user_id, full_response, session=session)
    return save_conversation, None, conversation_id

def extract_text_from_chunk(chunk: Any, full_response: str = "") -> str:
    if isinstance(chunk, str):
        return chunk
    try:
        choices = getattr(chunk, 'choices', None)
        if choices and len(choices) > 0:
            choice = choices[0]
            delta = getattr(choice, 'delta', None)
            if delta is not None:
                content = getattr(delta, 'content', None)
                if isinstance(content, str):
                    return content or ""
            text_piece = getattr(choice, 'text', None)
            if isinstance(text_piece, str):
                return text_piece or ""
    except Exception:
        pass
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
        except Exception:
            pass
    yield f"data: {json.dumps({'content': '', 'done': True})}\n\n"

def create_streaming_response(generator_func: Callable, **kwargs) -> StreamingResponse:
    return StreamingResponse(
        generator_func(**kwargs),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )



@router.post("/health/upload-csv")
async def upload_csv(file: UploadFile = File(...), request: Request = None):
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    content = await file.read()
    try:
        logger.info("upload_csv: user_id=%s filename=%s bytes=%s content_type=%s",
                    user_id, getattr(file, 'filename', None), len(content), getattr(file, 'content_type', None))
        b64 = base64.b64encode(content).decode("utf-8")
        task = process_csv_upload.delay(user_id, b64)
        logger.info("upload_csv: enqueued task_id=%s", task.id)
        return {"task_id": task.id}
    except Exception as e:
        logger.exception("upload_csv: failed to enqueue task for user_id=%s", user_id)
        raise HTTPException(status_code=500, detail=f"Failed to enqueue CSV ingest: {e}")


@router.get("/health/task-status/{task_id}")
async def task_status(task_id: str):
    res = process_csv_upload.AsyncResult(task_id)
    return {"id": task_id, "state": res.state, "result": res.result if res.ready() else None}


@router.post("/health/query/stream")
async def query_stream(payload: dict, request: Request):
    user = verify_clerk_jwt(request)
    user_id = user["sub"]
    question = payload.get("question")
    conversation_id = payload.get("conversation_id")
    if conversation_id is not None and isinstance(conversation_id, str) and conversation_id.strip() == "":
        raise HTTPException(status_code = 400, detail = "conversation_id cannot be empty string")
    if not conversation_id:
        conversation_id = str(uuid.uuid4())
    if not isinstance(question, str) or not question.strip():
        raise HTTPException(status_code = 400, detail = "Missing question")

    async def generator():
        client = OpenAI(api_key= os.getenv("OPENAI_API_KEY"))
        prompt_path = pathlib.Path(__file__).resolve().parents[1] / "resources" / "chat_prompt.txt"
        system = prompt_path.read_text(encoding = "utf-8")

        session = None
        history_msgs = []
        try:
            session = SessionLocal()
            try:
                prior = get_chat_history(session, conversation_id, user_id)
                for m in prior:
                    role = "assistant" if m.role == "assistant" else "user"
                    if isinstance(m.content, str) and m.content.strip():
                        history_msgs.append({"role": role, "content": m.content})
            except Exception:
                history_msgs = []
            try:
                create_chat_message(session, conversation_id, user_id, "user", question)
            except Exception:
                pass
        except Exception:
            session = None
            history_msgs = []

        tools = [{
            "type": "function",
            "function": {
                "name": "fetch_health_context",
                "description": "Fetch both SQL rows (exact numbers) and vector summaries (context) for a health question. Call at most once.",
                "parameters": {
                    "type": "object",
                    "properties": {"question": {"type": "string"}},
                    "required": ["question"]
                }
            }
        }]

        messages = [{"role": "system", "content": system}]
        messages.extend(history_msgs)
        messages.append({"role": "user", "content": question})

        try:
            yield f"data: {json.dumps({'conversation_id': conversation_id, 'content': '', 'done': False})}\n\n"
        except Exception:
            pass

        decide = client.chat.completions.create(
            model = "gpt-4o-mini",
            messages = messages,
            tools = tools,
            tool_choice = "auto",
            temperature = 0
        )
        choice = decide.choices[0]
        tool_calls = getattr(choice.message, "tool_calls", None)

        if tool_calls:
            try:
                tool_call = tool_calls[0]
                try:
                    arguments_json = getattr(getattr(tool_call, "function", None), "arguments", None) or "{}"
                except Exception:
                    arguments_json = "{}"
                try:
                    args = json.loads(arguments_json) if isinstance(arguments_json, str) else {}
                except Exception:
                    args = {}
                model_question = args.get("question")
                if not isinstance(model_question, str) or not model_question.strip():
                    model_question = question

                ctx = await _fetch_health_context(user_id, model_question)
                messages.append({"role": "assistant", "tool_calls": tool_calls})
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(ctx)
                })
            except Exception as e:
                messages.append({
                    "role": "assistant",
                    "content": f"Tool error: {str(e)}"
                })

        try:
            stream = client.chat.completions.create(
                model = "gpt-5-mini",
                messages = messages,
                stream = True
            )
            full_response = ""
            for chunk in stream:
                choice = chunk.choices[0]
                delta = getattr(choice, "delta", None)
                if delta is not None:
                    content = getattr(delta, "content", None)
                    if isinstance(content, str) and content:
                        full_response += content
                        yield f"data: {json.dumps({'content': content, 'done': False})}\n\n"
                text_piece = getattr(choice, "text", None)
                if isinstance(text_piece, str) and text_piece:
                    full_response += text_piece
                    yield f"data: {json.dumps({'content': text_piece, 'done': False})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"
            if session:
                try:
                    session.close()
                except Exception:
                    pass
            return

        # Persist assistant response if captured
        try:
            if session and full_response.strip():
                create_chat_message(session, conversation_id, user_id, "assistant", full_response.strip())
        except Exception:
            pass
        if session:
            try:
                session.close()
            except Exception:
                pass

        yield f"data: {json.dumps({'content': '', 'done': True})}\n\n"

    return StreamingResponse(generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})


@router.get("/chat/retrieve-chat-sessions/")
def get_chat_sessions(request: Request):
    user = verify_clerk_jwt(request)
    user_id = user['sub']
    db_session = SessionLocal()
    try:
        subquery = db_session.query(
            ChatsDB.conversation_id,
            func.max(ChatsDB.timestamp).label('last_message_at')
        ).filter(ChatsDB.user_id == user_id).group_by(ChatsDB.conversation_id).subquery()

        latest_messages = db_session.query(ChatsDB).join(
            subquery,
            (ChatsDB.conversation_id == subquery.c.conversation_id) &
            (ChatsDB.timestamp == subquery.c.last_message_at)
        ).filter(ChatsDB.user_id == user_id).all()

        sessions_data = [
            {
                "conversation_id": msg.conversation_id,
                "last_active_date": msg.timestamp.isoformat() if msg.timestamp else None
            }
            for msg in latest_messages
        ]
        return {"sessions": sessions_data}
    finally:
        db_session.close()


@router.get("/chat/all-messages/{conversation_id}")
def get_all_chat_messages(conversation_id: str, request: Request):
    user = verify_clerk_jwt(request)
    user_id = user['sub']
    db_session = SessionLocal()
    try:
        messages = get_chat_history(db_session, conversation_id, user_id)
        return [
            {
                "id": m.id,
                "role": m.role,
                "content": m.content,
                "timestamp": m.timestamp.isoformat() if m.timestamp else None
            }
            for m in messages
        ]
    finally:
        db_session.close()


@router.post("/chat/stream/")
async def chat_stream(request: ChatRequest, req: Request):
    user = verify_clerk_jwt(req)
    user_id = user['sub']

    async def generate_stream():
        session = SessionLocal()
        try:
            user_input_str = request.user_input
            save_conversation, _, new_conversation_id = setup_conversation_history(
                request.conversation_id, user_input_str, user_id, session, chat_agent
            )
            conversation_id = new_conversation_id or request.conversation_id
            try:
                yield f"data: {json.dumps({'conversation_id': conversation_id, 'content': '', 'done': False})}\n\n"
            except Exception:
                pass
            try:
                response = chat_agent.chat_stream(
                    user_id,
                    provider=request.provider,
                    model_override=request.model,
                    conversation_id=conversation_id,
                    session=session
                )
                for event in process_streaming_response(response, save_conversation, None):
                    yield event
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"
        finally:
            session.close()

    return create_streaming_response(generate_stream)


@router.post("/chat/add-message/")
def add_chat_message(conversation_id: Optional[str] = None, role: str = '', content: str = '', request: Request = None):
    if conversation_id is not None and conversation_id.strip() == '':
        raise HTTPException(status_code = 400, detail = "conversation_id cannot be empty string")

    original_conversation_id = conversation_id
    conversation_id = generate_conversation_id(conversation_id)
    is_new_conversation = original_conversation_id is None

    user = verify_clerk_jwt(request)
    user_id = user['sub']
    db_session = SessionLocal()
    try:
        msg = create_chat_message(db_session, conversation_id, user_id, role, content)
        response = {
            "id": msg.id,
            "conversation_id": conversation_id,
            "user_id": msg.user_id,
            "role": msg.role,
            "content": msg.content,
            "timestamp": msg.timestamp.isoformat() if msg.timestamp else None
        }
        if is_new_conversation:
            response["new_conversation"] = True
        return response
    finally:
        db_session.close()

