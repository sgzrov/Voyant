import asyncio
import json
import logging
import pathlib
import re
import time
from typing import Any, Awaitable, Callable, Optional
from fastapi.responses import StreamingResponse

from Backend.crud.chat import (create_chat_message, get_chat_history, get_or_create_conversation, update_conversation_title)
from Backend.database import SessionLocal
from Backend.services.openai_compatible_client import get_openai_compatible_client

logger = logging.getLogger(__name__)

_BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]


DEFAULT_MODEL = {
    "openai": "gpt-5-mini",
    "grok": "grok-4-fast",
    "gemini": "gemini-2.5-flash",
    "anthropic": "claude-sonnet-4-5",
}


# Streams assistant tokens over SSE and persists chat history to DB + calls tools if provided
def build_agent_stream_response(*, user_id: str, conversation_id: str, question: str, provider: str, answer_model: str, tools: list[dict], tool_handlers: dict[str, Callable[[dict], Awaitable[dict]]], tool_prefetch: Optional[Callable[[], Awaitable[dict]]] = None) -> StreamingResponse:
    final_client = get_openai_compatible_client(provider)

    async def generator():
        session = None
        sql_task: Optional[asyncio.Task] = None

        def _sse(payload: dict) -> str:
            return f"data: {json.dumps(payload)}\n\n"

        async def _cancel_task(task: Optional[asyncio.Task]) -> None:
            if task is None:
                return
            if task.done():
                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                return
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        def _safe_close_session() -> None:
            nonlocal session
            if session is None:
                return
            try:
                session.close()
            except Exception:
                pass
            session = None

        def _load_history_msgs() -> list[dict]:
            if session is None:
                return []
            try:
                prior = get_chat_history(session, conversation_id, user_id)
                msgs: list[dict] = []
                for m in prior:
                    role = "assistant" if m.role == "assistant" else "user"
                    if isinstance(m.content, str) and m.content.strip():
                        msgs.append({"role": role, "content": m.content})
                return msgs
            except Exception:
                return []

        def _persist_user_message() -> None:
            if session is None:
                return
            try:
                create_chat_message(session, conversation_id, user_id, "user", question)
            except Exception:
                pass

        async def _maybe_generate_title(is_new_conversation: bool) -> Optional[str]:
            if not is_new_conversation or session is None:
                return None
            try:
                conv = get_or_create_conversation(session, conversation_id, user_id)
                if conv and not conv.title:
                    title = await generate_chat_title(question)
                    update_conversation_title(session, conversation_id, user_id, title)
                    return title
            except Exception:
                pass
            return None

        async def _resolve_tool_ctx(tool_name: Optional[str], args: dict) -> dict:
            handler = tool_handlers.get(tool_name or "")
            if handler is None:
                return {"error": f"unknown-tool: {tool_name}"}

            if sql_task is not None and tool_name == "fetch_health_context":
                try:
                    res = await sql_task
                    return res if isinstance(res, dict) else {"result": res}
                except Exception:
                    pass
            res = await handler(args)
            return res if isinstance(res, dict) else {"result": res}

        try:
            system_prompt_path = _BACKEND_DIR / "resources" / "chat_prompt.txt"
            system = system_prompt_path.read_text(encoding="utf-8")

            history_msgs: list[dict] = []
            try:
                session = SessionLocal()
                history_msgs = _load_history_msgs()
                _persist_user_message()
            except Exception:
                session = None
                history_msgs = []

            generated_title = await _maybe_generate_title(is_new_conversation=(len(history_msgs) == 0))

            try:
                initial_payload: dict[str, object] = {"conversation_id": conversation_id, "content": "", "done": False}
                if generated_title:
                    initial_payload["title"] = generated_title
                yield _sse(initial_payload)
            except Exception:
                pass

            messages: list[dict] = [{"role": "system", "content": system}, *history_msgs, {"role": "user", "content": question}]

            if tool_prefetch is not None:
                try:
                    sql_task = asyncio.create_task(tool_prefetch())
                except Exception:
                    sql_task = None

            tool_calls_acc: dict[int, dict] = {}
            assistant_content = ""  # content from the first pass (before any tool call)
            full_response = ""      # full assistant response across both passes
            streamed_chars = 0
            finish_reason = None

            t0_stream = time.perf_counter()

            stream_kwargs: dict[str, object] = {
                "model": answer_model,
                "messages": messages,
                "stream": True,
            }
            if tools:
                stream_kwargs["tools"] = tools
                stream_kwargs["tool_choice"] = "auto"

            stream = final_client.chat.completions.create(**stream_kwargs)
            for chunk in stream:
                choice = chunk.choices[0]
                fr = getattr(choice, "finish_reason", None)
                if fr:
                    finish_reason = fr

                delta = getattr(choice, "delta", None)
                if delta is not None:
                    _parse_tool_calls_from_delta(delta, tool_calls_acc)
                    content = getattr(delta, "content", None)
                    if isinstance(content, str) and content:
                        assistant_content += content
                        full_response += content
                        streamed_chars += len(content)
                        yield _sse({"content": content, "done": False})

                text_piece = getattr(choice, "text", None)
                if isinstance(text_piece, str) and text_piece:
                    assistant_content += text_piece
                    full_response += text_piece
                    streamed_chars += len(text_piece)
                    yield _sse({"content": text_piece, "done": False})

            if finish_reason == "tool_calls" and tool_calls_acc:
                tool_calls_for_msg = _tool_calls_for_messages(tool_calls_acc)
                tool_call = tool_calls_for_msg[0]  # keep existing behavior: run first tool call only
                tool_name = tool_call.get("function", {}).get("name")
                args_json = tool_call.get("function", {}).get("arguments") or "{}"

                try:
                    args = json.loads(args_json) if isinstance(args_json, str) else {}
                    if not isinstance(args, dict):
                        args = {}
                except Exception:
                    args = {}

                ctx = await _resolve_tool_ctx(tool_name, args)

                # If we prefetched but ended up using a different tool, don't let that task leak.
                if sql_task is not None and tool_name != "fetch_health_context":
                    await _cancel_task(sql_task)

                messages.append({"role": "assistant", "content": assistant_content, "tool_calls": tool_calls_for_msg})
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.get("id") or "",
                        "content": _json_dumps_safe(ctx),
                    }
                )

                stream2 = final_client.chat.completions.create(model=answer_model, messages=messages, stream=True)
                for chunk in stream2:
                    choice = chunk.choices[0]
                    delta = getattr(choice, "delta", None)
                    if delta is not None:
                        content = getattr(delta, "content", None)
                        if isinstance(content, str) and content:
                            full_response += content
                            streamed_chars += len(content)
                            yield _sse({"content": content, "done": False})

                    text_piece = getattr(choice, "text", None)
                    if isinstance(text_piece, str) and text_piece:
                        full_response += text_piece
                        streamed_chars += len(text_piece)
                        yield _sse({"content": text_piece, "done": False})
            else:
                await _cancel_task(sql_task)

            try:
                logger.info(
                    "stream.done: conv=%s chars=%d ms=%d",
                    conversation_id,
                    streamed_chars,
                    int((time.perf_counter() - t0_stream) * 1000),
                )
            except Exception:
                pass

            try:
                if session and full_response.strip():
                    create_chat_message(session, conversation_id, user_id, "assistant", full_response.strip())
            except Exception:
                pass

            yield _sse({"content": "", "done": True})

        except Exception as e:
            yield _sse({"error": str(e), "done": True})
        finally:
            await _cancel_task(sql_task)
            _safe_close_session()

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


# JSON-serialize a value for message/tool payloads without raising
def _json_dumps_safe(obj: object) -> str:
    def _default(o):
        try:
            if hasattr(o, "isoformat"):
                return o.isoformat()
        except Exception:
            pass
        return str(o)

    return json.dumps(obj, default=_default)


# Accumulate streaming tool-call fragments from an OpenAI-compatible delta in an tool accumulator
def _parse_tool_calls_from_delta(delta: Any, acc: dict[int, dict]) -> None:
    tool_calls = getattr(delta, "tool_calls", None)
    if not tool_calls:
        return
    for tc in tool_calls:
        idx = getattr(tc, "index", None)
        if idx is None:
            continue
        entry = acc.setdefault(idx, {"id": None, "name": "", "arguments": ""})
        tc_id = getattr(tc, "id", None)
        if tc_id:
            entry["id"] = tc_id
        fn = getattr(tc, "function", None)
        if fn is not None:
            name = getattr(fn, "name", None)
            if name:
                entry["name"] = name
            args_piece = getattr(fn, "arguments", None)
            if isinstance(args_piece, str) and args_piece:
                entry["arguments"] = (entry.get("arguments") or "") + args_piece


# Convert accumulated tool-call fragments into chat completions tool_calls shape
def _tool_calls_for_messages(tool_calls_acc: dict[int, dict]) -> list[dict]:
    out: list[dict] = []
    for _idx in sorted(tool_calls_acc.keys()):
        tc = tool_calls_acc[_idx]
        out.append(
            {
                "id": tc.get("id") or "",
                "type": "function",
                "function": {"name": tc.get("name") or "", "arguments": tc.get("arguments") or ""},
            }
        )
    return out


# Generate a short conversation title for the current conversation
def generate_chat_title_sync(first_user_message: str) -> str:
    try:
        client = get_openai_compatible_client("openai")
        title_prompt_path = _BACKEND_DIR / "resources" / "chat_title_prompt.txt"
        title_prompt = title_prompt_path.read_text(encoding="utf-8")
        response = client.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": f"{title_prompt}{first_user_message[:100]}"}],
        )
        content = response.choices[0].message.content
        if not content:
            return "New Chat"
        title = content.strip().strip("\"'.:")
        title = re.sub(r"^(Title:|title:)\s*", "", title, flags=re.IGNORECASE)
        if len(title) > 60:
            title = title[:57] + "..."
        return title if title else "New Chat"
    except Exception:
        logger.exception("chat.title.error")
        return "New Chat"

# Async wrapper around 'generate_chat_title_sync' to keep event loop unblocked while generating titles
async def generate_chat_title(first_user_message: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, generate_chat_title_sync, first_user_message)