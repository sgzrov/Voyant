import asyncio
import json
import logging
import pathlib
import re
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Optional
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from Backend.crud.chat import (create_chat_message, get_chat_history, get_or_create_conversation, update_conversation_title)
from Backend.database import SessionLocal
from Backend.services.openai_compatible_client import get_async_openai_compatible_client

logger = logging.getLogger(__name__)
_BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]


DEFAULT_MODEL = {
    "openai": "gpt-5-mini",
    "grok": "grok-4-fast",
    "gemini": "gemini-2.5-flash-lite",
    "anthropic": "claude-sonnet-4-5",
}

# Streams assistant tokens over SSE and persists chat history to DB + calls tools if provided
def build_agent_stream_response(
    *,
    user_id: str,
    conversation_id: str,
    question: str,
    provider: str,
    answer_model: str,
    tools: list[dict],
    tool_handlers: dict[str, Callable[[dict], Awaitable[dict]]],
    tool_prefetch: Optional[Callable[[], Awaitable[dict]]] = None,
    db_session: Optional[Session] = None,
) -> StreamingResponse:
    # IMPORTANT:
    # On iOS, the SSE connection is frequently torn down when the app backgrounds or the user navigates
    # away from a screen. If generation runs inside the request coroutine, cancellation can stop the
    # LLM mid-response and we won't persist the assistant message.
    #
    # We solve this by running generation in a detached background task that always completes + writes
    # to the DB. The request handler just drains a bounded queue while the client is connected.

    async def _cancel_task(task: Optional[asyncio.Task]) -> None:
        if task is None:
            return
        if task.done():
            try:
                task.result()
            except Exception:
                pass
            return
        try:
            task.cancel()
            await task
        except Exception:
            pass

    async def _maybe_generate_title_isolated(is_new_conversation: bool) -> Optional[str]:
        """Generate & persist a title using an isolated DB session so it can't interfere with streaming."""
        if not is_new_conversation:
            return None
        isolated: Optional[Session] = None
        try:
            isolated = SessionLocal()
            conv = get_or_create_conversation(isolated, conversation_id, user_id)
            if not conv or getattr(conv, "title", None):
                return None
            title = await generate_chat_title(question)
            update_conversation_title(isolated, conversation_id, user_id, title)
            isolated.commit()
            return title
        except Exception:
            try:
                if isolated is not None:
                    isolated.rollback()
            except Exception:
                pass
            return None
        finally:
            try:
                if isolated is not None:
                    isolated.close()
            except Exception:
                pass

    def _title_task_done(task: asyncio.Task) -> None:
        try:
            _ = task.result()
        except Exception:
            logger.exception("chat.title.bg.error")

    async def generator():
        queue: asyncio.Queue[Optional[str]] = asyncio.Queue(maxsize=2048)
        stream_enabled = asyncio.Event()
        stream_enabled.set()

        def _sse(payload: dict) -> str:
            return f"data: {json.dumps(payload)}\n\n"

        def _emit_nowait(payload: dict) -> None:
            if not stream_enabled.is_set():
                return
            try:
                queue.put_nowait(_sse(payload))
            except asyncio.QueueFull:
                # If the client is slow, drop chunks rather than buffering unboundedly.
                pass
            except Exception:
                pass

        def _finish_queue() -> None:
            if not stream_enabled.is_set():
                return
            try:
                queue.put_nowait(None)
            except Exception:
                pass

        def _load_history_msgs(session: Session) -> list[dict]:
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

        async def _run_generation_bg() -> None:
            final_client = get_async_openai_compatible_client(provider)
            session: Optional[Session] = None
            sql_task: Optional[asyncio.Task] = None
            title_task: Optional[asyncio.Task] = None
            title_sent = False

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

                # Always use an isolated session for persistence so request lifecycle can't invalidate it.
                session = SessionLocal()
                history_msgs = _load_history_msgs(session)

                # Persist user message ASAP.
                try:
                    create_chat_message(session, conversation_id, user_id, "user", question)
                    session.commit()
                except Exception:
                    try:
                        session.rollback()
                    except Exception:
                        pass

                # Send initial metadata chunk (conversation_id) immediately.
                _emit_nowait({"conversation_id": conversation_id, "content": "", "done": False})

                # Title generation (isolated from the streaming session).
                try:
                    title_task = asyncio.create_task(_maybe_generate_title_isolated(is_new_conversation=(len(history_msgs) == 0)))
                    title_task.add_done_callback(_title_task_done)
                except Exception:
                    title_task = None

                # Emit the title as soon as it's ready, without waiting for the first assistant token.
                # Title generation is based ONLY on the first user message (guarded by history_msgs == 0).
                if title_task is not None:
                    async def _watch_title() -> None:
                        nonlocal title_sent
                        try:
                            title = await title_task
                            if not title_sent and isinstance(title, str) and title.strip():
                                _emit_nowait({"conversation_id": conversation_id, "title": title.strip(), "content": "", "done": False})
                                title_sent = True
                        except Exception:
                            # Best-effort: don't let title failures affect chat streaming.
                            pass

                    asyncio.create_task(_watch_title())

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

                # Stream the first pass and accumulate any tool-call fragments.
                async for chunk in _iter_chat_completion_chunks(final_client, **stream_kwargs):
                    if title_task is not None and title_task.done() and not title_sent:
                        try:
                            title = title_task.result()
                            if isinstance(title, str) and title.strip():
                                _emit_nowait({"conversation_id": conversation_id, "title": title.strip(), "content": "", "done": False})
                                title_sent = True
                        except Exception:
                            pass

                    try:
                        choice = chunk.choices[0]
                    except Exception:
                        continue

                    pieces, fr = _extract_text_pieces_and_finish_reason(choice, tool_calls_acc)
                    if fr:
                        finish_reason = fr

                    for piece in pieces:
                        assistant_content += piece
                        full_response += piece
                        streamed_chars += len(piece)
                        _emit_nowait({"content": piece, "done": False})

                if finish_reason == "tool_calls" and tool_calls_acc:
                    tool_calls_for_msg = _tool_calls_for_messages(tool_calls_acc)
                    tool_call = tool_calls_for_msg[0]  # run first tool call only
                    tool_name = tool_call.get("function", {}).get("name")
                    args_json = tool_call.get("function", {}).get("arguments") or "{}"

                    try:
                        args = json.loads(args_json) if isinstance(args_json, str) else {}
                        if not isinstance(args, dict):
                            args = {}
                    except Exception:
                        args = {}

                    ctx = await _resolve_tool_ctx(tool_name, args)

                    # If we prefetched but ended up using a different tool, don't let that task leak
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

                    async for chunk in _iter_chat_completion_chunks(
                        final_client,
                        model=answer_model,
                        messages=messages,
                        stream=True,
                    ):
                        if title_task is not None and title_task.done() and not title_sent:
                            try:
                                title = title_task.result()
                                if isinstance(title, str) and title.strip():
                                    _emit_nowait({"conversation_id": conversation_id, "title": title.strip(), "content": "", "done": False})
                                    title_sent = True
                            except Exception:
                                pass

                        try:
                            choice = chunk.choices[0]
                        except Exception:
                            continue

                        pieces, _fr = _extract_text_pieces_and_finish_reason(choice, None)
                        for piece in pieces:
                            full_response += piece
                            streamed_chars += len(piece)
                            _emit_nowait({"content": piece, "done": False})
                else:
                    await _cancel_task(sql_task)

                logger.info(
                    "stream.done: conv=%s chars=%d ms=%d",
                    conversation_id,
                    streamed_chars,
                    int((time.perf_counter() - t0_stream) * 1000),
                )

                # Persist final assistant message regardless of client connection.
                final_text = full_response.strip()
                if session and final_text:
                    try:
                        create_chat_message(session, conversation_id, user_id, "assistant", final_text)
                        session.commit()
                    except Exception:
                        try:
                            session.rollback()
                        except Exception:
                            pass

                _emit_nowait({"content": "", "done": True})

            except Exception as e:
                logger.exception("chat.stream.error: conv=%s", conversation_id)
                _emit_nowait({"error": str(e), "done": True})
            finally:
                await _cancel_task(sql_task)
                try:
                    if session is not None:
                        session.close()
                except Exception:
                    pass
                try:
                    await final_client.close()
                except Exception:
                    pass
                _finish_queue()

        def _bg_done(task: asyncio.Task) -> None:
            try:
                task.result()
            except Exception:
                logger.exception("chat.stream.bg.task.error")

        gen_task = asyncio.create_task(_run_generation_bg())
        gen_task.add_done_callback(_bg_done)

        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        except asyncio.CancelledError:
            # Client disconnected / request cancelled: stop emitting to the queue,
            # but intentionally DO NOT cancel the generation task (it persists to DB).
            stream_enabled.clear()
            raise
        finally:
            stream_enabled.clear()

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


# Yield streaming chat completion chunks and always close the upstream stream
async def _iter_chat_completion_chunks(client: Any, **stream_kwargs: object) -> AsyncIterator[Any]:
    stream = await client.chat.completions.create(**stream_kwargs)
    try:
        async for chunk in stream:
            yield chunk
    finally:
        try:
            await stream.close()
        except Exception:
            pass


# Extract streamed text fragments and optionally accumulate tool call fragments
def _extract_text_pieces_and_finish_reason(choice: Any, tool_calls_acc: Optional[dict[int, dict]] = None) -> tuple[list[str], Optional[str]]:
    finish_reason = getattr(choice, "finish_reason", None)
    pieces: list[str] = []

    delta = getattr(choice, "delta", None)
    if delta is not None:
        if tool_calls_acc is not None:
            try:
                _parse_tool_calls_from_delta(delta, tool_calls_acc)
            except Exception:
                pass
        content = getattr(delta, "content", None)
        if isinstance(content, str) and content:
            pieces.append(content)

    # Some providers surface streaming text on choice.text
    text_piece = getattr(choice, "text", None)
    if isinstance(text_piece, str) and text_piece:
        pieces.append(text_piece)

    return pieces, finish_reason


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
async def generate_chat_title(first_user_message: str) -> str:
    client = get_async_openai_compatible_client("openai")
    try:
        title_prompt_path = _BACKEND_DIR / "resources" / "chat_title_prompt.txt"
        title_prompt = title_prompt_path.read_text(encoding="utf-8")
        response = await client.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": f"{title_prompt}{first_user_message[:100]}"}],
        )
        content = response.choices[0].message.content if response.choices else None
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
    finally:
        try:
            await client.close()
        except Exception:
            pass