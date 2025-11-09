from __future__ import annotations

import os
import re
from typing import Any, Dict
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import text
from pathlib import Path

from Backend.database import SessionLocal

load_dotenv()


def _load_schema_prompt() -> str:
    prompt_path = Path(__file__).resolve().parents[2] / "resources" / "sql_schema_prompt.txt"
    return prompt_path.read_text(encoding = "utf-8")

# Make generated SQL safe and scoped: require top-level SELECT, user_id = :user_id, and LIMIT 200
def _sanitize_sql(sql: str) -> str:
    original = sql.strip()
    if original.endswith(";"):
        original = original[:-1].rstrip()

    s = original
    n = len(s)
    i = 0
    depth = 0
    in_line_comment = False
    in_block_comment = False
    in_string = False
    first_token = None
    where_idx = -1
    group_idx = -1
    order_idx = -1
    limit_idx = -1
    has_union_top = False
    has_with_top = False
    has_semicolon_outside = False

    def _match_word_at(pos: int, word: str) -> bool:
        end = pos + len(word)
        if end > n:
            return False
        segment = s[pos:end].lower()
        if segment != word:
            return False
        before_ok = pos == 0 or not s[pos - 1].isalnum()
        after_ok = end == n or not s[end:end + 1].isalnum()
        return before_ok and after_ok

    def _skip_ws(pos: int) -> int:
        while pos < n and s[pos].isspace():
            pos += 1
        return pos

    while i < n:
        ch = s[i]
        nxt = s[i + 1] if i + 1 < n else ""

        # Handle end of line comment
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        # Handle end of block comment
        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        # Detect start of comments
        if ch == "-" and nxt == "-":
            in_line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue

        # Handle string literal (single quotes)
        if in_string:
            if ch == "'":
                # Handle escaped single quote by doubling ''
                if nxt == "'":
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        else:
            if ch == "'":
                in_string = True
                i += 1
                continue

        # Dangerous: additional semicolons outside strings/comments
        if ch == ";":
            has_semicolon_outside = True
            i += 1
            continue

        # Track parentheses depth
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue

        # Top-level keyword detection
        if depth == 0 and ch.isalpha():
            j = i
            while j < n and s[j].isalpha():
                j += 1
            token = s[i:j].lower()
            if first_token is None and token:
                first_token = token

            if token == "where" and where_idx < 0:
                where_idx = i
            elif token == "group" and group_idx < 0:
                k = _skip_ws(j)
                if _match_word_at(k, "by"):
                    group_idx = i
            elif token == "order" and order_idx < 0:
                k = _skip_ws(j)
                if _match_word_at(k, "by"):
                    order_idx = i
            elif token == "limit" and limit_idx < 0:
                limit_idx = i
            elif token == "union":
                has_union_top = True
            elif token == "with" and first_token is None:
                has_with_top = True

            i = j
            continue

        i += 1

    if first_token != "select" and not has_with_top:
        # Allow CTEs starting with WITH, but we will not rewrite those safely
        raise ValueError("Only SELECT is allowed")

    if has_with_top or has_union_top:
        # Safer to reject rather than incorrectly rewriting complex top-level structures
        raise ValueError("Complex queries (WITH/UNION) are not allowed")

    if has_semicolon_outside:
        raise ValueError("Multiple statements are not allowed")

    # Determine where top-level WHERE ends (before GROUP/ORDER/LIMIT or end)
    clause_starts = [pos for pos in [group_idx, order_idx, limit_idx] if pos >= 0]
    next_clause_start = min(clause_starts) if clause_starts else len(s)

    # If WHERE exists, ensure user_id predicate present in top-level WHERE span
    if where_idx >= 0:
        where_body = s[where_idx:next_clause_start]
        if not re.search(r"(?is)\buser_id\s*=\s*:user_id\b", where_body):
            where_keyword_end = where_idx + len("where")
            s = s[:where_keyword_end] + " user_id = :user_id AND" + s[where_keyword_end:]
    else:
        # No WHERE: insert before the earliest of GROUP/ORDER/LIMIT or at end
        insert_pos = next_clause_start
        s = s[:insert_pos] + " WHERE user_id = :user_id" + s[insert_pos:]

    # Ensure top-level LIMIT present
    if limit_idx < 0:
        s = s + " LIMIT 200"

    # Final forbidden tokens check (case-insensitive)
    lowered = s.lower()
    forbidden_words = [" insert ", " update ", " delete ", " drop ", " alter "]
    if any(tok in lowered for tok in forbidden_words):
        raise ValueError("Forbidden tokens in SQL")

    return s

# Execute generated SQL command and return it and the result rows
def execute_generated_sql(user_id: str, question: str) -> Dict[str, Any]:
    client = OpenAI(api_key = os.getenv("OPENAI_API_KEY"))
    prompt = f"Question: {question}\nReturn only SQL."
    resp = client.chat.completions.create(
        model = "gpt-4o-mini",
        messages = [
            {"role": "system", "content": _load_schema_prompt()},
            {"role": "user", "content": prompt},
        ],
        temperature = 0.1,
    )
    sql = resp.choices[0].message.content if resp.choices else ""
    if not isinstance(sql, str) or not sql.strip():
        return {"sql": None, "rows": [], "error": "no-sql"}

    try:
        safe_sql = _sanitize_sql(sql)
    except Exception as e:
        return {"sql": sql, "rows": [], "error": f"invalid-sql: {e}"}

    with SessionLocal() as session:
        try:
            result = session.execute(text(safe_sql), {"user_id": user_id}).mappings().all()
            rows = [dict(r) for r in result]
            return {"sql": safe_sql, "rows": rows}
        except Exception as e:
            return {"sql": safe_sql, "rows": [], "error": str(e)}


