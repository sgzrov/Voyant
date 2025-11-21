from __future__ import annotations

import os
import re
from typing import Any, Dict
import logging
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import text
from pathlib import Path

from Backend.database import SessionLocal

logger = logging.getLogger(__name__)


def _extract_sql_from_text(text: str) -> str:
    """
    Extract a bare SQL statement from LLM output.
    - Strips ```sql ... ``` fences if present
    - Trims any prose before the first WITH or SELECT (preserve CTEs)
    """
    if not isinstance(text, str):
        return ""
    s = text.strip()
    # Remove fenced blocks
    if s.startswith("```"):
        lines = s.splitlines()
        # Drop the opening fence (``` or ```sql)
        if lines:
            lines = lines[1:]
        # Drop the closing fence if present
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    # Keep from the first top-level keyword start: WITH or SELECT
    m_with = re.search(r"(?is)^\s*with\b", s)
    if m_with:
        s = s[m_with.start():]
    else:
        m_select = re.search(r"(?is)\bselect\b", s)
        if m_select:
            s = s[m_select.start():]
    return s.strip()

load_dotenv()


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
            elif token == "with":
                has_with_top = True

            i = j
            continue

        i += 1

    if first_token != "select" and not has_with_top:
        # Allow CTEs starting with WITH, but we will not rewrite those safely
        raise ValueError("Only SELECT is allowed")

    if has_union_top:
        # Safer to reject UNION at top level for now
        raise ValueError("Complex queries (UNION) are not allowed")

    if has_semicolon_outside:
        raise ValueError("Multiple statements are not allowed")

    # Determine where top-level WHERE ends (before GROUP/ORDER/LIMIT or end)
    clause_starts = [pos for pos in [group_idx, order_idx, limit_idx] if pos >= 0]
    next_clause_start = min(clause_starts) if clause_starts else len(s)

    # Detect if the query already scopes by user_id in a JOIN condition
    has_join_scoped_user = bool(re.search(
        r"(?is)\bjoin\b[\s\S]*?\bon\b[\s\S]*?\b[a-zA-Z_][\w]*\.user_id\s*=\s*:user_id\b", s
    ))

    # Skip injecting a top-level predicate if :user_id appears anywhere (e.g., inside CTEs)
    references_user_param_anywhere = bool(re.search(r":user_id\b", s, flags=re.IGNORECASE))

    # If WHERE exists, ensure user_id predicate present in top-level WHERE span,
    # unless the query is already scoped via JOIN ... ON alias.user_id = :user_id, or
    # :user_id appears elsewhere (e.g., inside CTEs).
    if not references_user_param_anywhere:
        if where_idx >= 0:
            where_body = s[where_idx:next_clause_start]
            has_where_user = bool(re.search(r"(?is)\buser_id\s*=\s*:user_id\b", where_body))
            if not has_where_user and not has_join_scoped_user:
                where_keyword_end = where_idx + len("where")
                # Ensure a space after AND to avoid token merging with the following predicate
                s = s[:where_keyword_end] + " user_id = :user_id AND " + s[where_keyword_end:]
        else:
            # No WHERE: insert before the earliest of GROUP/ORDER/LIMIT or at end
            # Skip insertion if already appropriately scoped in a JOIN condition
            if not has_join_scoped_user:
                insert_pos = next_clause_start
                # Add a trailing space so the next token (e.g., ORDER) doesn't merge into :user_id
                s = s[:insert_pos] + " WHERE user_id = :user_id " + s[insert_pos:]

    # No default LIMIT injection; prompts should include an explicit LIMIT if desired.

    # Prefer real measurements from rollups by excluding empty buckets by default.
    # We only add "AND n > 0" when the query touches a health_rollup_* table AND
    # the SQL does NOT explicitly reason about missing data (n = 0 / n IS NULL / COALESCE(n,...) = 0).
    try:
        uses_rollup = bool(re.search(r"(?is)\bfrom\s+health_rollup_(5min|hourly|daily)\b", s))
        mentions_missing_n = bool(re.search(r"(?is)\bn\s*=\s*0\b|\bn\s+is\s+null\b|coalesce\s*\([^)]*\bn[^)]*\)\s*=\s*0", s))
        already_filters_n_pos = bool(re.search(r"(?is)\bn\s*>\s*0\b", s))
        if uses_rollup and not mentions_missing_n and not already_filters_n_pos:
            # Find the LAST WHERE (closest to end) to avoid matching WHEREs in earlier CTEs/subqueries
            where_matches = list(re.finditer(r"(?is)\bwhere\b", s))
            m_where_last = where_matches[-1] if where_matches else None
            # Find clause boundaries that APPEAR AFTER the chosen WHERE (or from the start if no WHERE)
            start_idx = m_where_last.end() if m_where_last else 0
            m_order_after = re.search(r"(?is)\border\s+by\b", s[start_idx:])
            m_group_after = re.search(r"(?is)\bgroup\s+by\b", s[start_idx:])
            m_limit_after = re.search(r"(?is)\blimit\b", s[start_idx:])
            # Compute absolute indices
            clause_indices = []
            for m in [m_group_after, m_order_after, m_limit_after]:
                if m:
                    clause_indices.append(start_idx + m.start())
            insert_pos = min(clause_indices) if clause_indices else len(s)
            if m_where_last:
                # Append to existing WHERE just before the next clause (or end)
                s = s[:insert_pos] + " AND n > 0 " + s[insert_pos:]
            else:
                # No WHERE: inject WHERE before LIMIT/ORDER/GROUP if present; otherwise at end
                s = s[:insert_pos] + " WHERE n > 0 " + s[insert_pos:]
    except Exception:
        # Best-effort; if anything goes wrong, skip injection
        pass

    # Rewrite improper HAVING without GROUP BY into an outer WHERE on a subquery,
    # so alias filters like HAVING prev_value ... become valid.
    try:
        m_having = re.search(r"(?is)\bhaving\b", s)
        if m_having:
            # Check if there's a GROUP BY at top-level (rough check: presence of ' group by ' before ORDER/LIMIT)
            m_group_any = re.search(r"(?is)\bgroup\s+by\b", s)
            if not m_group_any:
                # Extract the HAVING condition up to ORDER BY / LIMIT or end
                start_cond = m_having.end()
                m_order_after = re.search(r"(?is)\border\s+by\b", s[start_cond:])
                m_limit_after = re.search(r"(?is)\blimit\b", s[start_cond:])
                end_cond_candidates = []
                if m_order_after:
                    end_cond_candidates.append(start_cond + m_order_after.start())
                if m_limit_after:
                    end_cond_candidates.append(start_cond + m_limit_after.start())
                end_cond = min(end_cond_candidates) if end_cond_candidates else len(s)
                having_condition = s[start_cond:end_cond].strip()
                base_sql = s[:m_having.start()].rstrip()
                trailing_clauses = s[end_cond:]
                # Build the wrapped query
                s = f"SELECT * FROM ({base_sql}) AS sub WHERE {having_condition} {trailing_clauses}"
    except Exception:
        # If the rewrite fails, keep original SQL (the executor may still reject invalid forms)
        pass

    # Normalize risky numeric casts produced by the model:
    # JSON values may contain decimals (e.g., "1465.0691") which fail for ::int.
    # Prefer ::float for aggregates; callers can round if needed.
    s = re.sub(r"::\s*(int|integer)\b", "::float", s, flags=re.IGNORECASE)

    # Guardrail: some queries mistakenly filter inferred sessions via session_type='inferred'.
    # Our schema uses session_type for modality (running/walking/...) and source='inferred'|'workout'.
    # When the query touches health_sessions, rewrite that specific predicate.
    try:
        if re.search(r"(?is)\bfrom\s+health_sessions\b", s):
            s_new = re.sub(r"(?is)\bsession_type\s*=\s*'inferred'\b", "source = 'inferred'", s)
            if s_new != s:
                s = s_new
                try:
                    logging.getLogger(__name__).info("sql.rewrite.sessions: replaced session_type='inferred' with source='inferred'")
                except Exception:
                    pass
    except Exception:
        pass

    # If the query targets health_metrics but uses JSON metrics->>'...' syntax,
    # rewrite those JSON extracts to aggregate over rows by metric_type.
    # E.g., AVG((metrics->>'heart_rate')::float) -> AVG(CASE WHEN metric_type='heart_rate' THEN metric_value END)
    if re.search(r"\bfrom\s+health_metrics\b", s, flags=re.IGNORECASE):
        def _rewrite_json_extract(match: re.Match) -> str:
            metric = match.group(1)
            # Preserve any surrounding casts by replacing inner expression only
            return f"(CASE WHEN metric_type = '{metric}' THEN metric_value END)"

        # Replace metrics->>'name'
        s_new = re.sub(r"metrics\s*->>\s*'([a-zA-Z0-9_]+)'", _rewrite_json_extract, s, flags=re.IGNORECASE)
        if s_new != s:
            s = s_new
            try:
                logging.getLogger(__name__).info("sql.rewrite.metrics: applied CASE WHEN metric_type rewrite")
            except Exception:
                pass

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
    prompt_path = Path(__file__).resolve().parents[2] / "resources" / "chat_prompt.txt"
    system_prompt = prompt_path.read_text(encoding = "utf-8")
    resp = client.chat.completions.create(
        model = "gpt-5-mini",
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]
    )
    sql = resp.choices[0].message.content if resp.choices else ""
    try:
        trunc = (sql[:500] + "...") if isinstance(sql, str) and len(sql) > 500 else sql
        logger.info("sql.gen.raw: %s", trunc)
    except Exception:
        pass
    if not isinstance(sql, str) or not sql.strip():
        return {"sql": None, "rows": [], "error": "no-sql"}

    try:
        extracted = _extract_sql_from_text(sql)
        try:
            logger.info("sql.gen.extracted: %s", extracted)
        except Exception:
            pass
        safe_sql = _sanitize_sql(extracted)
        try:
            logger.info("sql.safe: %s", safe_sql)
        except Exception:
            pass
    except Exception as e:
        return {"sql": sql, "rows": [], "error": f"invalid-sql: {e}"}

    with SessionLocal() as session:
        try:
            result = session.execute(text(safe_sql), {"user_id": user_id}).mappings().all()
            rows = [dict(r) for r in result]
            try:
                logger.info("sql.exec: user_id=%s rows=%d", user_id, len(rows))
            except Exception:
                pass
            return {"sql": safe_sql, "rows": rows}
        except Exception as e:
            try:
                logger.exception("sql.error: user_id=%s err=%s", user_id, str(e))
            except Exception:
                pass
            return {"sql": safe_sql, "rows": [], "error": str(e)}


