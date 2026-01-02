from __future__ import annotations

import logging
import re


logger = logging.getLogger(__name__)


# Return SQL with string literals and comments replaced by whitespace
def _strip_sql_strings_and_comments(sql: object) -> str:
    if not isinstance(sql, str):
        return ""

    s = sql
    n = len(s)
    out: list[str] = []
    i = 0
    in_line_comment = False
    in_block_comment = False
    in_string = False

    while i < n:
        ch = s[i]
        nxt = s[i + 1] if i + 1 < n else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out.append("\n")
            else:
                out.append(" ")
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                out.append(" ")
                out.append(" ")
                i += 2
            else:
                out.append("\n" if ch == "\n" else " ")
                i += 1
            continue

        if not in_string and ch == "-" and nxt == "-":
            in_line_comment = True
            out.append(" ")
            out.append(" ")
            i += 2
            continue
        if not in_string and ch == "/" and nxt == "*":
            in_block_comment = True
            out.append(" ")
            out.append(" ")
            i += 2
            continue

        if in_string:
            if ch == "'":
                if nxt == "'":
                    out.append("'")
                    out.append("'")
                    i += 2
                    continue
                in_string = False
                out.append("'")
                i += 1
                continue
            out.append("\n" if ch == "\n" else " ")
            i += 1
            continue
        else:
            if ch == "'":
                in_string = True
                out.append("'")
                i += 1
                continue

        out.append(ch)
        i += 1

    return "".join(out)


# Scan the top-level SQL for guardrail flags (UNION, semicolons, etc)
def _scan_top_level_sql(sql_stripped: str) -> dict[str, object]:
    s = sql_stripped or ""
    n = len(s)
    i = 0
    depth = 0
    first_token: str | None = None
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

        if ch == ";":
            has_semicolon_outside = True
            i += 1
            continue

        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue

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

    return {
        "first_token": first_token,
        "where_idx": where_idx,
        "group_idx": group_idx,
        "order_idx": order_idx,
        "limit_idx": limit_idx,
        "has_union_top": has_union_top,
        "has_with_top": has_with_top,
        "has_semicolon_outside": has_semicolon_outside,
    }


# Apply a sequence of rewrite steps to SQL
def _apply_rewrite_pipeline(sql: str, steps: list[tuple[str, callable]]) -> tuple[str, bool]:
    cur = sql
    changed = False
    for _name, fn in steps:
        nxt = fn(cur)
        if nxt != cur:
            changed = True
            cur = nxt
    return cur, changed


def _parse_cte_names(sql_stripped: object) -> set[str]:
    if not isinstance(sql_stripped, str):
        return set()

    s = sql_stripped
    n = len(s)

    def _skip_ws(pos: int) -> int:
        while pos < n and s[pos].isspace():
            pos += 1
        return pos

    def _match_word_at(pos: int, word: str) -> int:
        end = pos + len(word)
        if end > n:
            return -1
        if s[pos:end].lower() != word:
            return -1
        before_ok = pos == 0 or not s[pos - 1].isalnum()
        after_ok = end == n or not s[end:end + 1].isalnum()
        return end if before_ok and after_ok else -1

    def _parse_identifier(pos: int) -> tuple[str | None, int]:
        pos = _skip_ws(pos)
        if pos >= n:
            return None, pos
        if s[pos] == '"':
            pos += 1
            start = pos
            while pos < n:
                if s[pos] == '"':
                    name = s[start:pos]
                    return name, pos + 1
                pos += 1
            return None, pos

        m = re.match(r"[a-zA-Z_]\w*", s[pos:])
        if not m:
            return None, pos
        name = m.group(0)
        return name, pos + len(name)

    def _skip_balanced_parens(pos: int) -> int:
        pos = _skip_ws(pos)
        if pos >= n or s[pos] != "(":
            return pos
        depth = 0
        while pos < n:
            if s[pos] == "(":
                depth += 1
            elif s[pos] == ")":
                depth = max(0, depth - 1)
                if depth == 0:
                    return pos + 1
            pos += 1
        return pos

    i = _skip_ws(0)
    end_with = _match_word_at(i, "with")
    if end_with < 0:
        return set()
    i = _skip_ws(end_with)
    end_recursive = _match_word_at(i, "recursive")
    if end_recursive >= 0:
        i = _skip_ws(end_recursive)

    names: set[str] = set()
    while i < n:
        name, i2 = _parse_identifier(i)
        if not name:
            break
        names.add(name.lower())
        i = _skip_ws(i2)
        if i < n and s[i] == "(":
            i = _skip_ws(_skip_balanced_parens(i))
        end_as = _match_word_at(i, "as")
        if end_as < 0:
            break
        i = _skip_ws(end_as)
        if i >= n or s[i] != "(":
            break
        i = _skip_ws(_skip_balanced_parens(i))
        if i < n and s[i] == ",":
            i = _skip_ws(i + 1)
            continue
        break

    return names


# Validate SQL sources (FROM/JOIN) against an allowlist; return which health tables are referenced
def _validate_sql_sources(sql: str, allowed_sources: set[str], health_sources: set[str]) -> set[str]:
    stripped = _strip_sql_strings_and_comments(sql)
    cte_names = _parse_cte_names(stripped)

    disallowed: set[str] = set()
    seen: set[str] = set()

    # Regex is a bit fussy: avoid treating "JOIN LATERAL" as a table named "lateral".
    for m in re.finditer(r"(?is)\b(from|join)\s+(?:lateral\s+)?(?!lateral\b)([a-zA-Z_][\w\.]*)\b", stripped):
        src = m.group(2)
        base = src.split(".")[-1].lower()
        if base in cte_names:
            continue
        seen.add(base)
        if base in allowed_sources:
            continue
        disallowed.add(base)

    if disallowed:
        raise ValueError(f"Disallowed SQL sources: {', '.join(sorted(disallowed))}")

    used_health = seen & {s.lower() for s in health_sources}
    # Require that the query references at least one health data source
    if not used_health:
        raise ValueError("Query must reference a health table (health_rollup_hourly, health_rollup_daily, and/or health_events)")
    return used_health


# Replace references to health_rollup_hourly table with a tz-normalized derived table
def _rewrite_rollup_hourly_to_tz_derived(sql: str) -> str:
    try:
        stripped = _strip_sql_strings_and_comments(sql)
        if not re.search(r"(?is)\b(from|join)\s+health_rollup_hourly\b", stripped):
            return sql

        hourly_subquery = (
            "(SELECT\n"
            "   user_id,\n"
            "   (date_trunc('hour', bucket_ts AT TIME ZONE :tz_name) AT TIME ZONE :tz_name) AS bucket_ts,\n"
            "   metric_type,\n"
            "   (ARRAY_AGG(meta) FILTER (WHERE meta IS NOT NULL))[1] AS meta,\n"
            "   CASE WHEN SUM(n) > 0 THEN SUM(COALESCE(avg_value, 0) * n) / SUM(n) END AS avg_value,\n"
            "   SUM(COALESCE(sum_value, 0)) AS sum_value,\n"
            "   MIN(min_value) AS min_value,\n"
            "   MAX(max_value) AS max_value,\n"
            "   SUM(n) AS n\n"
            " FROM health_rollup_hourly\n"
            " WHERE user_id = :user_id\n"
            " GROUP BY 1,2,3)"
        )

        def _rewrite_from(m: re.Match) -> str:
            alias = m.group("alias")
            alias_out = alias if alias else "health_rollup_hourly"
            return f"FROM {hourly_subquery} AS {alias_out}"

        def _rewrite_join(m: re.Match) -> str:
            alias = m.group("alias")
            alias_out = alias if alias else "health_rollup_hourly"
            return f"JOIN {hourly_subquery} AS {alias_out}"

        # Avoid treating SQL keywords as aliases (e.g. "FROM health_rollup_hourly WHERE ...")
        _no_alias_keywords = (
            r"where|group|order|limit|join|on|inner|left|right|full|cross|union|having|"
            r"window|offset|fetch|for|into|values|select|from"
        )

        out = re.sub(
            rf"(?is)\bfrom\s+health_rollup_hourly(?:\s+(?:as\s+)?(?P<alias>(?!({_no_alias_keywords})\b)[a-zA-Z_]\w*))?\b",
            _rewrite_from,
            sql,
        )
        out = re.sub(
            rf"(?is)\bjoin\s+health_rollup_hourly(?:\s+(?:as\s+)?(?P<alias>(?!({_no_alias_keywords})\b)[a-zA-Z_]\w*))?\b",
            _rewrite_join,
            out,
        )
        if out != sql:
            logger.info("sql.rewrite.hourly: rewrote health_rollup_hourly to tz-localized derived table (alias-preserving)")
        return out
    except Exception:
        logger.exception("sql.rewrite.hourly.failed")
        return sql


# Replace references to health_rollup_daily table with a tz-normalized derived table (day buckets).
def _rewrite_rollup_daily_to_tz_derived(sql: str) -> str:
    try:
        stripped = _strip_sql_strings_and_comments(sql)
        if not re.search(r"(?is)\b(from|join)\s+health_rollup_daily\b", stripped):
            return sql

        daily_subquery = (
            "(SELECT\n"
            "   user_id,\n"
            "   (date_trunc('day', bucket_ts AT TIME ZONE :tz_name) AT TIME ZONE :tz_name) AS bucket_ts,\n"
            "   metric_type,\n"
            "   (ARRAY_AGG(meta) FILTER (WHERE meta IS NOT NULL))[1] AS meta,\n"
            "   CASE WHEN SUM(n) > 0 THEN SUM(COALESCE(avg_value, 0) * n) / SUM(n) END AS avg_value,\n"
            "   SUM(COALESCE(sum_value, 0)) AS sum_value,\n"
            "   MIN(min_value) AS min_value,\n"
            "   MAX(max_value) AS max_value,\n"
            "   SUM(n) AS n\n"
            " FROM health_rollup_daily\n"
            " WHERE user_id = :user_id\n"
            " GROUP BY 1,2,3)"
        )

        def _rewrite_from(m: re.Match) -> str:
            alias = m.group("alias")
            alias_out = alias if alias else "health_rollup_daily"
            return f"FROM {daily_subquery} AS {alias_out}"

        def _rewrite_join(m: re.Match) -> str:
            alias = m.group("alias")
            alias_out = alias if alias else "health_rollup_daily"
            return f"JOIN {daily_subquery} AS {alias_out}"

        _no_alias_keywords = (
            r"where|group|order|limit|join|on|inner|left|right|full|cross|union|having|"
            r"window|offset|fetch|for|into|values|select|from"
        )

        out = re.sub(
            rf"(?is)\bfrom\s+health_rollup_daily(?:\s+(?:as\s+)?(?P<alias>(?!({_no_alias_keywords})\b)[a-zA-Z_]\w*))?\b",
            _rewrite_from,
            sql,
        )
        out = re.sub(
            rf"(?is)\bjoin\s+health_rollup_daily(?:\s+(?:as\s+)?(?P<alias>(?!({_no_alias_keywords})\b)[a-zA-Z_]\w*))?\b",
            _rewrite_join,
            out,
        )
        if out != sql:
            logger.info("sql.rewrite.daily: rewrote health_rollup_daily to tz-localized derived table (alias-preserving)")
        return out
    except Exception:
        logger.exception("sql.rewrite.daily.failed")
        return sql


# Replace references to health_events table with a user-scoped derived table (alias-preserving).
# This avoids requiring the LLM to remember a user_id predicate in multi-table queries.
def _rewrite_health_events_to_user_scoped(sql: str) -> str:
    try:
        stripped = _strip_sql_strings_and_comments(sql)
        if not re.search(r"(?is)\b(from|join)\s+health_events\b", stripped):
            return sql

        events_subquery = (
            "(SELECT\n"
            "   *\n"
            " FROM health_events\n"
            " WHERE user_id = :user_id)"
        )

        def _rewrite_from(m: re.Match) -> str:
            alias = m.group("alias")
            alias_out = alias if alias else "health_events"
            return f"FROM {events_subquery} AS {alias_out}"

        def _rewrite_join(m: re.Match) -> str:
            alias = m.group("alias")
            alias_out = alias if alias else "health_events"
            return f"JOIN {events_subquery} AS {alias_out}"

        _no_alias_keywords = (
            r"where|group|order|limit|join|on|inner|left|right|full|cross|union|having|"
            r"window|offset|fetch|for|into|values|select|from"
        )

        out = re.sub(
            rf"(?is)\bfrom\s+health_events(?:\s+(?:as\s+)?(?P<alias>(?!({_no_alias_keywords})\b)[a-zA-Z_]\w*))?\b",
            _rewrite_from,
            sql,
        )
        out = re.sub(
            rf"(?is)\bjoin\s+health_events(?:\s+(?:as\s+)?(?P<alias>(?!({_no_alias_keywords})\b)[a-zA-Z_]\w*))?\b",
            _rewrite_join,
            out,
        )
        if out != sql:
            logger.info("sql.rewrite.events: rewrote health_events to user-scoped derived table (alias-preserving)")
        return out
    except Exception:
        logger.exception("sql.rewrite.events.failed")
        return sql


# Extract a bare SQL statement from LLM output
def _extract_sql_from_text(text: object) -> str:
    if not isinstance(text, str):
        return ""

    # Drop fenced blocks
    s = text.strip()
    if s.startswith("```"):
        lines = s.splitlines()
        if lines:
            lines = lines[1:]
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


# Sanitize a SQL statement to enforce guardrails and enforce allowed sources
def _sanitize_sql(sql: str) -> str:
    original = sql.strip()
    if original.endswith(";"):
        original = original[:-1].rstrip()

    s = original
    stripped = _strip_sql_strings_and_comments(s)

    # Only allow :user_id and :tz_name binds (avoid matching PG casts like ::date)
    bind_names = set(re.findall(r"(?is)(?<!:):([a-zA-Z_]\w*)\b", stripped))
    unknown_binds = sorted([b for b in bind_names if b.lower() not in {"user_id", "tz_name"}])
    if unknown_binds:
        raise ValueError(f"Unsupported bind parameters: {', '.join(unknown_binds)}")

    used_health_sources = _validate_sql_sources(
        s,
        allowed_sources={
            "health_rollup_hourly",
            "health_rollup_daily",
            "health_events",
            "generate_series",
            "unnest",
        },
        health_sources={"health_rollup_hourly", "health_rollup_daily", "health_events"},
    )

    # Allow multi-table ONLY for the specific combos:
    # - health_events (workouts/events)
    # - health_rollup_hourly or health_rollup_daily (recovery metrics)
    if len(used_health_sources) > 1:
        if used_health_sources not in (
            {"health_events", "health_rollup_hourly"},
            {"health_events", "health_rollup_daily"},
        ):
            raise ValueError(
                "Query must use exactly one health table (health_rollup_hourly OR health_rollup_daily OR health_events), "
                "or the specific combos (health_events + health_rollup_hourly) / (health_events + health_rollup_daily)"
            )

    # Always rewrite to safe derived tables BEFORE further validation/rewrites:
    # - hourly rollups get tz-normalized and scoped to user_id
    # - events get scoped to user_id
    if "health_rollup_hourly" in used_health_sources:
        s = _rewrite_rollup_hourly_to_tz_derived(s)
        stripped = _strip_sql_strings_and_comments(s)
    if "health_rollup_daily" in used_health_sources:
        s = _rewrite_rollup_daily_to_tz_derived(s)
        stripped = _strip_sql_strings_and_comments(s)
    if "health_events" in used_health_sources:
        s = _rewrite_health_events_to_user_scoped(s)
        stripped = _strip_sql_strings_and_comments(s)

    scan = _scan_top_level_sql(stripped)
    first_token = scan["first_token"]
    where_idx = int(scan["where_idx"])
    group_idx = int(scan["group_idx"])
    order_idx = int(scan["order_idx"])
    limit_idx = int(scan["limit_idx"])
    has_union_top = bool(scan["has_union_top"])
    has_with_top = bool(scan["has_with_top"])
    has_semicolon_outside = bool(scan["has_semicolon_outside"])

    if first_token != "select" and not has_with_top:
        raise ValueError("Only SELECT is allowed")

    if has_union_top:
        raise ValueError("Complex queries (UNION) are not allowed")

    if has_semicolon_outside:
        raise ValueError("Multiple statements are not allowed")

    clause_starts = [pos for pos in [group_idx, order_idx, limit_idx] if pos >= 0]
    next_clause_start = min(clause_starts) if clause_starts else len(s)

    stripped = _strip_sql_strings_and_comments(s)

    has_join_scoped_user = bool(re.search(
        r"(?is)\bjoin\b[\s\S]*?\bon\b[\s\S]*?\b[a-zA-Z_][\w]*\.user_id\s*=\s*:user_id\b", stripped
    ))
    has_user_predicate_anywhere = bool(re.search(
        r"(?is)\b([a-zA-Z_][\w]*\.)?user_id\s*=\s*:user_id\b", stripped
    ))

    # Ensure queries are scoped to the authenticated user (unless already scoped elsewhere).
    # For health_events and health_rollup_hourly we rewrite to user-scoped derived tables above,
    # which prevents ambiguous `user_id` injection in multi-table queries.
    if not has_user_predicate_anywhere:
        if where_idx >= 0:
            where_body = s[where_idx:next_clause_start]
            where_body_stripped = _strip_sql_strings_and_comments(where_body)
            has_where_user = bool(re.search(r"(?is)\b([a-zA-Z_][\w]*\.)?user_id\s*=\s*:user_id\b", where_body_stripped))
            if not has_where_user and not has_join_scoped_user:
                where_keyword_end = where_idx + len("where")
                s = s[:where_keyword_end] + " user_id = :user_id AND " + s[where_keyword_end:]
        else:
            if not has_join_scoped_user:
                insert_pos = next_clause_start
                s = s[:insert_pos] + " WHERE user_id = :user_id " + s[insert_pos:]

    try:
        m_having = re.search(r"(?is)\bhaving\b", s)
        if m_having:
            m_group_any = re.search(r"(?is)\bgroup\s+by\b", s)
            if not m_group_any:
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
                s = f"SELECT * FROM ({base_sql}) AS sub WHERE {having_condition} {trailing_clauses}"
    except Exception:
        pass

    # JSON numeric strings may include decimals; ::int can fail in generated SQL.
    s = re.sub(r"::\s*(int|integer)\b", "::float", s, flags=re.IGNORECASE)

    # Normalize day-level comparisons to the user's timezone (best-effort rewrites).
    try:
        def _rewrite_day_eq_to_range(match: re.Match) -> str:
            expr = match.group(1).strip()
            base = "(bucket_ts AT TIME ZONE :tz_name)"
            return f"{base} >= ({expr})::timestamp AND {base} < (({expr})::timestamp + INTERVAL '1 day')"
        base = "(bucket_ts AT TIME ZONE :tz_name)"
        def _rewrite_ts_day_eq_to_range(match: re.Match) -> str:
            expr = match.group(1).strip()
            base = "(timestamp AT TIME ZONE :tz_name)"
            return f"{base} >= ({expr})::timestamp AND {base} < (({expr})::timestamp + INTERVAL '1 day')"
        def _rewrite_dates(sql_in: str) -> str:
            steps: list[tuple[str, callable]] = [
                ("current_date", lambda x: re.sub(r"(?is)\bcurrent_date\b", "(now() AT TIME ZONE :tz_name)::date", x)),
                ("date_bucket_ts", lambda x: re.sub(
                    r"(?is)\bdate\s*\(\s*((?:[a-zA-Z_][\w]*\.)?)bucket_ts\s*\)",
                    r"DATE(\1bucket_ts AT TIME ZONE :tz_name)",
                    x,
                )),
                ("date_timestamp", lambda x: re.sub(
                    r"(?is)\bdate\s*\(\s*((?:[a-zA-Z_][\w]*\.)?)timestamp\s*\)",
                    r"DATE(\1timestamp AT TIME ZONE :tz_name)",
                    x,
                )),
                ("bucket_ts_cast_date", lambda x: re.sub(
                    r"(?is)\b((?:[a-zA-Z_][\w]*\.)?)bucket_ts\s*::\s*date\b",
                    r"( \1bucket_ts AT TIME ZONE :tz_name )::date",
                    x,
                )),
                ("timestamp_cast_date", lambda x: re.sub(
                    r"(?is)\b((?:[a-zA-Z_][\w]*\.)?)timestamp\s*::\s*date\b",
                    r"( \1timestamp AT TIME ZONE :tz_name )::date",
                    x,
                )),
                ("bucket_ts_day_eq_yesterday", lambda x: re.sub(
                    r"(?is)\(\s*bucket_ts\s+at\s+time\s+zone\s*:tz_name\s*\)\s*::\s*date\s*=\s*\(\s*now\(\)\s+at\s+time\s+zone\s*:tz_name\s*\)\s*::\s*date\s*-\s*interval\s*'1\s*day'\b",
                    f"{base} >= (date_trunc('day', now() AT TIME ZONE :tz_name) - INTERVAL '1 day') AND {base} < date_trunc('day', now() AT TIME ZONE :tz_name)",
                    x,
                )),
                ("bucket_ts_day_eq_today", lambda x: re.sub(
                    r"(?is)\(\s*bucket_ts\s+at\s+time\s+zone\s*:tz_name\s*\)\s*::\s*date\s*=\s*\(\s*now\(\)\s+at\s+time\s+zone\s*:tz_name\s*\)\s*::\s*date\b",
                    f"{base} >= date_trunc('day', now() AT TIME ZONE :tz_name) AND {base} < (date_trunc('day', now() AT TIME ZONE :tz_name) + INTERVAL '1 day')",
                    x,
                )),
                ("bucket_ts_day_eq_generic", lambda x: re.sub(
                    r"(?is)\(\s*bucket_ts\s+at\s+time\s+zone\s*:tz_name\s*\)\s*::\s*date\s*=\s*(.+?)(?=\s+\bAND\b|\s+\bOR\b|\s+\bGROUP\b|\s+\bORDER\b|\s+\bLIMIT\b|\)|$)",
                    _rewrite_day_eq_to_range,
                    x,
                )),
                ("timestamp_day_eq_generic", lambda x: re.sub(
                    r"(?is)\(\s*timestamp\s+at\s+time\s+zone\s*:tz_name\s*\)\s*::\s*date\s*=\s*(.+?)(?=\s+\bAND\b|\s+\bOR\b|\s+\bGROUP\b|\s+\bORDER\b|\s+\bLIMIT\b|\)|$)",
                    _rewrite_ts_day_eq_to_range,
                    x,
                )),
                ("date_trunc_day_now", lambda x: re.sub(
                    r"(?is)date_trunc\s*\(\s*'day'\s*,\s*now\s*\(\s*\)\s*\)",
                    "date_trunc('day', now() AT TIME ZONE :tz_name)",
                    x,
                )),
                ("now", lambda x: re.sub(
                    r"(?is)\bnow\s*\(\s*\)\b(?!\s+at\s+time\s+zone\b)",
                    "(now() AT TIME ZONE :tz_name)",
                    x,
                )),
                ("bucket_ts_comparisons", lambda x: re.sub(
                    r"(?is)\b((?:[a-zA-Z_][\w]*\.)?)bucket_ts\s*(>=|>|<=|<)\s*",
                    r"(\1bucket_ts AT TIME ZONE :tz_name) \2 ",
                    x,
                )),
                ("timestamp_comparisons", lambda x: re.sub(
                    r"(?is)\b((?:[a-zA-Z_][\w]*\.)?)timestamp\s*(>=|>|<=|<)\s*",
                    r"(\1timestamp AT TIME ZONE :tz_name) \2 ",
                    x,
                )),
                ("placeholder_yesterday_start", lambda x: re.sub(
                    r"(?is)'yesterday_start'",
                    "(date_trunc('day', now() AT TIME ZONE :tz_name) - INTERVAL '1 day')",
                    x,
                )),
                ("placeholder_yesterday_end", lambda x: re.sub(
                    r"(?is)'yesterday_end'",
                    "date_trunc('day', now() AT TIME ZONE :tz_name)",
                    x,
                )),
                ("placeholder_today_start", lambda x: re.sub(
                    r"(?is)'today_start'",
                    "date_trunc('day', now() AT TIME ZONE :tz_name)",
                    x,
                )),
                ("placeholder_today_end", lambda x: re.sub(
                    r"(?is)'today_end'",
                    "(date_trunc('day', now() AT TIME ZONE :tz_name) + INTERVAL '1 day')",
                    x,
                )),
            ]
            out, _changed = _apply_rewrite_pipeline(sql_in, steps)
            return out

        s_new = _rewrite_dates(s)
        if s_new != s:
            s = s_new
            logger.info("sql.rewrite.dates: normalized CURRENT_DATE and DATE(bucket_ts) to user tz")
    except Exception:
        pass

    # Block direct queries to the raw metrics table (keep the tool limited to events + rollups).
    stripped_final = _strip_sql_strings_and_comments(s)
    if re.search(r"\b(from|join)\s+health_metrics\b", stripped_final, flags=re.IGNORECASE):
        raise ValueError("Raw metrics table is not available; use health_rollup_hourly and/or health_events")

    if re.search(r"(?is)\b(insert|update|delete|drop|alter|create|truncate|grant|revoke)\b", stripped_final):
        raise ValueError("Forbidden tokens in SQL")

    return s