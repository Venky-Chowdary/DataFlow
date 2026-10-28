"""SQL string literals and comments for safety scans and SQLAlchemy ``text()``.

Scanners that look for ``;``, ``DROP``, or ``:name`` binds must not read the
inside of a quote. ``'O''Brien'``, ``'x; y'`` and ``'{"a":1}'`` are data.
Collapsing whitespace or treating ``:1`` as a bind rewrites the extract
(QA R2-001..R2-004).

The text is lexed left to right, so a quote inside a ``--`` or ``/* */``
comment does not open a literal, and a quote inside a quoted identifier does
not either. Engines disagree on lexing (backslash escapes, ``#`` comments,
``[ident]``, ``$$`` bodies, nested comments), so read-only guards that cannot
know the engine judge every view in :func:`sql_code_views` and refuse if any
of them shows a write or a second statement.
"""

from __future__ import annotations

import re
from typing import NamedTuple

_PLACEHOLDER = re.compile(r"__DFQ(\d+)__")
_DOLLAR_TAG = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")


class _Lexing(NamedTuple):
    backslash: bool = False
    dq_string: bool = False
    hash_comment: bool = False
    slash_comment: bool = False
    dash_needs_space: bool = False
    nested_block: bool = False
    dollar: bool = False
    e_strings: bool = False
    brackets: bool = False
    backticks: bool = False
    cond_comments: bool = False


_ANSI = _Lexing()
_FLAVORS: tuple[_Lexing, ...] = (
    _ANSI,
    # PostgreSQL family
    _Lexing(nested_block=True, dollar=True, e_strings=True),
    # MySQL / MariaDB
    _Lexing(backslash=True, dq_string=True, hash_comment=True,
            dash_needs_space=True, backticks=True, cond_comments=True),
    # SQL Server / Sybase
    _Lexing(brackets=True, nested_block=True),
    # BigQuery / Snowflake / Spark / Databricks
    _Lexing(backslash=True, hash_comment=True, slash_comment=True,
            dollar=True, backticks=True),
    # SQLite
    _Lexing(brackets=True, backticks=True),
)


def _end_of_quoted(sql: str, i: int, quote: str, backslash: bool) -> int:
    """Index just past the quote run opened at ``sql[i]`` (doubled quote escapes)."""
    n = len(sql)
    j = i + 1
    while j < n:
        ch = sql[j]
        if backslash and ch == "\\":
            j += 2
            continue
        if ch == quote:
            if j + 1 < n and sql[j + 1] == quote:
                j += 2
                continue
            return j + 1
        j += 1
    return n


def _end_of_block_comment(sql: str, i: int, nested: bool) -> int:
    n = len(sql)
    depth = 1
    j = i + 2
    while j < n:
        if nested and sql.startswith("/*", j):
            depth += 1
            j += 2
            continue
        if sql.startswith("*/", j):
            depth -= 1
            j += 2
            if depth == 0:
                return j
            continue
        j += 1
    return n


def _line_end(sql: str, i: int) -> int:
    end = sql.find("\n", i)
    return len(sql) if end == -1 else end


def _tokens(sql: str, lex: _Lexing = _ANSI) -> list[tuple[str, str]]:
    """Split into ``code`` / ``literal`` / ``comment`` / ``ident`` runs."""
    out: list[tuple[str, str]] = []
    n = len(sql)
    i = 0
    code_start = 0

    def _emit(kind: str, start: int, end: int) -> None:
        if code_start < start:
            out.append(("code", sql[code_start:start]))
        out.append((kind, sql[start:end]))

    cond_open = False
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        end = -1
        kind = ""
        if lex.cond_comments and ch == "/" and nxt == "*" and sql.startswith("/*!", i):
            # MySQL runs ``/*! ... */`` as code. Only the delimiters are
            # comment; the body is lexed (and judged) as ordinary SQL.
            j = i + 3
            while j < n and j < i + 8 and sql[j].isdigit():
                j += 1
            kind, end, cond_open = "comment", j, True
        elif cond_open and ch == "*" and nxt == "/":
            kind, end, cond_open = "comment", i + 2, False
        elif ch == "-" and nxt == "-":
            after = sql[i + 2] if i + 2 < n else ""
            if not lex.dash_needs_space or after == "" or after.isspace() or ord(after) < 32:
                kind, end = "comment", _line_end(sql, i)
        elif ch == "#" and lex.hash_comment:
            kind, end = "comment", _line_end(sql, i)
        elif ch == "/" and nxt == "/" and lex.slash_comment:
            kind, end = "comment", _line_end(sql, i)
        elif ch == "/" and nxt == "*":
            kind, end = "comment", _end_of_block_comment(sql, i, lex.nested_block)
        elif ch == "'":
            escaped = lex.backslash or (
                lex.e_strings and i > 0 and sql[i - 1] in "eE"
                and (i < 2 or not (sql[i - 2].isalnum() or sql[i - 2] == "_"))
            )
            kind, end = "literal", _end_of_quoted(sql, i, "'", escaped)
        elif ch == '"':
            if lex.dq_string:
                kind, end = "literal", _end_of_quoted(sql, i, '"', lex.backslash)
            else:
                kind, end = "ident", _end_of_quoted(sql, i, '"', False)
        elif ch == "`" and lex.backticks:
            kind, end = "ident", _end_of_quoted(sql, i, "`", False)
        elif ch == "[" and lex.brackets:
            kind, end = "ident", _end_of_quoted(sql, i, "]", False)
        elif ch == "$" and lex.dollar and not (i > 0 and (sql[i - 1].isalnum() or sql[i - 1] == "_")):
            tag = _DOLLAR_TAG.match(sql, i)
            if tag:
                close = sql.find(tag.group(0), tag.end())
                kind, end = "literal", n if close == -1 else close + len(tag.group(0))
        if kind:
            _emit(kind, i, end)
            i = end
            code_start = end
            continue
        i += 1
    if code_start < n:
        out.append(("code", sql[code_start:]))
    return out


def mask_sql_string_literals(sql: str) -> tuple[str, list[str]]:
    """Replace single-quoted literals with ``__DFQ0__`` placeholders.

    The returned list is the original literals, in order, including their quotes.
    Comments and quoted identifiers are kept verbatim; quotes inside them are
    not literals.
    """
    found: list[str] = []
    parts: list[str] = []
    for kind, text in _tokens(sql or ""):
        if kind == "literal":
            found.append(text)
            parts.append(f"__DFQ{len(found) - 1}__")
        else:
            parts.append(text)
    return "".join(parts), found


def restore_sql_string_literals(sql: str, literals: list[str]) -> str:
    """Put masked literals back. Unknown placeholders are left unchanged."""

    def _repl(match: re.Match[str]) -> str:
        idx = int(match.group(1))
        if 0 <= idx < len(literals):
            return literals[idx]
        return match.group(0)

    return _PLACEHOLDER.sub(_repl, sql or "")


def _render(tokens: list[tuple[str, str]], *, idents: str) -> str:
    parts: list[str] = []
    lit = ident = 0
    for kind, text in tokens:
        if kind == "code":
            parts.append(text)
        elif kind == "comment":
            parts.append(" ")
        elif kind == "literal":
            parts.append(f"__DFQ{lit}__")
            lit += 1
        elif idents == "mask":
            parts.append(f"__DFI{ident}__")
            ident += 1
        elif idents == "name":
            parts.append(re.sub(r"\W", "_", text[1:-1] if len(text) > 1 else ""))
        else:
            parts.append(text)
    return "".join(parts)


def sql_outside_literals(sql: str) -> str:
    """SQL with string contents and comments removed, for keyword and semicolon scans."""
    return _render(_tokens(sql or ""), idents="keep")


def sql_code_views(sql: str) -> tuple[str, ...]:
    """Code outside literals, comments and quoted identifiers, once per lexing.

    A read-only guard must accept the text under every view: a payload that
    one engine reads as a comment or literal and another as code is refused.
    """
    views: list[str] = []
    for lex in _FLAVORS:
        view = _render(_tokens(sql or "", lex), idents="mask")
        if view not in views:
            views.append(view)
    return tuple(views)


def sql_call_views(sql: str) -> tuple[str, ...]:
    """Like :func:`sql_code_views`, but quoted identifiers keep their name.

    ``"pg_sleep"(5)`` and ``[xp_cmdshell] 'dir'`` still call the function, so
    a function-name scan must see them.
    """
    views: list[str] = []
    for lex in _FLAVORS:
        view = _render(_tokens(sql or "", lex), idents="name")
        if view not in views:
            views.append(view)
    return tuple(views)


def escape_sqlalchemy_literal_colons(sql: str) -> str:
    """Escape ``:`` inside string literals so ``sqlalchemy.text`` does not bind them.

    The compiler strips the backslash before the statement reaches the engine,
    so the stored value stays ``{"a":1}``. Binds outside quotes are unchanged.
    """
    return "".join(
        text.replace(":", r"\:") if kind == "literal" else text
        for kind, text in _tokens(sql or "")
    )
