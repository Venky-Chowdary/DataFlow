"""SQL string literals for safety scans and SQLAlchemy ``text()``.

Scanners that look for ``;``, ``DROP``, or ``:name`` binds must not read the
inside of a quote. ``'O''Brien'``, ``'x; y'`` and ``'{"a":1}'`` are data.
Collapsing whitespace or treating ``:1`` as a bind rewrites the extract
(QA R2-001..R2-004).
"""

from __future__ import annotations

import re

# Doubled quotes are one escaped quote, not the end of the literal.
_SQL_STRING = re.compile(r"'(?:''|[^'])*'")
_PLACEHOLDER = re.compile(r"__DFQ(\d+)__")


def mask_sql_string_literals(sql: str) -> tuple[str, list[str]]:
    """Replace single-quoted literals with ``__DFQ0__`` placeholders.

    The returned list is the original literals, in order, including their quotes.
    """
    found: list[str] = []

    def _repl(match: re.Match[str]) -> str:
        found.append(match.group(0))
        return f"__DFQ{len(found) - 1}__"

    return _SQL_STRING.sub(_repl, sql or ""), found


def restore_sql_string_literals(sql: str, literals: list[str]) -> str:
    """Put masked literals back. Unknown placeholders are left unchanged."""

    def _repl(match: re.Match[str]) -> str:
        idx = int(match.group(1))
        if 0 <= idx < len(literals):
            return literals[idx]
        return match.group(0)

    return _PLACEHOLDER.sub(_repl, sql or "")


def sql_outside_literals(sql: str) -> str:
    """SQL with string contents removed, for keyword and semicolon scans."""
    masked, _literals = mask_sql_string_literals(sql)
    return masked


def escape_sqlalchemy_literal_colons(sql: str) -> str:
    """Escape ``:`` inside string literals so ``sqlalchemy.text`` does not bind them.

    The compiler strips the backslash before the statement reaches the engine,
    so the stored value stays ``{"a":1}``. Binds outside quotes are unchanged.
    """

    def _repl(match: re.Match[str]) -> str:
        return match.group(0).replace(":", r"\:")

    return _SQL_STRING.sub(_repl, sql or "")
