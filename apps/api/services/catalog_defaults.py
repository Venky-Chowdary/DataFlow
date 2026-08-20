"""Canonical source-catalog DEFAULT normalization, per engine spelling.

Engines do not agree on what ``information_schema`` stores for a column
default, and the disagreement is not cosmetic:

* **MySQL 5.7 / 8.0** stores a *literal default as its bare value*.
  ``status VARCHAR(32) DEFAULT 'active'`` reads back as ``active`` — no quotes.
  Expression defaults (8.0.13+) read back as the expression and are marked
  ``DEFAULT_GENERATED`` in ``EXTRA``.
* **MariaDB 10.2.7+** stores the default as an *SQL expression*, so the same
  column reads back as ``'active'`` and a NULL default reads back as ``NULL``.
* **PostgreSQL / SQL Server / Oracle / SQLite** already hand back expressions
  (``'active'::text``, ``('active')``, ``datetime('now')``).

Downstream, ``services.schema_fidelity`` decides whether a default is a safe
literal it may re-emit on CREATE. It reasons about *SQL text*, so a bare MySQL
value fails the literal whitelist and the destination is created without the
default — the rows land, every checksum matches, and the client's first
application ``INSERT`` that omits the column fails or stores the wrong value.
That is a schema-fidelity loss a row-level proof cannot see.

This module is the one place that turns an engine's stored form into canonical
SQL text, so the whitelist has a single spelling to reason about.

Honesty rules:

* An engine-marked expression is never re-quoted into a string literal — a
  quoted expression would silently become the literal text of that expression.
* A value that cannot be classified is returned unchanged, so the whitelist
  downstream still gets to refuse it. Guessing here would be worse than a
  refusal an operator can read.
"""

from __future__ import annotations

import re

__all__ = ["normalize_catalog_default", "normalize_mysql_catalog_default"]

#: Clock/keyword defaults, in every spelling the two engines emit. MariaDB
#: renders the parenthesised form (``current_timestamp()``); MySQL does not.
_KEYWORD_DEFAULTS: dict[str, str] = {
    "null": "NULL",
    "true": "TRUE",
    "false": "FALSE",
    "current_timestamp": "CURRENT_TIMESTAMP",
    "current_timestamp()": "CURRENT_TIMESTAMP",
    "now()": "CURRENT_TIMESTAMP",
    "localtime": "CURRENT_TIMESTAMP",
    "localtime()": "CURRENT_TIMESTAMP",
    "localtimestamp": "CURRENT_TIMESTAMP",
    "localtimestamp()": "CURRENT_TIMESTAMP",
    "current_date": "CURRENT_DATE",
    "current_date()": "CURRENT_DATE",
    "curdate()": "CURRENT_DATE",
    "current_time": "CURRENT_TIME",
    "current_time()": "CURRENT_TIME",
    "curtime()": "CURRENT_TIME",
}

_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")

_NUMERIC_TYPES = (
    "int",
    "decimal",
    "numeric",
    "float",
    "double",
    "real",
    "bit",
    "year",
)


def _is_quoted_literal(text: str) -> bool:
    if len(text) < 2 or text[0] != "'" or text[-1] != "'":
        return False
    # Reject ``'a' || 'b'``-style concatenations: only a single literal counts.
    body = text[1:-1]
    i = 0
    while i < len(body):
        if body[i] == "'":
            if i + 1 < len(body) and body[i + 1] == "'":
                i += 2
                continue
            return False
        i += 1
    return True


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _is_numeric_column(data_type: str) -> bool:
    base = (data_type or "").strip().lower()
    return any(token in base for token in _NUMERIC_TYPES)


def normalize_mysql_catalog_default(
    raw: str | None,
    *,
    data_type: str = "",
    extra: str = "",
) -> str | None:
    """Canonical SQL text for one MySQL/MariaDB ``COLUMN_DEFAULT`` value.

    ``extra`` is the column's ``EXTRA`` field: MySQL marks expression defaults
    ``DEFAULT_GENERATED`` there, which is the only reliable signal that the
    stored text is SQL rather than a value.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        # MySQL stores ``DEFAULT ''`` as the empty string, which is a real
        # default and must not be confused with "no default" (NULL).
        return "''"
    if "default_generated" in (extra or "").lower():
        return text
    keyword = _KEYWORD_DEFAULTS.get(text.lower())
    if keyword is not None:
        return keyword
    if _is_quoted_literal(text):  # MariaDB expression form, already SQL
        return text
    if _NUMERIC_RE.match(text) and _is_numeric_column(data_type):
        return text
    if text.startswith("(") and text.endswith(")"):
        # MariaDB parenthesises expression defaults; MySQL 8 uses the same shape
        # for ``DEFAULT (expr)``. Either way it is SQL, not a value.
        return text
    # Bare MySQL literal: the stored form is the *value*, so give the whitelist
    # the SQL spelling of that value.
    return _quote(text)


def normalize_catalog_default(
    dialect: str,
    raw: str | None,
    *,
    data_type: str = "",
    extra: str = "",
) -> str | None:
    """Canonical SQL text for a catalog default, whatever engine stored it."""
    engine = (dialect or "").strip().lower()
    if engine in {"mysql", "mariadb"}:
        return normalize_mysql_catalog_default(raw, data_type=data_type, extra=extra)
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None
