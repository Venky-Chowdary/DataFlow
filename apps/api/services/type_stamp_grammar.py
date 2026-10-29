"""Grammar gate for column type stamps that reach destination DDL.

A type stamp (Map ``target_type``, a source's declared type, the output of
``materialize_dest_ddl``) is interpolated into ``CREATE TABLE`` / ``ALTER TABLE
... ADD COLUMN`` where identifiers are quoted but the type cannot be. This
module is the single owner of "is this text only a type?": one tokenizer, one
structural check, shared by ``materialize_dest_ddl``, mapping ingest and every
writer that emits a column definition.

Accepted shape (dialect-neutral)::

    name [ '.' name ]* group* ( modifier group* )*  [ (COLLATE|CHARSET|SET) ident ]

where ``group`` is a balanced ``( … )`` / ``< … >`` / ``[ … ]`` holding type
arguments (numbers, words, quoted literals, nested groups, ``, : * =``) and
``modifier`` is a fixed vocabulary (``PRECISION``, ``WITH TIME ZONE``,
``UNSIGNED``, ``TO SECOND`` …). Anything else — a top-level comma, ``;``,
comment markers, backslashes, unbalanced brackets, or a column/ALTER clause
keyword such as ``DEFAULT``/``REFERENCES``/``CHECK``/``DROP`` — is refused.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from typing import Any

logger = logging.getLogger(__name__)

MAX_TYPE_STAMP_LENGTH = 512

#: Words allowed at top level after the type name. They only ever continue a
#: type spelling (``DOUBLE PRECISION``, ``TIMESTAMP WITH LOCAL TIME ZONE``,
#: ``INTERVAL DAY TO SECOND``, ``INT UNSIGNED ZEROFILL``, ``INT ARRAY``).
_MODIFIER_WORDS = frozenset(
    {
        "PRECISION", "VARYING", "WITH", "WITHOUT", "LOCAL", "TIME", "ZONE",
        "UNSIGNED", "SIGNED", "ZEROFILL", "CHARACTER", "CHAR", "VARCHAR",
        "NATIONAL", "BYTE", "BYTES", "LARGE", "OBJECT", "RAW", "BINARY",
        "VARBINARY", "TEXT", "BIT", "INT", "INTEGER", "BIGINT", "SMALLINT",
        "TINYINT", "DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC",
        "TIMESTAMP", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "TO",
        "ARRAY", "MULTISET", "ASCII", "UNICODE", "LONG",
    }
)

#: Top-level words that introduce exactly one collation / charset name.
_NAME_INTRODUCERS = frozenset({"COLLATE", "CHARSET", "SET"})

#: Statement / column-clause keywords that never belong in a type's top level.
#: (Inside a balanced group they are legitimate struct field names.)
_FORBIDDEN_WORDS = frozenset(
    {
        "ADD", "ALTER", "AS", "ATTACH", "CALL", "CASCADE", "CHECK", "COMMENT",
        "CONSTRAINT", "COPY", "CREATE", "DEFAULT", "DELETE", "DETACH", "DO",
        "DROP", "EXEC", "EXECUTE", "FOREIGN", "FROM", "GENERATED", "GRANT",
        "INSERT", "INTO", "KEY", "MERGE", "ON", "PRAGMA", "PRIMARY",
        "REFERENCES", "RENAME", "REPLACE", "REVOKE", "SELECT", "TABLE",
        "TRUNCATE", "UNION", "UNIQUE", "UPDATE", "USING", "VALUES", "WHERE",
    }
)

#: Statement verbs: refused even inside a type argument list.
_STATEMENT_WORDS = frozenset(
    {
        "ALTER", "ATTACH", "CALL", "COPY", "CREATE", "DELETE", "DETACH", "DROP",
        "EXEC", "EXECUTE", "GRANT", "INSERT", "MERGE", "PRAGMA", "REVOKE",
        "SELECT", "TRUNCATE", "UNION", "UPDATE",
    }
)

_TOKEN_RE = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<word>[A-Za-z_][A-Za-z0-9_]*)
  | (?P<number>-?\d+(?:\.\d+)?)
  | (?P<string>'(?:[^'\\\x00-\x1f\x7f]|'')*')
  | (?P<qident>"(?:[^"\\\x00-\x1f\x7f]|"")+"|`(?:[^`\\\x00-\x1f\x7f]|``)+`)
  | (?P<punct>[()<>\[\],:*=.])
    """,
    re.VERBOSE,
)

_OPENERS = {"(": ")", "<": ">", "[": "]"}
_CLOSERS = {")", ">", "]"}


class UnsafeTypeStamp(ValueError):
    """A type stamp is not a bare type and must never reach DDL."""

    def __init__(self, stamp: object, reason: str, *, context: str = "") -> None:
        self.stamp = stamp
        self.reason = reason
        self.context = context
        where = f" ({context})" if context else ""
        super().__init__(
            f"Refused column type{where}: {_preview(stamp)} is not a column type ({reason})"
        )


def _preview(stamp: object) -> str:
    text = str(stamp)
    return repr(text if len(text) <= 64 else text[:64] + "…")


def _tokenize(text: str) -> list[tuple[str, str]] | str:
    tokens: list[tuple[str, str]] = []
    pos = 0
    while pos < len(text):
        m = _TOKEN_RE.match(text, pos)
        if m is None:
            return f"unexpected character {text[pos]!r}"
        kind = m.lastgroup or ""
        if kind != "ws":
            tokens.append((kind, m.group()))
        pos = m.end()
    return tokens


def type_stamp_violation(stamp: object) -> str | None:
    """Why ``stamp`` is not a bare column type, or ``None`` when it is."""
    if not isinstance(stamp, str):
        return "not a string"
    text = stamp.strip()
    if not text:
        return "empty"
    if len(text) > MAX_TYPE_STAMP_LENGTH:
        return f"longer than {MAX_TYPE_STAMP_LENGTH} characters"
    tokens = _tokenize(text)
    if isinstance(tokens, str):
        return tokens
    stack: list[str] = []
    # Top-level state: "name" (reading the dotted type name), "after_dot",
    # "body" (modifiers / groups), "intro" (expects one collation/charset name),
    # "done" (a collation name was read; only nothing more may follow).
    state = "start"
    for kind, value in tokens:
        if kind == "word":
            upper = value.upper()
            # Inside a balanced group a clause word is an argument or a field
            # name (``STRUCT<key STRING, comment STRING>``, ``ROW(value INT)``):
            # it cannot close the group, so it cannot start another clause.
            # Statement verbs are refused at any depth.
            if upper in (_STATEMENT_WORDS if stack else _FORBIDDEN_WORDS):
                return f"keyword {upper} is not part of a type"
        if stack:
            if value in _OPENERS and kind == "punct":
                stack.append(_OPENERS[value])
            elif kind == "punct" and value in _CLOSERS:
                if stack[-1] != value:
                    return f"unbalanced {value!r}"
                stack.pop()
            continue
        # depth 0
        if kind == "punct" and value in _CLOSERS:
            return f"unbalanced {value!r}"
        if state == "start":
            if kind not in {"word", "qident"}:
                return "must start with a type name"
            state = "name"
            continue
        if state == "after_dot":
            if kind not in {"word", "qident"}:
                return "dangling '.' in type name"
            state = "name"
            continue
        if state == "intro":
            if kind not in {"word", "qident", "string"}:
                return "COLLATE / CHARACTER SET needs a name"
            state = "done"
            continue
        if kind == "punct" and value == "." and state == "name":
            state = "after_dot"
            continue
        if kind == "punct" and value in _OPENERS:
            if state == "done":
                return "nothing may follow a collation name"
            stack.append(_OPENERS[value])
            state = "body"
            continue
        if kind == "word":
            upper = value.upper()
            if upper in _NAME_INTRODUCERS:
                state = "intro"
                continue
            if upper in _MODIFIER_WORDS:
                if state == "done":
                    return "nothing may follow a collation name"
                state = "body"
                continue
            return f"{value!r} is not a type modifier"
        return f"{value!r} is not allowed outside type arguments"
    if stack:
        return "unbalanced brackets"
    if state in {"after_dot", "intro"}:
        return "incomplete type"
    return None


def is_safe_type_stamp(stamp: object) -> bool:
    return type_stamp_violation(stamp) is None


def require_safe_type_stamp(stamp: object, *, context: str = "") -> str:
    """Return ``stamp`` unchanged when it is only a type; raise otherwise."""
    reason = type_stamp_violation(stamp)
    if reason is not None:
        logger.warning(
            "type_stamp_refused",
            extra={"context": context, "reason": reason, "stamp_preview": _preview(stamp)},
        )
        raise UnsafeTypeStamp(stamp, reason, context=context)
    return str(stamp)


def require_safe_type_stamps(stamps: Iterable[object], *, context: str = "") -> None:
    for stamp in stamps:
        require_safe_type_stamp(stamp, context=context)


#: Mapping keys whose value is a type carrier that can reach DDL.
#: ``source_type`` / ``type`` describe the source side (Arrow ``timestamp[us,
#: tz=Asia/Kolkata]``, ``string|null``, Mongo mixed types) and are never pasted
#: into DDL, so they are not judged here; the writers re-check what they emit.
MAPPING_TYPE_KEYS = ("target_type", "dest_type")


def validate_mapping_type_stamps(mappings: Iterable[Any] | None, *, context: str = "mapping") -> None:
    """Mapping ingest gate: every non-empty type carrier must be a bare type."""
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        for key in MAPPING_TYPE_KEYS:
            value = m.get(key)
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            from services.type_system import strip_identity_qualifier

            target = str(m.get("target") or m.get("source") or "")
            where = f"{context} {key} for {target!r}"[:120]
            if not isinstance(value, str):
                raise UnsafeTypeStamp(value, "not a string", context=where)
            # Generator qualifiers (IDENTITY / AUTO_INCREMENT / GENERATED …) are
            # carried by identity planning, never pasted — judge the type alone.
            require_safe_type_stamp(strip_identity_qualifier(value) or value, context=where)
