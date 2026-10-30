"""What a shared Snowflake plan login already knows, so it is not asked twice (D187).

Every Snowflake statement is a network round trip of 0.5-2s on a real account,
and a plan's steps were each written for a fresh session. On one shared login
they still re-sent ``USE WAREHOUSE / DATABASE / SCHEMA`` the login had already
set, asked ``SELECT CURRENT_ROLE()`` and re-read ``DESC TABLE`` once per step.
A PG -> Snowflake upsert into an existing table spent 38-41s of the 45s tool
deadline at a real account's worst costs, nearly all of it in those serial
round trips.

This module holds the facts that answer them locally:

* the session context read once after login (one statement for warehouse,
  database, schema and role) so a ``USE`` that names the current object is
  answered here, compared as Snowflake compares identifiers;
* read-only catalog answers (``DESC``, ``SHOW``, ``SELECT CURRENT_*()``) kept
  for the life of the plan login and dropped as soon as the session changes
  context or runs anything that is not a read.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

#: Session parameters a plan login sets at login instead of by statement.
PLAN_SESSION_PARAMETERS = {"STATEMENT_TIMEOUT_IN_SECONDS": 45}

_USE = re.compile(r"^\s*USE\s+(?P<kind>WAREHOUSE|DATABASE|SCHEMA|ROLE)\s+(?P<name>.+?)\s*;?\s*$", re.I | re.S)
_IDENT = re.compile(r'^(?:"(?P<q>(?:[^"]|"")+)"|(?P<u>[A-Za-z_][A-Za-z0-9_$]*))$')
_CACHEABLE = re.compile(
    r"^\s*(?:DESC(?:RIBE)?\s+(?:TABLE|VIEW)\s|SHOW\s|SELECT\s+CURRENT_(?:ROLE|DATABASE|SCHEMA|VERSION)\(\)\s*;?\s*$)",
    re.I,
)
_READ_OR_SESSION = re.compile(r"^\s*(?:SELECT|WITH|SHOW|DESC|DESCRIBE|USE|ALTER\s+SESSION|EXPLAIN|LIST)\b", re.I)


def canonical_identifier(text: str) -> str | None:
    """Snowflake's stored spelling of one identifier, or ``None`` if not simple."""
    match = _IDENT.match((text or "").strip())
    if not match:
        return None
    if match.group("q") is not None:
        return match.group("q").replace('""', '"')
    return match.group("u").upper()


def use_target(sql: Any) -> tuple[str, str] | None:
    """``(kind, stored name)`` of a ``USE`` naming one simple object."""
    if not isinstance(sql, str):
        return None
    match = _USE.match(sql)
    if not match:
        return None
    name = canonical_identifier(match.group("name"))
    return (match.group("kind").upper(), name) if name is not None else None


def cache_key(sql: Any) -> str | None:
    """Normalized text of a read-only catalog statement worth keeping, else ``None``."""
    if not isinstance(sql, str) or not _CACHEABLE.match(sql):
        return None
    return " ".join(sql.split()).rstrip(";").upper()


def ends_cached_answers(sql: Any) -> bool:
    """A statement after which a kept catalog answer may be stale."""
    return not isinstance(sql, str) or not _READ_OR_SESSION.match(sql)


@dataclass
class Answer:
    """One kept result: rows and description, or the error it raised."""

    description: Any = None
    rows: list[tuple[Any, ...]] | None = None
    error: BaseException | None = None


class Replay:
    """A finished result served to a consumer's cursor."""

    def __init__(self, answer: Answer) -> None:
        self.description = answer.description
        self._rows = list(answer.rows or [])
        self.rowcount = len(self._rows)
        self._at = 0

    def fetchone(self) -> Any:
        if self._at >= len(self._rows):
            return None
        row = self._rows[self._at]
        self._at += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[Any]:
        size = len(self._rows) - self._at if size is None else size
        out = self._rows[self._at:self._at + size]
        self._at += len(out)
        return out

    def fetchall(self) -> list[Any]:
        out = self._rows[self._at:]
        self._at = len(self._rows)
        return out

    def __iter__(self) -> Any:
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row


#: Context a USE changes: catalog answers read under another one are stale.
CATALOG_CONTEXT = frozenset({"DATABASE", "SCHEMA", "ROLE"})

_CONTEXT_SELECT = re.compile(
    r"^\s*SELECT\s+(?P<fns>CURRENT_\w+\(\)(?:\s*,\s*CURRENT_\w+\(\))*)\s*;?\s*$", re.I
)


def context_select(sql: Any) -> tuple[str, ...] | None:
    """Kinds asked by ``SELECT CURRENT_X(), CURRENT_Y()`` (the dialect's own probe)."""
    if not isinstance(sql, str):
        return None
    match = _CONTEXT_SELECT.match(sql)
    if not match:
        return None
    return tuple(
        fn.strip()[len("CURRENT_"):-2].upper() for fn in match.group("fns").split(",")
    )


def context_answer(kinds: tuple[str, ...], context: dict[str, str]) -> Answer | None:
    """The answer to a context ``SELECT`` when the login already said every value."""
    if not kinds or any(kind not in context for kind in kinds):
        return None
    return Answer(
        description=[(f"CURRENT_{kind}()", None, None, None, None, None, True) for kind in kinds],
        rows=[tuple(context[kind] for kind in kinds)],
    )


def context_query(local_emulator: bool) -> tuple[str, tuple[str, ...]]:
    """One statement for the session's context; fakesnow has no CURRENT_WAREHOUSE()."""
    kinds = ("DATABASE", "SCHEMA", "ROLE", "VERSION")
    if not local_emulator:
        kinds += ("WAREHOUSE",)
    return "SELECT " + ", ".join(f"CURRENT_{k}()" for k in kinds), kinds
