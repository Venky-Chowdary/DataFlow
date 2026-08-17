from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ConnectResult:
    ok: bool
    tables: list[str]
    error: str | None = None
    message: str | None = None
    driver: str = "stub"
    #: True when ``tables`` is a bounded page rather than the whole inventory.
    #: Callers that resolve a name against this list must not read absence from
    #: a truncated page as proof the object does not exist.
    tables_truncated: bool = False


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int = 0
    total_rows: int | None = 0
    # Optional reader metadata (e.g. DynamoDB native_types) — never required.
    meta: dict | None = None
