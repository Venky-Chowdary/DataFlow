from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ConnectResult:
    ok: bool
    tables: list[str]
    error: str | None = None
    message: str | None = None
    driver: str = "stub"


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int = 0
    total_rows: int | None = 0
    # Optional reader metadata (e.g. DynamoDB native_types) — never required.
    meta: dict | None = None
