from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ConnectResult:
    ok: bool
    tables: list[str]
    error: str | None = None
    message: str | None = None
    driver: str = "stub"
