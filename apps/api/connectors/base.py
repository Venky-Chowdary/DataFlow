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
    # Pagination facts of the page *as the source handed it over*, stamped once
    # before a source filter or a shaping recipe rewrites ``rows``. The next
    # OFFSET, the incremental watermark, the keyset bookmark and the run's read
    # population are all counted from these: a page that dropped rows still
    # consumed them, and advancing by the survivors skips the table's tail.
    # ``None``/empty means the page was never rewritten, so ``rows`` is still the
    # source's own page.
    raw_page_rows: int | None = None
    raw_page_cursor: str = ""
    raw_page_keyset: str = ""
    #: Rows the declared source filter removed from this page, before the
    #: shaping recipe ran. Proof names the two removals apart — a row the
    #: operator excluded by filter was never offered to the recipe, and a row
    #: the recipe removed was in scope and shaped out.
    raw_page_filtered: int = 0
