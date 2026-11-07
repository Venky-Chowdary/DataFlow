"""Referenced-table lookup — resolve a saved connector's table into rows.

A ``lookup`` step may name a ``reference`` block (``{connector_id, table,
columns}``) instead of inline ``rows``. The engine calls
:func:`load_reference_rows` once per run, lazily at the first shaped row, and
the cache is the run — a recipe binds to whatever the reference table held at
run start; it never re-reads mid-run. Refresh semantics are therefore:
**loaded fresh at each run start**. Operators see that stated on the step and
can prove it by re-running after changing the reference data.

Fail-closed rules:
- unknown connector id refuses (name the id — never a silent empty lookup);
- a connector family that cannot page a table read refuses by name rather than
  re-reading page one forever (keyset/continuation readers);
- the reference is capped at ``MAX_REFERENCE_ROWS`` — a lookup table beyond
  that fails loud instead of silently truncating keys.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

#: Largest reference table a lookup may hold in memory per run.
MAX_REFERENCE_ROWS = 500_000
_PAGE = 10_000

#: Readers that page by a continuation token, not OFFSET — paging them by
#: offset re-reads page one forever (see CONTINUATION_KWARG in batch_readers).
_NON_PAGING_TYPES = {"dynamodb", "elasticsearch", "redis", "kafka", "qdrant"}

#: Readers with no table concept at all.
_NON_TABLE_TYPES = {"sftp", "email", "webhook"}


class LookupReferenceError(ValueError):
    """The reference spec could not be resolved to rows — named, never silent."""


def load_reference_rows(spec: dict[str, Any], *, workspace_id: str = "") -> list[dict[str, Any]]:
    """Load ``{connector_id, table, columns}`` into a list of row dicts."""
    connector_id = str(spec.get("connector_id") or "").strip()
    table = str(spec.get("table") or "").strip()
    columns = [str(c) for c in (spec.get("columns") or []) if str(c).strip()]
    if not connector_id:
        raise LookupReferenceError("lookup reference needs connector_id")
    if not table:
        raise LookupReferenceError("lookup reference needs table")

    from services.connector_store import get_connector

    # Workspace is scoped literally: "" resolves only workspace-less connectors,
    # a named workspace resolves its own plus global ones — never another
    # workspace's connector by id alone.
    conn = get_connector(connector_id, workspace_id=workspace_id)
    if conn is None:
        raise LookupReferenceError(
            f"lookup reference: saved connector '{connector_id}' was not found"
        )
    src_type = str(getattr(conn, "type", "") or "").casefold()
    if src_type in _NON_TABLE_TYPES:
        raise LookupReferenceError(
            f"lookup reference: connector '{connector_id}' is a {src_type} source — "
            "it has no table to key on"
        )
    if src_type in _NON_PAGING_TYPES:
        raise LookupReferenceError(
            f"lookup reference: {src_type} cannot page a table read yet — "
            "use a SQL-family connector or inline rows"
        )

    from services.connector_probe import probe_cfg_from_saved
    from src.transfer.batch_readers import _read_batch_impl

    cfg = probe_cfg_from_saved(conn)
    select = list(columns) if columns else None
    rows: list[dict[str, Any]] = []

    # SQLite's read_table_batch falls back to the caller's shared source
    # snapshot connection when conn=None — correct for the source read, wrong
    # for a reference read on a different database. References always get a
    # dedicated connection so they can never read the source file.
    dedicated = None
    if src_type == "sqlite":
        import sqlite3

        from connectors.sqlite_common import sqlite_file_path

        ref_path = sqlite_file_path(
            cfg.get("database", ""), cfg.get("connection_string", ""), cfg.get("host", "")
        )
        if not ref_path:
            raise LookupReferenceError(
                f"lookup reference: sqlite connector '{connector_id}' has no database path"
            )
        dedicated = sqlite3.connect(ref_path, timeout=8)

    try:
        offset = 0
        while True:
            if dedicated is not None:
                from connectors.sqlite_reader import read_table_batch

                batch = read_table_batch(
                    host=cfg.get("host", ""),
                    port=0,
                    database=cfg.get("database", ""),
                    username=cfg.get("username", ""),
                    password=cfg.get("password", ""),
                    schema=cfg.get("schema", ""),
                    connection_string=cfg.get("connection_string", ""),
                    ssl=False,
                    table=table,
                    limit=_PAGE,
                    offset=offset,
                    conn=dedicated,
                )
            else:
                batch = _read_batch_impl(
                    src_type,
                    cfg,
                    table,
                    select,
                    offset,
                    _PAGE,
                )
            headers = list(batch.headers)
            for r in batch.rows:
                rows.append(
                    {h: (r[i] if i < len(r) else None) for i, h in enumerate(headers)}
                )
            if len(batch.rows) < _PAGE:
                break
            offset += len(batch.rows)
            if len(rows) > MAX_REFERENCE_ROWS:
                raise LookupReferenceError(
                    f"lookup reference '{table}' passed {MAX_REFERENCE_ROWS} rows — "
                    "a lookup table that large must be a join, not an in-memory reference"
                )
    finally:
        if dedicated is not None:
            dedicated.close()
    return rows
