"""Shared SQLite identity-COPY helpers.

Dest COUNT is ``SELECT COUNT(*)`` via ``destination_row_count`` — never
an estimate. ``:memory:`` declines (cannot prove ATTACH identity across
processes). Same resolved filesystem path **and** same table declines.
Same file + different tables uses ``INSERT SELECT`` on one connection
(no ATTACH). BLOB declines.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from connectors.sql_identifiers import quote_sql_identifier
from connectors.sqlite_common import sqlite_file_path
from services.copy_fast_path import FastPathResult, FastPathUnavailable

_UNSAFE_SQLITE_BASES = frozenset({
    "BLOB",
    "BINARY",
    "VARBINARY",
    "BYTEA",
})

_UNSAFE_SQLITE_PG_BASES = _UNSAFE_SQLITE_BASES | frozenset({
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "JSON",
    "JSONB",
    "BOOLEAN",
    "BOOL",
})


def sqlite_ident(name: str) -> str:
    return quote_sql_identifier(name, '"')


def sqlite_resolved_path(cfg: dict[str, Any]) -> str:
    path = sqlite_file_path(
        str(cfg.get("database") or ""),
        str(cfg.get("connection_string") or ""),
        str(cfg.get("host") or ""),
    )
    if not path:
        raise FastPathUnavailable("SQLite database path required")
    if path == ":memory:" or path.lower().startswith("file:memory:"):
        raise FastPathUnavailable("SQLite :memory: stays on the row path")
    return path


def sqlite_same_file(src_cfg: dict[str, Any], dest_cfg: dict[str, Any]) -> bool:
    src = Path(sqlite_resolved_path(src_cfg)).expanduser().resolve(strict=False)
    dest = Path(sqlite_resolved_path(dest_cfg)).expanduser().resolve(strict=False)
    return src == dest


def sqlite_bind_from_text(ddl: str) -> Callable[[str | None], Any]:
    """Bind a CSV/COPY-text cell to a SQLite value. NULL stays None."""
    base = (ddl or "").split("(")[0].strip().upper().replace(" ", "")
    if base in {
        "BIGINT",
        "INT",
        "INTEGER",
        "SMALLINT",
        "TINYINT",
        "INT2",
        "INT4",
        "INT8",
    }:
        return _bind_int
    if base in {"FLOAT", "REAL", "FLOAT4", "FLOAT8"} or base.startswith("DOUBLE"):
        return _bind_float
    return _bind_text


def _bind_int(value: str | None) -> int | None:
    return None if value is None else int(value)


def _bind_float(value: str | None) -> float | None:
    return None if value is None else float(value)


def _bind_text(value: str | None) -> str | None:
    return value


def sqlite_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().upper().replace(" ", "")
    if not raw:
        return True
    base = raw.split("(", 1)[0]
    if base in _UNSAFE_SQLITE_BASES:
        return False
    return True


def sqlite_pg_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().upper().replace(" ", "")
    if not raw:
        return True
    base = raw.split("(", 1)[0]
    if base in _UNSAFE_SQLITE_PG_BASES:
        return False
    return True


def sqlite_dest_count(cfg: dict[str, Any], table: str) -> int:
    from services.dest_precount import destination_row_count

    path = sqlite_resolved_path(cfg)
    n = destination_row_count(
        "sqlite",
        {**cfg, "database": path},
        schema="",
        table_name=table,
    )
    if n is None:
        raise ValueError(f"SQLite dest COUNT unmeasured for {table}")
    return int(n)


def sqlite_connect(cfg: dict[str, Any]) -> sqlite3.Connection:
    path = sqlite_resolved_path(cfg)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def sqlite_pragma_types(conn: sqlite3.Connection, table: str) -> dict[str, str]:
    rows = conn.execute(f"PRAGMA table_info({sqlite_ident(table)})").fetchall()
    return {str(r[1]): str(r[2] or "TEXT") for r in rows}


def sqlite_create_sql(
    table: str,
    pairs: list[tuple[str, str]],
    sqlite_ddls: list[str],
) -> str:
    from connectors.sqlite_writer import sqlite_type

    cols = []
    for (_src, target), ddl in zip(pairs, sqlite_ddls, strict=True):
        cols.append(f"{sqlite_ident(target)} {sqlite_type(ddl or 'TEXT')}")
    return f"CREATE TABLE {sqlite_ident(table)} ({', '.join(cols)})"


def skip_complete_sqlite(
    *,
    source_count: int,
    dest_count: int,
    extra_snapshot: dict[str, Any] | None = None,
) -> FastPathResult:
    proof = f"dest_count:{dest_count}"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "skip",
        "copy_partitions": 1,
        "partitions_skipped": 1,
        "partitions_loaded": 0,
        "shard_mode": "table",
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=source_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
    )
