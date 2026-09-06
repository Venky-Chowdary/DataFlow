"""Shared SQLite identity-COPY helpers.

Dest COUNT is ``SELECT COUNT(*)`` via ``destination_row_count`` — never
an estimate. ``:memory:`` declines (cannot prove ATTACH identity across
processes). Same resolved filesystem path **and** same table declines.
Same file + different tables uses ``INSERT SELECT`` on one connection
(no ATTACH). BLOB declines.

DATE cells must be an ISO calendar day (Python ``date``, or TEXT
``YYYY-MM-DD`` / midnight). DATETIME cells must be a naive wall-clock
(Python ``datetime`` without ``tzinfo``, or TEXT ISO with a time
component and no ``Z`` / offset). INTEGER unix and REAL julian decline
— those would invent a destination clock. BOOLEAN cells must be
SQL-boolean 0/1 (Python ``bool``, INTEGER 0/1, or TEXT ``'0'``/``'1'``).
``true``/``yes``/``t`` synonyms decline (would invent a boolean). JSON /
BYTEA / TIMESTAMPTZ dest DDL stay COPY-unsafe.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import date, datetime
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
    "TIMESTAMPTZ",
    "TIMETZ",
    "JSON",
    "JSONB",
})

_DATE_MIDNIGHT_CLOCKS = frozenset({
    "00:00:00",
    "00:00:00.000",
    "00:00:00.000000",
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


def sqlite_ddl_base(declared: str) -> str:
    return (declared or "").split("(")[0].strip().upper().replace(" ", "")


def sqlite_pg_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().upper().replace(" ", "")
    if not raw:
        return True
    if "WITHTIMEZONE" in raw and "WITHOUT" not in raw:
        return False
    if raw.startswith("TIMESTAMPTZ") or raw.startswith("TIMETZ"):
        return False
    base = raw.split("(", 1)[0]
    if base in _UNSAFE_SQLITE_PG_BASES:
        return False
    return True


def sqlite_copy_date_value(value: Any) -> date | None:
    """Prove a SQLite cell is an ISO calendar day. Unix/julian/clock decline."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise FastPathUnavailable("tz-aware SQLite DATE is not COPY-safe")
        if value.hour or value.minute or value.second or value.microsecond:
            raise FastPathUnavailable(
                "DATETIME SQLite value is not DATE COPY-safe"
            )
        return date(value.year, value.month, value.day)
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float, bytes, bytearray, memoryview, bool)):
        raise FastPathUnavailable(
            f"DATE cell {value!r} is not ISO calendar-day COPY-safe"
        )
    if isinstance(value, str):
        text = value.strip().replace("T", " ", 1)
        if " " in text:
            day, clock = text.split(" ", 1)
            if clock and clock not in _DATE_MIDNIGHT_CLOCKS:
                raise FastPathUnavailable(
                    "DATETIME SQLite value is not DATE COPY-safe"
                )
            text = day
        try:
            return date.fromisoformat(text)
        except ValueError as exc:
            raise FastPathUnavailable(
                f"DATE cell {value!r} is not ISO calendar-day COPY-safe"
            ) from exc
    raise FastPathUnavailable(
        f"DATE cell {value!r} is not ISO calendar-day COPY-safe"
    )


def sqlite_copy_naive_datetime_value(value: Any) -> datetime | None:
    """Prove a SQLite cell is a naive wall-clock. Unix/tz/date-only decline."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise FastPathUnavailable(
                "tz-aware SQLite DATETIME is not COPY-safe"
            )
        return value
    if isinstance(value, date):
        raise FastPathUnavailable(
            "DATE cell is not DATETIME COPY-safe (would invent 00:00:00)"
        )
    if isinstance(value, (int, float, bytes, bytearray, memoryview, bool)):
        raise FastPathUnavailable(
            "unix/julian SQLite DATETIME is not COPY-safe"
        )
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise FastPathUnavailable("empty DATETIME cell is not COPY-safe")
        if text.endswith("Z") or text.endswith("z"):
            raise FastPathUnavailable(
                "tz-aware SQLite DATETIME is not COPY-safe"
            )
        normalized = text.replace("T", " ", 1)
        if " " not in normalized:
            raise FastPathUnavailable(
                "date-only cell is not DATETIME COPY-safe (would invent 00:00:00)"
            )
        try:
            parsed = datetime.fromisoformat(text.replace(" ", "T", 1))
        except ValueError as exc:
            raise FastPathUnavailable(
                f"DATETIME cell {value!r} is not naive ISO COPY-safe"
            ) from exc
        if parsed.tzinfo is not None:
            raise FastPathUnavailable(
                "tz-aware SQLite DATETIME is not COPY-safe"
            )
        return parsed
    raise FastPathUnavailable(
        f"DATETIME cell {value!r} is not naive ISO COPY-safe"
    )


def sqlite_copy_bool_value(value: Any) -> bool | None:
    """Prove a SQLite cell is SQL-boolean 0/1. Synonyms and other ints decline."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)
        raise FastPathUnavailable(
            f"BOOLEAN cell {value!r} is not 0/1 COPY-safe"
        )
    if isinstance(value, str):
        text = value.strip()
        if text == "0":
            return False
        if text == "1":
            return True
        raise FastPathUnavailable(
            f"BOOLEAN cell {value!r} is not 0/1 COPY-safe"
        )
    raise FastPathUnavailable(
        f"BOOLEAN cell {value!r} is not 0/1 COPY-safe"
    )


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


def sqlite_table_pk_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({sqlite_ident(table)})").fetchall()
    ranked = sorted(
        (int(r[5] or 0), str(r[1])) for r in rows if int(r[5] or 0) > 0
    )
    return [name for _ord, name in ranked]


def sqlite_create_sql(
    table: str,
    pairs: list[tuple[str, str]],
    sqlite_ddls: list[str],
    primary_key: list[str] | None = None,
) -> str:
    from connectors.sqlite_writer import sqlite_type

    cols = []
    targets = [t for _s, t in pairs]
    for (_src, target), ddl in zip(pairs, sqlite_ddls, strict=True):
        cols.append(f"{sqlite_ident(target)} {sqlite_type(ddl or 'TEXT')}")
    pk = [c for c in (primary_key or []) if c in targets]
    if pk:
        pk_sql = ", ".join(sqlite_ident(c) for c in pk)
        cols.append(f"PRIMARY KEY ({pk_sql})")
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
