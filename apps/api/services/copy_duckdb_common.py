"""Shared DuckDB identity-COPY helpers.

Dest COUNT is ``destination_row_count`` / ``SELECT COUNT(*)`` — never an
estimate, never ``duckdb_tables()`` metadata. ``:memory:`` declines
(cannot prove ATTACH identity across processes). MotherDuck (``md:``)
declines (a cloud catalog is not a local file to ATTACH). Same resolved
filesystem path **and** same table declines. Same file + different tables
uses ``INSERT SELECT`` on one connection (no ATTACH).

Connections go through ``generic_sql.get_sqlalchemy_engine`` — the one
owner of DuckDB URLs and ``NullPool``. A second native ``duckdb.connect``
would fight the dialect for the single-writer file lock (see
``reconciliation.verify_duckdb_table``).

Structure travels with the values. The dest DDL is rebuilt from the
**source catalog** (exact ``data_type`` text, ``NOT NULL``, ``DEFAULT``,
``PRIMARY KEY``, ``UNIQUE``) rather than from mapping stamps, so this path
never widens ``INTEGER`` to ``BIGINT`` or drops a constraint the source
enforces. A ``CHECK`` or ``FOREIGN KEY`` declines: this path cannot prove
it reproduced them.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from connectors.sql_identifiers import quote_sql_identifier
from connectors.sqlite_common import duckdb_file_path
from services.copy_fast_path import FastPathResult, FastPathUnavailable

_DUCKDB_FAMILY = frozenset({
    "duckdb",
    "duck_db",
    "motherduck",
    "md",
})

# Mapping stamps and DuckDB type names this path recognises. The dest DDL is
# built from the source catalog, not from these, so the check only fails
# closed on a stamp that is not a type at all.
_DUCKDB_COPY_SAFE_TYPES = frozenset({
    "bigint", "int8", "long",
    "integer", "int", "int4", "signed",
    "smallint", "int2", "short",
    "tinyint", "int1",
    "hugeint", "uhugeint",
    "ubigint", "uinteger", "usmallint", "utinyint",
    "boolean", "bool", "logical",
    "double", "float8", "float", "float4", "real", "number",
    "decimal", "numeric",
    "varchar", "char", "bpchar", "text", "string", "keyword",
    "blob", "bytea", "binary", "varbinary", "bytes",
    "date",
    "time", "timetz", "time with time zone",
    "timestamp", "datetime", "timestamptz",
    "timestamp with time zone", "timestamp_s", "timestamp_ms",
    "timestamp_ns", "timestamp_us",
    "interval",
    "uuid",
    "json",
    "bit", "bitstring",
    "struct", "row", "list", "array", "map", "union", "enum",
})

# Catalog type text is interpolated into dest DDL, so it must be a type
# fragment and nothing else. DuckDB spells nested types with parentheses,
# brackets, commas and quoted ENUM labels.
_TYPE_TEXT_OK = re.compile(r"^[A-Za-z0-9_ ,.'\"()\[\]+-]+$")
_DEFAULT_TEXT_OK = re.compile(r"^[^;]*$")


def duckdb_family_name(name: str, cfg: dict[str, Any] | None = None) -> str:
    """Resolve the concrete engine behind the shared ``generic_sql`` family.

    ``resolve_driver_type("duckdb")`` is ``generic_sql`` (one SQLAlchemy
    writer serves many brands), so the route name alone cannot tell DuckDB
    from ClickHouse. The endpoint config carries the brand.
    """
    n = (name or "").strip().lower()
    if n not in _DUCKDB_FAMILY and cfg:
        n = str(cfg.get("type") or cfg.get("format") or "").strip().lower()
    if n in _DUCKDB_FAMILY:
        return "duckdb"
    return (name or "").strip().lower()


def duckdb_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().lower()
    if not raw:
        return True
    base = raw.split("(", 1)[0].split("[", 1)[0].strip()
    if base in _DUCKDB_COPY_SAFE_TYPES:
        return True
    return raw in _DUCKDB_COPY_SAFE_TYPES


def duckdb_type_text_is_safe(text: str) -> bool:
    """Catalog type text is safe to interpolate into dest DDL."""
    raw = (text or "").strip()
    if not raw or len(raw) > 4000:
        return False
    if "--" in raw or "/*" in raw or ";" in raw:
        return False
    return bool(_TYPE_TEXT_OK.match(raw))


def duckdb_default_text_is_safe(text: str) -> bool:
    raw = (text or "").strip()
    if not raw or len(raw) > 4000:
        return False
    if "--" in raw or "/*" in raw:
        return False
    return bool(_DEFAULT_TEXT_OK.match(raw))


def duckdb_ident(name: str) -> str:
    return quote_sql_identifier(name, '"')


def duckdb_resolved_path(cfg: dict[str, Any]) -> str:
    path = duckdb_file_path(
        str(cfg.get("database") or ""),
        str(cfg.get("connection_string") or ""),
        str(cfg.get("host") or ""),
    )
    if not path:
        raise FastPathUnavailable("DuckDB database path required")
    if path.lower().startswith(("md:", "motherduck:")):
        raise FastPathUnavailable(
            "MotherDuck stays on the row path (no local file to ATTACH)"
        )
    if path == ":memory:" or path.lower().endswith(":memory:"):
        raise FastPathUnavailable("DuckDB :memory: stays on the row path")
    if "'" in path or "\x00" in path:
        raise FastPathUnavailable("DuckDB path is not ATTACH-safe")
    return path


def duckdb_same_file(src_cfg: dict[str, Any], dest_cfg: dict[str, Any]) -> bool:
    src = Path(duckdb_resolved_path(src_cfg)).expanduser().resolve(strict=False)
    dest = Path(duckdb_resolved_path(dest_cfg)).expanduser().resolve(strict=False)
    return src == dest


def duckdb_schema_name(cfg: dict[str, Any]) -> str:
    from services.dialect_profiles import normalize_schema

    return normalize_schema("duckdb", cfg.get("schema")) or "main"


def duckdb_engine(cfg: dict[str, Any]):
    """One owner for DuckDB URLs / NullPool: ``generic_sql``."""
    from connectors.generic_sql import get_sqlalchemy_engine

    path = duckdb_resolved_path(cfg)
    engine_cfg: dict[str, Any] = {"type": "duckdb", "database": path}
    conn_s = str(cfg.get("connection_string") or "").strip()
    if conn_s.lower().startswith("duckdb:"):
        engine_cfg["connection_string"] = conn_s
    return get_sqlalchemy_engine(engine_cfg)


def duckdb_dest_count(cfg: dict[str, Any], table: str) -> int:
    from services.dest_precount import destination_row_count

    path = duckdb_resolved_path(cfg)
    n = destination_row_count(
        "duckdb",
        {**cfg, "type": "duckdb", "database": path},
        schema=duckdb_schema_name(cfg),
        table_name=table,
    )
    if n is None:
        raise ValueError(f"DuckDB dest COUNT unmeasured for {table}")
    return int(n)


def duckdb_attach_sql(path: str, alias: str) -> str:
    """READ_ONLY attach. A reader lock also blocks another writer opening it."""
    literal = path.replace("'", "''")
    return f"ATTACH '{literal}' AS {duckdb_ident(alias)} (READ_ONLY)"


def duckdb_table_ref(*, catalog: str | None, schema: str, table: str) -> str:
    parts = [duckdb_ident(schema), duckdb_ident(table)]
    if catalog:
        parts.insert(0, duckdb_ident(catalog))
    return ".".join(parts)


def duckdb_table_exists(conn: Any, *, catalog: str, schema: str, table: str) -> bool:
    row = conn.exec_driver_sql(
        "SELECT 1 FROM duckdb_tables() WHERE database_name = ? "
        "AND schema_name = ? AND table_name = ? LIMIT 1",
        (catalog, schema, table),
    ).fetchone()
    return row is not None


def duckdb_source_columns(
    conn: Any, *, catalog: str, schema: str, table: str
) -> dict[str, dict[str, Any]]:
    """Exact catalog types / nullability / defaults, keyed by column name.

    ENUM, STRUCT, LIST, MAP and UNION come back fully expanded
    (``ENUM('a', 'b')``), so re-emitting this text reproduces the type in
    the destination catalog without inventing one.
    """
    rows = conn.exec_driver_sql(
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM duckdb_columns() WHERE database_name = ? AND schema_name = ? "
        "AND table_name = ? ORDER BY column_index",
        (catalog, schema, table),
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for name, data_type, is_nullable, default in rows:
        out[str(name)] = {
            "data_type": str(data_type or ""),
            "nullable": bool(is_nullable),
            "default": None if default is None else str(default),
        }
    return out


def duckdb_source_constraints(
    conn: Any, *, catalog: str, schema: str, table: str
) -> list[tuple[str, list[str]]]:
    rows = conn.exec_driver_sql(
        "SELECT constraint_type, constraint_column_names FROM duckdb_constraints() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
        (catalog, schema, table),
    ).fetchall()
    out: list[tuple[str, list[str]]] = []
    for ctype, cols in rows:
        names = [str(c) for c in (cols or [])]
        out.append((str(ctype or "").strip().upper(), names))
    return out


def duckdb_create_sql_from_source(
    *,
    dest_ref: str,
    pairs: list[tuple[str, str]],
    live: dict[str, dict[str, Any]],
    constraints: list[tuple[str, list[str]]],
) -> str:
    """Reproduce the source structure for the mapped columns.

    Declines rather than hand back a table that enforces fewer rules than
    its source: a ``CHECK`` / ``FOREIGN KEY`` this path cannot prove
    equivalent, or a key whose columns are not all mapped.
    """
    mapped = {src: tgt for src, tgt in pairs}
    lowered = {k.lower(): k for k in live}
    cols_sql: list[str] = []
    for src_col, tgt_col in pairs:
        actual = lowered.get(src_col.lower())
        if actual is None:
            raise FastPathUnavailable(f"source column {src_col!r} absent")
        meta = live[actual]
        type_text = meta["data_type"]
        if not duckdb_type_text_is_safe(type_text):
            raise FastPathUnavailable(
                f"source column {src_col!r} type {type_text!r} is not DDL-safe"
            )
        piece = f"{duckdb_ident(tgt_col)} {type_text}"
        default = meta.get("default")
        if default:
            if not duckdb_default_text_is_safe(default):
                raise FastPathUnavailable(
                    f"source column {src_col!r} default is not DDL-safe"
                )
            piece += f" DEFAULT({default})"
        if not meta["nullable"]:
            piece += " NOT NULL"
        cols_sql.append(piece)

    mapped_lower = {k.lower() for k in mapped}
    for ctype, cols in constraints:
        if ctype in {"NOT NULL", ""}:
            continue
        if ctype in {"CHECK", "FOREIGN KEY"}:
            raise FastPathUnavailable(
                f"source {ctype} constraint stays on the row path "
                "(identity COPY cannot prove it reproduced it)"
            )
        if ctype not in {"PRIMARY KEY", "UNIQUE"}:
            raise FastPathUnavailable(
                f"source {ctype} constraint stays on the row path"
            )
        if not cols or any(c.lower() not in mapped_lower for c in cols):
            raise FastPathUnavailable(
                f"source {ctype} covers an unmapped column; identity COPY would "
                "enforce fewer rules than the source"
            )
        targets = ", ".join(
            duckdb_ident(mapped[_match_case(c, mapped)]) for c in cols
        )
        cols_sql.append(f"{ctype}({targets})")

    return f"CREATE TABLE {dest_ref} ({', '.join(cols_sql)})"


def _match_case(col: str, mapped: dict[str, str]) -> str:
    for key in mapped:
        if key.lower() == col.lower():
            return key
    raise FastPathUnavailable(f"constraint column {col!r} is not mapped")


def skip_complete_duckdb(
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
