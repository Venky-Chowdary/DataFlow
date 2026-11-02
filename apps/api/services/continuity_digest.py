"""Destination self-digest for the X-97 append continuity baseline.

The continuity check compares a destination with *itself* over time, so the
digest only has to be stable for unchanged data and sensitive to any change —
it never has to equal Gate-8's cross-engine Python fingerprint. That frees it
to run inside the database (PostgreSQL, MySQL) where the scan is cheap.

Aggregate: every row is hashed with md5 and two 60-bit slices of the hash
(hex 1-15 and 16-30) are summed separately, plus the row count — 120 bits of
row hash per row. A SUM, never an XOR: XOR cancels an identical row pair, so a
duplicated row would digest like the original population.

Rendering is pinned per session (time zone, date/float/bytea output) so the
same stored value always renders to the same text. The column set is returned
alongside so a schema change is reported as such, not as a digest mismatch.

MongoDB and SQLite hash a canonical rendering in process: MongoDB documents
with keys sorted at every level, SQLite rows by storage class.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Final

logger = logging.getLogger(__name__)

ALGO: Final[str] = "engine_v1"
SUPPORTED: Final[frozenset[str]] = frozenset({"postgresql", "mysql", "mariadb", "mongodb", "sqlite", "duckdb"})

PG_SESSION_PINS: Final[tuple[str, ...]] = (
    "SET LOCAL extra_float_digits = 1",
    "SET LOCAL TimeZone = 'UTC'",
    "SET LOCAL DateStyle = 'ISO, YMD'",
    "SET LOCAL IntervalStyle = 'postgres'",
    "SET LOCAL bytea_output = 'hex'",
)
MYSQL_SESSION_PINS: Final[tuple[str, ...]] = ("SET SESSION time_zone = '+00:00'",)

_HALF: Final[int] = 15  # hex chars per summed slice (60 bits)


def _result(rows: int, a: Any, b: Any, columns: list[list[str]] | None) -> dict[str, Any]:
    return {
        "rows": int(rows or 0),
        "checksum": f"{int(a or 0):x}.{int(b or 0):x}",
        "algo": ALGO,
        "columns": columns,
    }


def _pg_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _mysql_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _sorted_columns(rows: list[tuple[Any, Any]]) -> list[list[str]]:
    return sorted(([str(n), str(t or "").lower()] for n, t in rows), key=lambda c: c[0])


def postgresql_digest_sql(table_ref: str, columns: list[list[str]]) -> str:
    fields = " || ".join(
        f"md5(coalesce({_pg_ident(n)}::text, '') || ({_pg_ident(n)} IS NULL)::text)"
        for n, _t in columns
    )
    return (
        "SELECT count(*)::bigint, "
        f"coalesce(sum(('x' || substr(h, 1, {_HALF}))::bit(60)::bigint::numeric), 0), "
        f"coalesce(sum(('x' || substr(h, {_HALF + 1}, {_HALF}))::bit(60)::bigint::numeric), 0) "
        f"FROM (SELECT md5({fields}) AS h FROM {table_ref}) d"  # nosec B608 — quoted identifiers
    )


def _postgresql(cfg: dict[str, Any], schema: str, table: str) -> dict[str, Any] | None:
    from connectors.postgresql_conn import get_connection
    from connectors.sql_identifiers import quote_table_ref

    conn = get_connection(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 5432),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl")),
    )
    try:
        try:
            conn.autocommit = False
        except Exception:  # noqa: BLE001
            logger.debug("continuity digest: autocommit toggle unsupported", exc_info=True)
        cur = conn.cursor()
        for stmt in PG_SESSION_PINS:
            cur.execute(stmt)
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (schema or "public", table),
        )
        columns = _sorted_columns(cur.fetchall())
        if not columns:
            return None
        ref = quote_table_ref(table, schema or "public", dialect="postgresql")
        cur.execute(postgresql_digest_sql(ref, columns))
        n, a, b = cur.fetchone()
        return _result(n, a, b, columns)
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()


def _mysql_render(name: str, data_type: str) -> str:
    q = _mysql_ident(name)
    t = data_type.lower()
    if t in {"datetime", "timestamp"}:
        return f"DATE_FORMAT({q}, '%Y-%m-%d %H:%i:%s.%f')"
    if t == "date":
        return f"DATE_FORMAT({q}, '%Y-%m-%d')"
    if t in {"decimal", "numeric", "float", "double", "real", "time", "year", "json", "bit"}:
        return f"CAST({q} AS CHAR)"
    if t in {"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "geometry"}:
        return f"HEX({q})"
    return f"CAST({q} AS CHAR CHARACTER SET utf8mb4)"


def mysql_digest_sql(table_ref: str, columns: list[list[str]]) -> str:
    fields = ", ".join(
        f"MD5(COALESCE({_mysql_render(n, t)}, '')), IF({_mysql_ident(n)} IS NULL, '1', '0')"
        for n, t in columns
    )
    return (
        "SELECT COUNT(*), "
        f"COALESCE(SUM(CAST(CONV(SUBSTRING(h, 1, {_HALF}), 16, 10) AS UNSIGNED)), 0), "
        f"COALESCE(SUM(CAST(CONV(SUBSTRING(h, {_HALF + 1}, {_HALF}), 16, 10) AS UNSIGNED)), 0) "
        f"FROM (SELECT MD5(CONCAT({fields})) AS h FROM {table_ref}) d"  # nosec B608
    )


def _mysql(cfg: dict[str, Any], table: str) -> dict[str, Any] | None:
    from connectors.mysql_conn import get_connection

    database = str(cfg.get("database") or "")
    conn = get_connection(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 3306),
        database=database,
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl")),
        purpose="reconcile",
    )
    try:
        return mysql_digest_on(conn, database, table)
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()


def mysql_digest_on(conn: Any, database: str, table: str) -> dict[str, Any] | None:
    """Digest on an open connection; the session pins are applied here, not assumed."""
    cur = conn.cursor()
    for stmt in MYSQL_SESSION_PINS:
        cur.execute(stmt)
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = COALESCE(NULLIF(%s, ''), DATABASE()) AND TABLE_NAME = %s",
        (database, table),
    )
    columns = _sorted_columns(cur.fetchall())
    if not columns:
        return None
    ref = (_mysql_ident(database) + "." if database else "") + _mysql_ident(table)
    cur.execute(mysql_digest_sql(ref, columns))
    n, a, b = cur.fetchone()
    return _result(n, a, b, columns)


def canonical(value: Any) -> str:
    """Type-tagged rendering; dict keys sorted at every level."""
    if value is None:
        return "n"
    if isinstance(value, bool):
        return "b1" if value else "b0"
    if isinstance(value, int):
        return f"i{value}"
    if isinstance(value, float):
        return f"f{value!r}"
    if isinstance(value, str):
        return "s" + json.dumps(value, ensure_ascii=False)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "x" + bytes(value).hex()
    if isinstance(value, dict):
        return "{" + ",".join(
            json.dumps(str(k), ensure_ascii=False) + ":" + canonical(value[k])
            for k in sorted(value, key=str)
        ) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(canonical(v) for v in value) + "]"
    if isinstance(value, (datetime, date, time)):
        return "t" + value.isoformat()
    if isinstance(value, Decimal):
        return "d" + str(value)
    return f"o{type(value).__name__}:{value}"


def _sum_rows(renderings: Any) -> tuple[int, int, int]:
    n = a = b = 0
    md5 = hashlib.md5
    for text in renderings:
        h = md5(text.encode("utf-8"), usedforsecurity=False).hexdigest()
        a += int(h[:_HALF], 16)
        b += int(h[_HALF : 2 * _HALF], 16)
        n += 1
    return n, a, b


def _mongodb(cfg: dict[str, Any], table: str) -> dict[str, Any] | None:
    from connectors.mongodb_common import _mongo_client, normalize_mongodb_connection_string

    database = str(cfg.get("database") or "")
    conn_str = normalize_mongodb_connection_string(
        str(cfg.get("connection_string") or ""),
        database=database,
        host=cfg.get("host", ""),
        port=cfg.get("port"),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        ssl=bool(cfg.get("ssl")),
        auth_source=cfg.get("auth_source", ""),
    )
    client = _mongo_client(conn_str)
    coll = client[database or "test"][table]
    n, a, b = _sum_rows(canonical(doc) for doc in coll.find({}, batch_size=10000))
    # Schemaless: a new field is data, so no column set is tracked.
    return _result(n, a, b, None)


def _sqlite(path: str, table: str) -> dict[str, Any] | None:
    import sqlite3

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        q = '"' + table.replace('"', '""') + '"'
        info = conn.execute(f"PRAGMA table_info({q})").fetchall()  # nosec B608
        columns = _sorted_columns([(r[1], r[2]) for r in info])
        if not columns:
            return None
        sel = ", ".join('"' + c.replace('"', '""') + '"' for c, _t in columns)
        cur = conn.execute(f"SELECT {sel} FROM {q}")  # nosec B608
        n, a, b = _sum_rows(canonical(row) for row in cur)
        return _result(n, a, b, columns)
    finally:
        conn.close()


def _duckdb(path: str, schema: str, table: str) -> dict[str, Any] | None:
    import duckdb

    try:
        conn = duckdb.connect(path, read_only=True)
    except duckdb.ConnectionException:
        # Same-process handle already open with another config: reuse its mode.
        conn = duckdb.connect(path)
    try:
        info = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema or "main", table],
        ).fetchall()
        columns = _sorted_columns([(r[0], r[1]) for r in info])
        if not columns:
            return None
        sel = ", ".join('"' + c.replace('"', '""') + '"' for c, _t in columns)
        ref = '"' + (schema or "main").replace('"', '""') + '"."' + table.replace('"', '""') + '"'
        cur = conn.execute(f"SELECT {sel} FROM {ref}")  # nosec B608

        def _rows():
            while batch := cur.fetchmany(10000):
                yield from batch

        n, a, b = _sum_rows(canonical(row) for row in _rows())
        return _result(n, a, b, columns)
    finally:
        conn.close()


def digest(db_type: str, cfg: dict[str, Any], *, schema: str, table: str, path: str = "") -> dict[str, Any] | None:
    """``{rows, checksum, algo, columns}`` or ``None`` when the table is unreadable."""
    if db_type == "postgresql":
        return _postgresql(cfg, schema, table)
    if db_type in {"mysql", "mariadb"}:
        return _mysql(cfg, table)
    if db_type == "mongodb":
        return _mongodb(cfg, table)
    if db_type == "sqlite":
        return _sqlite(path, table)
    if db_type == "duckdb":
        return _duckdb(path, schema, table)
    return None
