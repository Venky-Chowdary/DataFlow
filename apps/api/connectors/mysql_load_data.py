"""MySQL ``LOAD DATA LOCAL INFILE`` — bulk insert that refuses silent coerce.

Fivetran/Airbyte MySQL destinations typically JDBC-batch INSERT. Warehouse
engines (Snowflake COPY, BigQuery load, Postgres COPY) get bulk I/O. This
path is that bulk lever for MySQL **only** when quality holds:

* Session ``sql_mode`` must already prove ``STRICT_ALL_TABLES`` and
  ``STRICT_TRANS_TABLES`` (set by ``apply_mysql_session_guards``).
* Autocommit must be off so a warned batch can roll back.
* ``SHOW WARNINGS`` Warning/Error rows abort the attempt — never leave a
  truncated INT/date in the destination. Caller rolls back and falls through
  to ``executemany`` + per-row quarantine.
* Upsert, LSN, binary/geometry, and public-proxy routes stay on INSERT.

Dest ``COUNT(*)`` is still required at transfer end. This module does not
weaken conservation.
"""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)

# LOAD DATA cannot safely round-trip these as TSV text under STRICT.
_UNSAFE_LOAD_BASES = frozenset({
    "BLOB",
    "TINYBLOB",
    "MEDIUMBLOB",
    "LONGBLOB",
    "BINARY",
    "VARBINARY",
    "BIT",
    "GEOMETRY",
    "POINT",
    "LINESTRING",
    "POLYGON",
    "MULTIPOINT",
    "MULTILINESTRING",
    "MULTIPOLYGON",
    "GEOMETRYCOLLECTION",
    "VECTOR",
})

# Note-level SHOW WARNINGS (e.g. "Records: N …") are not coercion.
_BLOCKING_WARNING_LEVELS = frozenset({"WARNING", "ERROR"})


@dataclass(frozen=True)
class LoadDataAttempt:
    """One chunk attempt. ``ok`` means the caller may commit this chunk."""

    ok: bool
    rows_loaded: int = 0
    reason: str = ""
    warnings: tuple[str, ...] = ()


def mysql_load_data_enabled() -> bool:
    """Operator gate — default **on**. ``DATAFLOW_MYSQL_LOAD_DATA=0`` disables."""
    raw = (getenv_brand("MYSQL_LOAD_DATA", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def mysql_load_data_eligible(
    *,
    write_mode: str,
    conflict_columns: list[str] | None,
    target_cols: list[str],
    target_types: list[str] | None,
    proxy: bool,
) -> tuple[bool, str]:
    """Whether this write *may* use LOAD DATA. Session probe is separate."""
    if not mysql_load_data_enabled():
        return False, "DATAFLOW_MYSQL_LOAD_DATA=0"
    if (write_mode or "insert").strip().lower() != "insert":
        return False, "upsert/CDC stays on INSERT"
    if conflict_columns:
        return False, "conflict keys require INSERT/ON DUPLICATE"
    if proxy:
        return False, "public proxy: LOCAL INFILE not assumed"
    from connectors.lsn_guards import DF_LSN_COL

    if DF_LSN_COL in (target_cols or []):
        return False, "LSN column requires INSERT path"
    unsafe = _unsafe_load_columns(target_cols or [], target_types or [])
    if unsafe:
        return False, f"binary/geometry column(s) {unsafe}"
    return True, ""


def _unsafe_load_columns(target_cols: list[str], target_types: list[str]) -> list[str]:
    from connectors.sql_temporal import sql_base_type

    bad: list[str] = []
    for i, col in enumerate(target_cols):
        ddl = target_types[i] if i < len(target_types) else ""
        base = sql_base_type(ddl)
        if base in _UNSAFE_LOAD_BASES:
            bad.append(col)
    return bad


def mysql_strict_sql_mode_proven(cursor: Any) -> tuple[bool, str]:
    """Fail closed unless both STRICT flags are on this session."""
    try:
        cursor.execute("SELECT @@SESSION.sql_mode")
        row = cursor.fetchone()
        current = (row[0] if row else "") or ""
    except Exception as exc:
        return False, f"could not read sql_mode: {exc}"
    parts = {p.strip().upper() for p in str(current).split(",") if p.strip()}
    missing = [m for m in ("STRICT_TRANS_TABLES", "STRICT_ALL_TABLES") if m not in parts]
    if missing:
        return False, f"sql_mode missing {missing}"
    return True, ""


def mysql_local_infile_on(cursor: Any) -> tuple[bool, str]:
    """Server must accept LOCAL INFILE. Client flag is set at connect."""
    try:
        cursor.execute("SELECT @@GLOBAL.local_infile")
        row = cursor.fetchone()
        raw = row[0] if row else 0
    except Exception as exc:
        return False, f"could not read local_infile: {exc}"
    on = str(raw).strip().lower() in {"1", "on", "true"}
    if not on:
        return False, "server local_infile=OFF (mysqld --local-infile=1)"
    return True, ""


def mysql_autocommit_off(conn: Any) -> bool:
    getter = getattr(conn, "get_autocommit", None)
    if callable(getter):
        try:
            return not bool(getter())
        except Exception:
            return False
    mode = getattr(conn, "autocommit_mode", None)
    if mode is not None:
        return not bool(mode)
    return False


def mysql_load_data_session_ready(cursor: Any, conn: Any) -> tuple[bool, str]:
    """STRICT + LOCAL INFILE + transactional session — all required."""
    if not mysql_autocommit_off(conn):
        return False, "refuse LOAD DATA under autocommit"
    ok, reason = mysql_strict_sql_mode_proven(cursor)
    if not ok:
        return False, reason
    return mysql_local_infile_on(cursor)


def load_data_text_value(value: Any) -> str:
    """MySQL LOAD DATA text field (tab-delimited, ``\\N`` = NULL)."""
    from services.value_serializer import (
        is_missing_sentinel,
        is_reader_null_cell,
        safe_decimal_text,
    )

    if value is None or is_reader_null_cell(value) or is_missing_sentinel(value):
        return "\\N"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.replace(tzinfo=None)
        if value.microsecond:
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, time):
        if value.microsecond:
            return value.strftime("%H:%M:%S.%f")
        return value.strftime("%H:%M:%S")
    if isinstance(value, Decimal):
        return safe_decimal_text(value) or str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError("binary cell refuses LOAD DATA text (use INSERT)")
    if isinstance(value, (dict, list)):
        import json

        from services.value_serializer import json_default

        raw = json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), default=json_default
        )
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        raw = str(value)
    return _escape_load_text(raw)


def _escape_load_text(text: str) -> str:
    return (
        text.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def render_load_data_tsv(rows: list[tuple]) -> str:
    """Whole-batch TSV for tests. Does not write a file."""
    lines: list[str] = []
    for row in rows:
        lines.append("\t".join(load_data_text_value(v) for v in row))
    return "\n".join(lines) + ("\n" if rows else "")


def quote_load_data_path(path: str) -> str:
    """SQL string literal for LOCAL INFILE. Refuse quotes/NUL."""
    if not path or any(c in path for c in "'\x00"):
        raise ValueError("refusing LOAD DATA path with quote or NUL")
    return "'" + path.replace("\\", "\\\\") + "'"


def build_load_data_sql(
    *,
    table_q: str,
    columns: list[str],
    infile_sql: str,
    field_terminator: str = "\t",
    optionally_enclosed: bool = False,
    ignore_lines: int = 0,
) -> str:
    from connectors.writer_common import quote_sql_identifier

    col_sql = ", ".join(quote_sql_identifier(c, "`") for c in columns)
    if field_terminator == "\t":
        term_sql = r"'\t'"
    elif field_terminator == ",":
        term_sql = "','"
    else:
        raise ValueError("LOAD DATA terminator must be tab or comma")
    enclosed = " OPTIONALLY ENCLOSED BY '\"'" if optionally_enclosed else ""
    ignore = f" IGNORE {int(ignore_lines)} LINES" if int(ignore_lines) > 0 else ""
    # table_q and infile_sql are already quoted identifiers / literals.
    return (
        f"LOAD DATA LOCAL INFILE {infile_sql} INTO TABLE {table_q} "
        "CHARACTER SET utf8mb4 "
        f"FIELDS TERMINATED BY {term_sql}{enclosed} "
        r"ESCAPED BY '\\' "
        r"LINES TERMINATED BY '\n'"
        f"{ignore} "
        f"({col_sql})"
    )


def blocking_load_data_warnings(warning_rows: list[tuple[Any, ...]]) -> list[str]:
    """Warning/Error SHOW WARNINGS must not be committed. Notes may pass."""
    blocked: list[str] = []
    for row in warning_rows or []:
        if not row:
            continue
        level = str(row[0] or "").strip().upper()
        code = row[1] if len(row) > 1 else ""
        msg = str(row[2] if len(row) > 2 else row[-1])
        if level in _BLOCKING_WARNING_LEVELS:
            blocked.append(f"{level} {code}: {msg}"[:300])
    return blocked


def try_mysql_load_data_local(
    cursor: Any,
    *,
    table_q: str,
    columns: list[str],
    rows: list[tuple],
    conn: Any | None = None,
) -> LoadDataAttempt:
    """Load one chunk. Does **not** commit. Caller commits or rolls back."""
    if not rows:
        return LoadDataAttempt(ok=True, rows_loaded=0)
    connection = conn if conn is not None else getattr(cursor, "connection", None)
    if connection is None or not mysql_autocommit_off(connection):
        return LoadDataAttempt(ok=False, reason="refuse LOAD DATA under autocommit")
    ok, reason = mysql_strict_sql_mode_proven(cursor)
    if not ok:
        return LoadDataAttempt(ok=False, reason=reason)

    fd, path = tempfile.mkstemp(prefix="df_mysql_ld_", suffix=".tsv")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write("\t".join(load_data_text_value(v) for v in row))
                handle.write("\n")
        sql = build_load_data_sql(
            table_q=table_q,
            columns=columns,
            infile_sql=quote_load_data_path(path),
        )
        try:
            cursor.execute(sql)
        except Exception as exc:
            from connectors.write_resilience import is_connection_lost

            if is_connection_lost(exc):
                raise
            return LoadDataAttempt(ok=False, reason=str(exc)[:300])
        try:
            cursor.execute("SHOW WARNINGS")
            warning_rows = list(cursor.fetchall() or [])
        except Exception as exc:
            return LoadDataAttempt(
                ok=False,
                reason=f"SHOW WARNINGS failed after LOAD DATA: {exc}"[:300],
            )
        blocked = blocking_load_data_warnings(warning_rows)
        if blocked:
            return LoadDataAttempt(
                ok=False,
                reason=blocked[0],
                warnings=tuple(blocked),
            )
        return LoadDataAttempt(ok=True, rows_loaded=len(rows))
    except TypeError as exc:
        return LoadDataAttempt(ok=False, reason=str(exc)[:300])
    finally:
        try:
            os.unlink(path)
        except OSError:
            logger.debug("LOAD DATA tempfile unlink skipped", exc_info=True)
