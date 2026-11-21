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
import weakref
import os
import re
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


# MariaDB 10.6.0 added an InnoDB mode for inserts into an EMPTY table
# (MDEV-515; MDEV-24621 in 10.7 extended it to sorted index builds): one undo
# record and an exclusive table lock instead of per-row undo and locks. An
# INSERT or LOAD DATA uses it only when unique_checks=0 AND
# foreign_key_checks=0. Measured on MariaDB 10.11.14: a LOAD DATA in that mode
# plus a concurrent SELECT COUNT(*) on the table hangs both statements for
# good; KILL has no effect and only a server restart clears it. With either
# check left on, the mode never starts and the same load + reader completes.
# MySQL has no such mode (its exclusive-lock bulk load is the explicit,
# HeatWave-only LOAD DATA ... ALGORITHM=BULK), so MySQL keeps checks off.
MARIADB_EMPTY_TABLE_BULK_INSERT_SINCE = (10, 6)

_VERSION_NUMBER = re.compile(r"(\d+)\.(\d+)(?:\.(\d+))?")
_OFF_VALUES = frozenset({"0", "off", "false"})


def parse_mysql_family_version(version: Any) -> tuple[bool, tuple[int, int, int] | None]:
    """``(is_mariadb, (major, minor, patch) or None)`` from a ``VERSION()`` string.

    MariaDB before 11 reports ``5.5.5-10.11.14-MariaDB-...`` in the handshake
    (a replication-compat prefix); the real version follows it.
    """
    if isinstance(version, (bytes, bytearray)):
        version = bytes(version).decode("utf-8", "replace")
    text = str(version or "").strip()
    is_mariadb = "mariadb" in text.lower()
    if is_mariadb and text.startswith("5.5.5-"):
        text = text[len("5.5.5-"):]
    match = _VERSION_NUMBER.search(text)
    if not match:
        return is_mariadb, None
    return is_mariadb, (int(match.group(1)), int(match.group(2)), int(match.group(3) or 0))


def mariadb_empty_table_bulk_insert_possible(version: Any) -> bool:
    """Whether this server can enter MariaDB's empty-table bulk insert mode.

    True for MariaDB 10.6+ and for a MariaDB whose version number cannot be
    read (fail safe). False for MySQL (Percona, Aurora) and MariaDB < 10.6.
    """
    is_mariadb, parsed = parse_mysql_family_version(version)
    if not is_mariadb:
        return False
    if parsed is None:
        return True
    return parsed[:2] >= MARIADB_EMPTY_TABLE_BULK_INSERT_SINCE


_IDENTITY_ATTR = "_dataflow_server_identity"
_IDENTITY_CACHE: "weakref.WeakKeyDictionary[Any, str]" = weakref.WeakKeyDictionary()
_PROXY_MARKERS = ("proxysql", "maxscale")


def _cached_identity(connection: Any) -> str | None:
    cached = getattr(connection, _IDENTITY_ATTR, None)
    if isinstance(cached, str):
        return cached
    try:
        return _IDENTITY_CACHE.get(connection)
    except TypeError:  # not weak-referenceable
        return None


def _remember_identity(connection: Any, text: str) -> None:
    if connection is None:
        return
    try:
        setattr(connection, _IDENTITY_ATTR, text)
        return
    except (AttributeError, TypeError):
        pass
    try:
        _IDENTITY_CACHE[connection] = text
    except TypeError:
        pass


def mysql_server_version_text(cursor: Any, conn: Any | None = None, *, query: bool = True) -> str:
    """What the server says it is: ``@@version`` plus ``@@version_comment``.

    Asked once per connection and cached on it. The driver's handshake version
    is not trusted: MariaDB behind ProxySQL or MaxScale can announce an 8.0.x
    MySQL version there (review of the MariaDB bulk-insert guard). A server
    that cannot be asked, or whose answer names a proxy, returns ``""``
    (unknown), which every caller treats as "keep the checks on".
    ``query`` is kept for callers; the server is always asked once.
    """
    del query
    connection = conn if conn is not None else getattr(cursor, "connection", None)
    cached = _cached_identity(connection)
    if cached is not None:
        return cached
    text = ""
    try:
        cursor.execute("SELECT @@version, @@version_comment")
        row = cursor.fetchone()
        text = " ".join(str(v).strip() for v in (row or ()) if v is not None and str(v).strip())
    except Exception as exc:  # noqa: BLE001 - unknown version fails safe (checks on)
        logger.debug("could not read the MySQL-family server version: %s", exc)
    if any(marker in text.lower() for marker in _PROXY_MARKERS):
        text = ""  # the proxy answered for itself: the backend is unknown
    _remember_identity(connection, text)
    return text


def mysql_bulk_load_session_settings(version: str) -> dict[str, int]:
    """``unique_checks`` / ``foreign_key_checks`` for one bulk LOAD DATA.

    MySQL: both off (unchanged). MariaDB 10.6+, and a server whose version
    could not be read: both on, so the empty-table bulk insert mode (and its
    hang under a concurrent reader) cannot start. Measured on MariaDB 10.11
    with the PG->MySQL COPY route: no throughput cost (docs/use).
    """
    if not str(version or "").strip() or mariadb_empty_table_bulk_insert_possible(version):
        return {"unique_checks": 1, "foreign_key_checks": 1}
    return {"unique_checks": 0, "foreign_key_checks": 0}


def apply_mysql_bulk_load_session(cursor: Any, conn: Any | None = None) -> dict[str, int]:
    """Set the session checks for one bulk LOAD DATA; returns what was set.

    MySQL: skip secondary unique/FK maintenance (InnoDB still enforces the
    PRIMARY KEY). MariaDB 10.6+: both checks stay on, set explicitly so a
    server whose global defaults are off cannot start the bulk insert mode.
    Callers restore immediately after SHOW WARNINGS so a later MERGE / COUNT
    session is back to default checks.
    """
    settings = mysql_bulk_load_session_settings(mysql_server_version_text(cursor, conn))
    cursor.execute(f"SET SESSION unique_checks={settings['unique_checks']}")
    cursor.execute(f"SET SESSION foreign_key_checks={settings['foreign_key_checks']}")
    return settings


def guard_mariadb_bulk_insert_session(cursor: Any, conn: Any | None = None) -> bool:
    """On MariaDB 10.6+, never leave unique_checks and foreign_key_checks both off.

    Both off (a caller's SET, or a server whose global defaults are off) is
    what starts the empty-table bulk insert mode on the next INSERT or
    LOAD DATA into an empty table. Turning ``unique_checks`` back on keeps
    the mode off and only restores a default check. Returns whether the
    session was changed. An unknown server (no answer, or a proxy answering
    for itself) is guarded too: setting ``unique_checks=1`` is harmless on MySQL.
    """
    version = mysql_server_version_text(cursor, conn)
    if version and not mariadb_empty_table_bulk_insert_possible(version):
        return False  # a server known not to be MariaDB 10.6+
    cursor.execute("SELECT @@SESSION.unique_checks, @@SESSION.foreign_key_checks")
    row = cursor.fetchone()
    if not row or len(row) < 2:
        return False
    if str(row[0]).strip().lower() not in _OFF_VALUES or str(row[1]).strip().lower() not in _OFF_VALUES:
        return False
    cursor.execute("SET SESSION unique_checks=1")
    logger.warning(
        "MariaDB session had unique_checks=0 and foreign_key_checks=0; set "
        "unique_checks=1 so an insert into an empty table cannot start the "
        "bulk insert mode that hangs under concurrent reads"
    )
    return True


def guard_mariadb_bulk_insert_pool_session(dbapi_conn: Any) -> None:
    """Pooled MariaDB 10.6+ sessions never start with unique+FK checks both off.

    Both off is what puts an INSERT into an empty table in InnoDB's bulk
    insert mode, which hangs the server under a concurrent reader (see
    connectors.mysql_load_data). Best effort: no SQL on non-MariaDB servers.
    """
    try:
        cur = dbapi_conn.cursor()
        try:
            guard_mariadb_bulk_insert_session(cur, dbapi_conn)
        finally:
            cur.close()
    except Exception as exc:  # noqa: BLE001 - a guard must not break the pool
        logger.debug("MariaDB bulk insert pool guard not applied: %s", exc)


def restore_mysql_bulk_load_session(cursor: Any) -> None:
    cursor.execute("SET SESSION unique_checks=1")
    cursor.execute("SET SESSION foreign_key_checks=1")


def load_data_text_value(value: Any) -> str:
    """MySQL LOAD DATA text field (tab-delimited, ``\\N`` = NULL)."""
    from services.value_serializer import (
        is_missing_sentinel,
        is_reader_null_cell,
        safe_decimal_text,
        unescape_wire_text,
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
        raw = str(unescape_wire_text(value))
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
    line_terminator: str = "\n",
) -> str:
    from connectors.writer_common import quote_sql_identifier

    col_sql = ", ".join(quote_sql_identifier(c, "`") for c in columns)
    if field_terminator == "\t":
        term_sql = r"'\t'"
    elif field_terminator == ",":
        term_sql = "','"
    else:
        raise ValueError("LOAD DATA terminator must be tab or comma")
    if line_terminator == "\n":
        lines_sql = r"'\n'"
    elif line_terminator == "\r\n":
        lines_sql = r"'\r\n'"
    else:
        raise ValueError("LOAD DATA line terminator must be \\n or \\r\\n")
    enclosed = " OPTIONALLY ENCLOSED BY '\"'" if optionally_enclosed else ""
    ignore = f" IGNORE {int(ignore_lines)} LINES" if int(ignore_lines) > 0 else ""
    # table_q and infile_sql are already quoted identifiers / literals.
    return (
        f"LOAD DATA LOCAL INFILE {infile_sql} INTO TABLE {table_q} "
        "CHARACTER SET utf8mb4 "
        f"FIELDS TERMINATED BY {term_sql}{enclosed} "
        r"ESCAPED BY '\\' "
        f"LINES TERMINATED BY {lines_sql}"
        f"{ignore} "
        f"({col_sql})"
    )


def execute_load_data_local(
    cursor: Any,
    *,
    path: str,
    table_q: str,
    columns: list[str],
    field_terminator: str = "\t",
    optionally_enclosed: bool = False,
    ignore_lines: int = 0,
    conn: Any | None = None,
    line_terminator: str = "\n",
) -> None:
    """Run ``LOAD DATA LOCAL INFILE`` for ``path``, the only file the server may read."""
    from connectors.mysql_conn import local_infile_allowed

    sql = build_load_data_sql(
        table_q=table_q,
        columns=columns,
        infile_sql=quote_load_data_path(path),
        field_terminator=field_terminator,
        optionally_enclosed=optionally_enclosed,
        ignore_lines=ignore_lines,
        line_terminator=line_terminator,
    )
    connection = conn if conn is not None else getattr(cursor, "connection", None)
    # Every LOAD DATA route ends here: whatever the session inherited, a
    # MariaDB 10.6+ load never runs with both checks off (bulk insert mode).
    guard_mariadb_bulk_insert_session(cursor, connection)
    with local_infile_allowed(connection, path):
        cursor.execute(sql)


def fetch_load_data_warnings(cursor: Any) -> list[str]:
    """``SHOW WARNINGS`` right after a LOAD DATA, read before anything else runs.

    The one place every LOAD DATA consumer reads its warnings. The rows are
    fetched immediately: any later statement on the cursor (a session restore,
    a COUNT) replaces the result set, and a truncation that was never read
    committed silently (E1-018). Returns the blocking Warning/Error texts.
    """
    cursor.execute("SHOW WARNINGS")
    return blocking_load_data_warnings(list(cursor.fetchall() or []))


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
        try:
            execute_load_data_local(
                cursor,
                path=path,
                table_q=table_q,
                columns=columns,
                conn=connection,
            )
        except Exception as exc:
            from connectors.write_resilience import is_connection_lost

            if is_connection_lost(exc):
                raise
            return LoadDataAttempt(ok=False, reason=str(exc)[:300])
        try:
            blocked = fetch_load_data_warnings(cursor)
        except Exception as exc:
            return LoadDataAttempt(
                ok=False,
                reason=f"SHOW WARNINGS failed after LOAD DATA: {exc}"[:300],
            )
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
