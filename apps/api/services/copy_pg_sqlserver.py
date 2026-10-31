"""PostgreSQL COPY text → SQL Server fast_executemany (cross-engine bulk).

Same-engine SQL Server already uses INSERT SELECT. Cross-engine cannot.
This path streams ``COPY (SELECT …) TO STDOUT`` text (tab / ``\\N``),
decodes each field (whole-field ``\\N`` is NULL; empty string stays
empty string), and binds batches with pyodbc ``fast_executemany``.

This is **not** BCP and **not** ``BULK INSERT FORMAT='CSV'``. This host
has no client ``bcp``; Linux SQL Server rejects ``CODEPAGE``; CSV bulk
collapses quoted empty string ``""`` to NULL — silent empty-string loss,
forbidden for identity. Docker volume FS is not writable from the agent.

Python materializes decoded batches only. Proof is dest ``COUNT(*)`` vs
the source snapshot taken under ``REPEATABLE READ`` +
``pg_export_snapshot()``. A mapped single PK still proves dest COUNT
per key range. Empty dest COPYs the table once (serial INSERT into a
PK dest). Occupied dest + mapped PK: skip a complete range. Append declines a partial range; overwrite may
DELETE+reload. No mapped single PK on an occupied dest: decline.

Declines (row path keeps quarantine): transforms that change values,
jsonb/bytea/timestamptz/arrays, public proxy, occupied dest without a
mapped single PK.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from services.brand_env import getenv_brand
from services.copy_fast_path import (
    occupied_pk_range_action,
    FastPathResult,
    FastPathUnavailable,
    _quote,
    _table_ref as _pg_table_ref,
    settle_fast_path_create_on,
    source_column_types,
    source_table_shape,
)
from services.copy_pg_mysql import (
    _INTEGER_PK_BASES,
    _copy_select_sql,
    _jsonable_bound,
    _pg_base,
    _pg_connect,
    _pg_copy_select_expr,
    _pg_quoted_literal,
    fetch_integer_pk_cuts,
    fetch_pk_interior_cuts,
    key_ranges_from_cuts,
    mapped_single_pk,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
    pg_type_is_load_safe,
    pk_range_predicate,
)
from services.copy_sqlserver_sqlserver import (
    _count as _ss_count,
    _create_sql as _ss_create_sql,
    _delete_range as _ss_delete_range,
    _drop_sql as _ss_drop_sql,
    _has_identity,
    _ident as _ss_ident,
    _range_count as _ss_range_count,
    _schema_of as _ss_schema_of,
    _ss_connect,
    _table_exists as _ss_table_exists,
    _table_ref as _ss_table_ref,
)

from services.copy_range_proof import range_keys_match  # noqa: E402

logger = logging.getLogger(__name__)

_COPY_SIMPLE_ESCAPES = {
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "v": "\v",
    "\\": "\\",
}


def pg_sqlserver_copy_enabled() -> bool:
    raw = (getenv_brand("PG_SQLSERVER_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def pg_sqlserver_copy_batch() -> int:
    raw = (getenv_brand("PG_SQLSERVER_COPY_BATCH", "5000") or "5000").strip()
    try:
        return max(1, min(int(raw), 100_000))
    except ValueError:
        return 5000


def unescape_copy_text(raw: str) -> str:
    """Undo PostgreSQL COPY text backslash escapes, left to right."""
    if "\\" not in raw:
        return raw
    out: list[str] = []
    i = 0
    n = len(raw)
    while i < n:
        ch = raw[i]
        if ch != "\\" or i + 1 >= n:
            out.append(ch)
            i += 1
            continue
        nxt = raw[i + 1]
        mapped = _COPY_SIMPLE_ESCAPES.get(nxt)
        if mapped is not None:
            out.append(mapped)
            i += 2
            continue
        if nxt in "01234567":
            j = i + 1
            digits: list[str] = []
            while j < n and len(digits) < 3 and raw[j] in "01234567":
                digits.append(raw[j])
                j += 1
            out.append(chr(int("".join(digits), 8)))
            i = j
            continue
        out.append(nxt)
        i += 2
    return "".join(out)


def decode_copy_text_field(raw: str) -> str | None:
    """Whole-field ``\\N`` is SQL NULL. Empty string is empty string."""
    if raw == "\\N":
        return None
    return unescape_copy_text(raw)


def decode_copy_text_row(line: str) -> list[str | None]:
    return [decode_copy_text_field(part) for part in line.split("\t")]


def _as_int(value: str | None) -> int | None:
    if value is None:
        return None
    return int(value)


def _as_float(value: str | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _as_date(value: str | None) -> date | None:
    if value is None:
        return None
    return date.fromisoformat(value[:10])


def _as_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text.replace(" ", "T", 1))
    except ValueError:
        return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")


def _identity(value: str | None) -> str | None:
    return value


def _as_decimal(value: str | None) -> Decimal | None:
    """Bind COPY text as a decimal, not as a character string.

    ``DECIMAL(6,2)`` holds ``1000.00``, but that text is 7 characters because
    of the decimal point. ODBC Driver 18 sizes the fast_executemany wchar
    buffer from the precision (6 characters, 12 bytes) and then rejects the
    text with ``String data, right truncation: length 14 buffer 12``. A
    ``Decimal`` is sent as a numeric, so the point is not an extra character.
    """
    if value is None:
        return None
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"SQL Server decimal value {value!r} is not a decimal") from exc


def converter_for_ddl(ddl: str) -> Callable[[str | None], Any]:
    base = (ddl or "").split("(")[0].strip().upper().replace(" ", "")
    if base in {"BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT", "BIT"}:
        return _as_int
    if base in {"FLOAT", "REAL", "FLOAT4", "FLOAT8"} or base.startswith("DOUBLE"):
        return _as_float
    if base in {"DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY"}:
        return _as_decimal
    if base == "DATE":
        return _as_date
    if base.startswith("DATETIME") or base.startswith("SMALLDATETIME"):
        return _as_datetime
    return _identity


def _enable_fast_executemany(cur: Any) -> bool:
    inner = getattr(cur, "_cur", cur)
    if hasattr(inner, "fast_executemany"):
        inner.fast_executemany = True
        return True
    return False


def _cursor_is_pymssql(cur: Any) -> bool:
    """True when the dest cursor is backed by pymssql (FreeTDS), which cannot
    RPC-pack a Python ``Decimal`` at wide precision — a DECIMAL(38,10) value
    aborts the batch with TDS error 8023. Canonical text binds instead;
    SQL Server converts varchar→decimal implicitly (X-25)."""
    return type(cur).__module__.startswith("pymssql")


def _cursor_is_pyodbc(cur: Any) -> bool:
    """True when the dest cursor is backed by pyodbc. fast_executemany packs
    ``Decimal`` into a SQL_NUMERIC_STRUCT, which cannot hold values beyond
    ~28 significant digits — a DECIMAL(38,10) max aborts the batch with the
    same TDS 8023 RPC error (X-25 live)."""
    inner = getattr(cur, "_cur", cur)
    return type(inner).__module__.startswith("pyodbc")


def _sqlserver_time_bind(value: Any) -> Any:
    """MySQL/row-path TIME arrives as ``datetime.timedelta``; neither pyodbc
    nor pymssql accepts that type (X-38 live: 'Unknown object type
    datetime.timedelta during describe'). In-range values become a
    ``datetime.time`` (micros intact — SQL Server ``time`` is 0–24h, and
    out-of-range MySQL TIME was already declined to the typed row path).
    """
    import datetime as _dt

    if isinstance(value, _dt.timedelta):
        if _dt.timedelta(0) <= value < _dt.timedelta(days=1):
            return (_dt.datetime.min + value).time()
        return value  # out-of-domain stays a typed-finding concern, not a bind
    return value


def bind_row_for_sqlserver(cur: Any, row: tuple[Any, ...]) -> tuple[Any, ...]:
    """Bind one row for the SQL Server driver this cursor uses.

    pymssql: ``Decimal`` values are sent as canonical decimal text
    (``format(d, 'f')`` — never scientific notation), which the server
    converts into the decimal column. pyodbc: Decimals bind the same way —
    fast_executemany sizes each parameter buffer from the FIRST row's
    Python type, so a column mixing ``str`` (the >28-digit wide values)
    and ``Decimal`` across rows aborts with HY090 'Invalid string or
    buffer length' (X-48b live). One canonical bind type per column —
    plain decimal text, no exponent, scale kept — binds every row;
    SQL Server converts varchar→decimal implicitly (X-25).
    ``timedelta`` (pymysql TIME) becomes ``datetime.time`` on both drivers.
    """
    if _cursor_is_pymssql(cur):
        from decimal import Decimal

        return tuple(
            format(v, "f") if isinstance(v, Decimal) else _sqlserver_time_bind(v)
            for v in row
        )
    if _cursor_is_pyodbc(cur):
        from decimal import Decimal

        return tuple(
            format(v, "f")
            if isinstance(v, Decimal)
            else _sqlserver_time_bind(v)
            for v in row
        )
    return row


def _sqlserver_decimal_cast_placeholder(ddl: str) -> str | None:
    """``CAST(%s AS DECIMAL(p,s))`` for wide-decimal dest columns.

    pyodbc fast_executemany describes the parameter from the column type and
    sizes the wchar buffer from the precision — DECIMAL(38) yields a 76-byte
    buffer that truncates the 39-char fixed-point text a >28-digit bind sends
    ('String data, right truncation: length 78 buffer 76'). Wrapping the
    placeholder in an explicit CAST declares the parameter a varchar-sizeable
    input, so the canonical text reaches the server and converts. Only
    ``DECIMAL(p>28)`` can receive a text-bound wide decimal, so narrower
    columns keep the plain ``%s``.
    """
    from services.type_system import (
        LOGICAL_DECIMAL,
        normalize_logical_type,
        parse_numeric_precision_scale,
    )

    if normalize_logical_type(ddl or "") != LOGICAL_DECIMAL:
        return None
    p, s = parse_numeric_precision_scale(ddl or "")
    if p is None or int(p) <= 28:
        return None
    return f"CAST(%s AS DECIMAL({int(p)},{0 if s is None else int(s)}))"


def sqlserver_insert_placeholders(sqlserver_ddls: list[str]) -> str:
    """``%s`` placeholder list; wide-decimal dest columns wrapped in CAST."""
    return ", ".join(
        _sqlserver_decimal_cast_placeholder(ddl) or "%s" for ddl in sqlserver_ddls
    )


def set_wide_decimal_inputsizes(cur: Any, sqlserver_ddls: list[str]) -> None:
    """Declare wide-decimal params ``SQL_VARCHAR`` so pyodbc does not truncate.

    ``setinputsizes((SQL_DECIMAL, …))`` keeps the same precision-sized buffer;
    declaring varchar with room for sign + digits + point lets the fixed-point
    text bind whole, and the CAST in the insert converts it (pyodbc#845).
    pymssql has no setinputsizes — its RPC packing already sends the varchar
    payload at real length.
    """
    if not _cursor_is_pyodbc(cur):
        return
    try:
        import pyodbc
    except ImportError:
        return
    from services.type_system import normalize_logical_type

    inner = getattr(cur, "_cur", cur)
    setter = getattr(inner, "setinputsizes", None)
    if setter is None:
        return
    sizes = [_pyodbc_size_for_ddl(ddl, pyodbc, normalize_logical_type) for ddl in sqlserver_ddls]
    if any(
        isinstance(el, tuple)
        and len(el) >= 2
        and el[0] == pyodbc.SQL_VARCHAR
        and _is_wide_decimal_ddl(ddl, normalize_logical_type)
        for el, ddl in zip(sizes, sqlserver_ddls, strict=True)
    ):
        try:
            # pyodbc takes ONE argument — the sizes sequence — not N
            # positional args (X-40: 'setinputsizes() takes exactly one
            # argument (N given)' reached the user as a raw TypeError).
            setter(sizes)
        except Exception as exc:
            # Decline, don't swallow: without the varchar declarations a
            # wide decimal could bind truncated by fast_executemany, so a
            # rejected setinputsizes sends the copy to the row path where
            # every bind failure is a typed finding and read-back verifies
            # exact precision — never a silent precision loss, never a raw
            # driver error reaching the user.
            raise FastPathUnavailable(
                f"pyodbc setinputsizes rejected wide-decimal declarations: {exc}"
            ) from exc


def _is_wide_decimal_ddl(ddl: str, normalize_logical_type: Any) -> bool:
    from services.type_system import LOGICAL_DECIMAL, parse_numeric_precision_scale

    if normalize_logical_type(ddl or "") != LOGICAL_DECIMAL:
        return False
    p, _s = parse_numeric_precision_scale(ddl or "")
    return p is not None and int(p) > 28


def _pyodbc_size_for_ddl(
    ddl: str, pyodbc: Any, normalize_logical_type: Any
) -> Any:
    """One ``setinputsizes`` element for a dest column — never ``None``.

    X-40 live: msodbcsql 18 maps a ``None`` slot to a zero-length parameter,
    so ``executemany`` dies with ``HY090 Invalid string or buffer length
    (0)`` on any mixed-type table that contains a wide decimal. Every
    position gets a real descriptor derived from the dest DDL instead.
    Unknown/exotic types fall back to the ``str`` type object (text bind;
    the server-side column type converts) — still never ``None``.
    """
    from services.type_system import (
        LOGICAL_BINARY,
        LOGICAL_BOOLEAN,
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_INTEGER,
        LOGICAL_TIME,
        LOGICAL_UUID,
        parse_numeric_precision_scale,
    )

    raw = (ddl or "").strip()
    base = raw.split("(")[0].strip().upper().replace(" ", "")
    tail = raw[len(raw.split("(")[0]):]
    arg = ""
    if tail.startswith("(") and tail.endswith(")"):
        arg = tail[1:-1].split(",")[0].strip().upper()

    logical = normalize_logical_type(raw)
    if logical == LOGICAL_DECIMAL:
        p, s = parse_numeric_precision_scale(raw)
        if p is not None and int(p) > 28:
            # Wide decimals bind as text through the CAST placeholder.
            return (pyodbc.SQL_VARCHAR, int(p) + 3)
        if p is not None:
            return (pyodbc.SQL_DECIMAL, int(p), int(s or 0))
        return pyodbc.SQL_VARCHAR
    if logical == LOGICAL_INTEGER:
        if base == "BIGINT":
            return pyodbc.SQL_BIGINT
        if base == "SMALLINT":
            return pyodbc.SQL_SMALLINT
        if base == "TINYINT":
            return pyodbc.SQL_TINYINT
        return pyodbc.SQL_INTEGER
    if logical == LOGICAL_BOOLEAN or base == "BIT":
        return pyodbc.SQL_BIT
    if logical == LOGICAL_FLOAT:
        return pyodbc.SQL_REAL if base == "REAL" else pyodbc.SQL_FLOAT
    if logical == LOGICAL_DATE or base == "DATE":
        return pyodbc.SQL_TYPE_DATE
    if logical == LOGICAL_TIME or base.startswith("TIME"):
        return pyodbc.SQL_TYPE_TIME
    if base == "DATETIMEOFFSET":
        return getattr(pyodbc, "SQL_SS_TIMESTAMPOFFSET", pyodbc.SQL_WVARCHAR)
    if logical == LOGICAL_DATETIME or base.startswith(("DATETIME", "SMALLDATETIME")):
        return pyodbc.SQL_TYPE_TIMESTAMP
    if logical == LOGICAL_UUID or base == "UNIQUEIDENTIFIER":
        return (pyodbc.SQL_VARCHAR, 36)
    if logical == LOGICAL_BINARY or base in {
        "BINARY", "VARBINARY", "IMAGE", "ROWVERSION", "TIMESTAMP",
    }:
        if arg.isdigit():
            return (pyodbc.SQL_VARBINARY, int(arg))
        return getattr(pyodbc, "SQL_LONGVARBINARY", pyodbc.SQL_VARBINARY)
    # Text family — explicit size, MAX, or a long-varchar fallback.
    if arg == "MAX" or base in {"TEXT", "NTEXT", "XML"}:
        return getattr(pyodbc, "SQL_WLONGVARCHAR", pyodbc.SQL_WVARCHAR)
    if arg.isdigit():
        # Always the wide variant: Python str is UTF-16, and declaring
        # SQL_VARCHAR would let the driver down-convert non-ASCII text to
        # '?' — silent loss. The server narrows to varchar codepage per
        # the column's own contract.
        return (pyodbc.SQL_WVARCHAR, int(arg))
    return str


class _CopyExecutemanySink:
    """File-like COPY TO STDOUT consumer that binds decoded batches."""

    def __init__(
        self,
        dst_cur: Any,
        insert_sql: str,
        converters: list[Callable[[str | None], Any]],
        batch_size: int,
        expected_cols: int,
    ) -> None:
        self._cur = dst_cur
        self._insert_sql = insert_sql
        self._converters = converters
        self._batch_size = batch_size
        self._expected_cols = expected_cols
        self._raw = bytearray()
        self._batch: list[tuple[Any, ...]] = []
        self.rows = 0

    def write(self, data: Any) -> int:
        if isinstance(data, str):
            data = data.encode("utf-8")
        elif isinstance(data, memoryview):
            data = data.tobytes()
        self._raw.extend(data)
        self._drain_lines(flush=False)
        return len(data)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self._drain_lines(flush=True)

    def _drain_lines(self, *, flush: bool) -> None:
        while True:
            nl = self._raw.find(b"\n")
            if nl < 0:
                break
            line = self._raw[:nl]
            del self._raw[: nl + 1]
            if line.endswith(b"\r"):
                line = line[:-1]
            if not line:
                continue
            self._add_line(line.decode("utf-8"))
        if flush and self._raw:
            line = bytes(self._raw)
            self._raw.clear()
            if line.endswith(b"\r"):
                line = line[:-1]
            if line:
                self._add_line(line.decode("utf-8"))
        if flush:
            self._flush_batch()

    def _add_line(self, line: str) -> None:
        fields = decode_copy_text_row(line)
        if len(fields) != self._expected_cols:
            raise ValueError(
                f"COPY row has {len(fields)} fields, expected {self._expected_cols}"
            )
        row = tuple(
            conv(value) for conv, value in zip(self._converters, fields, strict=True)
        )
        self._batch.append(row)
        if len(self._batch) >= self._batch_size:
            self._flush_batch()

    def _flush_batch(self) -> None:
        if not self._batch:
            return
        # D102: a cancel stops the next batch — without this probe the whole
        # COPY ran to completion before the engine's checkpoint noticed it.
        from services.cancel_check import cancel_checkpoint

        cancel_checkpoint(self.rows)
        bulk_executemany(
            self._cur,
            self._insert_sql,
            [bind_row_for_sqlserver(self._cur, r) for r in self._batch],
        )
        self.rows += len(self._batch)
        self._batch.clear()


def driver_failure_decline(exc: BaseException) -> "FastPathUnavailable | None":
    """A driver-layer failure becomes a typed decline, or ``None``.

    Residual pyodbc/pymssql/sqlalchemy errors must never reach the user as
    raw ODBC text (``('HY090', ...)``). The engine falls back to the row
    path where the same failure is reported as a typed finding naming the
    column — so any exception carrying a driver signature is re-raised as
    ``FastPathUnavailable``. Everything else is left alone (``None``).
    """
    if isinstance(exc, FastPathUnavailable):
        return None
    mod = type(exc).__module__ or ""
    if mod.startswith(("pyodbc", "pymssql", "sqlalchemy", "pytds", "tds_")):
        return FastPathUnavailable(
            f"SQL Server driver failure ({type(exc).__name__}): {exc}"
        )
    text = str(exc)
    if text.startswith("('") and "]" in text[:120]:
        return FastPathUnavailable(
            f"SQL Server driver failure ({type(exc).__name__}): {exc}"
        )
    return None


def bulk_executemany(cur: Any, insert_sql: str, rows: list[Any]) -> None:
    """``executemany`` that folds driver bind errors into a typed decline.

    X-40 live: a failed bulk bind must never surface a raw ODBC string to
    the user. Any ``executemany``/``SQLExecute`` crash becomes
    ``FastPathUnavailable`` so the engine falls back to the row path, where
    every bind failure is reported as a typed finding naming the column.
    """
    # NOTE: call the cursor's own executemany — _MssqlQmarkCursor is what
    # rewrites the pymssql-style ``%s`` markers to pyodbc ``?``. Unwrapping
    # to ``cur._cur`` skips that adaptation and pyodbc reports "Expected 0
    # parameters" — a silent row-path fallback on every insert (X-48).
    try:
        cur.executemany(insert_sql, rows)
    except FastPathUnavailable:
        raise
    except Exception as exc:
        raise FastPathUnavailable(
            f"SQL Server bulk bind failed ({exc.__class__.__name__}): {exc}"
        ) from exc


def _copy_into_sqlserver(
    src_cur: Any,
    dst_cur: Any,
    *,
    copy_sql: str,
    insert_sql: str,
    converters: list[Callable[[str | None], Any]],
) -> int:
    sink = _CopyExecutemanySink(
        dst_cur,
        insert_sql,
        converters,
        pg_sqlserver_copy_batch(),
        len(converters),
    )
    try:
        src_cur.copy_expert(copy_sql, sink)
    finally:
        sink.close()
    return sink.rows


def copy_postgres_to_sqlserver(
    *,
    source_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlserver_ddls: list[str],
    replace_destination: bool,
    dest_schema: str | None = None,
) -> FastPathResult:
    """COPY text from PostgreSQL into SQL Server. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(sqlserver_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not pg_sqlserver_copy_enabled():
        raise FastPathUnavailable("PostgreSQL→SQL Server COPY disabled")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: SQL Server bulk copy not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_ref = _pg_table_ref(source_schema, source_table)
    dst_schema = _ss_schema_of(dest_cfg, dest_schema)
    dest_ref = _ss_table_ref(dst_schema, dest_table)
    col_sql = ", ".join(_ss_ident(c) for c in target_cols)
    placeholders = sqlserver_insert_placeholders(sqlserver_ddls)
    converters = [converter_for_ddl(ddl) for ddl in sqlserver_ddls]

    source_conn = _pg_connect(source_cfg)
    dest_conn = _ss_connect(dest_cfg)
    created_here = False
    existed_before = False
    reset_empty_dest_on_failure = False
    pk_map: tuple[str, str] | None = None
    preserve_dest_on_failure = False
    try:
        source_conn.autocommit = False
        src_cur = source_conn.cursor()
        dst_cur = dest_conn.cursor()
        fast = False
        try:
            src_cur.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            live = source_column_types(
                src_cur, source_schema, source_table, source_cols
            )
            live_l = {k.lower(): v for k, v in live.items()}
            for col in source_cols:
                declared = live_l.get(col.lower())
                if not declared:
                    raise FastPathUnavailable(f"source column {col!r} absent")
                if not pg_type_is_load_safe(declared):
                    raise FastPathUnavailable(
                        f"source column {col!r} type {declared} is not COPY-text safe"
                    )
            shape = source_table_shape(
                src_cur, source_schema, source_table, source_cols
            )
            pk_map = mapped_single_pk(list(shape.primary_key or []), pairs)

            exists = _ss_table_exists(dst_cur, dst_schema, dest_table)
            existed_before = bool(exists)
            dest_occupied = False
            swap_at_end = False
            swap_schema = dst_schema
            swap_dest_ref = dest_ref
            swap_dest_table = dest_table
            if replace_destination and exists:
                # C03/X-OW2 — stage the overwrite, sp_rename swap inside the run
                # transaction: the old dest stays visible until COMMIT.
                from services.dest_overwrite_swap import (
                    overwrite_trash_name,
                    staged_overwrite_name,
                )

                stage_table = staged_overwrite_name(dest_table)
                swap_trash_table = overwrite_trash_name(dest_table)
                dest_ref = _ss_table_ref(swap_schema, stage_table)
                dest_table = stage_table
                swap_at_end = True
                exists = False
            if exists:
                dest_occupied = _ss_count(dst_cur, dest_ref) > 0
                reset_empty_dest_on_failure = not dest_occupied
                if dest_occupied and pk_map is None:
                    raise FastPathUnavailable(
                        "append into non-empty SQL Server dest stays on the row path"
                    )
            else:
                pk_dest = [
                    rename
                    for src_pk in shape.primary_key
                    for src_col, rename in pairs
                    if src_col.lower() == src_pk.lower()
                ]
                dst_cur.execute(
                    _ss_create_sql(
                        dest_ref, dest_table, pairs, sqlserver_ddls, pk_dest
                    )
                )
                settle_fast_path_create_on(
                    dst_cur,
                    dest_dialect="sqlserver",
                    dest_table=dest_table,
                    dest_schema=dst_schema,
                )
                dest_conn.commit()
                created_here = True

            src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
            source_count = int(src_cur.fetchone()[0])
            src_cur.execute("SELECT pg_export_snapshot()")
            snapshot_id = str(src_cur.fetchone()[0])
            workers = pg_mysql_copy_workers(source_count)
            n_parts = pg_mysql_copy_partitions(source_count, workers)
            select_list = ", ".join(
                _pg_copy_select_expr(col, live_l[col.lower()]) for col in source_cols
            )
            copy_sqls: list[str] = []
            partitions: list[dict[str, Any]] = []
            shard_mode = "serial"
            copy_split = "serial"

            if pk_map is not None:
                src_pk, dest_pk = pk_map
                src_ident = _quote(src_pk)
                shard_mode = "pk"
                preserve_dest_on_failure = True
                if n_parts <= 1:
                    key_ranges: list[tuple[Any | None, Any | None]] = [(None, None)]
                else:
                    pk_declared = live_l.get(src_pk.lower()) or ""
                    if _pg_base(pk_declared) in _INTEGER_PK_BASES:
                        cuts = fetch_integer_pk_cuts(
                            src_cur, source_ref, src_ident, n_parts
                        )
                    else:
                        cuts = fetch_pk_interior_cuts(
                            src_cur, source_ref, src_ident, n_parts
                        )
                    key_ranges = key_ranges_from_cuts(cuts)
                src_cur.execute(
                    f"SELECT COUNT(*) FROM {source_ref} WHERE {src_ident} IS NULL"  # nosec B608
                )
                nulls = int(src_cur.fetchone()[0])
                unbounded = (
                    len(key_ranges) == 1 and key_ranges[0] == (None, None)
                )
                plan: list[tuple[str, Any, Any, bool]] = []
                if nulls and not unbounded:
                    plan.append((f"{src_ident} IS NULL", None, None, True))
                for lo, hi in key_ranges:
                    lo_sql = (
                        _pg_quoted_literal(src_cur, lo) if lo is not None else None
                    )
                    hi_sql = (
                        _pg_quoted_literal(src_cur, hi) if hi is not None else None
                    )
                    pred = pk_range_predicate(src_ident, lo_sql, hi_sql)
                    plan.append((pred, lo, hi, False))
                for pred, lo, hi, is_null in plan:
                    if pred:
                        src_cur.execute(
                            f"SELECT COUNT(*) FROM {source_ref} WHERE {pred}"  # nosec B608
                        )
                    else:
                        src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
                    expected = int(src_cur.fetchone()[0])
                    partitions.append({
                        "lo": lo,
                        "hi": hi,
                        "null_shard": is_null,
                        "source_count": expected,
                        "dest_pk": dest_pk,
                        "predicate": pred,
                        "action": "load",
                    })
                accounted = sum(int(p["source_count"]) for p in partitions)
                if accounted != source_count:
                    raise ValueError(
                        "PK range source COUNTs "
                        f"{accounted} != snapshot {source_count}"
                    )
                dest_ident = _ss_ident(dest_pk)
                if dest_occupied:
                    copy_split = "pk"
                    dest_conn.commit()
                    for part in partitions:
                        already = _ss_range_count(
                            dst_cur, dest_ref, dest_ident, part
                        )
                        expected = int(part["source_count"])
                        # A count is not identity: skip only a range that holds this
                        # source range's keys (append of N new keys into N rows).
                        if already == expected and range_keys_match(
                            src_dialect="postgresql", src_cur=src_cur, src_ref=source_ref, src_ident=src_ident,
                            dst_dialect="sqlserver", dst_cur=dst_cur, dst_ref=dest_ref, dst_ident=dest_ident,
                            part=part,
                        ):
                            part["action"] = "skip"
                            part["dest_count"] = already
                        elif already == 0:
                            part["action"] = "load"
                        else:
                            occupied_pk_range_action(
                                already, expected, replace_destination=replace_destination,
                                keys_proven=False,
                            )
                            _ss_delete_range(
                                dst_cur, dest_ref, dest_ident, part
                            )
                            part["action"] = "reload"
                    dest_conn.commit()
                    copy_sqls = [
                        _copy_select_sql(
                            select_list, source_ref, str(p.get("predicate") or "")
                        )
                        for p in partitions
                        if p.get("action") in {"load", "reload"}
                    ]
                else:
                    copy_split = "serial"
                    copy_sqls = [_copy_select_sql(select_list, source_ref, "")]
            else:
                if dest_occupied:
                    raise FastPathUnavailable(
                        "append into non-empty SQL Server dest stays on the row path"
                    )
                copy_sqls = [_copy_select_sql(select_list, source_ref, "")]

            # Built after the overwrite decision: a staged overwrite loads the
            # staging table, never the live destination it later replaces.
            insert_sql = (
                f"INSERT INTO {dest_ref} WITH (TABLOCK) ({col_sql}) "  # nosec B608
                f"VALUES ({placeholders})"
            )
            identity = _has_identity(dst_cur, dst_schema, dest_table)
            if identity:
                dst_cur.execute(f"SET IDENTITY_INSERT {dest_ref} ON")  # nosec B608
            # Wide-decimal descriptors are declared ONLY on this write
            # cursor: pyodbc applies ``setinputsizes`` positionally to every
            # execute on the cursor, so declaring them on ``dst_cur``
            # poisons the OBJECT_ID probe / range counts with DECIMAL
            # descriptors (X-40: HY104 'Invalid precision value' live).
            write_cur = dest_conn.cursor()
            fast = _enable_fast_executemany(write_cur)
            set_wide_decimal_inputsizes(write_cur, sqlserver_ddls)
            try:
                for sql in copy_sqls:
                    _copy_into_sqlserver(
                        src_cur,
                        write_cur,
                        copy_sql=sql,
                        insert_sql=insert_sql,
                        converters=converters,
                    )
                    dest_conn.commit()
            finally:
                try:
                    write_cur.close()
                except Exception:
                    logger.debug("SQL Server write cursor close skipped", exc_info=True)
                if identity:
                    try:
                        dst_cur.execute(
                            f"SET IDENTITY_INSERT {dest_ref} OFF"  # nosec B608
                        )
                    except Exception:
                        logger.debug("IDENTITY_INSERT OFF skipped", exc_info=True)

            dest_count = _ss_count(dst_cur, dest_ref)
            if dest_count != source_count:
                raise ValueError(
                    "PG→SQL Server COPY refused: dest COUNT(*) "
                    f"{dest_count} != source snapshot {source_count}"
                )
            if shard_mode == "pk" and pk_map is not None:
                dest_ident = _ss_ident(pk_map[1])
                dest_conn.commit()
                for part in partitions:
                    dest_part = _ss_range_count(
                        dst_cur, dest_ref, dest_ident, part
                    )
                    part["dest_count"] = dest_part
                    if dest_part != int(part["source_count"]):
                        raise ValueError(
                            "PK range dest COUNT "
                            f"{dest_part} != source {part['source_count']} "
                            f"(lo={part['lo']!r} hi={part['hi']!r})"
                        )
            if swap_at_end:
                from services.dest_overwrite_swap import sqlserver_swap_staged_dest

                sqlserver_swap_staged_dest(
                    dst_cur,
                    schema=swap_schema,
                    dest_table=swap_dest_table,
                    staging_table=dest_table,
                    trash_table=swap_trash_table,
                    quote=_ss_ident,
                )
            dest_ref = swap_dest_ref
            dest_table = swap_dest_table
            dest_conn.commit()
            source_conn.commit()
            proof = f"dest_count:{dest_count}"
            partition_proof = [
                {
                    "lo": _jsonable_bound(p.get("lo")),
                    "hi": _jsonable_bound(p.get("hi")),
                    "null_shard": bool(p.get("null_shard")),
                    "source_count": int(p["source_count"]),
                    "dest_count": int(p.get("dest_count") or 0),
                    "action": str(p.get("action") or "load"),
                }
                for p in partitions
            ]
            skipped = sum(1 for p in partitions if p.get("action") == "skip")
            return FastPathResult(
                rows_copied=dest_count,
                source_rows=source_count,
                source_checksum=proof,
                target_rows=dest_count,
                target_checksum=proof,
                source_snapshot={
                    "pg_snapshot": snapshot_id,
                    "copy_workers": 1,
                    "copy_partitions": max(len(partitions), len(copy_sqls) or 1),
                    "partitions_skipped": skipped,
                    "partitions_loaded": len(copy_sqls),
                    "shard_mode": shard_mode,
                    "copy_split": copy_split,
                    "fast_executemany": fast,
                    "partition_proof": partition_proof,
                },
                proof_scope=(
                    "partition_dest_count_equals_source_snapshot"
                    if partition_proof
                    else "dest_count_equals_source_snapshot_count"
                ),
            )
        finally:
            try:
                src_cur.close()
            except Exception:
                logger.debug("pg source cursor close skipped", exc_info=True)
            try:
                dst_cur.close()
            except Exception:
                logger.debug("SQL Server dest cursor close skipped", exc_info=True)
    except Exception as exc:
        # A table this run created (or an existing dest that was empty before)
        # must end byte-identical to before the failure — never a partial
        # commit. PK-shard resume preserves committed rows only for a
        # pre-existing occupied dest.
        if created_here:
            try:
                with dest_conn.cursor() as cur:
                    cur.execute(_ss_drop_sql(dest_ref))
                dest_conn.commit()
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
        elif existed_before and reset_empty_dest_on_failure:
            try:
                with dest_conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("dest truncate after copy failure skipped", exc_info=True)
        elif preserve_dest_on_failure:
            raise
        # A residual driver failure declines to the row path — typed
        # finding per row, never raw ODBC text in the job error (X-40).
        _decline = driver_failure_decline(exc)
        if _decline is not None:
            raise _decline from exc
        raise
    finally:
        try:
            source_conn.close()
        except Exception:
            logger.debug("pg source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("SQL Server dest close skipped", exc_info=True)
