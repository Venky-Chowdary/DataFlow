"""Shared pg/mysql pair helpers — the connect and identifier wrappers the
pairwise copy modules used to each carry their own copy of.

One owner per concern (AGENTS/enterprise rule): ``mysql_connect`` and
``pg_connect`` both open write-mode connections with autocommit off so the
pair fast path can commit or roll back a batch range atomically; the ident
quoters delegate to the canonical ``quote_sql_identifier``.
"""

from __future__ import annotations

from typing import Any, Callable

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathUnavailable


def mysql_ident(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier

    return quote_sql_identifier(name, "`")


def pg_ident(name: str) -> str:
    from connectors.sql_identifiers import (
        quote_sql_identifier,
        require_safe_identifier,
    )

    return quote_sql_identifier(require_safe_identifier(name, preserve_case=True))


def mysql_write_conn(cfg: dict[str, Any]) -> Any:
    from connectors.mysql_conn import get_connection as mysql_connect

    conn = mysql_connect(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 3306),
        database=cfg.get("database", ""),
        username=cfg.get("username") or cfg.get("user") or "",
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
        purpose="write",
    )
    conn.autocommit = False
    return conn


def pg_write_conn(cfg: dict[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection as pg_connect

    conn = pg_connect(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 5432),
        database=cfg.get("database") or cfg.get("dbname") or "",
        username=cfg.get("username") or cfg.get("user") or "",
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
    )
    conn.autocommit = False
    return conn


# --- PK range-cut helpers (canonical owner; re-exported by copy_pg_mysql) ---


def key_ranges_from_cuts(cuts: list[Any]) -> list[tuple[Any | None, Any | None]]:
    """Equal-height ranges from interior cuts: ``[None, c0), [c0, c1), … [cN, None)``."""
    uniq: list[Any] = []
    for cut in cuts:
        if cut is None:
            continue
        if not uniq or uniq[-1] != cut:
            uniq.append(cut)
    if not uniq:
        return [(None, None)]
    ranges: list[tuple[Any | None, Any | None]] = [(None, uniq[0])]
    for i in range(len(uniq) - 1):
        ranges.append((uniq[i], uniq[i + 1]))
    ranges.append((uniq[-1], None))
    return ranges


def integer_pk_cuts(lo: int, hi: int, workers: int) -> list[int]:
    """Interior cuts on a closed integer interval ``[lo, hi]`` (Spark JDBC style)."""
    n = max(int(workers or 1), 1)
    if n <= 1:
        return []
    width = int(hi) - int(lo) + 1
    if width <= 1:
        return []
    n = min(n, width)
    cuts: list[int] = []
    for i in range(1, n):
        cut = int(lo) + (i * width) // n
        if cut <= int(lo) or cut > int(hi):
            continue
        if not cuts or cut != cuts[-1]:
            cuts.append(cut)
    return cuts


def pk_range_predicate(ident: str, lo: Any, hi: Any, *, null_shard: bool = False) -> str:
    if null_shard:
        return f"{ident} IS NULL"
    parts: list[str] = []
    if lo is not None:
        parts.append(f"{ident} >= {lo}")
    if hi is not None:
        parts.append(f"{ident} < {hi}")
    return " AND ".join(parts)


def mysql_pk_range_clause(
    ident: str, lo: Any, hi: Any, *, null_shard: bool = False
) -> tuple[str, list[Any]]:
    if null_shard:
        return f"{ident} IS NULL", []
    parts: list[str] = []
    params: list[Any] = []
    if lo is not None:
        parts.append(f"{ident} >= %s")
        params.append(lo)
    if hi is not None:
        parts.append(f"{ident} < %s")
        params.append(hi)
    if not parts:
        return "1=1", []
    return " AND ".join(parts), params


def plan_pk_range_partitions(
    src_cur: Any,
    source_ref: str,
    src_ident: str,
    cuts: list[Any],
    source_count: int,
    render_predicate: Callable[[Any, Any], str],
) -> list[dict[str, Any]]:
    """PK range-partition plan shared by the pg↔mysql bulk paths.

    ``cuts`` are interior cut values already sampled by the caller's dialect
    helper (empty when the table does not parallelise).  The plan is a
    leading ``IS NULL`` shard when the key column holds NULLs, then one
    partition per range from ``key_ranges_from_cuts``.  ``render_predicate``
    turns ``(lo, hi)`` bounds into SQL text for the source dialect; an empty
    or ``1=1`` predicate renders the unbounded partition.  Each partition's
    ``source_count`` is the real source COUNT(*) for its predicate and the
    counts must re-add to the caller's ``source_count`` snapshot — a
    mismatch means the table changed under the plan, so we fail closed.
    """
    key_ranges = key_ranges_from_cuts(cuts or [])
    src_cur.execute(
        f"SELECT COUNT(*) FROM {source_ref} "  # nosec B608
        f"WHERE {src_ident} IS NULL"
    )
    nulls = int(src_cur.fetchone()[0])
    unbounded = len(key_ranges) == 1 and key_ranges[0] == (None, None)
    plan: list[tuple[str, Any, Any, bool]] = []
    if nulls and not unbounded:
        plan.append((f"{src_ident} IS NULL", None, None, True))
    for lo, hi in key_ranges:
        plan.append((render_predicate(lo, hi), lo, hi, False))
    partitions: list[dict[str, Any]] = []
    for pred, lo, hi, is_null in plan:
        if pred and pred != "1=1":
            src_cur.execute(
                f"SELECT COUNT(*) FROM {source_ref} WHERE {pred}"  # nosec B608
            )
        else:
            src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
            pred = ""
        partitions.append({
            "lo": lo,
            "hi": hi,
            "null_shard": is_null,
            "source_count": int(src_cur.fetchone()[0]),
            "predicate": pred,
            "action": "load",
        })
    accounted = sum(int(p["source_count"]) for p in partitions)
    if accounted != source_count:
        raise ValueError(
            f"PK range source COUNTs {accounted} != snapshot {source_count}"
        )
    return partitions


# --- SQL Server write machinery (canonical owner; re-exported by
# --- copy_pg_sqlserver and the sqlserver-dest pair modules) ---

from datetime import date, datetime  # noqa: E402
from decimal import Decimal, InvalidOperation  # noqa: E402

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

