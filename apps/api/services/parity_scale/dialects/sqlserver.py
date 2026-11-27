"""SQL Server 2019+ dialect: CE1 built as NVARCHAR, hashed as UTF-8 with HASHBYTES.

Every text is kept as ``NVARCHAR`` (UTF-16, no code-page loss) while the row
is assembled, then converted once to UTF-8 through the
``Latin1_General_100_BIN2_UTF8`` collation (SQL Server 2019+) and hashed with
``HASHBYTES('SHA2_256', …)``. Lengths in CE1 are UTF-8 byte lengths
(``DATALENGTH`` of the UTF-8 form). Sums are ``DECIMAL(38,0)``.

SQL Server has no row-value comparison, so key ranges use the lexicographic
OR/AND expansion (``services.keyset_pagination``), and ``(a, b) IN (…)`` becomes
an OR of ANDs (2,100 parameters per statement bound the fetch page).

``datetime2(7)`` keeps a 100 ns tick: a timestamp is rendered from
``DATEDIFF_BIG`` in days plus nanoseconds within the day (no overflow
anywhere in 0001–9999), at the comparison precision; the legacy ``datetime``
and ``smalldatetime`` carriers and ``time`` are planned as ``stream``.
Tested only in CI (``parity-ce-matrix``): there is no local SQL Server.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator, Sequence
from typing import Any

from services.parity_scale.dialects.sqlbase import (
    PG_EPOCH_US,
    SqlSide,
    fit_field,
    fit_held_predicate,
    timeout_error,
)
from services.parity_scale.plan import ColumnPlan

UTF8 = "Latin1_General_100_BIN2_UTF8"
_EPOCH = "CAST('1970-01-01' AS datetime2(7))"


def quote_ident(name: str) -> str:
    return "[" + str(name).replace("]", "]]") + "]"


def _nv(x: str) -> str:
    return f"CONVERT(nvarchar(max), {x})"


def _u8(text: str) -> str:
    return f"CONVERT(varchar(max), ({text}) COLLATE {UTF8})"


def _inline(text_sql: str) -> str:
    return f"(N'V' + CONVERT(nvarchar(20), DATALENGTH({_u8(text_sql)})) + N':' + {text_sql})"


def _ascii_inline(text_sql: str) -> str:
    """Inline field of text known to be ASCII (numbers, tokens): length = LEN."""
    return f"(N'V' + CONVERT(nvarchar(20), LEN({text_sql})) + N':' + {text_sql})"


def sha256_hex(text: str) -> str:
    return f"LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', CONVERT(varbinary(max), {_u8(text)})), 2))"


def _tmod(a: str, b: int | str) -> str:
    """Truncated remainder without ``%`` (the DB-API parameter marker)."""
    return f"(({a}) - (({a}) / ({b})) * ({b}))"


def _floor_div(a: str, b: int) -> str:
    """Floor division (T-SQL ``/`` truncates toward zero)."""
    return f"((({a}) - {_tmod(f'{_tmod(a, b)} + {b}', b)}) / {b})"


def _t100(x: str) -> str:
    """100 ns units since 1970-01-01 of a datetime2 expression, without overflow."""
    return (
        f"(DATEDIFF_BIG(day, {_EPOCH}, {x}) * CAST(864000000000 AS bigint) + "
        f"DATEDIFF_BIG(nanosecond, CAST(CAST({x} AS date) AS datetime2(7)), CAST({x} AS datetime2(7))) / 100)"
    )


def units_sql(t100: str, digits: int, *, project: bool, pg_axis: bool) -> str:
    if digits >= 7:
        return _ascii_inline(f"CONVERT(nvarchar(30), {t100})")
    u = _floor_div(t100, 10)  # the 100 ns tick dropped, as carrier_instant drops it
    step = 10 ** (6 - digits)
    if project:
        if pg_axis:
            # bigint literals: a bare 946684800000000 is NUMERIC(15,0) in T-SQL,
            # and NUMERIC division keeps a fraction.
            v = f"(({u}) - CAST({PG_EPOCH_US} AS bigint))"
            rounded = (
                f"(CASE WHEN {v} >= 0 THEN {_floor_div(f'{v} + {step // 2}', step)} "
                f"ELSE -{_floor_div(f'-{v} + {step // 2}', step)} END + CAST({PG_EPOCH_US // step} AS bigint))"
            )
        else:
            rounded = _floor_div(f"{u} + {step // 2}", step)
        return _ascii_inline(f"CONVERT(nvarchar(30), {rounded})")
    exact = f"{_tmod(t100, 10 * step)} = 0"
    finer = f"(N'Xus:' + CONVERT(nvarchar(20), LEN(CONVERT(nvarchar(30), {u}))) + N':' + CONVERT(nvarchar(30), {u}))"
    return (
        f"(CASE WHEN {exact} THEN {_ascii_inline(f'CONVERT(nvarchar(30), {_floor_div(t100, 10 * step)})')} "
        f"ELSE {finer} END)"
    )


def _strip_decimal(c: str) -> str:
    # ``%%`` survives DB-API formatting as ``%``; unformatted, ``%%`` is still
    # "any characters" in a LIKE pattern, so the pattern means the same either way.
    trimmed = f"LEFT({c}, LEN({c}) - PATINDEX(N'%%[^0]%%', REVERSE({c})) + 1)"
    no_dot = f"(CASE WHEN RIGHT({trimmed}, 1) = N'.' THEN LEFT({trimmed}, LEN({trimmed}) - 1) ELSE {trimmed} END)"
    stripped = f"(CASE WHEN CHARINDEX(N'.', {c}) > 0 THEN {no_dot} ELSE {c} END)"
    return f"(CASE WHEN {stripped} IN (N'-0', N'') THEN N'0' ELSE {stripped} END)"


def decimal_classes(x: str, fit: dict[str, Any]) -> list[tuple[str, str]]:
    s = int(fit["scale"])
    max_int = max(int(fit["precision"]) - s, 0)
    limit = "1" + "0" * max_int
    scale_over = f"{x} <> ROUND({x}, {s}, 1)"
    int_over = f"NOT (ABS(ROUND({x}, 0, 1)) < {limit} AND ({x} <> 0 OR {max_int} >= 1))"
    return [("both", f"({scale_over}) AND ({int_over})"), ("scale", scale_over), ("int", int_over)]


def field_sql(col: ColumnPlan, *, column: str, role: str, dest_engine: str) -> str:
    x = quote_ident(column)
    kind = col.kind
    source = role == "source"
    pg_axis = str(dest_engine or "").lower() in {"postgresql", "postgres", "pg", "timescaledb", "alloydb",
                                                 "supabase", "pgvector"}
    if kind == "integer":
        body = _ascii_inline(f"CONVERT(nvarchar(40), {x})")
    elif kind == "boolean":
        body = f"(CASE WHEN {x} = 1 THEN N'V1:1' ELSE N'V1:0' END)"
    elif kind == "decimal":
        body = _ascii_inline(_strip_decimal(f"CONVERT(nvarchar(60), {x})"))
    elif kind in {"text", "char"}:
        t = _nv(x)
        if kind == "char":
            t = f"RTRIM({t})"
        body = f"(N'D' + {sha256_hex(t)})" if col.width == "digest" else _inline(t)
        if col.empty_is_null:
            body = f"(CASE WHEN DATALENGTH({x}) = 0 THEN N'N' ELSE {body} END)"
    elif kind == "date":
        body = _ascii_inline(f"CONVERT(nvarchar(20), DATEDIFF(day, CAST('1970-01-01' AS date), {x}))")
    elif kind == "timestamp":
        body = units_sql(_t100(x), col.digits, project=source, pg_axis=pg_axis)
    elif kind == "timestamptz":
        utc = f"CAST(SWITCHOFFSET({x}, '+00:00') AS datetime2(7))"
        body = units_sql(_t100(utc), col.digits, project=source, pg_axis=pg_axis)
    elif kind == "uuid":
        body = _ascii_inline(f"LOWER(CONVERT(nvarchar(36), {x}))")
    elif kind == "binary":
        body = f"(N'D' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', {x}), 2)))"
    elif kind == "native":
        # Same-engine floats only: style 3 is the lossless 17-digit rendering.
        body = _ascii_inline(f"CONVERT(nvarchar(40), {x}, 3)")
    else:  # pragma: no cover - the plan only emits known kinds
        raise ValueError(f"no SQL Server rendering for CE1 kind {kind!r}")
    if source and col.decimal_fit:
        return fit_field(x, body, col.decimal_fit, decimal_classes)
    return f"(CASE WHEN {x} IS NULL THEN N'N' ELSE {body} END)"


class SqlServerSide(SqlSide):
    """A SQL Server 2019+ table (pymssql DB-API connection)."""

    fetch_page = 2000

    def prepare_session(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int)")
            major = int(cur.fetchone()[0] or 0)
            if major and major < 15:
                raise ValueError("Parity at Scale needs SQL Server 2019+ (UTF-8 collations)")
            cur.execute("SET NOCOUNT ON")
        self.conn.commit()
        self.server = "sqlserver"

    def quote(self, name: str) -> str:
        return quote_ident(name)

    @property
    def table_ref(self) -> str:
        return f"{quote_ident(self.schema or 'dbo')}.{quote_ident(self.table)}"

    def row_values(self) -> bool:
        return False

    def field(self, col: ColumnPlan, column: str) -> str:
        return field_sql(col, column=column, role=self.role, dest_engine=self.plan.dest_engine)

    def concat(self, parts: Sequence[str]) -> str:
        return "(" + " + ".join(parts) + ")"

    def sha256_hex(self, text: str) -> str:
        return sha256_hex(text)

    def utf8_hex(self, text: str) -> str:
        return f"LOWER(CONVERT(varchar(max), CONVERT(varbinary(max), {_u8(text)}), 2))"

    def word(self, h: str, which: str) -> str:
        start = 2 if which == "a" else 18
        return f"CONVERT(bigint, CONVERT(varbinary(8), '0' + SUBSTRING({h}, {start}, 15), 2))"

    def hex4_int(self, hex4: str) -> str:
        return f"CONVERT(int, CONVERT(varbinary(2), {hex4}, 2))"

    def substr(self, text: str, start: int, length: int) -> str:
        return f"SUBSTRING({text}, {start}, {length})"

    def exact_sum(self, expr: str) -> str:
        return f"SUM(CAST({expr} AS DECIMAL(38,0)))"

    def idiv(self, a: str, b: int) -> str:
        return f"(({a}) / {int(b)})"

    def mod(self, a: str, b: str) -> str:
        # ``%`` would collide with the DB-API parameter marker; operands are >= 0.
        return _tmod(a, b)

    def limit(self, sql_without_limit: str, n: int) -> tuple[str, list[Any]]:
        assert sql_without_limit.startswith("SELECT ")
        return f"SELECT TOP ({int(n)}) " + sql_without_limit[len("SELECT "):], []

    def held_predicate(self) -> str:
        return fit_held_predicate(self, decimal_classes)

    def estimate_rows(self) -> int | None:
        rows = self.execute(
            "SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = OBJECT_ID(%s) AND p.index_id IN (0, 1)",
            [f"{quote_ident(self.schema or 'dbo')}.{quote_ident(self.table)}"])
        return int(rows[0][0]) if rows and rows[0][0] is not None else None

    def sample_keys(self, fraction: float, limit: int) -> list[tuple[Any, ...]] | None:
        keys = ", ".join(self.quoted_keys())
        pct = max(0.0001, min(100.0, fraction * 100.0))
        sql = (f"SELECT TOP ({int(limit)}) {keys} FROM {self.table_ref} "
               f"TABLESAMPLE SYSTEM ({pct:.6f} PERCENT) REPEATABLE (42) ORDER BY {keys}")
        return [tuple(r) for r in self.execute(sql, [])]

    def execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        inner = getattr(self.conn, "_conn", None)
        if self.timeout_seconds and inner is not None and hasattr(inner, "query_timeout"):
            try:
                inner.query_timeout = max(1, int(round(self.timeout_seconds)))
            except (AttributeError, TypeError, ValueError):
                pass
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, tuple(params)) if params else cur.execute(sql)
                rows = list(cur.fetchall())
            self.conn.commit()
            return rows
        except Exception as exc:
            with contextlib.suppress(Exception):  # the connection may be gone after a timeout
                self.conn.rollback()
            text = str(exc).lower()
            if "timeout" in text or "timed out" in text:
                raise timeout_error(exc) from exc
            raise

    def stream(self, sql: str, params: Sequence[Any], batch: int) -> Iterator[tuple[Any, ...]]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql, tuple(params)) if params else cur.execute(sql)
            while True:
                rows = cur.fetchmany(batch)
                if not rows:
                    break
                yield from rows
        finally:
            cur.close()
            self.conn.commit()
