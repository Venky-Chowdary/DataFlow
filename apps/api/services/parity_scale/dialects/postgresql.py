"""PostgreSQL dialect: CE1 rendered in SQL, hashed with ``sha256()``, summed as ``numeric``.

Every rule below is the SQL spelling of a rule in :mod:`services.parity_scale.ce1`
and is checked against the Python reference value by value on a live engine
(``tests/test_parity_scale_pg_live.py``). Requires PostgreSQL 14+ (exact
``numeric`` from ``extract(epoch …)``, ``trim_scale``, ``sha256``).

This module also owns the legacy whole-table digest of
:mod:`services.engine_checksum` (md5 of ``::text`` renderings, which Gate-8 uses
today when ``DATAFLOW_ENGINE_DIGEST=1``). It moved here unchanged so the
PostgreSQL rendering rules live in one place; Gate-8 keeps calling it until
phase 3 switches that path to the CE1 digest.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator, Sequence
from typing import Any

from services.parity_scale.dialects.sqlbase import SqlSide, fit_field, fit_held_predicate, timeout_error
from services.parity_scale.plan import ColumnPlan

#: Session settings every rendering depends on (``native`` ``::text`` above all).
SESSION_SETTINGS: tuple[str, ...] = (
    "SET TIME ZONE 'UTC'",
    "SET extra_float_digits = 1",
    "SET DateStyle = 'ISO, YMD'",
    "SET IntervalStyle = 'postgres'",
    "SET bytea_output = 'hex'",
)

#: The snapshot a pinned side reads, by function (PostgreSQL 13+ / 10 to 12).
_SNAPSHOT_SQL = {
    "pg_current_snapshot": "SELECT pg_current_snapshot()::text, now()::text",
    "txid_current_snapshot": "SELECT txid_current_snapshot()::text, now()::text",
}


def quote_ident(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    return quote_sql_identifier(require_safe_identifier(name, preserve_case=True))


# ── legacy whole-table digest (engine_checksum) ─────────────────────────────


def legacy_md5_checksum_sql(table_ref: str, columns: list[str], *, where: str = "") -> str:
    """``SELECT count, digest`` over every row of ``table_ref`` (pre-CE1 rule).

    Each row renders to one text value, is hashed with md5, and the leading 64
    bits are summed as ``numeric``. Summing rather than aggregating in order is
    what makes the result independent of scan order; ``numeric`` rather than
    ``bigint`` is what keeps a wide table from overflowing the accumulator.
    """
    fields = []
    for col in columns:
        quoted = quote_ident(col)
        # The IS NULL flag rides inside the hashed text so that NULL and the
        # empty string cannot digest alike — the same distinction the Python
        # fingerprint path draws, and one that has already been a real bug here.
        fields.append(
            f"md5(coalesce({quoted}::text, '') || ({quoted} IS NULL)::text)"
        )
    row_text = " || ".join(fields)
    where_sql = f" WHERE {where}" if (where or "").strip() else ""
    return (
        "SELECT count(*)::bigint AS n, "
        f"coalesce(sum(('x' || substr(md5({row_text}), 1, 15))::bit(60)::bigint::numeric), 0) AS digest "
        f"FROM {table_ref}{where_sql}"  # nosec B608 — identifiers quoted above
    )


# ── CE1 in SQL ──────────────────────────────────────────────────────────────


def _inline(text_sql: str) -> str:
    # The database encoding is checked to be UTF8, so octet_length is the
    # UTF-8 byte length CE1 prefixes.
    return f"('V' || octet_length({text_sql})::text || ':' || {text_sql})"


def _digest(text_sql: str) -> str:
    return f"('D' || encode(sha256(convert_to({text_sql}, 'UTF8')), 'hex'))"


def _foreign(cls: str, text_sql: str) -> str:
    return f"('X{cls}:' || octet_length({text_sql})::text || ':' || {text_sql})"


#: 2000-01-01 00:00 UTC in microseconds since 1970: PostgreSQL's timestamp epoch.
_PG_EPOCH_US = 946_684_800_000_000


def _units(u_sql: str, digits: int, *, project: bool, clock: bool = False, pg_axis: bool = False) -> str:
    """Epoch (or since-midnight) units at ``digits``; ``u_sql`` is exact microseconds."""
    if digits >= 7:
        # A 100 ns carrier (SQL Server datetime2(7)): microseconds hold no tick.
        return _inline(f"(({u_sql}) * 10)::text")
    if digits >= 6:
        return _inline(f"({u_sql})::text")
    step = 10 ** (6 - digits)
    if project:
        rounded = f"floor(({u_sql} + {step // 2}) / {step})"
        if pg_axis:
            # The destination is PostgreSQL: it rounds half away from zero on
            # its own axis (microseconds since 2000-01-01), so an instant before
            # 2000 rounds half down on the timeline (carrier_instant models it).
            v = f"({u_sql} - {_PG_EPOCH_US})"
            rounded = (
                f"(CASE WHEN {v} >= 0 THEN floor(({v} + {step // 2}) / {step}) "
                f"ELSE -floor((-{v} + {step // 2}) / {step}) END + {_PG_EPOCH_US // step})"
            )
        if clock:
            # 23:59:59.9… cannot carry into the next day inside a TIME column:
            # carrier_instant keeps the second it can store.
            rounded = (
                f"(CASE WHEN floor(({u_sql} + {step // 2}) / {step}) * {step} >= 86400000000 "
                f"THEN floor({u_sql} / 1000000) * {1_000_000 // step} "
                f"ELSE floor(({u_sql} + {step // 2}) / {step}) END)"
            )
        return _inline(f"({rounded})::text")
    return (
        f"(CASE WHEN mod({u_sql}, {step}) = 0 THEN {_inline(f'div({u_sql}, {step})::text')} "
        f"ELSE {_foreign('us', f'({u_sql})::text')} END)"
    )


def field_sql(
    col: ColumnPlan, *, column: str, role: str, column_type: str, dest_engine: str = "postgresql"
) -> str:
    """The CE1 field of one column as a SQL ``text`` expression."""
    x = quote_ident(column)
    kind = col.kind
    source = role == "source"
    dest_is_pg = str(dest_engine or "postgresql").lower() in {
        "postgresql", "postgres", "pg", "timescaledb", "alloydb", "supabase", "pgvector"}
    if kind == "integer":
        body = _inline(f"{x}::text")
    elif kind == "boolean":
        body = _inline(f"(CASE WHEN {x} THEN '1' ELSE '0' END)")
    elif kind == "decimal":
        body = _inline(f"trim_scale({x})::text")
    elif kind in {"text", "char"}:
        text = f"rtrim({x}::text, ' ')" if kind == "char" else f"{x}::text"
        body = _digest(text) if col.width == "digest" else _inline(text)
        if col.empty_is_null:
            body = f"(CASE WHEN octet_length({x}::text) = 0 THEN 'N' ELSE {body} END)"
    elif kind == "date":
        body = (
            f"(CASE WHEN isfinite({x}) THEN {_inline(f'({x} - DATE {chr(39)}1970-01-01{chr(39)})::text')} "
            f"WHEN {x} > DATE '1970-01-01' THEN 'V4:+inf' ELSE 'V4:-inf' END)"
        )
    elif kind in {"timestamp", "timestamptz"}:
        u = f"trunc(extract(epoch FROM {x}) * 1000000)"
        body = (
            f"(CASE WHEN NOT isfinite({x}) THEN (CASE WHEN {x} > 'epoch' THEN 'V4:+inf' ELSE 'V4:-inf' END) "
            f"ELSE {_units(u, col.digits, project=source, pg_axis=dest_is_pg)} END)"
        )
    elif kind == "time":
        u = f"trunc(extract(epoch FROM {x}) * 1000000)"
        body = _units(u, col.digits, project=source, clock=True)
    elif kind == "uuid":
        body = _inline(f"{x}::text")
    elif kind == "binary":
        body = f"('D' || encode(sha256({x}), 'hex'))"
    elif kind == "native":
        body = _inline(f"{x}::text")
    else:  # pragma: no cover - the plan only emits known kinds
        raise ValueError(f"no PostgreSQL rendering for CE1 kind {kind!r}")
    if source and col.decimal_fit:
        return fit_field(x, body, col.decimal_fit, decimal_fit_classes)
    return f"(CASE WHEN {x} IS NULL THEN 'N' ELSE {body} END)"


def decimal_fit_classes(x: str, fit: dict[str, Any]) -> list[tuple[str, str]]:
    """``(class, SQL condition)`` in evaluation order, replaying the writer's fit test.

    ``fits_decimal`` strips trailing zeros, refuses any significant digit past
    the scale and counts integer digits of the *normalised* value — zero counts
    one integer digit, so ``0`` does not fit ``NUMERIC(p,p)`` there, and does
    not here either. Conditions are tried in order (``CASE`` short-circuits),
    so ``trunc`` never sees ``NaN`` or an infinity.
    """
    s = int(fit["scale"])
    max_int = max(int(fit["precision"]) - s, 0)
    scale_over = f"{x} <> trunc({x}, {s})"
    int_over = f"NOT (abs(trunc({x})) < 1e{max_int} AND ({x} <> 0 OR {max_int} >= 1))"
    return [
        ("nan", f"{x} = 'NaN'"),
        ("inf", f"{x} IN ('Infinity'::numeric, '-Infinity'::numeric)"),
        ("both", f"({scale_over}) AND ({int_over})"),
        ("scale", scale_over),
        ("int", int_over),
    ]


class PostgresSide(SqlSide):
    """One PostgreSQL table. Read-only: every statement runs in a read-only transaction."""

    #: Keeps PostgreSQL from inlining the derived table (one sha256 per row).
    materialize = " OFFSET 0"

    def __init__(self, *, schema: str = "public", **kwargs: Any) -> None:
        super().__init__(schema=schema or "public", **kwargs)

    def prepare_session(self) -> None:
        self.conn.autocommit = False
        with self.conn.cursor() as cur:
            cur.execute("SHOW server_encoding")
            encoding = str(cur.fetchone()[0]).upper()
            cur.execute("SHOW server_version_num")
            version = int(cur.fetchone()[0])
        self.conn.rollback()
        if encoding not in {"UTF8", "UNICODE"}:
            raise ValueError(f"Parity at Scale needs a UTF8 database (this one is {encoding})")
        if version < 140000:
            raise ValueError("Parity at Scale needs PostgreSQL 14 or later (exact extract(epoch))")
        self.server_version_num = version
        self.server = "postgresql"
        self.conn.autocommit = True
        with self.conn.cursor() as cur:
            for stmt in SESSION_SETTINGS:
                cur.execute(stmt)
        self.conn.autocommit = False

    def quote(self, name: str) -> str:
        return quote_ident(name)

    @property
    def table_ref(self) -> str:
        from connectors.sql_identifiers import quote_table_ref

        return quote_table_ref(self.table, self.schema, dialect="postgresql")

    def field(self, col: ColumnPlan, column: str) -> str:
        return field_sql(col, column=column, role=self.role, column_type="", dest_engine=self.plan.dest_engine)

    def concat(self, parts: Sequence[str]) -> str:
        return "(" + " || ".join(parts) + ")"

    def sha256_hex(self, text: str) -> str:
        return f"encode(sha256(convert_to({text}, 'UTF8')), 'hex')"

    def utf8_hex(self, text: str) -> str:
        return f"encode(convert_to({text}, 'UTF8'), 'hex')"

    def word(self, h: str, which: str) -> str:
        start = 2 if which == "a" else 18
        return f"('x' || substr({h}, {start}, 15))::bit(60)::bigint"

    def hex4_int(self, hex4: str) -> str:
        return f"('x' || {hex4})::bit(16)::int"

    def substr(self, text: str, start: int, length: int) -> str:
        return f"substr({text}, {start}, {length})"

    def exact_sum(self, expr: str) -> str:
        return f"sum({expr})"

    def idiv(self, a: str, b: int) -> str:
        return f"(({a}) / {int(b)})"

    def mod(self, a: str, b: str) -> str:
        return f"mod({a}, {b})"

    def held_predicate(self) -> str:
        return fit_held_predicate(self, decimal_fit_classes)

    def pin(self, max_hold_s: float | None = None) -> dict[str, Any]:
        """One REPEATABLE READ, READ ONLY transaction for the rest of the scan (one snapshot).

        Each statement then runs under a savepoint, so a statement that times
        out is rolled back to it and the snapshot is kept.
        """
        # pg_current_snapshot() is PostgreSQL 13+; txid_current_snapshot() serves 10 to 12.
        function = "pg_current_snapshot" if int(getattr(self, "server_version_num", 0) or 0) >= 130000 \
            else "txid_current_snapshot"
        refusal = self._pin_refusal(max_hold_s)
        if refusal:
            return {"kind": "not_established", "reason": refusal}
        try:
            self.conn.rollback()
            self.conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with self.conn.cursor() as cur:
                cur.execute(_SNAPSHOT_SQL[function])
                snapshot, at = cur.fetchone()
        except Exception as exc:  # noqa: BLE001 — an engine that cannot pin is read statement by statement
            self.unpin()
            return {"kind": "not_established", "reason": f"the snapshot could not be taken ({type(exc).__name__})"}
        self.pinned = True
        return {"kind": "pinned", "method": "repeatable_read_transaction", "function": function,
                "snapshot": str(snapshot), "at": str(at)}

    def _pin_refusal(self, max_hold_s: float | None) -> str:
        """Why a held snapshot would be unsafe or cut short on this server, or ``""``."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery()")
                standby = bool(cur.fetchone()[0])
                cur.execute("SELECT name, setting FROM pg_settings WHERE name IN "
                            "('idle_in_transaction_session_timeout', 'transaction_timeout')")
                limits = {str(n): int(v or 0) for n, v in cur.fetchall()}
            self.conn.rollback()
        except Exception as exc:  # noqa: BLE001 — cannot tell: do not hold a snapshot
            self.unpin()
            return f"the server's settings could not be read ({type(exc).__name__})"
        if standby:
            return ("the server is a hot standby: a snapshot held there can be cancelled by replication "
                    "(a recovery conflict)")
        hold_ms = int((max_hold_s or 0) * 1000)
        for name, limit_ms in sorted(limits.items()):
            if limit_ms > 0 and (not hold_ms or limit_ms < hold_ms):
                return (f"the server's {name} ({limit_ms // 1000} s) is shorter than the longest hold; "
                        "the snapshot would be cut off")
        return ""

    def unpin(self) -> None:
        """Back to one transaction per statement (default isolation), whatever state the connection is in."""
        self.pinned = False
        with contextlib.suppress(Exception):
            self.conn.rollback()
        with contextlib.suppress(Exception):
            self.conn.set_session(isolation_level="DEFAULT", readonly="DEFAULT")

    def _execute_pinned(self, sql: str, params: Sequence[Any]) -> list[tuple[Any, ...]]:
        import psycopg2

        with self.conn.cursor() as cur:
            cur.execute("SAVEPOINT psd1_statement")
        try:
            with self.conn.cursor() as cur:
                if self.timeout_seconds:
                    cur.execute("SET LOCAL statement_timeout = %s", (int(self.timeout_seconds * 1000),))
                cur.execute(sql, list(params))
                rows = cur.fetchall()
                cur.execute("RELEASE SAVEPOINT psd1_statement")
            return rows
        except Exception as exc:
            with self.conn.cursor() as cur:
                cur.execute("ROLLBACK TO SAVEPOINT psd1_statement")
            if isinstance(exc, psycopg2.errors.QueryCanceled):
                raise timeout_error(exc) from exc
            raise

    def execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        import psycopg2

        self.tick()
        if self.pinned:
            return self._execute_pinned(sql, params)
        try:
            with self.conn.cursor() as cur:
                cur.execute("SET TRANSACTION READ ONLY")
                if self.timeout_seconds:
                    cur.execute("SET LOCAL statement_timeout = %s", (int(self.timeout_seconds * 1000),))
                cur.execute(sql, list(params))
                rows = cur.fetchall()
            self.conn.commit()
            return rows
        except psycopg2.errors.QueryCanceled as exc:
            self.conn.rollback()
            raise timeout_error(exc) from exc
        except Exception:
            self.conn.rollback()
            raise

    # Kept for callers (tests) of the phase-1 name.
    _run = execute

    def estimate_rows(self) -> int | None:
        rows = self.execute("SELECT reltuples::bigint FROM pg_class WHERE oid = %s::regclass", [self.table_ref])
        value = int(rows[0][0]) if rows and rows[0][0] is not None else -1
        return value if value >= 0 else None

    def sample_keys(self, fraction: float, limit: int) -> list[tuple[Any, ...]] | None:
        keys = ", ".join(self.quoted_keys())
        pct = max(0.0001, min(100.0, fraction * 100.0))
        sql = (f"SELECT {keys} FROM {self.table_ref} TABLESAMPLE SYSTEM ({pct:.6f}) REPEATABLE (42) "
               f"ORDER BY {keys} LIMIT %s")
        return [tuple(r) for r in self.execute(sql, [int(limit)])]

    def stream(self, sql: str, params: Sequence[Any], batch: int) -> Iterator[tuple[Any, ...]]:
        import uuid as _uuid

        self.tick()
        with self.conn.cursor() as plain:
            if self.pinned:
                # A savepoint keeps the pinned snapshot usable when the stream fails part way.
                plain.execute("SAVEPOINT psd1_stream")
            if self.timeout_seconds:
                # Every FETCH is its own statement under this server-side timeout.
                plain.execute("SET LOCAL statement_timeout = %s", (int(self.timeout_seconds * 1000),))
        cur = self.conn.cursor(name=f"psd1_stream_{_uuid.uuid4().hex[:12]}")
        cur.itersize = batch
        failed = False
        try:
            cur.execute(sql, list(params))
            while True:
                rows = cur.fetchmany(batch)
                self.tick()
                if not rows:
                    break
                yield from rows
        except BaseException:
            failed = True
            raise
        finally:
            if self.pinned:
                with contextlib.suppress(Exception):
                    cur.close()
                with self.conn.cursor() as plain:
                    plain.execute("ROLLBACK TO SAVEPOINT psd1_stream" if failed else "RELEASE SAVEPOINT psd1_stream")
            else:
                cur.close()
                self.conn.rollback()
