"""MySQL 8 and MariaDB dialect: CE1 in SQL, ``SHA2(…, 256)``, exact ``SUM`` as DECIMAL.

One dialect serves both servers; the side reads ``VERSION()`` and records
which it talked to, because MariaDB is certified as its own leg (owner
decision 3). Differences handled here: the statement timeout
(``max_execution_time`` in ms on MySQL, ``max_statement_time`` in s on
MariaDB) and its error codes.

Every rule is the SQL spelling of :mod:`services.parity_scale.ce1` and is
checked against the Python reference value by value on both servers
(``tests/test_parity_ce_matrix.py``). Kinds with no portable rendering here
(``TIME``, which can exceed 24 h and reads back as an interval; ``BIT``; MySQL
floats and JSON across engines) are planned as ``stream``.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

from services.parity_scale.dialects.base import UnitTimeout
from services.parity_scale.dialects.sqlbase import (
    PG_EPOCH_US,
    SqlSide,
    fit_field,
    fit_held_predicate,
    timeout_error,
)
from services.parity_scale.plan import ColumnPlan

_TIMEOUT_CODES = {3024, 1969, 1317, 1028}


def quote_ident(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _text(x: str) -> str:
    return f"CONVERT({x} USING utf8mb4)"


def _inline(text_sql: str) -> str:
    return f"CONCAT('V', LENGTH({text_sql}), ':', {text_sql})"


def _foreign(cls: str, text_sql: str) -> str:
    return f"CONCAT('X{cls}:', LENGTH({text_sql}), ':', {text_sql})"


def units_sql(u: str, digits: int, *, project: bool, pg_axis: bool, clock: bool = False) -> str:
    """Epoch units at ``digits`` from exact microseconds ``u`` (see ce1)."""
    if digits >= 7:
        return _inline(f"CAST(({u}) * 10 AS CHAR)")
    if digits == 6:
        return _inline(f"CAST({u} AS CHAR)")
    step = 10 ** (6 - digits)
    if project:
        rounded = f"FLOOR(({u} + {step // 2}) / {step})"
        if pg_axis:
            v = f"(({u}) - {PG_EPOCH_US})"
            rounded = (
                f"(CASE WHEN {v} >= 0 THEN FLOOR(({v} + {step // 2}) / {step}) "
                f"ELSE -FLOOR((-{v} + {step // 2}) / {step}) END + {PG_EPOCH_US // step})"
            )
        if clock:
            rounded = (
                f"(CASE WHEN {rounded} * {step} >= 86400000000 THEN FLOOR(({u}) / 1000000) * {1_000_000 // step} "
                f"ELSE {rounded} END)"
            )
        return _inline(f"CAST({rounded} AS CHAR)")
    return (
        f"(CASE WHEN MOD({u}, {step}) = 0 THEN {_inline(f'CAST(({u}) DIV {step} AS CHAR)')} "
        f"ELSE {_foreign('us', f'CAST({u} AS CHAR)')} END)"
    )


def decimal_classes(x: str, fit: dict[str, Any]) -> list[tuple[str, str]]:
    """The writer's fit classes (``fits_decimal``); MySQL DECIMAL holds no NaN / infinity."""
    s = int(fit["scale"])
    max_int = max(int(fit["precision"]) - s, 0)
    limit = "1" + "0" * max_int  # an exact DECIMAL literal, never a DOUBLE like 1e8
    scale_over = f"{x} <> TRUNCATE({x}, {s})"
    int_over = f"NOT (ABS(TRUNCATE({x}, 0)) < {limit} AND ({x} <> 0 OR {max_int} >= 1))"
    return [("both", f"({scale_over}) AND ({int_over})"), ("scale", scale_over), ("int", int_over)]


def field_sql(col: ColumnPlan, *, column: str, role: str, dest_engine: str) -> str:
    x = quote_ident(column)
    kind = col.kind
    source = role == "source"
    pg_axis = str(dest_engine or "").lower() in {"postgresql", "postgres", "pg", "timescaledb", "alloydb",
                                                 "supabase", "pgvector"}
    if kind == "integer":
        body = _inline(f"CAST({x} AS CHAR)")
    elif kind == "boolean":
        body = _inline(f"CAST({x} + 0 AS CHAR)")
    elif kind == "decimal":
        c = f"CAST({x} AS CHAR)"
        stripped = f"(CASE WHEN LOCATE('.', {c}) > 0 THEN TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM {c})) ELSE {c} END)"
        body = _inline(f"(CASE WHEN {stripped} IN ('-0', '') THEN '0' ELSE {stripped} END)")
    elif kind in {"text", "char"}:
        t = _text(x)
        if kind == "char":
            t = f"TRIM(TRAILING ' ' FROM {t})"
        body = f"CONCAT('D', SHA2({t}, 256))" if col.width == "digest" else _inline(t)
        if col.empty_is_null:
            body = f"(CASE WHEN LENGTH({x}) = 0 THEN 'N' ELSE {body} END)"
    elif kind == "date":
        body = _inline(f"CAST(DATEDIFF({x}, '1970-01-01') AS CHAR)")
    elif kind in {"timestamp", "timestamptz"}:
        u = f"TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', {x})"
        body = units_sql(u, col.digits, project=source, pg_axis=pg_axis)
    elif kind == "time":
        u = f"(TIME_TO_SEC({x}) * 1000000 + MICROSECOND({x}))"
        body = units_sql(u, col.digits, project=source, pg_axis=False, clock=True)
    elif kind == "uuid":
        body = _inline(f"LOWER(CAST({x} AS CHAR))")
    elif kind == "binary":
        body = f"CONCAT('D', SHA2({x}, 256))"
    elif kind == "native":
        body = _inline(_text(f"CAST({x} AS CHAR)"))
    else:  # pragma: no cover - the plan only emits known kinds
        raise ValueError(f"no MySQL rendering for CE1 kind {kind!r}")
    if source and col.decimal_fit:
        return fit_field(x, body, col.decimal_fit, decimal_classes)
    return f"(CASE WHEN {x} IS NULL THEN 'N' ELSE {body} END)"


class MySQLSide(SqlSide):
    """A MySQL 8 or MariaDB table."""

    def prepare_session(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            version = str(cur.fetchone()[0] or "")
            self.server = "mariadb" if "mariadb" in version.lower() else "mysql"
            self.server_version = version
            for stmt in (
                "SET NAMES utf8mb4",
                "SET SESSION time_zone = '+00:00'",
                # One evaluation of SHA2 per row: never merge the derived table.
                "SET SESSION optimizer_switch = 'derived_merge=off'",
                "SET SESSION TRANSACTION READ ONLY",
            ):
                cur.execute(stmt)
        self.conn.commit()

    def quote(self, name: str) -> str:
        return quote_ident(name)

    @property
    def table_ref(self) -> str:
        if self.schema:
            return f"{quote_ident(self.schema)}.{quote_ident(self.table)}"
        return quote_ident(self.table)

    def field(self, col: ColumnPlan, column: str) -> str:
        return field_sql(col, column=column, role=self.role, dest_engine=self.plan.dest_engine)

    def concat(self, parts: Sequence[str]) -> str:
        return f"CONCAT({', '.join(parts)})" if len(parts) > 1 else parts[0]

    def sha256_hex(self, text: str) -> str:
        return f"SHA2({_text(text)}, 256)"

    def utf8_hex(self, text: str) -> str:
        return f"LOWER(HEX({_text(text)}))"

    def word(self, h: str, which: str) -> str:
        start = 2 if which == "a" else 18
        return f"CAST(CONV(SUBSTRING({h}, {start}, 15), 16, 10) AS UNSIGNED)"

    def hex4_int(self, hex4: str) -> str:
        return f"CAST(CONV({hex4}, 16, 10) AS UNSIGNED)"

    def substr(self, text: str, start: int, length: int) -> str:
        return f"SUBSTRING({text}, {start}, {length})"

    def exact_sum(self, expr: str) -> str:
        return f"SUM({expr})"

    def idiv(self, a: str, b: int) -> str:
        return f"(({a}) DIV {int(b)})"

    def mod(self, a: str, b: str) -> str:
        return f"MOD({a}, {b})"

    def held_predicate(self) -> str:
        return fit_held_predicate(self, decimal_classes)

    def estimate_rows(self) -> int | None:
        rows = self.execute(
            "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = COALESCE(%s, DATABASE()) "
            "AND TABLE_NAME = %s", [self.schema or None, self.table])
        return int(rows[0][0]) if rows and rows[0][0] is not None else None

    def _timeout(self, cur: Any, seconds: float | None = None) -> None:
        seconds = seconds or self.timeout_seconds
        if not seconds:
            return
        if self.server == "mariadb":
            cur.execute("SET SESSION max_statement_time = %s", (float(seconds),))
        else:
            cur.execute("SET SESSION max_execution_time = %s", (max(1, int(seconds * 1000)),))

    def pin(self, max_hold_s: float | None = None) -> dict[str, Any]:
        """``START TRANSACTION WITH CONSISTENT SNAPSHOT``: one InnoDB snapshot for the rest of the scan.

        Only InnoDB keeps a consistent read view; any other storage engine is
        not pinned. A statement error (a timeout) rolls back the statement
        only, so the snapshot is kept.
        """
        try:
            return self._pin()
        except Exception as exc:  # noqa: BLE001 — an engine that cannot pin is read statement by statement
            self.unpin()
            return {"kind": "not_established", "reason": f"the snapshot could not be taken ({type(exc).__name__})"}

    def unpin(self) -> None:
        self.pinned = False
        try:
            self.conn.rollback()
        except Exception:  # noqa: BLE001 — a broken connection fails its next statement instead
            return

    def _pin(self) -> dict[str, Any]:
        self.conn.rollback()
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT ENGINE FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = COALESCE(NULLIF(%s, ''), DATABASE()) AND TABLE_NAME = %s",
                (self.schema or "", self.table),
            )
            row = cur.fetchone()
            engine = str((row or [""])[0] or "")
            if engine.lower() != "innodb":
                return {"kind": "not_established",
                        "reason": f"the table's storage engine ({engine or 'unknown'}) keeps no consistent snapshot"}
            cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            cur.execute("SELECT UTC_TIMESTAMP(6)")
            at = cur.fetchone()[0]
        self.pinned = True
        return {"kind": "pinned", "method": "consistent_snapshot_transaction", "at": str(at)}

    def _execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        import pymysql

        self.tick()
        try:
            with self.conn.cursor() as cur:
                self._timeout(cur)
                cur.execute(sql, list(params))
                rows = list(cur.fetchall())
            if not self.pinned:
                self.conn.commit()
            return rows
        except pymysql.err.OperationalError as exc:
            if not self.pinned:
                self.conn.rollback()
            if exc.args and exc.args[0] in _TIMEOUT_CODES:
                raise timeout_error(exc) from exc
            raise
        except UnitTimeout:
            raise
        except Exception:
            if not self.pinned:
                self.conn.rollback()
            raise

    # the lock watch (a pinned snapshot yields to sessions waiting on its metadata locks) --
    #: An unbuffered result is being read: the watch cancels the query (KILL QUERY) first.
    release_idle_stream = False

    def session_id(self) -> Any:
        with self.conn.cursor() as cur:
            cur.execute("SELECT CONNECTION_ID()")
            return int(cur.fetchone()[0])

    def prepare_monitor(self, monitor: Any) -> None:
        monitor.autocommit(True)

    def lock_waiters(self, monitor: Any, session: Any) -> list[str]:
        """Sessions waiting for a metadata lock on this table.

        MySQL 8 names the holder (performance_schema.metadata_locks); where that is
        off (MariaDB's default) a session in the "Waiting for table metadata lock"
        state whose statement names this table counts (its text is only matched,
        never kept).
        """
        schema = self.schema or None
        with monitor.cursor() as cur:
            found = self._metadata_lock_waiters(cur, session) if self.server == "mysql" else None
            if found is not None:
                return found
            cur.execute(
                "SELECT ID, INFO FROM information_schema.PROCESSLIST "
                "WHERE STATE LIKE 'Waiting for table metadata lock%%' AND ID <> %s "
                "AND (DB = COALESCE(%s, DATABASE()) OR INFO IS NULL OR INFO LIKE %s)",
                (int(session), schema, f"%{self.table}%"),
            )
            rows = cur.fetchall()
        table = self.table.lower()
        return [f"session {pid} (waiting for a metadata lock on {self.table})"
                for pid, info in rows if info is None or table in str(info).lower()]

    def _metadata_lock_waiters(self, cur: Any, session: Any) -> list[str] | None:
        """MySQL 8: who waits on a metadata lock this session holds; None when performance_schema is off."""
        try:
            cur.execute(
                "SELECT DISTINCT wt.PROCESSLIST_ID, w.LOCK_TYPE FROM performance_schema.metadata_locks w "
                "JOIN performance_schema.metadata_locks g ON g.OBJECT_TYPE = w.OBJECT_TYPE "
                "AND g.OBJECT_SCHEMA = w.OBJECT_SCHEMA AND g.OBJECT_NAME = w.OBJECT_NAME "
                "JOIN performance_schema.threads gt ON gt.THREAD_ID = g.OWNER_THREAD_ID "
                "JOIN performance_schema.threads wt ON wt.THREAD_ID = w.OWNER_THREAD_ID "
                "WHERE g.LOCK_STATUS = 'GRANTED' AND w.LOCK_STATUS = 'PENDING' AND gt.PROCESSLIST_ID = %s",
                (int(session),),
            )
            rows = cur.fetchall()
        except Exception:  # noqa: BLE001 — performance_schema off: the caller reads the process list
            return None
        return [f"session {pid} (waiting for a {str(kind).lower()} metadata lock on {self.table})"
                for pid, kind in rows]

    def cancel_running(self, monitor: Any, session: Any) -> None:
        with monitor.cursor() as cur:
            cur.execute("KILL QUERY %s", (int(session),))

    def stream(self, sql: str, params: Sequence[Any], batch: int) -> Iterator[tuple[Any, ...]]:
        import pymysql

        self.tick()
        cur = self.conn.cursor(pymysql.cursors.SSCursor)
        try:
            # One streamed SELECT runs under the stream's own server-side timeout.
            self._timeout(cur, self.stream_timeout_seconds)
            cur.execute(sql, list(params))
            while True:
                rows = cur.fetchmany(batch)
                self.tick()
                if not rows:
                    break
                yield from rows
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001 — a cancelled read: let the snapshot go, roll back below
                if self.pinned:
                    self.unpin()
            if not self.pinned:  # a pinned scan keeps its transaction (and its snapshot)
                self.conn.commit()
