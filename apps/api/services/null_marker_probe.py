"""Gate-8's NULL-marker probe, bounded (review F3).

The probe looks for Datawrap's NULL marker stored as destination text. It used
to read every text column of the whole destination table on every run, in
every validation mode, with no statement timeout, and compared a capped count
as if it were a census. Now:

* **Scope.** It reads the rows this run wrote when the route can name them:
  the run's keys (the writer's ``written_ids`` when they cover the run, else
  the keys of the run's whole record population), or the whole table when this
  run wrote all of it (an overwrite, or a table empty before the run).
  Otherwise the whole-table probe runs only in strict/maximum mode, and other
  modes record that it was skipped.
* **Timeout.** Every probe statement runs under a server-side timeout
  (:func:`probe_timeout_seconds`), inside one overall budget.
* **Not measured.** A timeout, a cap hit or a probe error is "not measured"
  with its reason, never a pass.
* **Exact, form by form** (review F4). Text is matched byte for byte, never
  through the column's collation (a ``_ci`` or PAD SPACE column matched
  ``__df_sql_null__`` and ``'__DF_SQL_NULL__ '``), and the raw marker and its
  escaped spelling are counted apart.
"""

from __future__ import annotations

import contextlib
import logging
import math
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

#: Seconds the whole probe of one table may take (every statement shares it).
PROBE_TIMEOUT_ENV = "NULL_MARKER_PROBE_SECONDS"
_DEFAULT_TIMEOUT = 20.0
#: Most marker rows one probe counts. Reaching it is "not measured".
PROBE_LIMIT = 1000
#: Most run keys a key-scoped probe binds; a larger run uses the mode rule.
KEY_SCOPE_LIMIT = 10_000
_KEY_BATCH = 500
STRICT_MODES = frozenset({"strict", "maximum"})

#: Server texts of a statement stopped at its timeout.
_TIMEOUT_MARKERS = (
    "canceling statement due to statement timeout",  # PostgreSQL 57014
    "maximum statement execution time exceeded",  # MySQL 3024
    "max_statement_time exceeded",  # MariaDB 1969
    "query execution was interrupted",  # MySQL / MariaDB
    "interrupted",  # SQLite progress handler
    "query timeout expired",  # SQL Server (ODBC HYT00)
    "dpi-1067",  # Oracle call timeout
)


def probe_timeout_seconds() -> float:
    from services.brand_env import getenv_brand

    raw = str(getenv_brand(PROBE_TIMEOUT_ENV, "") or "").strip()
    try:
        value = float(raw) if raw else _DEFAULT_TIMEOUT
    except ValueError:
        value = _DEFAULT_TIMEOUT
    return value if value > 0 else _DEFAULT_TIMEOUT


@dataclass(frozen=True)
class ProbeScope:
    """Which destination rows the probe reads."""

    kind: str  # "run_keys" | "table_written_by_run" | "whole_table"
    key_column: str = ""
    keys: tuple[str, ...] = ()

    def on_source(self, mappings: list[dict[str, Any]] | None) -> ProbeScope | None:
        """The same rows on the source side; ``None`` when the key has no source column."""
        if self.kind != "run_keys":
            return self
        for m in mappings or []:
            if isinstance(m, dict) and str(m.get("target") or "") == self.key_column and m.get("source"):
                return ProbeScope(self.kind, str(m["source"]), self.keys)
        return None


@dataclass
class MarkerCount:
    """Rows holding marker text; ``rows is None`` means not measured (``reason`` says why).

    ``raw`` counts rows holding the marker itself, ``escaped`` rows holding its
    escaped spelling. A capped count keeps what it saw as lower bounds.
    """

    rows: int | None
    reason: str = ""
    timeout_enforced: bool = True
    raw: int = 0
    escaped: int = 0
    capped: bool = False
    #: The probe has no route to this destination (Datawrap's gap, not the run's).
    not_applicable: bool = False
    #: Datawrap could not reach the destination for the probe after Gate-8's
    #: own read-back did: an internal error, logged, never a strict failure.
    internal: bool = False


#: The NULL markers as a leak stores them, and their escaped spellings.
def _forms() -> tuple[frozenset[str], frozenset[str]]:
    from services.value_serializer import DF_MISSING_SENTINEL, ESCAPED_WIRE_PREFIX, SQL_NULL_SENTINEL

    raw = frozenset({SQL_NULL_SENTINEL, DF_MISSING_SENTINEL})
    return raw, frozenset(ESCAPED_WIRE_PREFIX + m for m in raw)


def classify_cells(cells: Any, fixed: Any = ()) -> tuple[bool, bool]:
    """(holds the raw marker, holds an escaped marker), byte for byte.

    ``fixed`` are positions of fixed-width CHAR columns, whose stored value is
    padded with spaces by definition.
    """
    raw_forms, escaped_forms = _forms()
    raw = escaped = False
    for position, value in enumerate(cells):
        if isinstance(value, (bytes, bytearray)):
            value = bytes(value).decode("utf-8", "replace")
        if not isinstance(value, str):
            continue
        text = value.rstrip(" ") if position in fixed else value
        raw = raw or text in raw_forms
        escaped = escaped or text in escaped_forms
    return raw, escaped


def _key_values(dest_summary: dict[str, Any], records: list[Any] | None, mappings: Any,
                key: str, rows_written: int) -> list[str] | None:
    from services.value_serializer import is_encoded_null, unescape_wire_text

    ids = [str(x) for x in dest_summary.get("written_ids") or [] if x is not None and str(x) != ""]
    if rows_written > 0 and len(set(ids)) >= rows_written:
        return sorted(set(ids))
    if not records or rows_written <= 0 or len(records) < rows_written:
        return None
    source = next(
        (str(m.get("source")) for m in mappings or []
         if isinstance(m, dict) and str(m.get("target") or "") == key and m.get("source")),
        key,
    )
    keys: set[str] = set()
    for row in records:
        if not isinstance(row, dict):
            return None
        value = row.get(source, row.get(key))
        if is_encoded_null(value):
            continue
        keys.add(str(unescape_wire_text(value)))
        if len(keys) > KEY_SCOPE_LIMIT:
            return None
    return sorted(keys)


def resolve_probe_scope(
    *,
    dest_summary: dict[str, Any] | None,
    records: list[Any] | None,
    mappings: list[dict[str, Any]] | None,
    rows_written: int,
    validation_mode: str | None,
    sync_mode: str | None = None,
) -> ProbeScope | str:
    """The rows to probe, or the reason the probe does not run."""
    from services.dest_precount import PRECOUNT_KEY
    from services.sync_cursor import is_overwrite_sync
    from services.validation_mode_contract import normalize_validation_mode

    summary = dest_summary if isinstance(dest_summary, dict) else {}
    mode = sync_mode or str(summary.get("sync_mode") or summary.get("effective_sync_mode") or "")
    before = summary.get(PRECOUNT_KEY)
    if is_overwrite_sync(mode) or (isinstance(before, int) and not isinstance(before, bool) and before == 0):
        return ProbeScope("table_written_by_run")
    key_cols = list(summary.get("primary_key_columns") or summary.get("conflict_columns") or [])
    if len(key_cols) == 1:
        keys = _key_values(summary, records, mappings, str(key_cols[0]), int(rows_written or 0))
        if keys is not None:
            return ProbeScope("run_keys", str(key_cols[0]), tuple(keys))
    if normalize_validation_mode(validation_mode) in STRICT_MODES:
        return ProbeScope("whole_table")
    return (
        "the rows this run wrote cannot be named on this route (no complete key list, "
        "and the table held rows before the run), so the whole-table probe runs only "
        "in strict or maximum validation mode"
    )


def _dbapi(conn: Any) -> Any:
    raw = conn.connection
    return getattr(raw, "dbapi_connection", None) or raw


@contextlib.contextmanager
def statement_timeout(conn: Any, db_type: str, seconds: float) -> Iterator[bool]:
    """Bound every statement on ``conn`` for the block; yields whether a bound is in force."""
    import sqlalchemy as sa

    millis = max(1, int(seconds * 1000))
    restore: str | None = None
    undo: Any = None
    enforced = True
    try:
        if db_type in ("postgresql", "postgres"):
            # Transaction-local: ends with the probe's transaction.
            conn.execute(sa.text("SELECT set_config('statement_timeout', :v, true)"), {"v": str(millis)})
        elif db_type in ("mysql", "mariadb"):
            if getattr(conn.dialect, "is_mariadb", False):
                conn.execute(sa.text("SET SESSION max_statement_time = :s"), {"s": max(0.001, seconds)})
                restore = "SET SESSION max_statement_time = DEFAULT"
            else:
                conn.execute(sa.text("SET SESSION max_execution_time = :ms"), {"ms": millis})
                restore = "SET SESSION max_execution_time = DEFAULT"
        elif db_type == "sqlite":
            raw = _dbapi(conn)
            deadline = time.monotonic() + seconds
            raw.set_progress_handler(lambda: 1 if time.monotonic() > deadline else 0, 1000)
            undo = lambda: raw.set_progress_handler(None, 0)  # noqa: E731
        elif db_type in ("sqlserver", "mssql") and hasattr(_dbapi(conn), "timeout"):
            raw, previous = _dbapi(conn), _dbapi(conn).timeout
            raw.timeout = max(1, math.ceil(seconds))
            undo = lambda: setattr(raw, "timeout", previous)  # noqa: E731
        elif db_type == "duckdb" and hasattr(_dbapi(conn), "interrupt"):
            import threading

            timer = threading.Timer(seconds, _dbapi(conn).interrupt)
            timer.daemon = True
            timer.start()
            undo = timer.cancel
        elif db_type == "oracle" and hasattr(_dbapi(conn), "call_timeout"):
            raw, previous = _dbapi(conn), _dbapi(conn).call_timeout
            raw.call_timeout = millis
            undo = lambda: setattr(raw, "call_timeout", previous)  # noqa: E731
        else:
            enforced = False
    except Exception as exc:  # noqa: BLE001 — recorded as an unbounded probe
        logger.debug("NULL-marker probe timeout not applied: %s", exc)
        enforced = False
    try:
        yield enforced
    finally:
        try:
            if restore:
                conn.execute(sa.text(restore))
            if undo is not None:
                undo()
        except Exception as exc:  # noqa: BLE001 — a broken session is discarded by the pool
            logger.debug("NULL-marker probe timeout not lifted: %s", exc)


def timed_out(exc: BaseException) -> bool:
    text = str(exc or "").lower()
    return any(marker in text for marker in _TIMEOUT_MARKERS)


def bounded_marker_count(conn: Any, tbl: Any, text_cols: list[str], hit: Any, scope: ProbeScope,
                         deadline: float, fixed: Any = ()) -> MarkerCount:
    """Read the rows ``hit`` may match in ``scope`` (at most :data:`PROBE_LIMIT`) and classify them exactly."""
    import sqlalchemy as sa

    batches: list[tuple[str, ...] | None] = [None]
    if scope.kind == "run_keys":
        keys = list(scope.keys)
        batches = [tuple(keys[i:i + _KEY_BATCH]) for i in range(0, len(keys), _KEY_BATCH)]
    seen = rows = raw = escaped = 0
    for batch in batches:
        if time.monotonic() > deadline:
            return MarkerCount(None, f"timed out after {probe_timeout_seconds():g}s")
        where = hit
        if batch is not None:
            where = sa.and_(hit, tbl.c[scope.key_column].in_(list(batch)))
        stmt = sa.select(*[tbl.c[c] for c in text_cols]).where(where).limit(PROBE_LIMIT + 1 - seen)
        for row in conn.execute(stmt):
            seen += 1
            is_raw, is_escaped = classify_cells(tuple(row), fixed)
            rows += int(is_raw or is_escaped)
            raw += int(is_raw)
            escaped += int(is_escaped)
        if seen > PROBE_LIMIT:
            return MarkerCount(None, f"the probe reached its cap of {PROBE_LIMIT:,} candidate rows",
                               raw=raw, escaped=escaped, capped=True)
    return MarkerCount(rows, raw=raw, escaped=escaped)


def _columns(conn: Any, table: str, schema: str | None, wanted: list[str],
             key: str = "") -> tuple[list[str], set[int], Any]:
    """(text columns to probe, positions of fixed-width CHAR ones, the key column's type)."""
    import sqlalchemy as sa

    folded = {c.lower() for c in wanted}
    out: list[str] = []
    fixed: set[int] = set()
    key_type: Any = None
    for col in sa.inspect(conn).get_columns(table, schema=schema or None):
        name = str(col.get("name") or "")
        if key and name == key:
            key_type = col.get("type")
        if folded and name.lower() not in folded:
            continue
        kind = col.get("type")
        if isinstance(kind, sa.String):
            if isinstance(kind, sa.CHAR) and not isinstance(kind, sa.VARCHAR):
                fixed.add(len(out))
            out.append(name)
    return out, fixed, key_type


def _exact_in(column: Any, dialect: str, texts: list[str]) -> Any:
    """``column IN texts``, byte for byte where the engine's own collation may fold or pad."""
    import sqlalchemy as sa

    if dialect in ("mysql", "mariadb"):
        # CAST(... AS BINARY): no case folding, no PAD SPACE.
        return sa.cast(column, sa.LargeBinary).in_([t.encode("utf-8") for t in texts])
    return column.in_(texts)


class ProbeNotApplicable(Exception):
    """The probe has no route to this destination; the reason says why."""


#: Errors that mean Datawrap cannot build a SQL route to the destination at all
#: (no dialect plugin, no driver, a URL it cannot parse), never that the
#: destination failed.
def _no_route(exc: BaseException) -> bool:
    import sqlalchemy.exc as sa_exc

    seen: BaseException | None = exc
    while seen is not None:
        if isinstance(seen, (sa_exc.NoSuchModuleError, sa_exc.ArgumentError, ImportError, NotImplementedError)):
            return True
        seen = seen.__cause__ or seen.__context__
    return False


def _unreachable_by_a_second_connection(cfg: dict[str, Any]) -> str:
    """Why a second connection cannot see the destination the writer wrote, or ``""``."""
    kind = str(cfg.get("type") or "").lower()
    conn_s = str(cfg.get("connection_string") or "").strip()
    database = str(cfg.get("database") or "").strip()
    if kind == "motherduck":
        return "a MotherDuck database has no SQL read-back route"
    if kind == "duckdb" or conn_s.lower().startswith("duckdb:"):
        from connectors.sqlite_common import duckdb_file_path

        path = duckdb_file_path(database, conn_s) or ""
        if path.lower().startswith(("md:", "motherduck:")):
            return "a MotherDuck database has no SQL read-back route"
        if path in ("", ":memory:"):
            return "an in-memory DuckDB database is not visible to a second connection"
    if kind == "sqlite" and not conn_s and database in ("", ":memory:"):
        return "an in-memory SQLite database is not visible to a second connection"
    return ""


def probe_engine(cfg: dict[str, Any]) -> Any:
    """The writer's own engine for ``cfg`` (same config, same pool); raises :class:`ProbeNotApplicable`.

    ``cfg`` is the destination config the writer was handed: its ``type`` is
    the endpoint's own engine (``duckdb``, ``clickhouse``, ...), never the
    catalog's driver family (``generic_sql``), so the probe builds exactly the
    URL the writer built.
    """
    from connectors import generic_sql

    reason = _unreachable_by_a_second_connection(cfg)
    if reason:
        raise ProbeNotApplicable(reason)
    try:
        return generic_sql.get_sqlalchemy_engine(dict(cfg))
    except Exception as exc:
        if _no_route(exc) or isinstance(exc, ValueError):
            raise ProbeNotApplicable(f"Datawrap has no SQL route to this destination: {exc}") from exc
        raise


def count_marker_rows(
    cfg: dict[str, Any], table: str, columns: list[str], scope: ProbeScope | None = None,
) -> MarkerCount:
    """Rows of ``scope`` whose text columns hold a marker, raw and escaped apart.

    Opened through the writer's own engine (:func:`probe_engine`). Bounded three
    ways (review F3): the scope, a statement timeout and a cap. Any of them
    reached, or a query error, is "not measured" with its reason. No
    connection back after the write is "not measured" and ``internal``
    (Gate-8's own read-back already reached the destination). No route at all
    is ``not_applicable``. The SQL filter only narrows the candidates; every
    candidate is classified byte for byte here (review F4).
    """
    import sqlalchemy as sa

    from connectors.sql_identifiers import split_qualified_table
    from services.engine_pool import release_engine

    scope = scope or ProbeScope("whole_table")
    if not table:
        return MarkerCount(None, "no table to probe")
    raw_forms, escaped_forms = _forms()
    schema, bare = split_qualified_table(table, (str(cfg.get("schema") or "")).strip() or None)
    seconds = probe_timeout_seconds()
    enforced = False
    try:
        engine = probe_engine(cfg)
    except ProbeNotApplicable as exc:
        return MarkerCount(None, str(exc), False, not_applicable=True)
    except Exception as exc:  # noqa: BLE001 — the writer's own config would not build again
        logger.warning("NULL-marker probe of %s could not build the writer's engine", table, exc_info=True)
        return MarkerCount(None, f"the writer's engine could not be built again ({type(exc).__name__})",
                           internal=True)
    try:
        try:
            conn = engine.connect()  # SQLAlchemy 2 connects here
        except Exception as exc:  # noqa: BLE001 — recorded, never a pass
            if _no_route(exc):
                return MarkerCount(None, f"Datawrap has no SQL route to this destination: {exc}",
                                   False, not_applicable=True)
            logger.warning("NULL-marker probe of %s could not connect", table, exc_info=True)
            return MarkerCount(None, "Datawrap could not connect again for the probe "
                                     f"({type(exc).__name__}; internal error, logged)", internal=True)
        with conn:
            dialect = str(getattr(engine.dialect, "name", "") or "").lower()
            if dialect == "mysql" and getattr(engine.dialect, "is_mariadb", False):
                dialect = "mariadb"
            try:
                text_cols, fixed, key_type = _columns(conn, bare, schema, columns, scope.key_column)
                if not text_cols:
                    return MarkerCount(0)
                if scope.kind == "run_keys" and key_type is None:
                    return MarkerCount(None, f"the key column {scope.key_column!r} is not in {table}")
                cols = [sa.column(c) for c in text_cols]
                if scope.kind == "run_keys" and scope.key_column not in text_cols:
                    cols.append(sa.column(scope.key_column, key_type))
                tbl = sa.table(bare, *cols, schema=schema or None)
                texts = sorted(raw_forms | escaped_forms)
                hit = sa.or_(*[_exact_in(tbl.c[c], dialect, texts) for c in text_cols])
                with statement_timeout(conn, _TIMEOUT_DIALECT.get(dialect, dialect), seconds) as enforced:
                    result = bounded_marker_count(
                        conn, tbl, text_cols, hit, scope, time.monotonic() + seconds, fixed
                    )
                result.timeout_enforced = enforced
                return result
            except Exception as exc:  # noqa: BLE001 — an unreadable probe is "not measured", not a pass
                if timed_out(exc):
                    return MarkerCount(None, f"timed out after {seconds:g}s", enforced)
                logger.warning("NULL-marker probe of %s failed", table, exc_info=True)
                return MarkerCount(None, f"the probe query failed ({type(exc).__name__})", enforced)
    finally:
        release_engine(engine)


#: SQLAlchemy dialect name -> the timeout family :func:`statement_timeout` knows.
_TIMEOUT_DIALECT = {"mssql": "sqlserver"}


def count_literal_marker_records(records: Any) -> MarkerCount | None:
    """Source rows (wire records) holding a *literal* marker, raw and escaped apart.

    On the wire a user's raw marker text is escaped once (or a decoded
    :class:`~services.value_serializer.WireLiteral`), and a user's escaped
    spelling is escaped twice; the bare marker is a NULL. ``None`` when unreadable.
    """
    from services.value_serializer import ESCAPED_WIRE_PREFIX, WireLiteral

    raw_forms, escaped_forms = _forms()
    wire_raw = {ESCAPED_WIRE_PREFIX + m for m in raw_forms}
    wire_escaped = {ESCAPED_WIRE_PREFIX + m for m in escaped_forms}
    try:
        rows = list(records)
    except TypeError:
        return None
    count = raw = escaped = 0
    for row in rows:
        cells = row.values() if isinstance(row, dict) else row
        try:
            texts = [str(c) for c in cells if isinstance(c, str) and not isinstance(c, WireLiteral)]
            literal = [str(c) for c in cells if isinstance(c, WireLiteral)]
        except TypeError:
            return None
        is_raw = any(t in raw_forms for t in literal) or any(t in wire_raw for t in texts)
        is_escaped = any(t in escaped_forms for t in literal) or any(t in wire_escaped for t in texts)
        count += int(is_raw or is_escaped)
        raw += int(is_raw)
        escaped += int(is_escaped)
    return MarkerCount(count, raw=raw, escaped=escaped)
