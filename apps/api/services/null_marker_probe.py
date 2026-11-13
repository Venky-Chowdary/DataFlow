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
    """Rows holding marker text; ``rows is None`` means not measured (``reason`` says why)."""

    rows: int | None
    reason: str = ""
    timeout_enforced: bool = True
    #: Set when the cap stopped the count: at least this many rows match.
    at_least: int | None = None

    @property
    def capped(self) -> bool:
        return self.at_least is not None


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


def bounded_marker_count(conn: Any, tbl: Any, hit: Any, scope: ProbeScope, deadline: float) -> MarkerCount:
    """Count rows matching ``hit`` in ``scope``, at most :data:`PROBE_LIMIT`."""
    import sqlalchemy as sa

    batches: list[tuple[str, ...] | None] = [None]
    if scope.kind == "run_keys":
        keys = list(scope.keys)
        batches = [tuple(keys[i:i + _KEY_BATCH]) for i in range(0, len(keys), _KEY_BATCH)]
    total = 0
    for batch in batches:
        if time.monotonic() > deadline:
            return MarkerCount(None, f"timed out after {probe_timeout_seconds():g}s")
        where = hit
        if batch is not None:
            where = sa.and_(hit, tbl.c[scope.key_column].in_(list(batch)))
        inner = sa.select(sa.literal(1)).select_from(tbl).where(where).limit(PROBE_LIMIT + 1 - total).subquery()
        total += int(conn.execute(sa.select(sa.func.count()).select_from(inner)).scalar() or 0)
        if total > PROBE_LIMIT:
            return MarkerCount(None, f"the probe reached its cap of {PROBE_LIMIT:,} marker rows", at_least=total)
    return MarkerCount(total)
