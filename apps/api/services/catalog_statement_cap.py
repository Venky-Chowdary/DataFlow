"""Per-statement server-side cap for catalog probes run during preflight.

QA round 11 (Ensembl MySQL → PG): Validate hung for more than five minutes on
a server holding thousands of schemas. The optional catalog probes (physical
storage, partitions, CHECK constraints, secondary indexes) are evidence, not
requirements; one of them scanning ``information_schema`` server-wide must not
hold the operator's preflight. Each such statement now carries a server-side
limit — MySQL ``MAX_EXECUTION_TIME`` (ms), MariaDB ``max_statement_time``
(s) — never longer than the preflight's own remaining deadline. A probe that
hits it reports itself unmeasured with that reason instead of hanging.
"""

from __future__ import annotations

import contextlib
import contextvars
import logging
from collections.abc import Iterator
from typing import Any

logger = logging.getLogger(__name__)

#: One catalog statement of an optional probe. A new limit for statements that
#: had none; it is not a raise of an existing budget.
CATALOG_STATEMENT_SECONDS = 10.0

#: Server error texts of a statement stopped by its execution-time limit.
_BUDGET_MARKERS = (
    "maximum statement execution time exceeded",  # MySQL 3024
    "max_statement_time exceeded",  # MariaDB 1969
    "query execution was interrupted",  # MySQL / MariaDB generic interrupt
    "canceling statement due to statement timeout",  # PostgreSQL 57014
)


def catalog_statement_seconds() -> float:
    """The cap for one catalog statement, inside the preflight's remaining budget."""
    from src.ai.copilot.tool_deadline import remaining_seconds

    left = remaining_seconds()
    if left is None:
        return CATALOG_STATEMENT_SECONDS
    return max(1.0, min(CATALOG_STATEMENT_SECONDS, left))


def budget_exceeded(exc: BaseException | None) -> bool:
    """True when ``exc`` is a server stopping a statement at its time limit."""
    text = str(exc or "").lower()
    return any(marker in text for marker in _BUDGET_MARKERS)


def budget_exceeded_detail(what: str, exc: BaseException | None = None) -> str:
    seconds = catalog_statement_seconds()
    return (
        f"{what} did not answer within its {seconds:g}s per-statement catalog "
        "budget, so it is unmeasured (not proof of absence)"
        + (f": {exc}" if exc is not None else "")
    )


def _driver_cursor(cursor: Any) -> Any:
    from services.physical_storage_metadata import as_driver_cursor

    return as_driver_cursor(cursor)


def _is_mariadb(cur: Any) -> bool:
    cur.execute("SELECT VERSION()", ())
    row = cur.fetchall()
    return bool(row and "mariadb" in str(row[0][0] or "").lower())


@contextlib.contextmanager
def mysql_catalog_cap(cursor: Any) -> Iterator[None]:
    """Cap every statement of this session for the block, then lift the cap.

    Best effort: a server that refuses the session variable leaves the probes
    uncapped (as before) rather than failing them.
    """
    cur = None
    reset = ""
    try:
        cur = _driver_cursor(cursor)
        seconds = catalog_statement_seconds()
        if _is_mariadb(cur):
            cur.execute(f"SET SESSION max_statement_time = {seconds:.3f}", ())
            reset = "SET SESSION max_statement_time = 0"
        else:
            cur.execute(f"SET SESSION MAX_EXECUTION_TIME = {int(seconds * 1000)}", ())
            reset = "SET SESSION MAX_EXECUTION_TIME = 0"
    except Exception as exc:  # noqa: BLE001 — the cap is best effort
        logger.debug("catalog statement cap not applied: %s", exc)
    try:
        yield
    finally:
        if cur is not None and reset:
            try:
                cur.execute(reset, ())
            except Exception as exc:  # noqa: BLE001 — a broken session is discarded
                logger.debug("catalog statement cap not lifted: %s", exc)


#: Budget findings of the preflight in progress (see :func:`collect_budget_findings`).
_FOUND: contextvars.ContextVar[list[str] | None] = contextvars.ContextVar(
    "df_catalog_budget_findings", default=None
)


@contextlib.contextmanager
def collect_budget_findings() -> Iterator[list[str]]:
    """Collect every catalog probe that stopped at its budget during the block."""
    found: list[str] = []
    token = _FOUND.set(found)
    try:
        yield found
    finally:
        _FOUND.reset(token)


def note_budget_exceeded(what: str, exc: BaseException | None) -> bool:
    """Record ``what`` as stopped at its budget when ``exc`` says so."""
    if not budget_exceeded(exc):
        return False
    found = _FOUND.get()
    detail = budget_exceeded_detail(what)
    if found is not None and detail not in found:
        found.append(detail)
    return True


def attach_budget_findings(result: Any, found: list[str]) -> Any:
    """State the probes that hit their budget on the preflight result (warn)."""
    if not found or not isinstance(result, dict):
        return result
    warnings = result.get("warnings")
    if not isinstance(warnings, list):
        warnings = []
    for detail in found:
        warnings.append({"id": "catalog_probe_budget", "severity": "warn", "message": detail})
    result["warnings"] = warnings
    result["catalog_probe_budget"] = list(found)
    return result
