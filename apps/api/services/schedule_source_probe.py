"""Does the table a schedule reads exist? Asked when the schedule is saved.

QA SC5: a schedule naming a source table that does not exist was accepted,
and the operator learnt it only when the first unattended run failed. The
catalog answers the question in one metadata round trip, so the save asks it:

* ``present`` — save as asked;
* ``missing`` — refuse the save with 422 and the table named, the status and
  ``{"error": {"code", "message"}}`` shape of the empty-mapping refusal
  (QA r12 #5). A paused draft is refused too: QA expects a create naming a
  table that does not exist to fail, and a draft on it can only be enabled
  into a failure. (Round 11 answered 400 for an armed schedule and saved a
  paused one with a warning.)
* ``unknown`` — the source could not be reached or has no metadata probe in
  time: save, and say plainly that nothing was verified (the run still checks);
* ``skipped`` — the source is not a table (a CALL/SELECT read, a file, an
  object store, an API): there is no catalog question to ask.

The existence probe itself is ``services.dest_precount.destination_table_exists``
— one owner per engine for "does this relation exist", whichever side of the
route it sits on.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any, Mapping

from services.api_errors import ApiError

logger = logging.getLogger(__name__)

#: Error codes, alongside ``EMPTY_MAPPING_CONTRACT`` for a schedule without mappings.
SOURCE_TABLE_NOT_FOUND = "SOURCE_TABLE_NOT_FOUND"
SOURCE_TABLE_REQUIRED = "SOURCE_TABLE_REQUIRED"

#: Wall-clock bound on the save-time probe. An unreachable host must not hold
#: the create request for a driver's full connect timeout; past this the
#: answer is "unknown" and the save goes ahead with a warning.
PROBE_SECONDS = 8.0

#: Drivers ``destination_table_exists`` answers for (plus the generic_sql family).
_PROBED_DRIVERS = frozenset(
    {"sqlite", "mongodb", "postgresql", "mysql", "snowflake", "sqlserver", "mssql", "azure_sql"}
)

PRESENT = "present"
MISSING = "missing"
UNKNOWN = "unknown"
SKIPPED = "skipped"


class ScheduleSourceMissing(ApiError, ValueError):
    """The schedule names a source table that does not exist (HTTP 422).

    A ``ValueError`` as well, so callers that turn store refusals into their
    own error (GitOps apply, chat confirm) keep doing so.
    """


@dataclass(frozen=True)
class SourceProbe:
    status: str
    table: str = ""
    connector: str = ""
    detail: str = ""


def _callable_source(data: Mapping[str, Any]) -> bool:
    mode = str(data.get("source_read_mode") or "").strip().lower()
    if mode in {"procedure", "query"}:
        return True
    return bool(str(data.get("procedure_call") or "").strip() or str(data.get("source_query") or "").strip())


def _connector_cfg(connector_id: str, workspace_id: str) -> dict[str, Any] | None:
    from services.connector_store import get_connector

    try:
        conn = get_connector(connector_id, workspace_id=workspace_id or None)
    except Exception:  # noqa: BLE001 - a store hiccup is "unknown", not a refusal
        logger.debug("schedule source probe: connector lookup failed", exc_info=True)
        return None
    if conn is None:
        return None
    cfg = conn.to_dict()
    cfg["type"] = conn.type
    return cfg


def _driver(kind: str) -> str:
    token = (kind or "").strip().lower()
    if token in _PROBED_DRIVERS:
        return token
    try:
        from src.transfer.connector_capabilities import resolve_driver_type

        resolved = resolve_driver_type(token)
    except Exception:  # noqa: BLE001
        return ""
    if resolved in _PROBED_DRIVERS or resolved == "generic_sql":
        return resolved
    return ""


def _sqlite_file_exists(cfg: dict[str, Any]) -> bool:
    from pathlib import Path

    from connectors.sqlite_common import sqlite_file_path

    path = sqlite_file_path(
        str(cfg.get("database") or ""),
        str(cfg.get("connection_string") or ""),
        str(cfg.get("host") or ""),
    )
    return bool(path) and (path == ":memory:" or Path(path).is_file())


def _bounded_exists(driver: str, cfg: dict[str, Any], schema: str, table: str) -> bool | None:
    from services.dest_precount import destination_table_exists

    box: dict[str, bool | None] = {}

    def _run() -> None:
        try:
            box["exists"] = destination_table_exists(driver, cfg, schema=schema, table_name=table)
        except Exception:  # noqa: BLE001 - unknown is not missing
            box["exists"] = None

    worker = threading.Thread(target=_run, name="schedule-source-probe", daemon=True)
    worker.start()
    worker.join(PROBE_SECONDS)
    return box.get("exists") if not worker.is_alive() else None


def probe_schedule_source(data: Mapping[str, Any]) -> SourceProbe:
    """Classify the source table a schedule definition names."""
    if _callable_source(data):
        return SourceProbe(SKIPPED)
    connector_id = str(data.get("source_connector_id") or "").strip()
    if not connector_id:
        return SourceProbe(SKIPPED)
    cfg = _connector_cfg(connector_id, str(data.get("workspace_id") or ""))
    if cfg is None:
        return SourceProbe(SKIPPED)
    driver = _driver(str(cfg.get("type") or ""))
    if not driver:
        return SourceProbe(SKIPPED)
    label = str(cfg.get("name") or connector_id)
    raw = str(data.get("source_table") or "")
    if not raw.strip():
        return SourceProbe(
            MISSING,
            table=raw,
            connector=label,
            detail=(
                f"This schedule names no source table. Choose the table to read on "
                f"connector '{label}'."
            ),
        )
    if driver == "mongodb":
        schema, table = "", raw.strip()
    else:
        from connectors.sql_identifiers import split_qualified_table
        from services.dialect_profiles import schema_from_cfg

        sch, table = split_qualified_table(raw, schema_from_cfg(driver, cfg) or None)
        schema = sch or ""
    shown = f"{schema}.{table}" if schema else table
    if driver == "sqlite" and not _sqlite_file_exists(cfg):
        # sqlite3.connect would create an empty file: never as a side effect
        # of a save, and an absent file has no tables.
        exists: bool | None = False
    else:
        exists = _bounded_exists(driver, cfg, schema, table)
    if exists is True:
        return SourceProbe(PRESENT, table=shown, connector=label)
    if exists is False:
        return SourceProbe(
            MISSING,
            table=shown,
            connector=label,
            detail=(
                f"Source table '{shown}' not found on connector '{label}'. "
                "Create it, or choose an existing table — a schedule on a missing "
                "table can only fail."
            ),
        )
    return SourceProbe(
        UNKNOWN,
        table=shown,
        connector=label,
        detail=(
            f"Could not verify that source table '{shown}' exists: connector "
            f"'{label}' did not answer within {int(PROBE_SECONDS)}s or has no "
            "metadata access. Saved anyway; the first run checks it."
        ),
    )


def assert_schedule_source(
    data: Mapping[str, Any],
    *,
    armed: bool,
    warnings: list[str] | None,
) -> None:
    """Refuse a schedule on a missing table (422); record an unverifiable one.

    ``armed`` no longer decides the refusal (QA r12 #5) and is kept for the
    callers' signature; an unreachable source is never refused.
    """
    del armed
    probe = probe_schedule_source(data)
    if probe.status == MISSING:
        code = SOURCE_TABLE_NOT_FOUND if probe.table.strip() else SOURCE_TABLE_REQUIRED
        raise ScheduleSourceMissing(
            422,
            code,
            probe.detail,
            extra={"source_table": probe.table, "connector": probe.connector},
        )
    if probe.status == UNKNOWN and warnings is not None:
        warnings.append(probe.detail)
