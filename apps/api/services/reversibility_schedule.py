"""Refuse a schedule that requires reversibility and is not reversible.

Execute already runs g24. Create and update must run the same check, or an
unattended run fails every night for a reason Validate could have named
(D373). Migration rule 8.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from services.reversibility import GATE_ID, analyze_table, build_report


def schedule_reversibility_block(schedule: Any) -> str:
    """g24 message for a schedule payload or stored schedule, or ``""``.

    REST create, REST update, and the MCP cadence update all ask this, so a
    lossy ``require_reversibility`` schedule is refused before it is saved
    (D373). A schedule that does not require reversibility is left alone.
    """
    if isinstance(schedule, Mapping):
        require = bool(schedule.get("require_reversibility"))
        mappings = list(schedule.get("mappings") or [])
        nested = schedule.get("destination") if isinstance(schedule.get("destination"), Mapping) else {}
        table = str(schedule.get("dest_table") or nested.get("table") or "")
        dest_db = str(
            schedule.get("dest_db_type")
            or schedule.get("destination_db_type")
            or nested.get("format")
            or ""
        )
        source = schedule.get("source") if isinstance(schedule.get("source"), Mapping) else {}
        source_db = str(schedule.get("source_db_type") or source.get("format") or "")
    else:
        require = bool(getattr(schedule, "require_reversibility", False))
        mappings = list(getattr(schedule, "mappings", None) or [])
        table = str(getattr(schedule, "dest_table", "") or "")
        dest_db = str(getattr(schedule, "dest_db_type", "") or "")
        source_db = str(getattr(schedule, "source_db_type", "") or "")
    if not require:
        return ""
    return reversibility_refusal(
        mappings, table=table or "destination", dest_db=dest_db, source_db=source_db
    )


def reversibility_refusal(
    mappings: Sequence[Mapping[str, Any]],
    *,
    table: str,
    dest_db: str = "",
    source_db: str = "",
) -> str:
    """The g24 block message, or ``""`` when the mapping is reversible."""
    if not mappings:
        return ""
    analysed = analyze_table(
        table=table or "destination",
        mappings=list(mappings),
        dest_db=dest_db,
        source_db=source_db,
        enforce="at_execute",
    )
    report = build_report(
        [analysed],
        scope="schedule",
        subject_id=table or "schedule",
        require=True,
        data_rows=0,
        data_source="none",
    )
    gate = report.get("gate") or {}
    if gate.get("id") == GATE_ID and gate.get("status") == "block":
        return str(gate.get("message") or "This transfer must be reversible, and it is not.")
    return ""
