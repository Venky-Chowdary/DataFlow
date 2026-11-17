"""Refuse a schedule that requires reversibility and is not reversible.

Execute already runs g24. Create and update must run the same check, or an
unattended run fails every night for a reason Validate could have named
(D373). Migration rule 8.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from services.reversibility import GATE_ID, analyze_table, build_report


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
