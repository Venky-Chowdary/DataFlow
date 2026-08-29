"""Unattended runs must replay a persisted Validate mapping contract.

ScheduleForm (Operations → Pipelines) can save cadence without mappings.
The engine then invented ``_auto_map`` at execute — a second mapper, not
``services.semantic_mapper.map_columns`` signed in Studio. That is silent
identity invent (Airbyte-class). Fail closed instead.
"""

from __future__ import annotations

from typing import Any

EMPTY_MAPPING_REFUSAL = (
    "Schedule has no persisted column mappings — unattended runs must replay "
    "a Validate-approved mapping contract. Create from Transfer Studio after "
    "Validate, PATCH mappings onto this schedule, or import a GitOps manifest "
    "that includes mappings."
)

EMPTY_MAPPING_CODE = "EMPTY_MAPPING_CONTRACT"

EMPTY_MAPPING_CORRECTIVE = (
    "Open Transfer Studio with this schedule's source and destination. "
    "Map the columns, run Validate, then Schedule from the Studio footer — "
    "that persists the mapping contract the beat can replay. "
    "A signature here cannot invent column names."
)


def is_empty_mapping_refusal(message: str) -> bool:
    text = (message or "").lower()
    return "no persisted column mappings" in text


def persisted_mapping_rows(raw: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        source = str(item.get("source") or "").strip()
        target = str(item.get("target") or "").strip()
        if source or target:
            rows.append(item)
    return rows


def assert_schedule_mappings_replayable(raw: Any) -> list[dict[str, Any]]:
    rows = persisted_mapping_rows(raw)
    if not rows:
        raise ValueError(EMPTY_MAPPING_REFUSAL)
    return rows
