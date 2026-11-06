"""Unattended runs must replay a persisted Validate mapping contract.

ScheduleForm (Operations → Pipelines) can save cadence without mappings.
The engine then invented ``_auto_map`` at execute — a second mapper, not
``services.semantic_mapper.map_columns`` signed in Studio. That is silent
identity invent (Airbyte-class). Fail closed instead.
"""

from __future__ import annotations

from typing import Any

from services.api_errors import ApiError

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


def is_empty_mapping_finding(code: str = "", finding: str = "") -> bool:
    """True for the empty-mapping park — a plan change, not a signature."""
    if str(code or "").strip().upper() == EMPTY_MAPPING_CODE:
        return True
    return is_empty_mapping_refusal(finding)


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


MAPPING_REVIEW_CODE = "SCHEDULE_MAPPING_REVIEW_REQUIRED"


class ScheduleMappingsRefused(ApiError, ValueError):
    """Stored mappings every run would refuse (HTTP 422). A ``ValueError`` too,
    so GitOps apply and chat Confirm keep turning store refusals into theirs."""

#: The two ways out, named in every refusal (QA C10).
MAPPING_OVERRIDE_ROUTE = (
    "Re-map these columns in Transfer Studio and Schedule from the Studio "
    "footer, or — once you have reviewed each pair — send it with "
    '"user_override": true on its mapping row (POST/PATCH /api/v1/schedules '
    "mappings). A lossy, narrowing or value-changing pair also needs a signed "
    "Migration Risk Contract (Map → Accept risk)."
)


def schedule_mapping_findings(raw: Any, validation_mode: str = "") -> list[dict[str, Any]]:
    """What Execute would refuse in these stored mapping rows, before any run.

    The same two owners every unattended run meets: G4's floor and clearances
    (``preflight.gates.mapping_confidence_layers`` at the floor
    ``confidence_threshold_for_mode`` gives Execute) and Execute's review gate
    (``mapping_pipeline.assert_mappings_executable``). Empty means a run would
    not be refused for its mappings.
    """
    import copy

    from preflight.gates import mapping_confidence_layers

    from services.mapping_constraints import is_intentional_omit
    from services.mapping_pipeline import assert_mappings_executable
    from services.preflight_service import confidence_threshold_for_mode

    rows = persisted_mapping_rows(raw)
    if not rows:
        return []
    floor = max(0.55, float(confidence_threshold_for_mode(validation_mode or "strict") or 0.85))
    active = [r for r in rows if not is_intentional_omit(r)]
    findings = [
        {"gate": "g4_mapping_confidence", "layer": layer, "message": message, "mappings": names}
        for layer, message, names in mapping_confidence_layers(active, floor)
    ]
    try:
        assert_mappings_executable(copy.deepcopy(rows), review=True)
    except ValueError as exc:
        text = str(exc)
        if not any(text == f["message"] for f in findings):
            findings.append({"gate": "execute_review", "layer": "review", "message": text,
                             "mappings": []})
    return findings


def assert_schedule_mappings_executable(raw: Any, validation_mode: str = "") -> None:
    """Refuse (422) a schedule save whose mappings every run would refuse (QA C10)."""
    findings = schedule_mapping_findings(raw, validation_mode)
    if not findings:
        return
    head = "; ".join(f["message"] for f in findings)
    raise ScheduleMappingsRefused(
        422,
        MAPPING_REVIEW_CODE,
        f"This schedule's mappings would be refused on every run: {head}. "
        f"{MAPPING_OVERRIDE_ROUTE}",
        extra={
            "findings": findings,
            "mappings": sorted({n for f in findings for n in f["mappings"]}),
            "validation_mode": validation_mode or "strict",
            "override": MAPPING_OVERRIDE_ROUTE,
        },
    )
