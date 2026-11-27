"""Execute's half of gate G13: no source column leaves a run without a decision.

Validate refuses a run while a source column is neither mapped nor declared
omitted (:mod:`services.source_coverage_gate`). Overwrite into an existing
table relies on that: its auto-map keeps only the columns the destination
already has and leaves the rest unaccounted "so G13 can block"
(``retain_dest_exists_write_mappings``). A run that skips Validate
(``skip_preflight``: quarantine replay, the proof ledger, the SDK, the desktop
lab) never reached G13. The column was read, never written, and Gate-8 then
compared the mapped columns only, so the job read "Row fidelity verified" over
a destination that lacked a source column. A replay of mappings stored before
the column existed did the same in every sync mode.

Rules applied:

* R8 (``defects.txt`` section 3) and ``docs/migration-rules.md`` rule 12:
  "The plan and execute run the same gates on the same inputs." This is the
  G13 predicate itself (:func:`build_source_coverage_gate`), evaluated at
  Execute for every sync mode and for the COPY and row paths alike, because
  all three engine flows call it before the first write.
* ``docs/migration-rules.md`` rule 7: a source column "is added on the
  destination ... or the schedule parks on a schema-drift finding. Events are
  not applied while a new column is dropped on the floor." A propagate policy
  (or backfill) adopts the column as an ADD COLUMN before this check, so only
  a column nobody decided on reaches the refusal.
* R7 and the schema policy (``SCHEMA_POLICY_HONESTY``: under
  ``manual_review`` "Execute does not ADD COLUMN"): a run without an
  approving policy does not add the column by itself here. It stops before
  the first write and names the choices.

What a finished run then reports about the columns it left out by decision is
:mod:`services.column_coverage_report`.
"""

from __future__ import annotations

from typing import Any

from .engine_schema_propagation import SchemaPropagationRefused

_REMEDY = (
    "Nothing was written. Set the schema policy to propagate columns (or turn on "
    "backfill new fields) so the run adds the column to the destination, map the "
    "column to a destination column, or declare it omitted, then run again."
)


_ADD_REMEDY = (
    "Nothing was written. Set the schema policy to propagate columns (or turn on "
    "backfill new fields), or acknowledge the schema drift for this run, so the run "
    "adds the column; or declare it omitted, then run again."
)


class SourceColumnsUnaccounted(SchemaPropagationRefused):
    """A run without Validate would have dropped, or added unapproved, a source column."""

    def __init__(
        self,
        message: str,
        *,
        columns: list[str],
        gate: str = "g13_source_coverage",
        rule_id: str = "g13_source_coverage.unaccounted",
        fix: str = _REMEDY,
    ) -> None:
        super().__init__(message)
        self.columns = list(columns)
        #: Merged into the job's ``error_details`` (``_job_failure_fields``), so
        #: the refusal is classified by the gate id Validate blocks with.
        self.failure_details = {
            "retriable": False,
            "unaccounted_sources": list(columns),
            "rule_id": rule_id,
            "guidance": [{"gate": gate, "message": message, "fix": fix}],
        }


def source_column_coverage(
    columns: list[str] | None, mappings: list[dict[str, Any]] | None
) -> dict[str, Any]:
    """Which source columns this plan writes, leaves out by decision, or leaves unanswered."""
    from services.mapping_constraints import classify_source_coverage

    return classify_source_coverage(list(columns or []), list(mappings or []))


def refuse_unaccounted_without_validate(
    request: Any, columns: list[str] | None, mappings: list[dict[str, Any]] | None
) -> None:
    """Raise when Validate will not run and a source column has no decision.

    With Validate on, G13 owns the block, its approval request and the recorded
    acknowledgement, so this stays silent and the job reaches that gate.
    """
    if request is None or not bool(getattr(request, "skip_preflight", False)):
        return
    from services.source_coverage_gate import build_source_coverage_gate

    coverage, gate = build_source_coverage_gate(
        source_columns=list(columns or []), mappings=list(mappings or [])
    )
    if not coverage["unaccounted"]:
        return
    raise SourceColumnsUnaccounted(
        f"{gate['message']} {_REMEDY}", columns=list(coverage["unaccounted"])
    )


def _live_has(target: str, live: set[str]) -> bool:
    """Whether the destination already holds ``target``, under any spelling it stores."""
    from connectors.sql_identifiers import sanitized_spellings

    name = str(target or "").strip()
    if not name:
        return True
    if name.lower() in live:
        return True
    return any(str(spelling).lower() in live for spelling in sanitized_spellings(name))


def refuse_unapproved_add_without_validate(
    request: Any,
    columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
    dest_schema_types: dict[str, str] | None,
) -> None:
    """Raise when Validate will not run and a mapping would ADD a column unapproved.

    Under ``manual_review`` Execute does not ADD COLUMN in any sync mode
    (``SCHEMA_POLICY_HONESTY``); Validate blocks the same proposal. Without
    Validate, auto-map (or a stored ``create_new`` stamp) still armed the
    writer's ADD COLUMN lane in every mode but overwrite. The approval is the
    predicate Validate and Execute share (``destination_evolution_approved``):
    a propagate policy, backfill new fields, or an acknowledged drift. The
    refusal is Validate's own sentence (``schema_review_copy``).
    """
    if request is None or not bool(getattr(request, "skip_preflight", False)):
        return
    live = {str(c).strip().lower() for c in (dest_schema_types or {}) if str(c).strip()}
    if not live:
        return  # no existing table: its columns are a CREATE, not an ADD COLUMN
    from services.db_type_utils import dest_declares_column_ddl

    if not dest_declares_column_ddl(str(getattr(request.destination, "format", "") or "")):
        return
    from services.schema_drift import (
        destination_evolution_approved,
        effective_schema_policy,
        schema_review_copy,
    )

    policy = effective_schema_policy(
        getattr(request, "schema_policy", None),
        backfill_new_fields=bool(getattr(request, "backfill_new_fields", False)),
    )
    if destination_evolution_approved(
        policy, acknowledged=bool(getattr(request, "schema_drift_acknowledged", False))
    ):
        return
    from services.shape_contract import write_ready_mappings

    source_names = {str(c).strip().lower() for c in columns or [] if str(c).strip()}
    missing: list[str] = []
    for mapping in write_ready_mappings(mappings):
        source = str(mapping.get("source") or "").strip()
        if source.lower() not in source_names or _live_has(str(mapping.get("target") or ""), live):
            continue
        if source not in missing:
            missing.append(source)
    if not missing:
        return
    copy = schema_review_copy(
        {
            "saved_contract": False,
            "schema_evolution": {
                "additive": [{"kind": "add_column", "column": name} for name in missing]
            },
        },
        policy=policy,
    )
    raise SourceColumnsUnaccounted(
        f"{copy['message']} Schema policy '{policy}' does not add columns. {_ADD_REMEDY}",
        columns=missing,
        gate=str(copy["gate_id"]),
        rule_id=str(copy["rule_id"]),
        fix=_ADD_REMEDY,
    )


#: Stamped on ``destination.extra`` by the engine before the first write, so a
#: stream that discovers a field mid-run knows whether the run's policy adds it.
MID_RUN_POLICY_KEY = "_df_mid_run_columns"

_MID_RUN_REMEDY = (
    "Nothing of that batch was written; the run stopped at the batch before it, "
    "so a resume continues from there. Set the schema policy to propagate columns "
    "(or turn on backfill new fields) and resume to add the field, or map it or "
    "declare it omitted."
)


def stamp_mid_run_column_policy(request: Any, dest_schema_types: dict[str, str] | None) -> None:
    """Record, for the stream, whether this run may add a field first seen mid-run.

    Only a propagate policy or backfill new fields approves it. An acknowledged
    drift approves the columns Validate disclosed, and a field no page had shown
    yet was not among them.
    """
    destination = getattr(request, "destination", None)
    if destination is None:
        return
    from services.schema_drift import destination_evolution_approved, effective_schema_policy

    policy = effective_schema_policy(
        getattr(request, "schema_policy", None),
        backfill_new_fields=bool(getattr(request, "backfill_new_fields", False)),
    )
    if not isinstance(getattr(destination, "extra", None), dict):
        destination.extra = {}
    destination.extra[MID_RUN_POLICY_KEY] = {
        "adds_columns": destination_evolution_approved(policy, acknowledged=False),
        "policy": policy,
        "dest_columns": sorted({str(c).strip().lower() for c in dest_schema_types or {} if str(c).strip()}),
    }


def omitted_source_names(mappings: list[dict[str, Any]] | None) -> frozenset[str]:
    """Source fields the operator declared omitted (kept before omits leave the write map)."""
    from services.mapping_constraints import is_intentional_omit

    return frozenset(
        str(m.get("source") or "").strip().lower()
        for m in mappings or []
        if isinstance(m, dict) and is_intentional_omit(m) and str(m.get("source") or "").strip()
    )


def refuse_mid_run_column(
    destination: Any, dest_type: str, fields: list[str], mappings: list[dict[str, Any]] | None
) -> None:
    """Raise before a batch is written when it brings a field the policy does not add.

    ``fields`` are the batch's new, unmapped, not-omitted fields. A field whose
    name the destination table already has is not an ADD COLUMN. A stream run
    without the engine's stamp (a benchmark or a direct caller) keeps the
    writer's previous behaviour.
    """
    stamp = (getattr(destination, "extra", None) or {}).get(MID_RUN_POLICY_KEY)
    if not isinstance(stamp, dict) or stamp.get("adds_columns") is not False:
        return
    from services.db_type_utils import dest_declares_column_ddl

    if not dest_declares_column_ddl(str(dest_type or "")):
        return
    live = {str(c) for c in stamp.get("dest_columns") or []}
    new = [f for f in fields if f and not _live_has(f, live)]
    if not new:
        return
    from services.source_coverage_gate import build_source_coverage_gate

    _coverage, gate = build_source_coverage_gate(source_columns=new, mappings=list(mappings or []))
    raise SourceColumnsUnaccounted(
        f"{gate['message']} First seen in a later batch; schema policy "
        f"'{stamp.get('policy') or 'manual_review'}' does not add columns. {_MID_RUN_REMEDY}",
        columns=new,
        fix=_MID_RUN_REMEDY,
    )


__all__ = [
    "MID_RUN_POLICY_KEY",
    "omitted_source_names",
    "refuse_mid_run_column",
    "stamp_mid_run_column_policy",
    "SourceColumnsUnaccounted",
    "refuse_unaccounted_without_validate",
    "refuse_unapproved_add_without_validate",
    "source_column_coverage",
]
