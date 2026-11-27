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


class SourceColumnsUnaccounted(SchemaPropagationRefused):
    """A run without Validate would have dropped a source column nobody decided on."""

    def __init__(self, message: str, *, columns: list[str]) -> None:
        super().__init__(message)
        self.columns = list(columns)
        #: Merged into the job's ``error_details`` (``_job_failure_fields``), so
        #: the refusal is classified by gate id, as the Validate block is.
        self.failure_details = {
            "retriable": False,
            "unaccounted_sources": list(columns),
            "guidance": [{"gate": "g13_source_coverage", "message": message, "fix": _REMEDY}],
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


__all__ = [
    "SourceColumnsUnaccounted",
    "refuse_unaccounted_without_validate",
    "source_column_coverage",
]
