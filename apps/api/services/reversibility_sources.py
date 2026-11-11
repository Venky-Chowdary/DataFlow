"""Where the reversibility analysis reads its inputs: a plan, a job, an Execute request.

The analysis itself is :mod:`services.reversibility` (pure). This module only
gathers the mappings, the declared source/destination types, the shape recipe,
the key and the observed rows each surface already holds, so the plan tab, the
job view, Validate (plan preflight) and Execute (policy gates) all answer from
one rule set.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from services.reversibility import analyze_table, build_report, gate_for

POLICY = "require_reversibility"
#: Where Execute keeps the columns its shape recipe read (set when the runner opens; never serialised).
_RECIPE_INPUTS = "_reversibility_recipe_inputs"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _key_columns(stream_contracts: Any) -> list[str]:
    for contract in stream_contracts or []:
        if not isinstance(contract, Mapping) or contract.get("selected") is False:
            continue
        pk = contract.get("primary_key")
        cols = [pk] if isinstance(pk, str) else list(pk or [])
        cols = [str(c).strip() for c in cols if str(c or "").strip()]
        if cols:
            return cols
    return []


def _limit(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _steps(recipe: Any) -> list[dict[str, Any]]:
    return [s for s in _dict(recipe).get("steps") or [] if isinstance(s, dict)]


def _engine(endpoint: Mapping[str, Any] | Any) -> str:
    if isinstance(endpoint, Mapping):
        return str(endpoint.get("format") or endpoint.get("type") or "").lower()
    return str(getattr(endpoint, "format", "") or "").lower()


def _table(dest: Any, src: Any) -> str:
    for endpoint in (dest, src):
        if isinstance(endpoint, Mapping):
            name = endpoint.get("table") or endpoint.get("collection")
        else:
            name = getattr(endpoint, "table", "") or getattr(endpoint, "collection", "")
        if str(name or "").strip():
            return str(name).strip()
    return "destination"


def note_recipe_inputs(request: Any, columns: Any) -> None:
    """Execute opened its shape recipe on ``columns`` (the read, before any step).

    The gate runs later on the shaped columns, where a column a step created and
    a source column a step overwrote look alike; this keeps the difference.
    """
    try:
        setattr(request, _RECIPE_INPUTS, [str(c) for c in columns or []])
    except (AttributeError, TypeError):
        pass


def _recipe_inputs(recipe: Any, columns: Any) -> list[str] | None:
    """The recipe's input columns when known; ``None`` without a recipe to read them."""
    if not _steps(recipe):
        return None
    return [str(c) for c in columns] if columns else None


def plan_requires_reversibility(policies: Mapping[str, Any] | None) -> bool:
    return _truthy(_dict(policies).get(POLICY))


def plan_report(plan: Any) -> dict[str, Any]:
    """The plan tab's ReversibilityReport: active mappings, declared types, the plan sample."""
    rev = plan.active_revision() if plan is not None else None
    policies = _dict(getattr(plan, "policies", None))
    sample = [r for r in getattr(plan, "sample_rows", None) or [] if isinstance(r, Mapping)]
    table = analyze_table(
        table=_table(plan.destination, plan.source),
        mappings=list((rev.mappings if rev else None) or []),
        source_columns=list(plan.source_columns or []),
        source_types=dict(plan.source_schema or {}),
        target_types=dict(plan.target_schema or {}),
        recipe_steps=_steps(policies.get("shape_recipe")),
        # The plan's source columns are the read before the recipe (the shaped image is derived).
        recipe_input_columns=_recipe_inputs(policies.get("shape_recipe"), plan.source_columns),
        key_columns=_key_columns(policies.get("stream_contracts")),
        sample_rows=sample,
        dest_db=_engine(plan.destination),
        source_db=_engine(plan.source),
        row_limit=_limit(policies.get("row_limit")),
        source_filter=_dict(policies.get("source_filter")),
    )
    return build_report([table], scope="plan", subject_id=str(plan.id),
                        require=plan_requires_reversibility(policies),
                        data_rows=len(sample), data_source="plan_sample")


def job_report(job: dict[str, Any], job_id: str) -> dict[str, Any]:
    """A job's ReversibilityReport from its stored request (no rows are read)."""
    from services.parity_evidence import dest_types_of, mappings_of, request_of

    req = request_of(job)
    src, dst = _dict(req.get("source")), _dict(req.get("destination"))
    table = analyze_table(
        table=_table(dst, src),
        mappings=mappings_of(job),
        source_columns=list(_dict(req.get("column_types")).keys()),
        source_types={str(k): str(v) for k, v in _dict(req.get("column_types")).items()},
        # The destination types the run recorded (Map stamps, the DDL it ran, Gate-8's digest types).
        target_types=dest_types_of(job),
        recipe_steps=_steps(req.get("shape_recipe")),
        recipe_input_columns=_recipe_inputs(
            req.get("shape_recipe"),
            _dict(_dict(job.get("destination_summary")).get("shape_proof")).get("input_columns")),
        key_columns=_key_columns(req.get("stream_contracts")),
        dest_db=_engine(dst),
        source_db=_engine(src),
        row_limit=_limit(req.get("limit")),
        source_filter=_dict(req.get("source_filter")),
    )
    return build_report([table], scope="job", subject_id=job_id,
                        require=_truthy(req.get(POLICY)), data_rows=0, data_source="none")


def request_report(
    request: Any,
    mappings: list[dict[str, Any]] | None,
    column_types: Mapping[str, str] | None,
    dest_types: Mapping[str, str] | None,
    *,
    sample_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Execute's view: the run's planned mappings and types (Validate == Execute)."""
    src, dst = getattr(request, "source", None), getattr(request, "destination", None)
    table = analyze_table(
        table=_table(dst, src),
        mappings=list(mappings or getattr(request, "mappings", None) or []),
        source_columns=list((column_types or {}).keys()),
        source_types=dict(column_types or {}),
        target_types=dict(dest_types or {}),
        recipe_steps=_steps(getattr(request, "shape_recipe", None)),
        recipe_input_columns=_recipe_inputs(getattr(request, "shape_recipe", None),
                                            getattr(request, _RECIPE_INPUTS, None)),
        key_columns=_key_columns(getattr(request, "stream_contracts", None)),
        sample_rows=list(sample_rows or []),
        dest_db=_engine(dst),
        source_db=_engine(src),
        row_limit=_limit(getattr(request, "limit", 0)),
        source_filter=_dict(getattr(request, "source_filter", None)),
    )
    return build_report([table], scope="job", subject_id=str(getattr(request, "job_id", "") or ""),
                        require=True, data_rows=len(sample_rows or []), data_source="preflight_sample")


def preflight_gate(report: Mapping[str, Any]) -> dict[str, Any]:
    """The report's gate in the preflight gate shape (``duration_ms``, ``details``)."""
    gate = dict(gate_for(report))
    gate["duration_ms"] = 0
    gate["details"] = {
        "verdict": report.get("verdict"),
        "lossy_columns": list(report.get("lossy_columns") or [])[:50],
    }
    return gate


def execute_gate(request: Any, planned: tuple[Any, Any, Any] | None) -> dict[str, Any] | None:
    """``g24_reversibility`` for Execute when the request requires reversibility, else ``None``."""
    if not _truthy(getattr(request, POLICY, False)):
        return None
    mappings, column_types, dest_types = planned or (None, None, None)
    if column_types is None:
        column_types = dict(getattr(request, "column_types", None) or {})
    return preflight_gate(request_report(request, mappings, column_types, dest_types))


def stamp_plan_requirement(request: Any, policies: Mapping[str, Any] | None) -> None:
    """A plan that requires reversibility makes its Execute require it (never the reverse)."""
    if plan_requires_reversibility(policies):
        request.require_reversibility = True
