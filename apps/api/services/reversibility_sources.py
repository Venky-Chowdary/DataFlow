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


def flag(value: Any) -> bool:
    """``require_reversibility`` as sent: a bool, or a form / query / stored string ("1", "true", "yes", "on")."""
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


def _read_window(source: Any, read_options: Any) -> dict[str, Any]:
    """How the source is read: a query / procedure in place of the table, data rows a file read drops."""
    from services.procedure_source import MODE_QUERY, source_read_mode_of

    try:
        mode = source_read_mode_of(source)
    except Exception:  # noqa: BLE001 - an ambiguous or unknown read mode is not the table: fail closed
        mode = MODE_QUERY
    options = _dict(read_options)
    return {"read_mode": mode, "skip_rows": _limit(options.get("skip_rows")),
            "skip_footer": _limit(options.get("skip_footer"))}


def _catalog_keys_of_endpoint(endpoint: Any) -> list[str]:
    """The source catalog's primary key (the key rows are matched back on when no contract names one)."""
    if endpoint is None or str(getattr(endpoint, "kind", "") or "") != "database":
        return []
    try:
        import copy

        from services.source_schema_authority import endpoint_primary_key_columns

        return list(endpoint_primary_key_columns(copy.deepcopy(endpoint)))
    except Exception:  # noqa: BLE001 - an unread catalog names no key
        return []


def _catalog_keys_of_saved(source: Mapping[str, Any]) -> list[str]:
    """The catalog primary key of a stored source (plan / job) that names its saved connector."""
    connector_id = str(source.get("connector_id") or "").strip()
    table = str(source.get("table") or source.get("collection") or "").strip()
    if not connector_id or not table:
        return []
    try:
        from services.source_schema_authority import live_source_primary_key_columns

        return list(live_source_primary_key_columns(
            source_connector_id=connector_id, source_table=table, source_schema=str(source.get("schema") or "")))
    except Exception:  # noqa: BLE001 - an unread catalog names no key
        return []


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
    return flag(_dict(policies).get(POLICY))


def plan_report(plan: Any) -> dict[str, Any]:
    """The plan tab's ReversibilityReport: active mappings, declared types, the plan sample."""
    rev = plan.active_revision() if plan is not None else None
    policies = _dict(getattr(plan, "policies", None))
    sample = [r for r in getattr(plan, "sample_rows", None) or [] if isinstance(r, Mapping)]
    # The plan's source row count (0 = not known): the sample covers the table only when it holds every row.
    population = _limit(getattr(plan, "row_count_estimate", 0)) or None
    table = analyze_table(
        table=_table(plan.destination, plan.source),
        mappings=list((rev.mappings if rev else None) or []),
        source_columns=list(plan.source_columns or []),
        source_types=dict(plan.source_schema or {}),
        target_types=dict(plan.target_schema or {}),
        recipe_steps=_steps(policies.get("shape_recipe")),
        # The plan's source columns are the read before the recipe (the shaped image is derived).
        recipe_input_columns=_recipe_inputs(policies.get("shape_recipe"), plan.source_columns),
        key_columns=(_key_columns(policies.get("stream_contracts"))
                     or _catalog_keys_of_saved(_dict(plan.source))),
        sample_rows=sample,
        dest_db=_engine(plan.destination),
        source_db=_engine(plan.source),
        row_limit=_limit(policies.get("row_limit")),
        source_filter=_dict(policies.get("source_filter")),
        read_window=_read_window(_dict(plan.source),
                                 _dict(plan.source).get("read_options") or policies.get("read_options")),
        population_rows=population,
    )
    return build_report([table], scope="plan", subject_id=str(plan.id),
                        require=plan_requires_reversibility(policies),
                        data_rows=len(sample), data_source="plan_sample", population_rows=population)


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
        key_columns=_key_columns(req.get("stream_contracts")) or _catalog_keys_of_saved(src),
        dest_db=_engine(dst),
        source_db=_engine(src),
        row_limit=_limit(req.get("limit")),
        source_filter=_dict(req.get("source_filter")),
        read_window=_read_window(src, req.get("read_options") or src.get("read_options")),
    )
    return build_report([table], scope="job", subject_id=job_id,
                        require=flag(req.get(POLICY)), data_rows=0, data_source="none")


def request_report(
    request: Any,
    mappings: list[dict[str, Any]] | None,
    column_types: Mapping[str, str] | None,
    dest_types: Mapping[str, str] | None,
    *,
    sample_rows: list[Any] | None = None,
    population_rows: int | None = None,
) -> dict[str, Any]:
    """Execute's view: the run's planned mappings and types (Validate == Execute).

    ``population_rows`` is the source row count Execute measured (``None``:
    unknown); conditional columns count as fitting only when the sample is the
    whole population.
    """
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
        key_columns=(_key_columns(getattr(request, "stream_contracts", None))
                     or _catalog_keys_of_endpoint(src)),
        sample_rows=list(sample_rows or []),
        dest_db=_engine(dst),
        source_db=_engine(src),
        row_limit=_limit(getattr(request, "limit", 0)),
        source_filter=_dict(getattr(request, "source_filter", None)),
        read_window=_read_window(src, getattr(request, "read_options", None)),
        population_rows=population_rows,
    )
    return build_report([table], scope="job", subject_id=str(getattr(request, "job_id", "") or ""),
                        require=True, data_rows=len(sample_rows or []), data_source="preflight_sample",
                        population_rows=population_rows)


def preflight_gate(report: Mapping[str, Any]) -> dict[str, Any]:
    """The report's gate in the preflight gate shape (``duration_ms``, ``details``)."""
    gate = dict(gate_for(report))
    gate["duration_ms"] = 0
    gate["details"] = {
        "verdict": report.get("verdict"),
        "lossy_columns": list(report.get("lossy_columns") or [])[:50],
    }
    return gate


def execute_gate(request: Any, planned: tuple[Any, ...] | None) -> dict[str, Any] | None:
    """``g24_reversibility`` for Execute when the request requires reversibility, else ``None``."""
    if not flag(getattr(request, POLICY, False)):
        return None
    planned = tuple(planned or ())
    mappings, column_types, dest_types = (planned + (None, None, None))[:3]
    # The design sample Execute read and the source row count it measured (when the caller
    # has them): conditional columns are checked against the sample, and count as fitting
    # only when that sample is every source row (review 2, N2: fail closed).
    sample = [r for r in (planned[3] if len(planned) > 3 else None) or [] if isinstance(r, Mapping)]
    total = planned[4] if len(planned) > 4 else None
    population = None if total is None or isinstance(total, bool) else _limit(total)
    if column_types is None:
        column_types = dict(getattr(request, "column_types", None) or {})
    return preflight_gate(request_report(request, mappings, column_types, dest_types, sample_rows=sample,
                                         population_rows=population))


def stamp_plan_requirement(request: Any, policies: Mapping[str, Any] | None) -> None:
    """A plan that requires reversibility makes its Execute require it (never the reverse)."""
    if plan_requires_reversibility(policies):
        request.require_reversibility = True
