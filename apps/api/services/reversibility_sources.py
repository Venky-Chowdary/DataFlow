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
            "skip_footer": _limit(options.get("skip_footer")),
            # A header taken from row N > 1 leaves the N - 1 rows above it out (review 3, Low).
            "header_row": _limit(options.get("header_row"))}


#: A Parity Studio key re-sync restores named keys of its parent job: it is a repair of rows already
#: decided, so the row-change rule (its key filter, its row cap) does not apply to it (review 3, M5).
ROW_RULE_EXEMPTION = "reversibility_row_rule_exemption"
PARITY_KEY_RESYNC = "parity_key_resync"


def row_rule_exempt(request: Any) -> bool:
    """A Parity key re-sync child: its source filter names the keys to restore, by design."""
    if isinstance(request, Mapping):
        return (str(request.get(ROW_RULE_EXEMPTION) or "") == PARITY_KEY_RESYNC
                and bool(str(request.get("parent_job_id") or "").strip()))
    return (str(getattr(request, ROW_RULE_EXEMPTION, "") or "") == PARITY_KEY_RESYNC
            and bool(str(getattr(request, "parent_job_id", "") or "").strip()))


#: File exports whose every value keeps its own text (JSON strings, exact numbers, null distinct from
#: ""): the file holds each column as the source typed it. CSV / TSV (and other file formats) write
#: NULL and empty text alike and drop the type, so their columns stay ``type_unknown``.
_EXACT_TEXT_EXPORTS = frozenset({"jsonl", "ndjson"})


def _file_export_view(dst: Any, mappings: list[Any], source_types: Mapping[str, str],
                      dest_types: Mapping[str, str] | None) -> tuple[list[Any], dict[str, str]]:
    """(mappings, destination types) as a file export holds them.

    A file has no column types of its own, so a type Map stamped on its
    mapping says nothing about what the file keeps. A JSON Lines export keeps
    each value exactly, so a column's destination type is its source type (a
    re-serialised ``json`` is ``jsonb``: key order and duplicates are not
    kept); any other format is ``type_unknown`` (fail closed).
    """
    from services.reversibility_types import logical

    kind = str((dst.get("kind") if isinstance(dst, Mapping) else getattr(dst, "kind", "")) or "").lower()
    if kind != "file_export":
        return mappings, dict(dest_types or {})
    rows = [{k: v for k, v in m.items() if k != "target_type"} if isinstance(m, Mapping) else m for m in mappings]
    if _engine(dst) not in _EXACT_TEXT_EXPORTS:
        return rows, {}
    out: dict[str, str] = {}
    for m in rows:
        if not isinstance(m, Mapping) or not str(m.get("source") or "").strip():
            continue
        source = str(m["source"]).strip()
        stype = str(m.get("source_type") or source_types.get(source) or "")
        if stype:
            target = str(m.get("target") or source).strip()
            out[target] = "JSONB" if logical(stype) == "json" and stype.strip().lower() != "jsonb" else stype
    return rows, out


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


def _enforce(require: bool, how: str) -> str | None:
    """How conditional columns are held to their condition: only a run that must be reversible enforces."""
    return how if require else None


def _streams(contracts: Any) -> tuple[list[str], Any]:
    from services.multi_stream_plan import contract_for_stream
    from services.reversibility_streams import selected_stream_names

    names = selected_stream_names(contracts)
    return names, (lambda name: contract_for_stream(contracts, name))


def _primary(raw: Mapping[str, Any], shared: list[Any], source: Any, read_options: Any,
             catalog_keys: Any) -> tuple[list[Any], list[str], dict[str, Any]]:
    """(mappings, key, read window) of the first selected stream: its own contract wins over the run's."""
    from services.reversibility_streams import _window, own_mappings, stream_keys, stream_read_mode

    window = _read_window(source, read_options)
    mode = stream_read_mode(raw) if raw else "table"
    if mode != "table":
        window = {**window, **{k: v for k, v in _window(raw, read_options, mode).items() if k == "read_mode"}}
    return (own_mappings(raw) or list(shared)), (stream_keys(raw) or list(catalog_keys() or [])), window


def plan_report(plan: Any) -> dict[str, Any]:
    """The plan tab's ReversibilityReport: active mappings, declared types, the plan sample.

    One table per selected stream (review 3, C1). The plan's row count is a
    client-supplied estimate, so the sample never claims to cover the table
    here (``covers_population`` false, ``population_rows`` null; review 3, M4):
    a plan that must be reversible has its conditional columns checked on the
    sample and enforced on every row at Execute.
    """
    from services.reversibility import AT_EXECUTE
    from services.reversibility_streams import declared_stream_table

    rev = plan.active_revision() if plan is not None else None
    policies = _dict(getattr(plan, "policies", None))
    require = plan_requires_reversibility(policies)
    sample = [r for r in getattr(plan, "sample_rows", None) or [] if isinstance(r, Mapping)]
    names, contract_of = _streams(policies.get("stream_contracts"))
    several = len(names) > 1
    source = _dict(plan.source)
    read_options = source.get("read_options") or policies.get("read_options")
    steps = _steps(policies.get("shape_recipe"))
    by_stream = _steps_by_stream(policies.get("shape_recipe"), names) if several else {}
    first = contract_of(names[0]) if names else {}
    mappings, keys, window = _primary(first, list((rev.mappings if rev else None) or []), source, read_options,
                                      lambda: _catalog_keys_of_saved(source))
    common = dict(dest_db=_engine(plan.destination), source_db=_engine(plan.source),
                  row_limit=_limit(policies.get("row_limit")), source_filter=_dict(policies.get("source_filter")))
    tables = [analyze_table(
        table=(names[0] if several else _table(plan.destination, plan.source)),
        mappings=mappings,
        source_columns=list(plan.source_columns or []),
        source_types=dict(plan.source_schema or {}),
        target_types=dict(plan.target_schema or {}),
        recipe_steps=by_stream.get(names[0], steps) if several else steps,
        # The plan's source columns are the read before the recipe (the shaped image is derived).
        recipe_input_columns=_recipe_inputs(policies.get("shape_recipe"), plan.source_columns),
        key_columns=keys, sample_rows=sample, read_window=window,
        population_rows=None, enforce=_enforce(require, AT_EXECUTE), **common,
    )]
    for name in names[1:] if several else []:
        tables.append(declared_stream_table(
            name, contract_of(name), source=source, read_options=read_options,
            recipe_steps=by_stream.get(name, []), enforce=_enforce(require, AT_EXECUTE), **common))
    return build_report(tables, scope="plan", subject_id=str(plan.id), require=require,
                        data_rows=len(sample), data_source="plan_sample", population_rows=None)


def _steps_by_stream(recipe: Any, names: list[str]) -> dict[str, list[dict[str, Any]]]:
    """A multi-table recipe split per stream; steps that cannot be assigned count against every stream."""
    from services.multi_stream_plan import partition_shape_steps

    steps = _steps(recipe)
    if not steps:
        return {}
    try:
        grouped, refusal = partition_shape_steps(recipe, names)
    except Exception:  # noqa: BLE001 - an unreadable recipe is not assumed harmless
        grouped, refusal = {}, "unreadable"
    if refusal:
        return {name: list(steps) for name in names}
    return {name: list(grouped.get(name) or []) for name in names}


def job_report(job: dict[str, Any], job_id: str) -> dict[str, Any]:
    """A job's ReversibilityReport from its stored request (no rows are read)."""
    from services.parity_evidence import dest_types_of, mappings_of, request_of
    from services.reversibility import WHILE_WRITING
    from services.reversibility_enforce import SUMMARY_KEY
    from services.reversibility_streams import declared_stream_table

    req = request_of(job)
    src, dst = _dict(req.get("source")), _dict(req.get("destination"))
    require = flag(req.get(POLICY))
    source_types = {str(k): str(v) for k, v in _dict(req.get("column_types")).items()}
    names, contract_of = _streams(req.get("stream_contracts"))
    several = len(names) > 1
    read_options = req.get("read_options") or src.get("read_options")
    exempt = row_rule_exempt(req)
    common = dict(dest_db=_engine(dst), source_db=_engine(src),
                  row_limit=0 if exempt else _limit(req.get("limit")),
                  source_filter={} if exempt else _dict(req.get("source_filter")))
    # The destination types the run recorded (Map stamps, the DDL it ran, Gate-8's digest types).
    first = contract_of(names[0]) if names else {}
    mappings, keys, window = _primary(first, mappings_of(job), src, read_options,
                                      lambda: _catalog_keys_of_saved(src))
    rows, target_types = _file_export_view(dst, mappings, source_types, dest_types_of(job))
    by_stream = _steps_by_stream(req.get("shape_recipe"), names) if several else {}
    steps = _steps(req.get("shape_recipe"))
    summary = _dict(job.get("destination_summary"))
    tables = [analyze_table(
        table=(names[0] if several else _table(dst, src)),
        mappings=rows,
        source_columns=list(source_types.keys()),
        source_types=source_types,
        target_types=target_types,
        recipe_steps=by_stream.get(names[0], steps) if several else steps,
        recipe_input_columns=_recipe_inputs(
            req.get("shape_recipe"), _dict(summary.get("shape_proof")).get("input_columns")),
        key_columns=keys, read_window=window,
        enforce=_enforce(require, WHILE_WRITING), **common,
    )]
    for name in names[1:] if several else []:
        tables.append(declared_stream_table(
            name, contract_of(name), source=src, read_options=read_options,
            recipe_steps=by_stream.get(name, []), enforce=_enforce(require, WHILE_WRITING), **common))
    report = build_report(tables, scope="job", subject_id=job_id,
                          require=require, data_rows=0, data_source="none")
    enforcement = summary.get(SUMMARY_KEY)
    if isinstance(enforcement, dict):
        # What the run's own transform stage checked, row by row (rows per enforced column).
        report["data_checked"]["enforcement"] = enforcement
    if exempt:
        report["row_rule_exemption"] = PARITY_KEY_RESYNC
    return report


def _request_tables(
    request: Any,
    mappings: list[dict[str, Any]] | None,
    column_types: Mapping[str, str] | None,
    dest_types: Mapping[str, str] | None,
    *,
    sample_rows: list[Any],
    population_rows: int | None,
    enforce: str | None,
    live: bool,
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], list[Any] | None]]]:
    """The run's tables, and for each the mapping dicts the writer will read (``None``: not reachable).

    ``live`` (Execute) reads every non-primary stream the way the sequential
    writer will; otherwise (Validate) they are analysed from what is declared.
    """
    from services.reversibility import UNAVAILABLE, WHILE_WRITING
    from services.reversibility_streams import declared_stream_table, execute_stream_table

    src, dst = getattr(request, "source", None), getattr(request, "destination", None)
    contracts = getattr(request, "stream_contracts", None)
    names, contract_of = _streams(contracts)
    several = len(names) > 1
    read_options = getattr(request, "read_options", None)
    recipe = getattr(request, "shape_recipe", None)
    by_stream = _steps_by_stream(recipe, names) if several else {}
    exempt = row_rule_exempt(request)
    common = dict(dest_db=_engine(dst), source_db=_engine(src),
                  row_limit=0 if exempt else _limit(getattr(request, "limit", 0)),
                  source_filter={} if exempt else _dict(getattr(request, "source_filter", None)))
    first = contract_of(names[0]) if names else {}
    shared = list(mappings or getattr(request, "mappings", None) or [])
    declared_first = bool(several and first and isinstance(first.get("mappings"), list) and first.get("mappings"))
    primary_maps, keys, window = _primary(first if several else {}, shared, src, read_options,
                                          lambda: _catalog_keys_of_endpoint(src))
    if not several:
        keys = _key_columns(contracts) or _catalog_keys_of_endpoint(src)
    rows, target_types = _file_export_view(dst, primary_maps, dict(column_types or {}), dest_types)
    tables = [analyze_table(
        table=(names[0] if several else _table(dst, src)),
        mappings=rows,
        source_columns=list((column_types or {}).keys()),
        source_types=dict(column_types or {}),
        target_types=target_types,
        recipe_steps=by_stream.get(names[0], _steps(recipe)) if several else _steps(recipe),
        recipe_input_columns=_recipe_inputs(recipe, getattr(request, _RECIPE_INPUTS, None)),
        key_columns=keys, sample_rows=list(sample_rows or []), read_window=window,
        population_rows=population_rows, enforce=enforce, **common,
    )]
    # The dicts the writer reads for the first stream: its own contract's, else the run's planned list.
    writers: list[tuple[dict[str, Any], list[Any] | None]] = [(tables[0], primary_maps)]
    for name in names[1:] if several else []:
        raw = contract_of(name)
        steps = by_stream.get(name, [])
        if not live:
            table = declared_stream_table(name, raw, source=_dict(src) if isinstance(src, Mapping) else {},
                                          read_options=read_options, recipe_steps=steps, enforce=enforce, **common)
            tables.append(table)
            writers.append((table, None))
            continue

        def analyse(design: dict[str, Any], *, name: str = name, raw: Mapping[str, Any] = raw,
                    steps: list[dict[str, Any]] = steps) -> dict[str, Any]:
            from services.reversibility_streams import _window, stream_keys, stream_read_mode

            maps, types = _file_export_view(design["destination"], design["mappings"], design["column_types"],
                                            design["dest_types"])
            total = design["total_rows"]
            stampable = design["stamped"] is not None
            how = enforce if (stampable or enforce != WHILE_WRITING) else UNAVAILABLE
            return analyze_table(
                table=name, mappings=maps, source_columns=list(design["column_types"]),
                source_types=design["column_types"], target_types=types, recipe_steps=steps,
                recipe_input_columns=list(design["column_types"]) if steps else None,
                key_columns=stream_keys(raw) or _catalog_keys_of_endpoint(design["source"]),
                sample_rows=[r for r in design["sample_rows"] if isinstance(r, Mapping)],
                read_window=_window(raw, read_options, stream_read_mode(raw)),
                population_rows=None if total is None or isinstance(total, bool) else _limit(total),
                enforce=how, **common)

        table, design = execute_stream_table(request, name, raw, shared, analyse)
        tables.append(table)
        writers.append((table, (design or {}).get("stamped")))
    if several and not declared_first and live:
        # The first stream runs on the run's planned mappings (the list the engine hands the writer).
        writers[0] = (tables[0], shared)
    return tables, writers


def request_report(
    request: Any,
    mappings: list[dict[str, Any]] | None,
    column_types: Mapping[str, str] | None,
    dest_types: Mapping[str, str] | None,
    *,
    sample_rows: list[Any] | None = None,
    population_rows: int | None = None,
    enforce: str | None = None,
    live: bool = False,
    stamp_writers: bool = False,
) -> dict[str, Any]:
    """Execute's view: the run's planned mappings and types (Validate == Execute).

    ``population_rows`` is the source row count the server measured (``None``:
    unknown). ``enforce`` says how conditional columns are held to their
    condition (``at_execute`` for a Validate, ``while_writing`` for the run
    itself); with ``stamp_writers`` every enforced mapping is marked for the
    writer's transform stage (:mod:`services.reversibility_enforce`).
    """
    rows = list(sample_rows or [])
    tables, writers = _request_tables(request, mappings, column_types, dest_types, sample_rows=rows,
                                      population_rows=population_rows, enforce=enforce, live=live)
    if stamp_writers:
        for table, dicts in writers:
            _stamp_enforced(table, dicts)
    report = build_report(tables, scope="job", subject_id=str(getattr(request, "job_id", "") or ""),
                          require=True, data_rows=len(rows), data_source="preflight_sample",
                          population_rows=population_rows)
    if row_rule_exempt(request):
        report["row_rule_exemption"] = PARITY_KEY_RESYNC
    return report


def _stamp_enforced(table: dict[str, Any], dicts: list[Any] | None) -> None:
    """Mark each enforced conditional column of ``table`` on the mapping dict the writer reads."""
    from services.reversibility import enforced
    from services.reversibility_enforce import stamp

    by_source = {str(m.get("source") or "").strip().lower(): m for m in dicts or [] if isinstance(m, dict)}
    for column in table["columns"]:
        if not enforced(column):
            continue
        mapping = by_source.get(str(column.get("source") or "").lower())
        stamp(mapping, table=table["name"], column=str(column.get("source") or column.get("target") or ""),
              code=str(column.get("reason_code") or ""), transform=", ".join(column.get("transforms") or []),
              target_type=str(column.get("target_type") or ""))


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
    # The design sample Execute read and the source row count it measured (when the caller has
    # them): conditional columns are checked against the sample here, and then on every row by the
    # writer's transform stage, which stops the run on the first value that would change (review
    # 3, M3) - a row committed after this gate, or past the first page, is checked like any other.
    sample = [r for r in (planned[3] if len(planned) > 3 else None) or [] if isinstance(r, Mapping)]
    total = planned[4] if len(planned) > 4 else None
    population = None if total is None or isinstance(total, bool) else _limit(total)
    if column_types is None:
        column_types = dict(getattr(request, "column_types", None) or {})
    from services.reversibility import WHILE_WRITING

    gate = preflight_gate(request_report(request, mappings, column_types, dest_types, sample_rows=sample,
                                         population_rows=population, enforce=WHILE_WRITING, live=True,
                                         stamp_writers=True))
    if gate.get("status") == "pass":
        # An append or upsert commits batch by batch: its enforced columns are checked on the whole
        # source population before anything is written (review 4, M-1).
        from services.reversibility_prescan import prescan

        found = prescan(request, mappings, column_types, dest_types, held=sample, population=population)
        if found:
            gate.update(status="block", message=found)
    return gate


def stamp_plan_requirement(request: Any, policies: Mapping[str, Any] | None) -> None:
    """A plan that requires reversibility makes its Execute require it (never the reverse)."""
    if plan_requires_reversibility(policies):
        request.require_reversibility = True
