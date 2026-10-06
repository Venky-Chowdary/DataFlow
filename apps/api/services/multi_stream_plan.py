"""One plan for several selected source tables and the procedures that move them.

Several selected tables are N independent streams. A stored procedure is one
CALL/EXEC (source extract) or one CALL per row (destination write). Replaying
one CALL onto every selected table would load the same result set N times —
that is silent duplication, so this module refuses it.

A transform recipe is partitioned by the ``source_table`` stamp the rule
compiler already writes. An untagged step among several tables is refused:
applying it to every stream is a silent remap. ShapeEngine remains the only
executor; this module only decides which steps and which CALL belong to which
stream.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Mapping

from services.rule_compiler.normalize import fold

_HISTORY_SYNCS = frozenset({"cdc", "scd2", "full_refresh_mirror", "mirror"})


def recipe_steps(payload: Any) -> list[dict[str, Any]]:
    """Enabled and disabled steps from a recipe payload. Non-objects are skipped."""
    if payload is None or payload == "" or payload == {}:
        return []
    raw: Any
    if isinstance(payload, Mapping):
        raw = payload.get("steps", [])
    elif isinstance(payload, (list, tuple)):
        raw = payload
    else:
        return []
    if not isinstance(raw, (list, tuple)):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, Mapping):
            out.append(dict(item))
    return out


def enabled_recipe_steps(payload: Any) -> list[dict[str, Any]]:
    return [step for step in recipe_steps(payload) if step.get("enabled", True) is not False]


def _step_label(step: Mapping[str, Any], index: int) -> str:
    op = str(step.get("op") or step.get("type") or "step").strip() or "step"
    column = str(step.get("column") or "").strip()
    where = f"step {index + 1} {op}"
    if column:
        where += f"({column})"
    return where


def _match_name(name: str, streams: list[str]) -> str:
    want = fold(name)
    if not want:
        return ""
    for item in streams:
        if fold(item) == want:
            return item
    return ""


def partition_shape_steps(
    payload: Any,
    stream_names: list[str],
) -> tuple[dict[str, list[dict[str, Any]]], str]:
    """Split a recipe into one step list per selected stream.

    Returns ``(by_stream, refusal)``. ``refusal`` is empty when the split is
    safe. A stream with no steps is absent — that stream is copied unchanged.
    Returned steps drop the table stamps; ShapeEngine does not read them.
    """
    names = [str(name).strip() for name in stream_names if str(name).strip()]
    steps = recipe_steps(payload)
    enabled = [(i, step) for i, step in enumerate(steps) if step.get("enabled", True) is not False]
    if not enabled or not names:
        return {}, ""

    grouped: dict[str, list[dict[str, Any]]] = {}
    if len(names) == 1:
        only = names[0]
        for index, step in enabled:
            tagged = str(step.get("source_table") or "").strip()
            if tagged and not _match_name(tagged, names):
                return {}, (
                    f"{_step_label(step, index)} names source table “{tagged}”, "
                    f"which is not the selected stream ({only}). It was not applied."
                )
            grouped.setdefault(only, []).append(_engine_step(step))
        return grouped, ""

    for index, step in enabled:
        tagged = str(step.get("source_table") or "").strip()
        if not tagged:
            return {}, (
                f"{_step_label(step, index)} has no source table. "
                f"{len(names)} streams are selected ({', '.join(names)}). "
                "Name the table on the step — applying one recipe to every "
                "stream would rewrite unrelated columns."
            )
        owner = _match_name(tagged, names)
        if not owner:
            return {}, (
                f"{_step_label(step, index)} names source table “{tagged}”, "
                f"which is not among the selected streams ({', '.join(names)}). "
                "It was not applied."
            )
        grouped.setdefault(owner, []).append(_engine_step(step))
    return grouped, ""


def _engine_step(step: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in step.items()
        if key not in {"source_table", "dest_table"}
    }


def full_recipe_hash(payload: Any) -> str:
    """Identity of the whole program, including every table's steps.

    ``source_table`` is not part of the hash (ShapeEngine ignores it), so a
    stamped multi-table recipe and the same steps without stamps approve as
    one program. Per-stream hashes differ; Execute checks this one.
    """
    from services.shape_models import ShapeRecipe

    recipe = ShapeRecipe.parse(payload)
    return recipe.recipe_hash


def approved_recipe_refusal(payload: Any, approved_hash: str) -> str:
    """'' when the payload is the program Validate approved."""
    from services.shape_models import ShapeError

    approved = str(approved_hash or "").strip()
    try:
        digest = full_recipe_hash(payload)
    except ShapeError as exc:
        return str(exc)
    if approved and not digest:
        return (
            "this run was approved with a transform recipe "
            f"({approved}), but no recipe was supplied — re-validate before running"
        )
    if approved and digest != approved:
        return (
            f"transform recipe {digest} is not the one approved at Validate "
            f"({approved}) — re-validate the changed recipe before running"
        )
    return ""


def history_shape_refusal(sync_mode: str) -> str:
    sync = (sync_mode or "").strip().lower()
    if sync not in _HISTORY_SYNCS:
        return ""
    return (
        f"Transform (pre-load) is not applied on the {sync} route: it merges "
        "each row against history already stored on the destination, which was "
        "not written by this recipe. Remove the transform recipe for this sync "
        "mode, or use full refresh / incremental append and transform on the read."
    )


def _extra_dict(endpoint: Any) -> dict[str, Any]:
    if endpoint is None:
        return {}
    if isinstance(endpoint, Mapping):
        nested = endpoint.get("extra") if isinstance(endpoint.get("extra"), Mapping) else {}
        return {**dict(nested or {}), **{k: v for k, v in endpoint.items() if k != "extra"}}
    extra = getattr(endpoint, "extra", None)
    if isinstance(extra, dict):
        return extra
    return {}


def _contract_dict(contract: Any) -> dict[str, Any]:
    if isinstance(contract, Mapping):
        return dict(contract)
    return {}


def contract_for_stream(
    contracts: list[Mapping[str, Any]] | None,
    name: str,
) -> dict[str, Any]:
    """The contract for this stream. Case and whitespace do not hide its CALL."""
    rows = [dict(raw) for raw in (contracts or []) if isinstance(raw, Mapping)]
    want = str(name or "").strip()
    for raw in rows:
        if str(raw.get("name") or "").strip() == want:
            return raw
    folded = fold(want)
    if not folded:
        return {}
    for raw in rows:
        if fold(str(raw.get("name") or "")) == folded:
            return raw
    return {}


def _contracts_by_name(
    contracts: list[Mapping[str, Any]] | None,
    names: list[str],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for name in names:
        raw = contract_for_stream(contracts, name)
        if raw:
            out[name] = raw
    return out


def _align_source(mode: str, sql: str) -> tuple[str, str]:
    """The statement decides the mode. A SELECT stamped as a CALL is still a SELECT."""
    from services.procedure_source import leading_statement_kind

    kind = leading_statement_kind(sql) if sql else ""
    if kind in {"procedure", "query", "dest_dml"}:
        return kind, sql
    return mode, sql


def _align_dest(mode: str, sql: str) -> tuple[str, str]:
    """CALL stays a procedure. INSERT/MERGE is a dest query. A general SELECT is not a write."""
    from services.procedure_source import leading_statement_kind

    kind = leading_statement_kind(sql) if sql else ""
    if kind == "dest_dml":
        return "query", sql
    if kind == "procedure":
        return "procedure", sql
    if kind == "query":
        return "select", sql
    return mode, sql


def _source_sql(extra: Mapping[str, Any]) -> tuple[str, str]:
    """Return (mode, sql) for a callable extract, or ('', '').

    An explicit table read is not callable, even when a leftover CALL is
    still on the endpoint. ``dest_dml`` and ``select`` are refusals: that
    statement is not an extract.
    """
    mode = str(extra.get("source_read_mode") or "").strip().lower()
    call = str(extra.get("procedure_call") or extra.get("source_procedure") or "").strip()
    query = str(extra.get("source_query") or "").strip()
    if mode == "table":
        return "", ""
    if mode == "procedure" or (not mode and call):
        if call:
            return _align_source("procedure", call)
    if mode == "query" or (not mode and query and not call):
        if query:
            return _align_source("query", query)
    if mode in {"procedure", "query"}:
        text = call or query
        if text:
            return _align_source(mode, text)
        return mode, ""
    return "", ""


def _dest_sql(extra: Mapping[str, Any]) -> tuple[str, str]:
    """Row-apply mode and its statement. Hooks and plain tables are not a CALL.

    ``select`` means the operator pasted a source extract into the destination.
    """
    from services.procedure_destination import dest_write_mode_of

    mode = dest_write_mode_of(dict(extra))
    if mode == "row_apply":
        sql = str(extra.get("dest_procedure_call") or extra.get("dest_procedure") or "").strip()
        return _align_dest("procedure", sql)
    if mode == "query":
        sql = str(extra.get("dest_query_sql") or extra.get("dest_query") or "").strip()
        return _align_dest("query", sql)
    return "", ""


def _prove_source(mode: str, sql: str, source: Any, params: Mapping[str, Any]) -> str:
    """Refuse a source statement the extract parser will not run. '' when it will."""
    if mode == "dest_dml":
        return (
            "This statement writes rows. A source extract is one CALL or one "
            "read-only SELECT. Put INSERT/MERGE on the destination."
        )
    if mode not in {"procedure", "query"} or not sql:
        return ""
    from services.procedure_source import ProcedureSourceError, dialect_of, parse_callable_source

    try:
        parse_callable_source(
            sql,
            dialect=dialect_of(source),
            mode=mode,
            params=dict(params),
        )
    except ProcedureSourceError as exc:
        return str(exc)
    return ""


def _prove_dest(mode: str, sql: str, destination: Any, contract: Mapping[str, Any]) -> str:
    """Refuse a destination statement the writer will not run. '' when it will."""
    if mode == "select":
        return (
            "A destination statement writes rows: one CALL, or one "
            "INSERT/MERGE/UPDATE. A SELECT is a source extract."
        )
    if mode not in {"procedure", "query"} or not sql:
        return ""
    from services.procedure_destination import ProcedureDestinationError, plan_dest_procedure

    extra: dict[str, Any] = {
        "dest_write_mode": "procedure" if mode == "procedure" else "query",
    }
    if mode == "procedure":
        extra["dest_procedure_call"] = sql
        extra["dest_query_sql"] = ""
    else:
        extra["dest_query_sql"] = sql
        extra["dest_procedure_call"] = ""
    param_map = contract.get("dest_procedure_param_map") or contract.get("procedure_param_map") or {}
    if isinstance(param_map, Mapping) and param_map:
        extra["dest_procedure_param_map"] = dict(param_map)
    params = contract.get("dest_procedure_params")
    if isinstance(params, Mapping) and params:
        extra["dest_procedure_params"] = dict(params)
    probe: dict[str, Any] = {"extra": extra}
    if isinstance(destination, Mapping):
        for key in ("type", "format"):
            if destination.get(key):
                probe[key] = destination.get(key)
    else:
        fmt = getattr(destination, "format", None)
        if fmt:
            probe["format"] = fmt
    try:
        plan_dest_procedure(probe)
    except ProcedureDestinationError as exc:
        return str(exc)
    return ""


def _sync_callable_refusal(sync_mode: str, *, kind: str) -> str:
    from services.sync_cursor import normalize_sync_mode

    sync = normalize_sync_mode(sync_mode, default="")
    if sync not in {"cdc", "scd2", "mirror"}:
        return ""
    if kind == "dest":
        return (
            "Destination CALL row-apply is one statement per row, not a table "
            "identity. CDC / SCD2 / mirror would delete or version rows the "
            "statement never listed — refuse. Use upsert/append, or a table "
            "write with before/after hooks."
        )
    return (
        "Stored-procedure and custom-SQL sources are a result-set snapshot, "
        "not a CDC log. Use Full refresh (or incremental only when the "
        "procedure itself is cursor-stable). CDC stays at-least-once on "
        "table sources until dest-engine exactly-once is proven."
    )


def review_stream_procedures(
    source: Any,
    destination: Any,
    contracts: list[Mapping[str, Any]] | None,
    stream_names: list[str],
    *,
    sync_mode: str = "",
) -> str:
    """Refuse a CALL that would be replayed onto every selected table.

    One stream is the existing source/dest procedure path — this returns ''.
    """
    names = [str(name).strip() for name in stream_names if str(name).strip()]
    if len(names) < 2:
        return ""
    by_name = _contracts_by_name(contracts, names)
    source_extra = _extra_dict(source)
    dest_extra = _extra_dict(destination)
    endpoint_mode, endpoint_sql = _source_sql(source_extra)
    dest_mode, dest_sql = _dest_sql(dest_extra)

    for name in names:
        contract = by_name.get(name) or {}
        stream_sync = str(contract.get("sync_mode") or sync_mode or "")
        mode, sql = _source_sql(contract)
        if not mode and endpoint_mode:
            return (
                f"Stored procedure on the source ({endpoint_sql or endpoint_mode}) "
                f"returns one result set. {name} has no statement of its own, and "
                f"{len(names)} streams are selected ({', '.join(names)}). "
                "Replaying that statement would load the same rows into each "
                "destination. Write a statement on each table, or run the "
                "procedure as a single stream."
            )
        if mode:
            refused = _sync_callable_refusal(stream_sync, kind="source")
            if refused:
                return f"{name}: {refused}"
            if not sql:
                return (
                    f"{name} is set to {mode} but has no CALL or SELECT. "
                    "Paste one statement, or switch that stream back to the table."
                )
            params = contract.get("procedure_params")
            proved = _prove_source(
                mode,
                sql,
                source,
                params if isinstance(params, Mapping) else {},
            )
            if proved:
                return f"{name}: {proved}"

        contract_dest_mode, contract_dest_sql = _dest_sql(contract)
        explicit_table = str(contract.get("dest_write_mode") or "").strip().lower() == "table"
        if not contract_dest_mode and dest_mode and not explicit_table:
            return (
                f"Destination {dest_mode} ({dest_sql or 'row-apply'}) is one "
                f"statement per row of one result set. {name} has no destination "
                f"statement of its own, and {len(names)} streams are selected "
                f"({', '.join(names)}). Name a CALL or INSERT/MERGE on each "
                "stream, or run one stream."
            )
        if contract_dest_mode:
            refused = _sync_callable_refusal(stream_sync, kind="dest")
            if refused:
                return f"{name}: {refused}"
            if not contract_dest_sql:
                return (
                    f"{name} destination write is {contract_dest_mode} but the "
                    "statement is empty. Paste one CALL or one INSERT/MERGE."
                )
            proved = _prove_dest(contract_dest_mode, contract_dest_sql, destination, contract)
            if proved:
                return f"{name}: {proved}"
    return ""


def patches_for_stream(
    contract: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Extra patches that make this stream's CALL win over the endpoint's.

    ``None`` means leave the endpoint extra untouched. A table stream under a
    callable endpoint is not produced here — ``review_stream_procedures``
    refuses that shape before any patch is applied.
    """
    raw = _contract_dict(contract)
    source_patch: dict[str, Any] | None = None
    mode, sql = _source_sql(raw)
    if mode in {"procedure", "query"} and sql:
        params = raw.get("procedure_params") if isinstance(raw.get("procedure_params"), Mapping) else {}
        source_patch = {
            "source_read_mode": mode,
            "procedure_call": sql if mode == "procedure" else "",
            "source_query": sql if mode == "query" else "",
            "procedure_params": dict(params),
        }

    dest_patch: dict[str, Any] | None = None
    dest_mode, dest_sql = _dest_sql(raw)
    explicit_table = str(raw.get("dest_write_mode") or "").strip().lower() == "table"
    if explicit_table and not dest_mode:
        dest_patch = {"dest_write_mode": "table"}
    elif dest_mode in {"procedure", "query"} and dest_sql:
        dest_patch = {"dest_write_mode": "procedure" if dest_mode == "procedure" else "query"}
        if dest_mode == "procedure":
            dest_patch["dest_procedure_call"] = dest_sql
            dest_patch["dest_query_sql"] = ""
        else:
            dest_patch["dest_query_sql"] = dest_sql
            dest_patch["dest_procedure_call"] = ""
        param_map = raw.get("dest_procedure_param_map") or raw.get("procedure_param_map") or {}
        if isinstance(param_map, Mapping) and param_map:
            dest_patch["dest_procedure_param_map"] = dict(param_map)
        params = raw.get("dest_procedure_params") if isinstance(raw.get("dest_procedure_params"), Mapping) else {}
        if params:
            dest_patch["dest_procedure_params"] = dict(params)
    return source_patch, dest_patch


def adopt_inherited_mappings(
    mappings: list[Mapping[str, Any]] | None,
    columns: list[str],
) -> tuple[list[dict[str, Any]], str]:
    """Use another table's map only when it names exactly these columns.

    Customers mapped as ``id, email`` applied to orders would omit ``amount``.
    That is silent column loss. A mismatch returns an identity map of this
    stream's own columns (each name written to itself). An empty inherited
    list is the same identity map, with no note.
    """
    cols = [str(c).strip() for c in columns or [] if str(c).strip()]
    comparable = [c for c in cols if fold(c) != fold("_df_lsn")]
    mapped: set[str] = set()
    for item in mappings or []:
        if not isinstance(item, Mapping):
            continue
        src = str(item.get("source") or "").strip()
        if src and fold(src) != fold("_df_lsn"):
            mapped.add(fold(src))
    have = {fold(c) for c in comparable}
    identity = [
        {"source": c, "target": c, "confidence": 0.95}
        for c in comparable
    ]
    if mapped and mapped == have:
        return [dict(item) for item in (mappings or []) if isinstance(item, Mapping)], ""
    if not mapped:
        return identity, ""
    missing = sorted(have - mapped)
    extra = sorted(mapped - have)
    parts: list[str] = []
    if missing:
        parts.append("omits " + ", ".join(missing))
    if extra:
        parts.append("names " + ", ".join(extra) + ", which this stream does not have")
    note = (
        "Inherited column map was not applied to this stream ("
        + "; ".join(parts)
        + "). Each column is written under its own name."
    )
    return identity, note


def endpoint_session_hooks(endpoint: Any) -> tuple[str, str]:
    """The destination's before/after statements. Empty when this dest has none.

    One Advanced field is one session, not one execution per selected table.
    """
    extra = _extra_dict(endpoint)
    return (
        str(extra.get("dest_procedure_before") or "").strip(),
        str(extra.get("dest_procedure_after") or "").strip(),
    )


def strip_session_hooks(patch: Mapping[str, Any] | None) -> dict[str, Any]:
    """Clear session hooks on a per-stream extra patch so the chunk writer cannot replay them."""
    out = dict(patch or {})
    out["dest_procedure_before"] = ""
    out["dest_procedure_after"] = ""
    return out


def design_source_patch(
    contracts: list[Mapping[str, Any]] | None,
    stream_names: list[str],
) -> dict[str, Any] | None:
    """The primary stream's extract, when several tables are selected.

    Design-time peek, Map, and preflight read one sample. That sample has to
    be this stream's statement when it has one — the table behind a
    ``SELECT`` is not the result the writer will map. Other streams are
    peeked on their own read. One stream is the existing endpoint path.
    """
    names = [str(name).strip() for name in stream_names if str(name).strip()]
    if len(names) < 2:
        return None
    source_patch, _dest = patches_for_stream(contract_for_stream(contracts, names[0]))
    return source_patch


@contextmanager
def patched_endpoint_extra(endpoint: Any, patch: Mapping[str, Any] | None) -> Iterator[None]:
    """Apply ``patch`` onto ``endpoint.extra`` for one stream, then restore it.

    A test double whose ``extra`` is not a dict is left alone when there is
    nothing to apply. A real CALL still gets a dict so the reader sees it.
    """
    if not patch:
        yield
        return
    current = getattr(endpoint, "extra", None)
    if isinstance(current, dict):
        saved = dict(current)
        current.update(patch)
        try:
            yield
        finally:
            current.clear()
            current.update(saved)
        return
    endpoint.extra = dict(patch)
    try:
        yield
    finally:
        endpoint.extra = current if isinstance(current, dict) else {}
