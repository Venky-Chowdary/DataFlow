"""Oracle stores ``''`` as NULL: a NOT NULL text column cannot take an empty string.

Owner decision 4 (D210) writes ``''`` to Oracle as NULL. Into a NOT NULL
column that is ORA-01400, and it used to surface only at Execute, after
Validate had passed (oracle-live CI, ``test_postgresql_to_oracle_typed_preflight_on``).
No value may change silently (docs/migration-rules.md), so Validate decides,
the same way on every entry point (REST and Studio Validate, MCP, Execute):

* the candidates are the text mappings into an Oracle destination whose column
  will be NOT NULL: on create-new, a source column the source catalog proves
  NOT NULL (the gate reads the catalog itself when its caller did not pass
  it, and the source primary key with it); on an existing table, a
  destination column the catalog proves NOT NULL. Each mapping is its own
  candidate, keyed by (source, target);
* the empty strings are counted exactly where that is possible: a SQL source
  with no transform on the column is asked ``COUNT(*) WHERE col = ''``
  (bounded by a statement timeout); otherwise the rows Execute will read are
  walked (the fit scan's scope: incremental window, row filter, shape recipe,
  stored upload, the batch Execute already holds), after the mapping's
  transform, bounded by rows and time;
* none, exactly: no gate. Some, or a count that could not be made exact (a
  preview-only source, a scan that stopped, a read error): on a create-new
  NOT NULL column the run is blocked, named in plain words ("could not rule
  out empty strings"). A scan error never crashes Validate.

On create-new there is one approvable option, "Create <col> as NULL-able;
empty strings land as NULL", recorded as a signed Migration Risk Contract on
the mapping (``empty_as_null_contract``; ``risk_acceptance`` with ``option``
:data:`OPTION`), bound to its workspace, destination table and (source,
target) pair. The table is then created with that column NULL-able, Gate-8
reports the rows this run wrote as explained by the contract
(:func:`explain_empty_as_null`), and the signed proof pack lists the
contract. A key column (contract, destination or source primary key) is never
approvable. An existing NOT NULL column is never altered: the gate blocks
and suggests a DEFAULT on the column or a transform that writes a non-empty
value.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable, Mapping
from typing import Any

logger = logging.getLogger(__name__)

GATE_ID = "g3o_oracle_empty_not_null"
OPTION = "oracle_empty_as_null"
RISK_KIND = "oracle_empty_string_as_null"
CONTRACT_KEY = "empty_as_null_contract"
#: Bounds of the walk, like the other population walks Validate runs.
SCAN_MAX_ROWS = 1_000_000
SCAN_MAX_SECONDS = 60.0
#: Statement timeout of the SQL ``COUNT(*) WHERE col = ''`` push-down.
PUSHDOWN_TIMEOUT_SECONDS = 30.0
_RUN_SCOPES = {"table_written_by_run", "run_keys"}


def is_oracle(db_type: str | None) -> bool:
    return "oracle" in str(db_type or "").strip().lower()


def _lookup(mapping: Mapping[str, Any] | None, name: str) -> Any:
    if not mapping or not name:
        return None
    if name in mapping:
        return mapping[name]
    lower = name.lower()
    return next((v for k, v in mapping.items() if str(k).lower() == lower), None)


def _bare(table: Any) -> str:
    return str(table or "").strip().rsplit(".", 1)[-1].strip().strip('"').lower()


def pair_key(source: str, target: str) -> str:
    """One mapping's bookkeeping key: the same source may feed two targets."""
    return f"{source}->{target}"


def _is_text(mapping: Mapping[str, Any], column_types: Mapping[str, str] | None) -> bool:
    from services.type_system import normalize_logical_type

    declared = str(mapping.get("target_type") or "").strip()
    if not declared:
        declared = str(_lookup(column_types, str(mapping.get("source") or "")) or "")
    return normalize_logical_type(declared) in {"string", "text"} if declared else False


def _no_transform(transform: str) -> bool:
    return not transform or transform.strip().lower() in {"none", "identity"}


def current_workspace() -> str:
    try:
        from services.transform_engine import _current_workspace

        return str(_current_workspace() or "")
    except Exception:  # noqa: BLE001 - no bound workspace is "unknown", never a refusal
        return ""


def candidates(
    *,
    destination_db_type: str,
    mappings: Iterable[Mapping[str, Any]] | None,
    column_nullability: Mapping[str, bool] | None,
    destination_table_exists: bool | None,
    destination_column_nullability: Mapping[str, bool] | None,
    column_types: Mapping[str, str] | None = None,
    key_columns: Iterable[str] = (),
) -> list[dict[str, Any]]:
    """Text mappings whose Oracle column will be NOT NULL, with ``kind`` and ``approvable``."""
    if not is_oracle(destination_db_type):
        return []
    keys = {str(k).strip().lower() for k in key_columns or () if str(k).strip()}
    out: list[dict[str, Any]] = []
    for m in mappings or []:
        if not isinstance(m, Mapping):
            continue
        source, target = str(m.get("source") or ""), str(m.get("target") or "")
        if not source or not target or not _is_text(m, column_types):
            continue
        if destination_table_exists is False:
            if _lookup(column_nullability, source) is not False:
                continue
            kind = "create_new"
        elif destination_table_exists and _lookup(destination_column_nullability, target) is False:
            kind = "existing"
        else:
            continue
        out.append({
            "key": pair_key(source, target),
            "source": source,
            "target": target,
            "transform": str(m.get("transform") or ""),
            "kind": kind,
            # A key column stays NOT NULL whatever is approved.
            "approvable": kind == "create_new"
            and source.lower() not in keys and target.lower() not in keys,
        })
    return out


def _written(cell: Any, transform: str) -> Any:
    if _no_transform(transform):
        return cell
    from services.transform_engine import apply_transform
    from services.value_serializer import is_reader_null_cell, wire_cell_text

    if is_reader_null_cell(cell):
        return None
    value, err = apply_transform(wire_cell_text(cell), transform)
    return None if err is not None else value


def count_empty_strings(
    rows: Iterable[Mapping[str, Any]] | None,
    targets: list[dict[str, Any]],
    *,
    max_rows: int | None = None,
    max_seconds: float | None = None,
) -> dict[str, Any]:
    """Empty strings each target will be sent, over at most ``max_rows`` rows.

    A read error mid-walk (the walk is lazy) ends the scan with ``truncated``
    ``"error"``: Validate reports "could not check", it never crashes.
    """
    counts = {t["key"]: 0 for t in targets}
    scanned = 0
    truncated = ""
    error = ""
    max_rows = SCAN_MAX_ROWS if max_rows is None else max_rows
    deadline = time.monotonic() + (SCAN_MAX_SECONDS if max_seconds is None else max_seconds)
    try:
        for row in rows or ():
            if scanned >= max_rows:
                truncated = "rows"
                break
            if scanned % 1000 == 0 and time.monotonic() > deadline:
                truncated = "time"
                break
            scanned += 1
            for t in targets:
                value = _written(_lookup(row, t["source"]), t["transform"])
                if isinstance(value, str) and value == "":
                    counts[t["key"]] += 1
    except Exception as exc:  # noqa: BLE001 - an unreadable source is "could not check"
        logger.warning("Oracle empty-string scan stopped: %s", exc, exc_info=exc)
        truncated, error = "error", f"{type(exc).__name__}: {str(exc)[:200]}"
    return {"counts": counts, "rows_scanned": scanned, "truncated": truncated, "error": error}


def approved_contract(
    mapping: Mapping[str, Any] | None,
    *,
    table: str = "",
    workspace: str | None = None,
) -> dict[str, Any] | None:
    """The mapping's signed, intact empty-as-NULL contract, or ``None``.

    The contract is bound to what it was signed for: the (source, target)
    pair, the destination table and the workspace. One pasted onto another
    column, signed for another table or carried into another workspace is
    not an approval.
    """
    raw = (mapping or {}).get(CONTRACT_KEY) if isinstance(mapping, Mapping) else None
    if not isinstance(raw, dict):
        return None
    from services.migration_risk_contract import verify_risk_contract

    if str(raw.get("loss_classification") or "") != RISK_KIND or not verify_risk_contract(raw):
        return None
    source, target = str(mapping.get("source") or ""), str(mapping.get("target") or "")
    if str(raw.get("column") or "").lower() != source.lower():
        return None
    if str(raw.get("target") or "").lower() != (target or source).lower():
        return None
    if table and str(raw.get("table") or "") and _bare(raw.get("table")) != _bare(table):
        return None
    bound = str((raw.get("metadata") or {}).get("workspace_id") or "")
    here = current_workspace() if workspace is None else str(workspace or "")
    if bound and here and bound != here:
        return None
    return raw


def _measured(t: Mapping[str, Any]) -> str:
    how = t.get("how") or ""
    if how == "pushdown":
        return "counted by the source database"
    if how in {"population", "batch"}:
        return "every source row Execute will read"
    return "preview rows only"


def _unknown_why(t: Mapping[str, Any]) -> str:
    why = str(t.get("why") or "")
    if why == "preview":
        return "only a preview of the source could be read"
    if why == "rows":
        return f"the scan stopped after {int(t.get('rows_scanned') or 0):,} rows"
    if why == "time":
        return f"the scan stopped after {SCAN_MAX_SECONDS:g}s ({int(t.get('rows_scanned') or 0):,} rows)"
    return f"the source could not be read ({t.get('error') or why or 'unknown error'})"


def build_gate(
    targets: list[dict[str, Any]],
    *,
    mappings: Iterable[Mapping[str, Any]] | None,
    table: str = "",
    workspace: str | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """``(gate, blockers)`` from targets carrying ``count``, ``exact`` and ``how``.

    No gate when every count is an exact zero. A create-new NOT NULL column
    whose count is not exact is blocked: "could not rule out" is a decision
    the operator makes, never a pass.
    """
    by_pair = {
        pair_key(str(m.get("source") or ""), str(m.get("target") or "")): m
        for m in mappings or [] if isinstance(m, Mapping)
    }
    blocked, approved, unproven_existing = [], [], []
    for t in targets:
        found = int(t.get("count") or 0) > 0
        unknown = not t.get("exact") and not found
        if not found and not unknown:
            continue
        if t["kind"] == "existing" and unknown:
            unproven_existing.append(t)
            continue
        contract = (
            approved_contract(by_pair.get(t["key"]), table=table, workspace=workspace)
            if t["approvable"] else None
        )
        if contract:
            approved.append({**t, "risk_id": contract.get("risk_id"),
                             "approved_by": contract.get("approved_by")})
        else:
            blocked.append(t)
    if not blocked and not approved and not unproven_existing:
        return None, []
    details = {"columns": blocked, "approved": approved, "unproven": unproven_existing,
               "option": OPTION}
    if not blocked:
        parts = [
            f"{a['target']}: {a['count'] if a.get('count') else 'any'} empty string(s) from "
            f"{a['source']} land as NULL, created NULL-able under signed Risk Contract "
            f"{a['risk_id']}" for a in approved
        ] + [
            f"{u['target']} is an existing NOT NULL column and Datawrap could not rule out "
            f"empty strings in {u['source']} ({_unknown_why(u)}); one would fail the write "
            "(ORA-01400), nothing is written as a different value" for u in unproven_existing
        ]
        return {"id": GATE_ID, "status": "warn" if unproven_existing else "pass",
                "message": "Oracle stores '' as NULL. " + "; ".join(parts) + ".",
                "duration_ms": 0, "details": details}, []
    actions: list[dict[str, Any]] = []
    approvable = [b for b in blocked if b["approvable"]]
    if approvable:
        actions.append({
            "id": OPTION,
            "kind": OPTION,
            "label": "; ".join(
                f"Create {b['target']} as NULL-able; empty strings land as NULL" for b in approvable
            ),
            "risk_acceptance": {
                "option": OPTION, "columns": [b["target"] for b in approvable],
                "approved_by": "<who approves>", "reason": "<why>",
            },
        })
    for b in blocked:
        if not b["approvable"]:
            actions.append({
                "id": "oracle_not_null_fill",
                "kind": "oracle_not_null_fill",
                "label": (
                    f"Give {b['target']} a DEFAULT on the Oracle table, or map a transform "
                    f"that writes a non-empty value for {b['source']}'s empty strings"
                    if b["kind"] == "existing"
                    else f"{b['target']} is a key column: map a transform that writes a "
                    "non-empty value for its empty strings"
                ),
            })

    def _part(b: Mapping[str, Any]) -> str:
        what = "existing NOT NULL column" if b["kind"] == "existing" else "NOT NULL from the source"
        if int(b.get("count") or 0) > 0:
            return (f"{b['target']} ({what}) would receive {b['count']} empty string(s) from "
                    f"{b['source']} ({_measured(b)})")
        return (f"{b['target']} ({what}): Datawrap could not rule out empty strings in "
                f"{b['source']}, because {_unknown_why(b)}")

    message = (
        "Oracle stores '' as NULL, so a NOT NULL column refuses it (ORA-01400): "
        + "; ".join(_part(b) for b in blocked) + ". "
        + ("Approve the column as NULL-able, or " if approvable else "")
        + "write a non-empty value instead; an existing column is never altered."
    )
    fixes = []
    if approvable:
        fixes.append(
            "approve with risk_acceptance {\"option\": \"%s\", \"columns\": %s, \"approved_by\": ..., "
            "\"reason\": ...} to create %s NULL-able (empty strings land as NULL)"
            % (OPTION, json.dumps([b["target"] for b in approvable]),
               ", ".join(b["target"] for b in approvable))
        )
    fixes += [a["label"] for a in actions if a["id"] != OPTION]
    details["recommended_fix"] = "; or ".join(fixes)
    gate = {"id": GATE_ID, "status": "block", "message": message, "duration_ms": 0,
            "details": details}
    blocker = {"id": GATE_ID, "message": message, "details": details,
               "suggested_actions": actions}
    return gate, [blocker]


def _empty_predicate(column: Any, dialect: str) -> Any:
    """``column`` holds exactly the empty string, as the source's reader returns it."""
    import sqlalchemy as sa

    d = (dialect or "").lower()
    if d == "mssql":  # '' = '  ' is true in T-SQL (trailing blanks are ignored)
        return sa.func.datalength(column) == 0
    if d in {"postgresql", "mysql", "mariadb"}:  # PAD SPACE collations compare '  ' = ''
        return sa.and_(column == "", sa.func.char_length(column) == 0)
    if d == "sqlite":
        return sa.and_(column == "", sa.func.length(column) == 0)
    return column == ""


def sql_empty_counts(
    *,
    source_connector_id: str,
    source_config: Mapping[str, Any] | None,
    source_table: str,
    columns: list[str],
    timeout_seconds: float = PUSHDOWN_TIMEOUT_SECONDS,
) -> dict[str, int] | None:
    """``COUNT(*) WHERE col = ''`` per column over the whole source table, or ``None``.

    ``None`` when the source is not SQL, the table or a column is not found, or
    the statement fails or times out: the caller then walks the rows.
    """
    if not columns or not str(source_table or "").strip():
        return None
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import _engine
        from connectors.sql_identifiers import split_qualified_table
        from services.code_crosswalk import _normalize_source_cfg
        from services.null_marker_probe import statement_timeout
        from services.procedure_source import is_callable_source
        from services.source_duplicate_probe import is_sqlish_source
        from services.sql_object_identity import resolve_object_identity

        if is_callable_source(source_config):
            return None
        cfg, db_type = _normalize_source_cfg(source_connector_id, source_config)
        if not cfg or not is_sqlish_source(db_type) or cfg.get("source_query") or cfg.get("query"):
            return None
        engine = _engine(dict(cfg))
        schema, name = split_qualified_table(source_table, (cfg.get("schema") or "").strip() or None)
        ident = resolve_object_identity(engine, name, schema, columns=list(columns))
        if not ident.known_present:
            return None
        names = [ident.columns.get(c) for c in columns]
        if any(n is None for n in names):
            return None
        tbl = sa.table(sa.sql.quoted_name(ident.table, True),
                       *[sa.column(sa.sql.quoted_name(n, True)) for n in names],
                       schema=sa.sql.quoted_name(ident.schema, True) if ident.schema else None)
        dialect = str(getattr(engine.dialect, "name", "") or "")
        stmt = sa.select(*[
            sa.func.coalesce(sa.func.sum(sa.case((_empty_predicate(tbl.c[n], dialect), 1), else_=0)), 0)
            for n in names
        ]).select_from(tbl)
        with engine.connect() as conn:
            with statement_timeout(conn, {"mssql": "sqlserver"}.get(dialect, dialect), timeout_seconds):
                row = conn.execute(stmt).one()
        return {c: int(v or 0) for c, v in zip(columns, row)}
    except Exception as exc:  # noqa: BLE001 - no exact count: the walk decides
        logger.info("Oracle empty-string push-down not used for %s: %s", source_table, exc)
        return None


def measure(
    targets: list[dict[str, Any]],
    *,
    population_rows: Any = None,
    pushdown: Any = None,
    whole_scope: bool = False,
    walk: Any = None,
    sample_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Give each target ``count``, ``exact`` and ``how`` (``why`` when not exact)."""
    out = [dict(t) for t in targets]
    if isinstance(population_rows, (list, tuple)):
        scan = count_empty_strings(population_rows, out)
        for t in out:
            t.update(count=scan["counts"][t["key"]], exact=not scan["truncated"], how="batch",
                     why=scan["truncated"], rows_scanned=scan["rows_scanned"], error=scan["error"])
        return out
    pending = list(out)
    plain = sorted({t["source"] for t in out if _no_transform(t["transform"])})
    counted = None
    if plain and pushdown is not None:
        try:
            counted = pushdown(plain)
        except Exception as exc:  # noqa: BLE001 - the walk decides instead
            logger.info("Oracle empty-string push-down failed: %s", exc)
    if counted is not None:
        pending = []
        for t in out:
            n = counted.get(t["source"]) if _no_transform(t["transform"]) else None
            # A whole-table zero is a zero for any narrower scope too.
            if n is not None and (n == 0 or whole_scope):
                t.update(count=int(n), exact=True, how="pushdown")
            else:
                pending.append(t)
    if not pending:
        return out
    rows, how, why, error = None, "population", "", ""
    try:
        rows = walk(sorted({t["source"] for t in pending})) if walk is not None else None
    except Exception as exc:  # noqa: BLE001 - an unreadable walk falls back to the preview
        error = f"{type(exc).__name__}: {str(exc)[:200]}"
        rows = None
    if rows is None:
        rows, how, why = list(sample_rows or []), "preview", "error" if error else "preview"
    scan = count_empty_strings(rows, pending)
    for t in pending:
        t.update(count=scan["counts"][t["key"]], how=how, rows_scanned=scan["rows_scanned"],
                 exact=how == "population" and not scan["truncated"],
                 why=scan["truncated"] or why, error=scan["error"] or error)
    return out


def source_catalog_facts(ctx: Mapping[str, Any]) -> tuple[dict[str, bool], list[str]]:
    """``(nullability, primary key columns)`` of the source table, or empty when unknown."""
    kind = str(ctx.get("source_kind") or "").strip().lower()
    table = str(ctx.get("source_table") or "").strip()
    if kind not in {"database", "cloud"} or not table:
        return {}, []
    if not (ctx.get("source_connector_id") or ctx.get("source_config")):
        return {}, []
    try:
        from src.transfer.endpoint_intelligence import introspect_endpoint
        from src.transfer.models import EndpointConfig

        data = dict(ctx.get("source_config") or {})
        if ctx.get("source_connector_id"):
            data["connector_id"] = ctx.get("source_connector_id")
        data.setdefault("table", table)
        fmt = str(ctx.get("source_format") or "")
        if fmt:
            data.setdefault("format", fmt)
            data.setdefault("type", fmt)
        endpoint = EndpointConfig.from_dict("database", data)
        endpoint.extra = {**(endpoint.extra or {}), "introspect_purpose": "source"}
        info = introspect_endpoint(endpoint) or {}
        nulls = {str(k): bool(v) for k, v in dict(info.get("schema_nullability") or {}).items()}
        return nulls, [str(c) for c in info.get("primary_key_columns") or [] if str(c)]
    except Exception as exc:  # noqa: BLE001 - unknown facts keep the caller's
        logger.info("Oracle empty-string gate: source catalog unreadable: %s", exc)
        return {}, []


def _recipe_of(recipe: Mapping[str, Any]) -> Any:
    if not recipe:
        return None
    try:
        from services.shape_models import ShapeRecipe

        parsed = ShapeRecipe.parse(dict(recipe))
        return parsed if parsed.enabled_steps else None
    except Exception:  # noqa: BLE001 - an unparseable recipe is refused by its own gate
        return None


def _walk_rows(ctx: Mapping[str, Any], cols: list[str], maps: list[Any], column_types: dict[str, str],
               read_scope: Any, row_filter: dict[str, Any], recipe: Any) -> Any:
    """The fit scan's walk over ``cols`` (and what the recipe reads), or ``None``."""
    fid = str(ctx.get("source_file_id") or "").strip()
    if fid:
        from services.file_parser import iter_stored_upload_rows

        return iter_stored_upload_rows(fid)
    if recipe is not None:
        produced = {w for step in recipe.enabled_steps for w in step.writes}
        reads = list(recipe.read_columns)
        cols = [*reads, *[c for c in cols if c not in produced and c not in reads]]
    from services.preflight_population import _iter_table_population_for_preflight

    return _iter_table_population_for_preflight(
        source_columns=list(cols), source_kind=str(ctx.get("source_kind") or ""),
        source_format=str(ctx.get("source_format") or ""),
        source_connector_id=str(ctx.get("source_connector_id") or ""),
        source_config=ctx.get("source_config"), source_table=str(ctx.get("source_table") or ""),
        mappings=list(maps), column_types=dict(column_types),
        dest_types=dict(ctx.get("destination_column_types") or {}),
        dest_db=str(ctx.get("destination_db_type") or ""), sync_mode=str(ctx.get("sync_mode") or ""),
        read_scope=read_scope, source_filter=row_filter or None,
    )


def preflight_hook(ctx: Mapping[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Validate's entry, called by ``run_file_preflight`` with its own arguments.

    Reads the scope the population-fit scan reads (incremental window, row
    filter, shape recipe, stored upload, the in-memory batch) and the source
    catalog when the caller did not pass it, so every entry point decides
    alike.
    """
    dest_db = str(ctx.get("destination_db_type") or "")
    if not is_oracle(dest_db):
        return None, []
    maps = [m for m in (ctx.get("write_maps") or ctx.get("mappings") or []) if isinstance(m, Mapping)]
    nulls = dict(ctx.get("column_nullability") or {})
    keys = [*str(ctx.get("contract_primary_key") or "").split(","),
            *(ctx.get("destination_pk_columns") or [])]
    table_exists = ctx.get("dest_table_exists", ctx.get("destination_table_exists"))
    if table_exists is False:
        # REST and Studio Validate do not pass the source catalog; the decision
        # must not depend on the entry point. The source PK is never approvable.
        probed, source_pk = source_catalog_facts(ctx)
        nulls = {**probed, **nulls}
        keys += source_pk
    column_types = dict(ctx.get("column_types") or {})
    targets = candidates(
        destination_db_type=dest_db, mappings=maps, column_nullability=nulls,
        destination_table_exists=table_exists,
        destination_column_nullability=ctx.get("destination_column_nullability") or {},
        column_types=column_types, key_columns=keys,
    )
    if not targets:
        return None, []
    from services.sync_cursor import incremental_read_narrows

    sync_mode = str(ctx.get("sync_mode") or "")
    cfg = dict(ctx.get("source_config") or {})
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    row_filter = dict(ctx.get("source_filter") or cfg.get("source_filter") or extra.get("source_filter") or {})
    read_scope = ctx.get("read_scope") if incremental_read_narrows(sync_mode) else None
    recipe_payload = dict(ctx.get("shape_recipe") or {})
    recipe = _recipe_of(recipe_payload)
    stored = bool(str(ctx.get("source_file_id") or "").strip())

    def walk(cols: list[str]) -> Any:
        rows = _walk_rows(ctx, cols, maps, column_types, read_scope, row_filter, recipe)
        if rows is not None and recipe is not None:
            from services.shape_preflight import shaped_population_rows

            rows = shaped_population_rows(recipe_payload, rows) or rows
        return rows

    def pushdown(cols: list[str]) -> Any:
        if recipe is not None or stored:
            return None
        return sql_empty_counts(
            source_connector_id=str(ctx.get("source_connector_id") or ""),
            source_config=ctx.get("source_config"), source_table=str(ctx.get("source_table") or ""),
            columns=cols,
        )

    population = ctx.get("population_rows") if ctx.get("rows_are_population") else None
    measured = measure(
        targets, population_rows=population, pushdown=pushdown,
        whole_scope=not row_filter and read_scope is None and recipe is None, walk=walk,
        sample_rows=list(ctx.get("sample_rows") or []),
    )
    return build_gate(measured, mappings=maps, table=str(ctx.get("destination_table") or ""))


def preflight_gate(
    *,
    destination_db_type: str,
    mappings: list[dict[str, Any]],
    column_nullability: Mapping[str, bool] | None,
    destination_table_exists: bool | None,
    destination_column_nullability: Mapping[str, bool] | None,
    column_types: Mapping[str, str] | None,
    key_columns: Iterable[str],
    sample_rows: list[dict[str, Any]] | None,
    walk: Any = None,
    population_rows: Any = None,
    pushdown: Any = None,
    whole_scope: bool = True,
    destination_table: str = "",
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Candidates, then the measurement, then the gate (explicit inputs)."""
    targets = candidates(
        destination_db_type=destination_db_type, mappings=mappings,
        column_nullability=column_nullability,
        destination_table_exists=destination_table_exists,
        destination_column_nullability=destination_column_nullability,
        column_types=column_types, key_columns=key_columns,
    )
    if not targets:
        return None, []
    measured = measure(targets, population_rows=population_rows, pushdown=pushdown,
                       whole_scope=whole_scope, walk=walk, sample_rows=sample_rows)
    return build_gate(measured, mappings=mappings, table=destination_table)


def sign_empty_as_null(
    mappings: list[dict[str, Any]],
    acceptance: Mapping[str, Any],
    *,
    table: str,
) -> list[dict[str, Any]]:
    """Sign the empty-as-NULL contract on the named columns' mappings.

    ``columns`` names targets (or ``source->target`` pairs, or a source that
    feeds one target). Each contract is bound to its (source, target) pair,
    ``table`` and the current workspace. Needs ``approved_by``, ``reason`` and
    ``columns``. Raises ``ValueError`` (nothing signed) otherwise, or when a
    named column is not mapped.
    """
    from services.migration_risk_contract import create_migration_risk_contract

    approved_by = str(acceptance.get("approved_by") or "").strip()
    reason = str(acceptance.get("reason") or "").strip()
    named = [str(c).strip() for c in (acceptance.get("columns") or []) if str(c or "").strip()]
    if not approved_by or not reason or not named:
        raise ValueError(
            f"risk_acceptance option {OPTION} needs approved_by, reason and columns. "
            "Nothing was signed."
        )
    wanted = {c.lower() for c in named}
    rows = [m for m in mappings or [] if isinstance(m, Mapping)]
    targets_named = {str(m.get("target") or "").lower() for m in rows}
    feeds: dict[str, int] = {}
    for m in rows:
        feeds[str(m.get("source") or "").lower()] = feeds.get(str(m.get("source") or "").lower(), 0) + 1
    workspace = current_workspace()
    out, signed = [], set()
    for raw in mappings or []:
        row = dict(raw)
        source, target = str(row.get("source") or ""), str(row.get("target") or "")
        pair = pair_key(source, target).lower()
        hit = next((n for n in (pair, target.lower()) if n and n in wanted), "")
        # A source name approves only a source that feeds one target and is
        # not itself some target's name: note->a and note->b approve apart.
        if not hit and source.lower() in wanted and feeds.get(source.lower()) == 1 \
                and source.lower() not in targets_named - {target.lower()}:
            hit = source.lower()
        if hit and approved_contract(row, table=table) is None:
            contract = create_migration_risk_contract(
                column=source or target,
                source_type=str(row.get("source_type") or ""),
                destination_type=str(row.get("target_type") or "VARCHAR2"),
                approved_by=approved_by,
                reason=reason,
                # Nothing else changes: a value that fails for another reason
                # still fails the run.
                execution_policy="FAIL_JOB",
                root_cause=(
                    "Oracle stores '' as NULL; the column is created NULL-able so empty "
                    "strings land as NULL"
                ),
                severity="medium",
                expected_precision_loss=False,
                expected_nulls=True,
                target=target or source,
                table=table,
                loss_classification=RISK_KIND,
                metadata={"option": OPTION, "workspace_id": workspace},
            )
            row[CONTRACT_KEY] = contract.to_dict()
        if hit:
            signed.add(hit)
        out.append(row)
    if mappings and wanted - signed:
        raise ValueError(
            f"risk_acceptance option {OPTION} named unmapped column(s) "
            f"{', '.join(sorted(wanted - signed))}. Nothing was signed."
        )
    return out


def relax_create_plan(
    plan: Any, mappings: Iterable[Mapping[str, Any]] | None, dest: str, *, dest_table: str = "",
) -> Any:
    """Create an approved column NULL-able on Oracle (never a key column)."""
    if not is_oracle(dest):
        return plan
    approved = {}
    for m in mappings or []:
        if isinstance(m, Mapping) and (contract := approved_contract(m, table=dest_table)):
            approved[str(m.get("target") or m.get("source") or "").lower()] = contract
    if not approved:
        return plan
    keys = {str(c).lower() for c in (getattr(plan, "primary_key", None) or [])}
    from services.schema_fidelity import SchemaFidelityItem

    for col in list(getattr(plan, "not_null_columns", []) or []):
        contract = approved.get(str(col).lower())
        if contract is None or str(col).lower() in keys:
            continue
        plan.not_null_columns.remove(col)
        suffixes = plan.column_suffixes.get(col) or []
        plan.column_suffixes[col] = [s for s in suffixes if str(s).strip().upper() != "NOT NULL"]
        # The carried NOT NULL is no longer the plan: left in, post-create
        # verification would report "destination did not take NOT NULL".
        plan.report.items[:] = [
            i for i in plan.report.items
            if not (getattr(i, "aspect", "") == "not_null"
                    and str(getattr(i, "name", "")).lower() == str(col).lower())
        ]
        plan.report.items.append(SchemaFidelityItem(
            aspect="not_null",
            name=col,
            status="relaxed_by_contract",
            reason=(
                "Created NULL-able: Oracle stores '' as NULL and the operator approved "
                f"empty strings landing as NULL (Risk Contract {contract.get('risk_id')})."
            ),
            dest_ddl="NULL",
        ))
    return plan


def destination_null_rows(
    endpoint: Any, table: str, columns: list[str], scope: Any = None,
) -> dict[str, int] | None:
    """NULL cells per column of the Oracle destination table, or ``None`` if unreadable.

    A ``scope`` of kind ``run_keys`` (a NULL-marker ``ProbeScope``) limits the
    count to the rows this run wrote; otherwise the whole table is read.
    Opened through the writer's own engine and bounded like the NULL-marker
    probe.
    """
    if not table or not columns:
        return None
    try:
        import sqlalchemy as sa

        from connectors.sql_identifiers import split_qualified_table
        from services.engine_pool import release_engine
        from services.null_marker_probe import _columns, probe_engine, probe_timeout_seconds, statement_timeout
        from src.transfer.adapters import resolve_connector_config

        cfg = resolve_connector_config(endpoint)
        schema, bare = split_qualified_table(table, str(cfg.get("schema") or "").strip() or None)
        key = str(getattr(scope, "key_column", "") or "") if getattr(scope, "kind", "") == "run_keys" else ""
        engine = probe_engine(cfg)
        try:
            with engine.connect() as conn:
                names, _fixed, key_type, types, key_name = _columns(conn, bare, schema, columns, key)
                if not names or (key and not key_name):
                    return None
                cols = [sa.column(n, types.get(n)) for n in names]
                if key_name and key_name not in names:
                    cols.append(sa.column(key_name, key_type))
                tbl = sa.table(bare, *cols, schema=schema or None)
                batches: list[list[Any] | None] = [None]
                if key_name:
                    keys = list(scope.keys)
                    batches = [keys[i:i + 500] for i in range(0, len(keys), 500)]
                totals = [0] * len(names)
                with statement_timeout(conn, "oracle", probe_timeout_seconds()):
                    for batch in batches:
                        query = sa.select(*[
                            sa.func.sum(sa.case((tbl.c[n].is_(None), 1), else_=0)) for n in names
                        ]).select_from(tbl)
                        if batch is not None:
                            query = query.where(tbl.c[key_name].in_(batch))
                        row = conn.execute(query).one()
                        totals = [a + int(b or 0) for a, b in zip(totals, row)]
                return {n.lower(): v for n, v in zip(names, totals)}
        finally:
            release_engine(engine)
    except Exception:  # noqa: BLE001 - an unreadable count is reported as unknown, never a pass flip
        return None


def explain_empty_as_null(
    recon: dict[str, Any],
    *,
    endpoint: Any,
    mappings: Iterable[Mapping[str, Any]] | None,
    records: list[Mapping[str, Any]] | None,
    dest_summary: Mapping[str, Any] | None = None,
    rows_written: int = 0,
    validation_mode: str = "strict",
) -> dict[str, Any]:
    """Gate-8 names the rows of this run an approved contract explains (never a pass flip).

    Oracle's read-back already treats ``''`` and NULL as one value
    (services.reconciliation.destination_empty_string_is_null). This counts
    the run's own rows: the run's records as written (after the transform)
    when the run holds them, else the destination's NULL cells over the rows
    this run wrote (the table an overwrite or a first load wrote, or the
    run's keys). When the run's rows cannot be named, the table-wide NULL
    count is stated as such and not called explained.
    """
    dest = str(getattr(endpoint, "format", "") or getattr(endpoint, "type", "") or "")
    if not isinstance(recon, dict) or not is_oracle(dest):
        return recon
    table = str((dest_summary or {}).get("table") or getattr(endpoint, "table", "") or "")
    approved = [
        (m, c) for m in mappings or []
        if isinstance(m, Mapping) and (c := approved_contract(m, table=table))
    ]
    if not approved:
        return recon
    carried = [r for r in records or [] if isinstance(r, Mapping)]
    scope: Any = None
    counted: dict[str, int] = {}
    explained, unscoped = [], []
    for m, contract in approved:
        source = str(m.get("source") or "")
        target = str(m.get("target") or source)
        entry = {"column": target, "source_column": source, "kind": RISK_KIND,
                 "risk_id": contract.get("risk_id"), "approved_by": contract.get("approved_by")}
        if carried and any(_lookup(r, source) is not None or source in r for r in carried):
            transform = str(m.get("transform") or "")
            n = sum(1 for r in carried if _written(_lookup(r, source), transform) == "")
            explained.append({**entry, "rows": n, "counted_by": "the run's records, as written"})
            continue
        if scope is None:
            from services.null_marker_probe import resolve_probe_scope

            scope = resolve_probe_scope(
                dest_summary=dict(dest_summary or {}), records=carried,
                mappings=[dict(x) for x in mappings or [] if isinstance(x, Mapping)],
                rows_written=int(rows_written or 0), validation_mode=validation_mode,
            )
            counted = destination_null_rows(
                endpoint, table, [str(x.get("target") or x.get("source") or "") for x, _ in approved],
                scope=scope if getattr(scope, "kind", "") in _RUN_SCOPES else None,
            ) or {}
        n = counted.get(target.lower())
        if getattr(scope, "kind", "") in _RUN_SCOPES and n is not None:
            explained.append({**entry, "rows": n, "counted_by": "NULL cells in the rows this run wrote"})
        else:
            unscoped.append({**entry, "table_wide_null_cells": n})
    out = dict(recon)
    notes = []
    if explained:
        out["explained_changes"] = [*(recon.get("explained_changes") or []), *explained]
        notes += [
            f"{e['column']}: {e['rows']} empty string(s) are NULL in Oracle, explained by signed "
            f"Risk Contract {e['risk_id']} (approved by {e['approved_by']})." for e in explained
        ]
    if unscoped:
        out["unscoped_null_cells"] = unscoped
        notes += [
            f"{u['column']}: NULL cells in this column: "
            f"{u['table_wide_null_cells'] if u['table_wide_null_cells'] is not None else 'not counted'} "
            "(table-wide). The rows this run wrote cannot be named, so they are not counted as "
            f"explained by Risk Contract {u['risk_id']}." for u in unscoped
        ]
    out["message"] = f"{recon.get('message') or ''} {' '.join(notes)}".strip()
    return out
