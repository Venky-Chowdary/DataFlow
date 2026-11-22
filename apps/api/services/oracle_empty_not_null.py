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


#: Logical types that provably cannot be ``''`` (a number, a date or time, a
#: boolean, a uuid). Everything else (an enum, a domain over text, citext,
#: name, bpchar, tsvector, sql_variant, MySQL ENUM/SET, json, user-defined or
#: unknown types) may hold ``''`` and is checked.
NEVER_EMPTY_LOGICAL_TYPES = frozenset({
    "integer", "decimal", "float", "boolean", "date", "datetime", "time", "interval", "uuid",
})


def _source_can_hold_text(source_type: Any) -> bool:
    if not str(source_type or "").strip():
        return True
    from services.type_system import normalize_logical_type

    return normalize_logical_type(str(source_type)) not in NEVER_EMPTY_LOGICAL_TYPES


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
    native_source_types: bool = False,
) -> list[dict[str, Any]]:
    """Text mappings whose Oracle column will be NOT NULL, with ``kind`` and ``approvable``.

    ``native_source_types``: ``column_types`` are a SQL catalog's own types, so
    a number, date, boolean or uuid column is skipped (it can never be ``''``).
    Inferred types (files, documents) are never trusted for that: a column
    inferred as a number can still hold an empty cell.
    """
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
        if native_source_types and not _source_can_hold_text(_lookup(column_types, source)):
            continue  # a uuid, json, array, date or number is never ''
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


def written_value_is_never_empty(transform: str) -> bool:
    """True when the mapping's transform can never send ``''``.

    ``default:<value>`` writes ``<value>`` for an empty, blank or NULL cell and
    leaves every other cell (non-blank text) unchanged, so with a non-empty
    ``<value>`` the written value is never ``''`` (services.transform_engine).
    """
    t = str(transform or "").strip()
    return t.lower().startswith("default:") and t.split(":", 1)[1] != ""


#: Engines that bill a full scan: Validate never scans them unasked.
BILLED_SCAN_ENGINES = frozenset({"snowflake", "bigquery", "databricks", "clickhouse", "trino", "presto"})
#: SQLAlchemy dialects whose "exactly ''" predicate is proven (see _empty_predicate).
_EXACT_DIALECTS = frozenset({"postgresql", "mysql", "mariadb", "mssql", "sqlite", "duckdb"})
#: One total time budget per check, across every column (existence first,
#: then counts, then the walk); what is not answered in it is "could not check".
VALIDATE_BUDGET_SECONDS = 60.0
#: "Check this column now": the stated total budget the operator asked for.
CHECK_NOW_SECONDS = 300.0
#: A "Check this column now" request is honoured for this long, once asked.
CHECK_NOW_VALID_SECONDS = 3600.0
CHECK_OPTION = "oracle_empty_check_now"
CHECK_KEY = "empty_check_now"
DEFAULT_TRANSFORM_HINT = "default:<value>"


def _measured(t: Mapping[str, Any]) -> str:
    how = t.get("how") or ""
    if how == "pushdown":
        return "counted by the source database"
    if how == "transform":
        return "its transform never writes an empty string"
    if how in {"population", "batch"}:
        return "every source row Execute will read"
    return "preview rows only"


def _unknown_why(t: Mapping[str, Any]) -> str:
    why = str(t.get("why") or "")
    rows = int(t.get("rows_scanned") or 0)
    if why == "preview":
        return "only a preview of the source could be read"
    if why == "billed":
        return (f"checking it scans the whole table, which {t.get('engine') or 'this engine'} bills; "
                "Datawrap does not do that unasked")
    if why == "rows":
        return f"the scan stopped after {rows:,} rows"
    if why == "time":
        return f"the scan stopped after {t.get('seconds') or SCAN_MAX_SECONDS:g}s ({rows:,} rows)"
    if why == "catalog":
        return f"the source catalog could not be read ({t.get('error') or 'no detail'})"
    if why == "timeout":
        return f"the source did not answer within {t.get('seconds') or PUSHDOWN_TIMEOUT_SECONDS:g}s"
    if why == "budget":
        return (f"this check's {t.get('seconds') or VALIDATE_BUDGET_SECONDS:g}s budget ran out before "
                "this column was reached")
    return f"the source could not be read ({t.get('error') or why or 'unknown error'})"


def _count_words(t: Mapping[str, Any]) -> str:
    if int(t.get("count") or 0) > 0:
        return f"{t['count']} empty string(s)"
    return "at least one empty string (the count did not finish in time)"


def _actions(blocked: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """At least one action that works on this source, for every blocked column."""
    actions: list[dict[str, Any]] = []
    approvable = [b for b in blocked if b["approvable"]]
    if approvable:
        actions.append({
            "id": OPTION, "kind": OPTION,
            "label": "; ".join(f"Create {b['target']} as NULL-able; empty strings land as NULL"
                               for b in approvable),
            "risk_acceptance": {"option": OPTION, "columns": [b["target"] for b in approvable],
                                "approved_by": "<who approves>", "reason": "<why>"},
        })
    unknown = [b for b in blocked if not b.get("found") and b.get("why") != "catalog"]
    if unknown:
        billed = any(b.get("why") == "billed" for b in unknown)
        actions.append({
            "id": CHECK_OPTION, "kind": CHECK_OPTION,
            "label": "Check " + ", ".join(b["source"] for b in unknown) + " now: Datawrap reads the "
            f"whole column, within a total of {CHECK_NOW_SECONDS / 60:g} minutes for this check"
            + (" (the warehouse bills the scan)" if billed else ""),
            "budget_seconds": CHECK_NOW_SECONDS,
            "risk_acceptance": {"option": CHECK_OPTION, "columns": [b["target"] for b in unknown],
                                "approved_by": "<who asks>"},
        })
    catalog = [b for b in blocked if b.get("why") == "catalog"]
    if catalog:
        actions.append({
            "id": "oracle_fix_connection", "kind": "oracle_fix_connection",
            "label": "Fix the source connection, then validate again: " + str(catalog[0].get("error") or ""),
        })
    for b in blocked:
        actions.append({
            "id": "oracle_default_transform", "kind": "oracle_default_transform",
            "label": (f"Map the transform {DEFAULT_TRANSFORM_HINT} on {b['source']}: empty and blank "
                      f"values are written as <value>, every other value is unchanged"),
            "column": b["target"], "transform": DEFAULT_TRANSFORM_HINT,
        })
        if b.get("found"):
            actions.append({
                "id": "oracle_fix_source_values", "kind": "oracle_fix_source_values",
                "label": f"Replace the empty values of {b['source']} in the source, then validate again",
                "column": b["target"],
            })
    return actions


def build_gate(
    targets: list[dict[str, Any]],
    *,
    mappings: Iterable[Mapping[str, Any]] | None,
    table: str = "",
    workspace: str | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """``(gate, blockers)`` from targets carrying ``count``, ``found``, ``exact`` and ``how``.

    No gate when every column is an exact zero. A column with empty strings,
    or one that could not be checked, is blocked unless an approved contract
    covers it; every block names the column, what is known, and actions that
    work on this source.
    """
    by_pair = {
        pair_key(str(m.get("source") or ""), str(m.get("target") or "")): m
        for m in mappings or [] if isinstance(m, Mapping)
    }
    blocked, approved = [], []
    for t in targets:
        t["found"] = bool(t.get("found")) or int(t.get("count") or 0) > 0
        if not t["found"] and t.get("exact"):
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
    if not blocked and not approved:
        return None, []
    details: dict[str, Any] = {"columns": blocked, "approved": approved, "option": OPTION}
    if not blocked:
        return {"id": GATE_ID, "status": "pass", "duration_ms": 0, "details": details,
                "message": "Oracle stores '' as NULL. " + "; ".join(
                    f"{a['target']}: {_count_words(a) if a['found'] else 'any empty strings'} from "
                    f"{a['source']} land as NULL, created NULL-able under signed Risk Contract "
                    f"{a['risk_id']}" for a in approved) + "."}, []
    actions = _actions(blocked)

    def _part(b: Mapping[str, Any]) -> str:
        what = "existing NOT NULL column" if b["kind"] == "existing" else (
            "key column" if not b["approvable"] else "NOT NULL from the source")
        if b["found"]:
            return (f"{b['target']} ({what}) would receive {_count_words(b)} from {b['source']} "
                    f"({_measured(b)})")
        return (f"{b['target']} ({what}): Datawrap could not rule out empty strings in "
                f"{b['source']}, because {_unknown_why(b)}")

    message = (
        "Oracle stores '' as NULL, so a NOT NULL column refuses it (ORA-01400): "
        + "; ".join(_part(b) for b in blocked) + ". "
        + "Choose one of the actions listed; an existing column is never altered."
    )
    fixes = []
    for a in actions:
        if a.get("risk_acceptance"):
            fixes.append(f"{a['label']} (risk_acceptance {json.dumps(a['risk_acceptance'])})")
        else:
            fixes.append(a["label"])
    details["recommended_fix"] = "; or ".join(fixes)
    gate = {"id": GATE_ID, "status": "block", "message": message, "duration_ms": 0, "details": details}
    return gate, [{"id": GATE_ID, "message": message, "details": details, "suggested_actions": actions}]


def _empty_predicate(column: Any, dialect: str) -> Any:
    """``column`` holds exactly the empty string, as the source's reader returns it.

    Each spelling is proven against the engine (``col = ''`` keeps an index
    usable; the length term removes what the engine's ``=`` also matches):

    * PostgreSQL: ``character(n)`` is blank-padded and ``=`` ignores the
      padding (https://www.postgresql.org/docs/current/datatype-character.html),
      so ``''::char(5) = ''`` is true while the reader returns five spaces;
      ``octet_length`` counts the padding (5). Checked on PostgreSQL 16.
    * MySQL / MariaDB: PAD SPACE collations compare ``'  ' = ''`` as true
      (https://dev.mysql.com/doc/refman/8.4/en/char.html); ``CHAR_LENGTH``
      keeps a VARCHAR's trailing spaces (2) and a CHAR's are stripped on
      read, as the reader returns them. Checked on MySQL 8.0.
    * SQL Server: ``=`` ignores trailing blanks (ANSI padding, KB 316626);
      ``DATALENGTH`` counts them
      (https://learn.microsoft.com/en-us/sql/t-sql/functions/datalength-transact-sql).
    * SQLite, DuckDB: ``=`` compares exactly.
    """
    import sqlalchemy as sa

    d = (dialect or "").lower()
    if d == "postgresql":
        return sa.and_(column == "", sa.func.octet_length(column) == 0)
    if d in {"mysql", "mariadb"}:
        return sa.and_(column == "", sa.func.char_length(column) == 0)
    if d == "mssql":
        return sa.and_(column == "", sa.func.datalength(column) == 0)
    return column == ""


def provably_never_empty(col_type: Any) -> bool:
    """A reflected column type that can never hold ``''``: a number, date or time, boolean, uuid.

    Everything else is asked or walked: a PostgreSQL enum accepts the label
    ``''``, ``''::tsvector`` reads back as ``''``, a SQL Server sql_variant, a
    MySQL ENUM/SET, citext, name, bpchar, json and user-defined or unknown
    types can all hold it.
    """
    import sqlalchemy as sa

    never = [sa.Integer, sa.Numeric, sa.Date, sa.DateTime, sa.Time, sa.Interval, sa.Boolean]
    if hasattr(sa, "Uuid"):
        never.append(sa.Uuid)
    try:
        from sqlalchemy.dialects.postgresql import UUID as PgUUID

        never.append(PgUUID)
    except ImportError:  # pragma: no cover
        pass
    return isinstance(col_type, tuple(never))


def oracle_source_never_empty(col_type: Any) -> bool:
    """An Oracle source column that cannot be read as ``''``.

    A zero-length character value is NULL in Oracle, so VARCHAR2/CHAR/NVARCHAR2
    never read as ``''``. A LOB is different: ``EMPTY_CLOB()`` is an empty LOB,
    not NULL (Oracle SQL Reference, Nulls; DBMS_LOB), and the driver reads it
    as ``''``, so CLOB/NCLOB columns are walked.
    """
    import sqlalchemy as sa

    return not isinstance(col_type, (sa.Text, sa.CLOB, sa.types.NullType))


def sql_empty_probe(
    *,
    source_connector_id: str,
    source_config: Mapping[str, Any] | None,
    source_table: str,
    column: str,
    timeout_seconds: float = PUSHDOWN_TIMEOUT_SECONDS,
    check_now: bool = False,
    count: bool = False,
) -> dict[str, Any] | None:
    """Whether ``column`` holds an exactly-empty string, asked of the source, bounded.

    ``count=False``: ``SELECT 1 ... LIMIT 1`` (index-friendly, stops at the first
    hit) answers zero or found. ``count=True``: the count (``None`` when it ran
    out of time: "at least one"). Returns ``None`` when the source cannot
    answer exactly and bounded (not SQL, an engine whose predicate is not
    proven, no server-side timeout in force, a billed engine not asked, a type
    the predicate does not apply to): the caller then walks or reports.
    """
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import _engine
        from connectors.sql_identifiers import split_qualified_table
        from services.code_crosswalk import _normalize_source_cfg
        from services.null_marker_probe import statement_timeout, timed_out
        from services.procedure_source import is_callable_source
        from services.source_duplicate_probe import is_sqlish_source

        if is_callable_source(source_config) or not str(source_table or "").strip():
            return None
        cfg, db_type = _normalize_source_cfg(source_connector_id, source_config)
        if not cfg or cfg.get("source_query") or cfg.get("query"):
            return None
        if not is_sqlish_source(db_type) or (db_type in BILLED_SCAN_ENGINES and not check_now):
            return None
        engine = _engine(dict(cfg))
        dialect = str(getattr(engine.dialect, "name", "") or "")
        schema, name = split_qualified_table(source_table, (cfg.get("schema") or "").strip() or None)
        with engine.connect() as conn:
            reflected = {str(c["name"]).lower(): c for c in sa.inspect(conn).get_columns(name, schema=schema)}
            col = reflected.get(column.lower())
            if col is None:
                return None
            if provably_never_empty(col.get("type")):
                return {"state": "zero", "count": 0, "note": f"a {col.get('type')} column cannot hold text"}
            if is_oracle(db_type) or dialect == "oracle":
                return {"state": "zero", "count": 0} if oracle_source_never_empty(col.get("type")) else None
            if dialect not in _EXACT_DIALECTS:
                return None
            tbl = sa.table(name, sa.column(str(col["name"])), schema=schema or None)
            pred = _empty_predicate(tbl.c[str(col["name"])], dialect)
            family = {"mssql": "sqlserver"}.get(dialect, dialect)
            with statement_timeout(conn, family, max(0.001, timeout_seconds)) as enforced:
                if not enforced:
                    return None
                try:
                    if not count:
                        hit = conn.execute(sa.select(sa.literal(1)).select_from(tbl).where(pred).limit(1)).first()
                        return {"state": "found", "count": None} if hit is not None else {"state": "zero", "count": 0}
                    n = conn.execute(sa.select(sa.func.count()).select_from(tbl).where(pred)).scalar()
                    return {"state": "found", "count": int(n or 0)} if n else {"state": "zero", "count": 0}
                except Exception as exc:  # noqa: BLE001 - a timeout is reported, not raised
                    if timed_out(exc):
                        return ({"state": "found", "count": None} if count
                                else {"state": "unknown", "why": "timeout", "seconds": timeout_seconds})
                    raise
    except Exception as exc:  # noqa: BLE001 - no exact answer: the walk decides
        logger.info("Oracle empty-string push-down not used for %s.%s: %s", source_table, column, exc)
        return None


def measure(
    targets: list[dict[str, Any]],
    *,
    population_rows: Any = None,
    probe: Any = None,
    whole_scope: bool = False,
    walk: Any = None,
    sample_rows: list[dict[str, Any]] | None = None,
    walk_allowed: bool = True,
    engine: str = "",
    check_now: Iterable[str] = (),
    budget_seconds: float | None = None,
) -> list[dict[str, Any]]:
    """Give each target ``count``, ``found``, ``exact`` and ``how`` (``why`` when not exact).

    One total budget for the whole check (``VALIDATE_BUDGET_SECONDS``, or the
    stated ``CHECK_NOW_SECONDS`` when the operator asked): existence questions
    first (columns the preview already shows empty first), then the counts,
    then the walk; what the budget does not reach is "could not check".
    """
    out = [dict(t) for t in targets]
    asked = {str(c).lower() for c in check_now}
    long_scan = any(t["key"].lower() in asked or t["target"].lower() in asked for t in out)
    budget = budget_seconds if budget_seconds is not None else (
        CHECK_NOW_SECONDS if long_scan else VALIDATE_BUDGET_SECONDS)
    deadline = time.monotonic() + budget

    def remaining() -> float:
        return deadline - time.monotonic()

    def out_of_budget(t: dict[str, Any]) -> None:
        t.update(count=0, found=False, exact=False, how="pushdown", why="budget", seconds=budget)

    preview_empty = {
        str(k) for r in sample_rows or [] if isinstance(r, Mapping)
        for k, v in r.items() if isinstance(v, str) and v == ""
    }
    todo = [t for t in out if t.get("why") != "catalog"]
    pending: list[dict[str, Any]] = []
    asking: list[dict[str, Any]] = []
    for t in todo:
        if written_value_is_never_empty(t["transform"]):
            t.update(count=0, found=False, exact=True, how="transform")
        elif isinstance(population_rows, (list, tuple)) or probe is None or not _no_transform(t["transform"]):
            pending.append(t)
        else:
            asking.append(t)
    asking.sort(key=lambda t: t["source"] not in preview_empty)
    counting: list[dict[str, Any]] = []
    for t in asking:
        if remaining() <= 0:
            out_of_budget(t)
            continue
        res = probe(t["source"], t["key"].lower() in asked or t["target"].lower() in asked, remaining(), False)
        state = (res or {}).get("state")
        if state == "zero":
            t.update(count=0, found=False, exact=True, how="pushdown")
        elif state == "found" and whole_scope:
            t.update(count=None, found=True, exact=False, how="pushdown")
            counting.append(t)
        elif state == "unknown" and not walk_allowed:
            t.update(count=0, found=False, exact=False, how="pushdown", why=res.get("why"),
                     seconds=res.get("seconds"))
        else:
            pending.append(t)
    for t in counting:
        res = probe(t["source"], t["key"].lower() in asked or t["target"].lower() in asked,
                    remaining(), True) if remaining() > 0 else None
        n = (res or {}).get("count")
        t.update(count=n, exact=n is not None)
    if not pending:
        return out
    if isinstance(population_rows, (list, tuple)):
        scan = count_empty_strings(population_rows, pending)
        for t in pending:
            n = scan["counts"][t["key"]]
            t.update(count=n, found=n > 0, exact=not scan["truncated"], how="batch",
                     why=scan["truncated"], rows_scanned=scan["rows_scanned"], error=scan["error"])
        return out
    if remaining() <= 0:
        for t in pending:
            out_of_budget(t)
        return out
    rows, how, why, error = None, "population", "", ""
    if walk_allowed or long_scan:
        try:
            rows = walk(sorted({t["source"] for t in pending})) if walk is not None else None
        except Exception as exc:  # noqa: BLE001 - an unreadable walk falls back to the preview
            error = f"{type(exc).__name__}: {str(exc)[:200]}"
            rows = None
    if rows is None:
        why = "error" if error else ("preview" if walk_allowed or long_scan else "billed")
        rows, how = list(sample_rows or []), "preview"
    seconds = max(0.0, remaining())
    scan = count_empty_strings(rows, pending, max_rows=10**12 if long_scan else None, max_seconds=seconds)
    for t in pending:
        n = scan["counts"][t["key"]]
        t.update(count=n, found=n > 0, how=how, rows_scanned=scan["rows_scanned"],
                 exact=how == "population" and not scan["truncated"], engine=engine,
                 why=scan["truncated"] or why, error=scan["error"] or error, seconds=round(seconds, 1))
    return out


def source_catalog_facts(ctx: Mapping[str, Any]) -> tuple[dict[str, bool], list[str], str]:
    """``(nullability, primary key columns, error)`` of the source table.

    Read strictly, as Execute reads it; ``error`` is the reason in plain words
    when the catalog could not be read (never a silent "no facts").
    """
    table = str(ctx.get("source_table") or "").strip()
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
        endpoint.extra = {**(endpoint.extra or {}), "introspect_purpose": "source", "introspect_strict": True}
        info = introspect_endpoint(endpoint) or {}
        nulls = {str(k): bool(v) for k, v in dict(info.get("schema_nullability") or {}).items()}
        if not nulls and (info.get("connected") is False or info.get("error") or info.get("error_code")):
            return {}, [], str(info.get("message") or info.get("error") or info.get("error_code"))[:300]
        return nulls, [str(c) for c in info.get("primary_key_columns") or [] if str(c)], ""
    except Exception as exc:  # noqa: BLE001 - reported as "could not check", never a pass
        logger.info("Oracle empty-string gate: source catalog unreadable: %s", exc)
        first = (str(getattr(exc, "orig", None) or exc).strip().splitlines() or [""])[0]
        return {}, [], f"{type(exc).__name__}: {first[:300]}"


def _catalog_source(ctx: Mapping[str, Any]) -> bool:
    kind = str(ctx.get("source_kind") or "").strip().lower()
    return (kind in {"database", "cloud"} and bool(str(ctx.get("source_table") or "").strip())
            and bool(ctx.get("source_connector_id") or ctx.get("source_config")))


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


def _row_filter(ctx: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    """The run's row filter as a dict, or ``({}, error)`` when it cannot be read."""
    cfg = ctx.get("source_config") if isinstance(ctx.get("source_config"), Mapping) else {}
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), Mapping) else {}
    raw = ctx.get("source_filter") or cfg.get("source_filter") or extra.get("source_filter") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw) if raw.strip() else {}
        except ValueError as exc:
            return {}, f"the row filter is not valid JSON ({exc})"
    if not isinstance(raw, Mapping):
        return {}, f"the row filter is not an object ({type(raw).__name__})"
    return dict(raw), ""


#: Measurements kept for reuse, bounded (LRU) and keyed by everything the
#: answer depends on: workspace, destination, source, table, column type,
#: mapping pair and transform, and the run's scope (filter, recipe,
#: incremental watermark). Reused only (a) by Execute confirming an approved
#: Validate, for EXECUTE_REUSE_SECONDS, and (b) for a billed warehouse, the
#: result of an operator's "Check this column now", for CHECK_REUSE_SECONDS,
#: so a schedule does not re-bill the scan on every run. Any '' that reaches
#: the source after the measurement is refused by Oracle at write time
#: (ORA-01400 fails that batch; nothing is written as a different value).
_MEASURED: dict[str, dict[str, Any]] = {}
_MEASURED_MAX = 512
EXECUTE_REUSE_SECONDS = 3600.0
CHECK_REUSE_SECONDS = 86400.0


def _measure_key(ctx: Mapping[str, Any], t: Mapping[str, Any], scope: str) -> str:
    import hashlib

    def ident(cfg: Any) -> dict[str, Any]:
        cfg = cfg if isinstance(cfg, Mapping) else {}
        return {k: cfg.get(k) for k in ("type", "format", "host", "port", "database", "schema", "project",
                                        "dataset", "account", "warehouse")}

    raw = json.dumps([
        current_workspace(),
        str(ctx.get("destination_db_type") or ""), str(ctx.get("destination_table") or ""),
        ident(ctx.get("destination_config")),
        str(ctx.get("source_connector_id") or ""), ident(ctx.get("source_config")),
        str(ctx.get("source_table") or ""), str(ctx.get("source_file_id") or ""),
        str(_lookup(ctx.get("column_types") or {}, t["source"]) or ""),
        scope, t["source"], t["target"], t["transform"],
    ], sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _remember(key: str, result: dict[str, Any], *, checked_now: bool) -> None:
    _MEASURED.pop(key, None)
    _MEASURED[key] = {"at": time.time(), "result": result, "checked_now": checked_now}
    while len(_MEASURED) > _MEASURED_MAX:
        _MEASURED.pop(next(iter(_MEASURED)))


def _recall(key: str, *, execute_confirm: bool, billed: bool) -> dict[str, Any] | None:
    hit = _MEASURED.get(key)
    if not hit:
        return None
    age = time.time() - float(hit["at"])
    if execute_confirm and age < EXECUTE_REUSE_SECONDS:
        return dict(hit["result"])
    if billed and hit["checked_now"] and age < CHECK_REUSE_SECONDS:
        return dict(hit["result"])
    if age >= max(EXECUTE_REUSE_SECONDS, CHECK_REUSE_SECONDS):
        _MEASURED.pop(key, None)
    return None


def _check_now_asked(mapping: Mapping[str, Any]) -> bool:
    """An unexpired "Check this column now" request on this mapping."""
    from datetime import datetime, timezone

    stamp = mapping.get(CHECK_KEY)
    if not isinstance(stamp, Mapping):
        return False
    try:
        expires = datetime.fromisoformat(str(stamp.get("expires_at") or ""))
    except ValueError:
        return False
    bound, here = str(stamp.get("workspace_id") or ""), current_workspace()
    if bound and here and bound != here:
        return False
    return datetime.now(timezone.utc) < expires


def preflight_hook(ctx: Mapping[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Validate's entry, called by ``run_file_preflight`` with its own arguments.

    Never raises: anything unexpected is reported as "could not check".
    """
    if not is_oracle(str(ctx.get("destination_db_type") or "")):
        return None, []
    try:
        return _preflight_hook(ctx)
    except Exception as exc:  # noqa: BLE001 - Validate reports, it never crashes here
        logger.warning("Oracle empty-string gate could not run: %s", exc, exc_info=exc)
        message = (f"Datawrap could not check the Oracle NOT NULL text columns for empty strings "
                   f"({type(exc).__name__}: {str(exc)[:200]}). Validate again; if it persists, "
                   "report this message.")
        details = {"error": str(exc)[:400]}
        return ({"id": GATE_ID, "status": "block", "message": message, "duration_ms": 0, "details": details},
                [{"id": GATE_ID, "message": message, "details": details, "suggested_actions": []}])


def _preflight_hook(ctx: Mapping[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    dest_db = str(ctx.get("destination_db_type") or "")
    maps = [m for m in (ctx.get("write_maps") or ctx.get("mappings") or []) if isinstance(m, Mapping)]
    column_types = dict(ctx.get("column_types") or {})
    nulls = dict(ctx.get("column_nullability") or {})
    keys = [*str(ctx.get("contract_primary_key") or "").split(","),
            *(ctx.get("destination_pk_columns") or [])]
    table_exists = ctx.get("dest_table_exists", ctx.get("destination_table_exists"))
    catalog_error, pk_known = "", False
    text_sources = [str(m.get("source") or "") for m in maps if _is_text(m, column_types)]
    if table_exists is False and _catalog_source(ctx) and any(_lookup(nulls, s) is None for s in text_sources):
        # REST and Studio Validate do not pass the source catalog: read it, as
        # Execute does. An unreadable catalog is "could not check".
        probed, source_pk, catalog_error = source_catalog_facts(ctx)
        nulls = {**probed, **nulls}
        keys += source_pk
        pk_known = not catalog_error
    from services.source_duplicate_probe import is_sqlish_source

    native = str(ctx.get("source_kind") or "").lower() in {"database", "cloud"} and is_sqlish_source(
        str(ctx.get("source_format") or "")) and not str(ctx.get("source_file_id") or "").strip()
    targets = candidates(
        destination_db_type=dest_db, mappings=maps, column_nullability=nulls,
        destination_table_exists=table_exists,
        destination_column_nullability=ctx.get("destination_column_nullability") or {},
        column_types=column_types, key_columns=keys, native_source_types=native,
    )
    if catalog_error:
        seen = {t["key"] for t in targets}
        for m in maps:
            source, target = str(m.get("source") or ""), str(m.get("target") or "")
            if source and target and _is_text(m, column_types) and _lookup(nulls, source) is None \
                    and pair_key(source, target) not in seen:
                targets.append({"key": pair_key(source, target), "source": source, "target": target,
                                "transform": str(m.get("transform") or ""), "kind": "create_new",
                                "approvable": True, "count": 0, "found": False, "exact": False,
                                "how": "catalog", "why": "catalog", "error": catalog_error})
    if not targets:
        return None, []
    from services.sync_cursor import incremental_read_narrows

    sync_mode = str(ctx.get("sync_mode") or "")
    row_filter, filter_error = _row_filter(ctx)
    read_scope = ctx.get("read_scope") if incremental_read_narrows(sync_mode) else None
    recipe_payload = dict(ctx.get("shape_recipe") or {})
    recipe = _recipe_of(recipe_payload)
    stored = bool(str(ctx.get("source_file_id") or "").strip())
    from services.code_crosswalk import _normalize_source_cfg

    _cfg, src_engine = _normalize_source_cfg(str(ctx.get("source_connector_id") or ""), ctx.get("source_config"))
    src_engine = (src_engine or str(ctx.get("source_format") or "")).lower()
    asked = [str(m.get("target") or m.get("source") or "") for m in maps if _check_now_asked(m)]

    def walk(cols: list[str]) -> Any:
        if filter_error:
            raise ValueError(filter_error)
        rows = _walk_rows(ctx, cols, maps, column_types, read_scope, row_filter, recipe)
        if rows is not None and recipe is not None:
            from services.shape_preflight import shaped_population_rows

            rows = shaped_population_rows(recipe_payload, rows) or rows
        return rows

    def probe(column: str, check_now: bool, seconds: float, count: bool) -> Any:
        if recipe is not None or stored or filter_error:
            return None
        return sql_empty_probe(
            source_connector_id=str(ctx.get("source_connector_id") or ""),
            source_config=ctx.get("source_config"), source_table=str(ctx.get("source_table") or ""),
            column=column, check_now=check_now, count=count,
            timeout_seconds=min(seconds, CHECK_NOW_SECONDS if check_now else PUSHDOWN_TIMEOUT_SECONDS),
        )

    scope = json.dumps([row_filter, repr(read_scope), recipe_payload], sort_keys=True, default=str)
    billed = src_engine in BILLED_SCAN_ENGINES
    execute_confirm = bool(ctx.get("skip_population_fit"))
    fresh, measured = [], []
    for t in targets:
        asked_here = t["target"] in asked
        hit = (None if t.get("why") == "catalog" or asked_here
               else _recall(_measure_key(ctx, t, scope), execute_confirm=execute_confirm, billed=billed))
        if hit:
            measured.append({**t, **hit, "reused": True})
        else:
            fresh.append(t)
    population = ctx.get("population_rows") if ctx.get("rows_are_population") else None
    done = measure(
        fresh, population_rows=population, probe=probe,
        whole_scope=not row_filter and read_scope is None and recipe is None, walk=walk,
        sample_rows=list(ctx.get("sample_rows") or []),
        walk_allowed=not billed, engine=src_engine, check_now=asked,
    )
    for t in done:
        if t.get("why") != "catalog" and (t.get("exact") or t.get("found")):
            kept = {k: t.get(k) for k in ("count", "found", "exact", "how", "why", "rows_scanned",
                                          "error", "seconds", "engine")}
            _remember(_measure_key(ctx, t, scope), kept, checked_now=t["target"] in asked)
    measured += done
    needs_decision = [t for t in measured if t["kind"] == "create_new" and t["approvable"]
                      and (int(t.get("count") or 0) > 0 or t.get("found") or not t.get("exact"))]
    if needs_decision and not pk_known and not catalog_error and _catalog_source(ctx):
        # The source primary key decides what is approvable: read only when needed.
        _n, source_pk, _err = source_catalog_facts(ctx)
        pk = {str(c).lower() for c in source_pk}
        for t in measured:
            if t["source"].lower() in pk or t["target"].lower() in pk:
                t["approvable"] = False
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
    probe: Any = None,
    whole_scope: bool = True,
    destination_table: str = "",
    walk_allowed: bool = True,
    check_now: Iterable[str] = (),
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
    measured = measure(targets, population_rows=population_rows, probe=probe,
                       whole_scope=whole_scope, walk=walk, sample_rows=sample_rows,
                       walk_allowed=walk_allowed, check_now=check_now)
    return build_gate(measured, mappings=mappings, table=destination_table)


def stamp_check_now(mappings: list[dict[str, Any]], acceptance: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Record "Check this column now" on the named columns' mappings (not a risk: no contract).

    It is asked through the same entry points, under the same permission, as a
    risk acceptance (the calling route or tool checks it). With authentication
    on, the request records the signed-in user, never free text, and is
    refused without one. It is honoured for CHECK_NOW_VALID_SECONDS; the
    measurement it produces is kept and reused as described at _MEASURED.
    """
    from datetime import datetime, timedelta, timezone

    named = {str(c).strip().lower() for c in (acceptance.get("columns") or []) if str(c or "").strip()}
    if not named:
        raise ValueError(f"risk_acceptance option {CHECK_OPTION} needs columns. Nothing was recorded.")
    from services.workspace_filter import current_actor
    from src.services.auth_service import auth_required

    actor = str(current_actor() or "").strip()
    if not actor:
        if auth_required():
            raise ValueError(f"risk_acceptance option {CHECK_OPTION} needs a signed-in user. "
                             "Nothing was recorded.")
        actor = "local operator (authentication is off)"
    now = datetime.now(timezone.utc)
    out, hit = [], set()
    for raw in mappings or []:
        row = dict(raw)
        source, target = str(row.get("source") or "").lower(), str(row.get("target") or "").lower()
        name = next((n for n in (target, pair_key(source, target), source) if n in named), "")
        if name:
            row[CHECK_KEY] = {
                "requested_by": actor, "at": now.isoformat(),
                "expires_at": (now + timedelta(seconds=CHECK_NOW_VALID_SECONDS)).isoformat(),
                "workspace_id": current_workspace(), "budget_seconds": CHECK_NOW_SECONDS,
            }
            hit.add(name)
        out.append(row)
    if mappings and named - hit:
        raise ValueError(f"risk_acceptance option {CHECK_OPTION} named unmapped column(s) "
                         f"{', '.join(sorted(named - hit))}. Nothing was recorded.")
    return out


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
