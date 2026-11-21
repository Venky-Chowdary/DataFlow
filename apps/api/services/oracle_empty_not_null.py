"""Oracle stores ``''`` as NULL: a NOT NULL text column cannot take an empty string.

Owner decision 4 (D210) writes ``''`` to Oracle as NULL. Into a NOT NULL
column that is ORA-01400, and it used to surface only at Execute, after
Validate had passed (oracle-live CI, ``test_postgresql_to_oracle_typed_preflight_on``).
No value may change silently (docs/migration-rules.md), so Validate decides:

* the candidates are the text mappings into an Oracle destination whose column
  will be NOT NULL: on create-new, a source column the catalog proves NOT
  NULL (create-new carries it); on an existing table, a destination column the
  catalog proves NOT NULL;
* the source population is scanned for empty strings (the projected table walk
  Validate's fit scan uses, bounded by rows and time; a preview otherwise),
  after the mapping's transform, which is the value the writer sends;
* none found: no gate. Found: a blocking gate naming the column and count.

On create-new there is one approvable option, "Create <col> as NULL-able;
empty strings land as NULL", recorded as a signed Migration Risk Contract on
the mapping (``empty_as_null_contract``; ``risk_acceptance`` with ``option``
:data:`OPTION`). The table is then created with that column NULL-able, Gate-8
reports the rows as explained by the contract (:func:`explain_empty_as_null`),
and the signed proof pack lists the contract among the accepted risks. An
existing NOT NULL column is never altered: the gate blocks and suggests a
DEFAULT on the column or a transform that writes a non-empty value.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable, Mapping
from typing import Any

GATE_ID = "g3o_oracle_empty_not_null"
OPTION = "oracle_empty_as_null"
RISK_KIND = "oracle_empty_string_as_null"
CONTRACT_KEY = "empty_as_null_contract"
#: Bounds of the scan, like the other population walks Validate runs.
SCAN_MAX_ROWS = 1_000_000
SCAN_MAX_SECONDS = 60.0


def is_oracle(db_type: str | None) -> bool:
    return "oracle" in str(db_type or "").strip().lower()


def _lookup(mapping: Mapping[str, Any] | None, name: str) -> Any:
    if not mapping or not name:
        return None
    if name in mapping:
        return mapping[name]
    lower = name.lower()
    return next((v for k, v in mapping.items() if str(k).lower() == lower), None)


def _is_text(mapping: Mapping[str, Any], column_types: Mapping[str, str] | None) -> bool:
    from services.type_system import normalize_logical_type

    declared = str(mapping.get("target_type") or "").strip()
    if not declared:
        declared = str(_lookup(column_types, str(mapping.get("source") or "")) or "")
    return normalize_logical_type(declared) in {"string", "text"} if declared else False


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
    if not transform or transform.strip().lower() in {"none", "identity"}:
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
    max_rows: int = SCAN_MAX_ROWS,
    max_seconds: float = SCAN_MAX_SECONDS,
) -> dict[str, Any]:
    """Empty strings each target will be sent, over at most ``max_rows`` rows."""
    counts = {t["source"]: 0 for t in targets}
    scanned = 0
    truncated = ""
    deadline = time.monotonic() + max_seconds
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
                counts[t["source"]] += 1
    return {"counts": counts, "rows_scanned": scanned, "truncated": truncated}


def approved_contract(mapping: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """The mapping's signed, intact empty-as-NULL contract, or ``None``."""
    raw = (mapping or {}).get(CONTRACT_KEY) if isinstance(mapping, Mapping) else None
    if not isinstance(raw, dict):
        return None
    from services.migration_risk_contract import verify_risk_contract

    if str(raw.get("loss_classification") or "") != RISK_KIND or not verify_risk_contract(raw):
        return None
    return raw


def build_gate(
    targets: list[dict[str, Any]],
    scan: dict[str, Any],
    *,
    mappings: Iterable[Mapping[str, Any]] | None,
    measured_over: str,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """``(gate, blockers)``; no gate when the scan found no empty string."""
    by_source = {str(m.get("source") or ""): m for m in mappings or [] if isinstance(m, Mapping)}
    counts = scan.get("counts") or {}
    found = [t for t in targets if int(counts.get(t["source"]) or 0) > 0]
    if not found:
        if not scan.get("truncated"):
            return None, []
        return {
            "id": GATE_ID,
            "status": "warn",
            "message": (
                f"Oracle stores '' as NULL. {', '.join(t['target'] for t in targets)} will be "
                f"NOT NULL; {scan.get('rows_scanned', 0)} row(s) scanned had no empty string, "
                "but the scan stopped before the end of the source. A later empty string "
                "fails the write (ORA-01400); nothing is written as a different value."
            ),
            "duration_ms": 0,
            "details": {**scan, "measured_over": measured_over, "columns": targets},
        }, []
    blocked, approved = [], []
    for t in found:
        n = int(counts[t["source"]])
        contract = approved_contract(by_source.get(t["source"])) if t["approvable"] else None
        entry = {**t, "empty_rows": n}
        if contract:
            approved.append({**entry, "risk_id": contract.get("risk_id"),
                             "approved_by": contract.get("approved_by")})
        else:
            blocked.append(entry)
    details = {
        **scan, "measured_over": measured_over, "columns": blocked, "approved": approved,
        "option": OPTION,
    }
    if not blocked:
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": "; ".join(
                f"{a['target']}: {a['empty_rows']} empty string(s) land as NULL, created "
                f"NULL-able under signed Risk Contract {a['risk_id']}" for a in approved
            ),
            "duration_ms": 0,
            "details": details,
        }, []
    actions: list[dict[str, Any]] = []
    approvable = [b for b in blocked if b["approvable"]]
    if approvable:
        cols = [b["source"] for b in approvable]
        actions.append({
            "id": OPTION,
            "kind": OPTION,
            "label": "; ".join(
                f"Create {b['target']} as NULL-able; empty strings land as NULL" for b in approvable
            ),
            "risk_acceptance": {
                "option": OPTION, "columns": cols,
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
    parts = [
        f"{b['target']} ({'existing NOT NULL column' if b['kind'] == 'existing' else 'NOT NULL from the source'}) "
        f"would receive {b['empty_rows']} empty string(s) from {b['source']}"
        for b in blocked
    ]
    message = (
        "Oracle stores '' as NULL, so a NOT NULL column refuses it (ORA-01400): "
        + "; ".join(parts) + f" ({measured_over}). "
        + ("Approve the column as NULL-able, or " if approvable else "")
        + "write a non-empty value instead; an existing column is never altered."
    )
    fixes = []
    if approvable:
        fixes.append(
            "approve with risk_acceptance {\"option\": \"%s\", \"columns\": %s, \"approved_by\": ..., "
            "\"reason\": ...} to create %s NULL-able (empty strings land as NULL)"
            % (OPTION, json.dumps([b["source"] for b in approvable]),
               ", ".join(b["target"] for b in approvable))
        )
    fixes += [a["label"] for a in actions if a["id"] != OPTION]
    details["recommended_fix"] = "; or ".join(fixes)
    gate = {"id": GATE_ID, "status": "block", "message": message, "duration_ms": 0,
            "details": details}
    blocker = {"id": GATE_ID, "message": message, "details": details,
               "suggested_actions": actions}
    return gate, [blocker]


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
    walk: Any,
    population_rows: Any = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Validate's entry: candidates, then the bounded scan, then the gate.

    ``population_rows``: a batch Execute already holds in memory (a list) is
    the population; the source is not read a second time.
    """
    targets = candidates(
        destination_db_type=destination_db_type, mappings=mappings,
        column_nullability=column_nullability,
        destination_table_exists=destination_table_exists,
        destination_column_nullability=destination_column_nullability,
        column_types=column_types, key_columns=key_columns,
    )
    if not targets:
        return None, []
    rows = population_rows if isinstance(population_rows, (list, tuple)) else None
    try:
        if rows is None and walk is not None:
            rows = walk([t["source"] for t in targets])
    except Exception:  # noqa: BLE001 - an unreadable walk falls back to the preview
        rows = None
    measured_over = "source population"
    if rows is None:
        rows, measured_over = list(sample_rows or []), "preview rows only"
    scan = count_empty_strings(rows, targets)
    if measured_over != "source population" and not scan["truncated"]:
        scan["truncated"] = "preview"
    return build_gate(targets, scan, mappings=mappings, measured_over=measured_over)


def sign_empty_as_null(
    mappings: list[dict[str, Any]],
    acceptance: Mapping[str, Any],
    *,
    table: str,
) -> list[dict[str, Any]]:
    """Sign the empty-as-NULL contract on the named columns' mappings.

    Needs ``approved_by``, ``reason`` and ``columns``. Raises ``ValueError``
    (nothing signed) otherwise, or when a named column is not mapped.
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
    out, signed = [], set()
    for raw in mappings or []:
        row = dict(raw)
        source, target = str(row.get("source") or ""), str(row.get("target") or "")
        hit = next((n for n in (source, target) if n and n.lower() in wanted), "")
        if hit and approved_contract(row) is None:
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
                metadata={"option": OPTION},
            )
            row[CONTRACT_KEY] = contract.to_dict()
        if hit:
            signed.add(hit.lower())
        out.append(row)
    if mappings and wanted - signed:
        raise ValueError(
            f"risk_acceptance option {OPTION} named unmapped column(s) "
            f"{', '.join(sorted(wanted - signed))}. Nothing was signed."
        )
    return out


def relax_create_plan(plan: Any, mappings: Iterable[Mapping[str, Any]] | None, dest: str) -> Any:
    """Create an approved column NULL-able on Oracle (never a key column)."""
    if not is_oracle(dest):
        return plan
    approved = {}
    for m in mappings or []:
        if isinstance(m, Mapping) and approved_contract(m):
            approved[str(m.get("target") or m.get("source") or "").lower()] = approved_contract(m)
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


def destination_null_rows(endpoint: Any, table: str, columns: list[str]) -> dict[str, int] | None:
    """NULL cells per column of the Oracle destination table, or ``None`` if unreadable.

    The column was NOT NULL in the source, so each NULL it holds landed as an
    empty string under the contract. Opened through the writer's own engine
    and bounded like the NULL-marker probe.
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
        engine = probe_engine(cfg)
        try:
            with engine.connect() as conn:
                names, _fixed, _kt, types, _kn = _columns(conn, bare, schema, columns)
                if not names:
                    return None
                tbl = sa.table(bare, *[sa.column(n, types.get(n)) for n in names], schema=schema or None)
                query = sa.select(*[
                    sa.func.sum(sa.case((tbl.c[n].is_(None), 1), else_=0)) for n in names
                ]).select_from(tbl)
                with statement_timeout(conn, "oracle", probe_timeout_seconds()):
                    row = conn.execute(query).one()
                return {n.lower(): int(v or 0) for n, v in zip(names, row)}
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
) -> dict[str, Any]:
    """Gate-8 names the rows an approved contract explains (never a pass flip).

    Oracle's read-back already treats ``''`` and NULL as one value
    (services.reconciliation.destination_empty_string_is_null); this records
    how many rows that covers and under which signed contract, so the Trust
    Report shows them as explained, not as an unexplained difference. The
    count comes from the run's records when they carry the column, else from
    the destination (NULL cells of a column the source proves NOT NULL).
    """
    dest = str(getattr(endpoint, "format", "") or getattr(endpoint, "type", "") or "")
    if not isinstance(recon, dict) or not is_oracle(dest):
        return recon
    approved = [
        (m, c) for m in mappings or [] if isinstance(m, Mapping) and (c := approved_contract(m))
    ]
    if not approved:
        return recon
    carried = [r for r in records or [] if isinstance(r, Mapping)]
    counted_at_dest: dict[str, int] | None = None
    explained = []
    for m, contract in approved:
        source = str(m.get("source") or "")
        target = str(m.get("target") or source)
        rows, counted_by = None, ""
        if carried and any(_lookup(r, source) is not None or source in r for r in carried):
            rows, counted_by = sum(1 for r in carried if _lookup(r, source) == ""), "run records"
        else:
            if counted_at_dest is None:
                table = str((dest_summary or {}).get("table") or getattr(endpoint, "table", "") or "")
                counted_at_dest = destination_null_rows(
                    endpoint, table, [str(x.get("target") or x.get("source") or "") for x, _ in approved]
                ) or {}
            if target.lower() in counted_at_dest:
                rows, counted_by = counted_at_dest[target.lower()], "destination NULL cells"
        explained.append({
            "column": target,
            "source_column": source,
            "kind": RISK_KIND,
            "rows": rows,
            "counted_by": counted_by or "not counted",
            "risk_id": contract.get("risk_id"),
            "approved_by": contract.get("approved_by"),
        })
    out = dict(recon)
    out["explained_changes"] = [*(recon.get("explained_changes") or []), *explained]
    note = " ".join(
        f"{e['column']}: {e['rows'] if e['rows'] is not None else 'its'} empty string(s) "
        f"are NULL in Oracle, explained by signed Risk Contract {e['risk_id']} "
        f"(approved by {e['approved_by']})." for e in explained
    )
    out["message"] = f"{recon.get('message') or ''} {note}".strip()
    return out
