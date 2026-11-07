"""Plan-time FK parent probe: is each source reference's parent at the destination?

A single-table copy of a child table (QA round 11: Rfam ``family`` →
PostgreSQL) used to pass Validate and then fail mid-run, when the write-path
orphan guard looked for the parent in the destination and refused ("could not
verify … refusing to write unverified child rows"). The same question is now
asked once, at plan time, and answered as one finding:

* parent absent at the destination → ``warn``: the reference is not guarded
  and not carried (no constraint to a table that is not there);
* parent present → the Validate sample's FK tuples are looked up in it; sample
  orphans are one finding that blocks in strict mode unless the operator
  acknowledged the FK risk (then they are quarantined at write time);
* route option ``foreign_keys=ignore`` → ``info``: rows are copied as read;
* destination catalog unreadable → no claim (the write guard still fails
  closed for that reference, as before).

Never invents population proof: the orphan check covers the sample only.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_MAX_TUPLES = 500


def _label(table: str, fk: dict[str, Any]) -> str:
    cols = ",".join(str(c) for c in fk.get("columns") or [])
    pcols = ",".join(str(c) for c in fk.get("referenced_columns") or [])
    return f"{table}.{cols} -> {fk.get('referenced_table')}.{pcols}"


def route_fk_policy(destination_config: Any) -> str:
    """``foreign_keys`` route option carried on the destination config."""
    from src.transfer.stream_foreign_keys import FK_ROUTE_OPTION, fk_route_policy

    cfg = dict(destination_config or {})
    nested = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    return fk_route_policy(
        {"extra": {FK_ROUTE_OPTION: cfg.get(FK_ROUTE_OPTION) or nested.get(FK_ROUTE_OPTION) or ""}}
    )


def with_fk_route_option(probe_cfg: Any, foreign_keys: Any) -> Any:
    """Destination probe config carrying the route's ``foreign_keys`` option.

    The preflight probe config is the connector's, so the route option chosen
    for this run (MCP/REST/Studio destination options) is laid on top of it.
    """
    if not probe_cfg:
        return probe_cfg or None
    policy = str(foreign_keys or "").strip().lower()
    if not policy:
        return probe_cfg
    from src.transfer.stream_foreign_keys import FK_ROUTE_OPTION

    merged = dict(probe_cfg)
    merged[FK_ROUTE_OPTION] = policy
    return merged


def _destination_tables(dest_cfg: dict[str, Any]) -> tuple[set[str] | None, str]:
    try:
        from connectors.generic_sql import _engine
        from services.dialect_profiles import catalog_namespace
        from services.foreign_key_orchestration import _destination_tables as _list
        from src.transfer.connector_capabilities import resolve_driver_type

        d_type = resolve_driver_type(str(dest_cfg.get("type") or dest_cfg.get("db_type") or ""))
        return _list(_engine(dest_cfg), d_type, catalog_namespace(d_type, dest_cfg)), ""
    except Exception as exc:  # noqa: BLE001 — unreadable catalog is a state
        return None, f"{type(exc).__name__}: {exc}"


def _sample_tuples(
    sample_rows: list[dict[str, Any]] | None, columns: list[str]
) -> list[tuple[Any, ...]]:
    from services.value_serializer import present_cell_text

    seen: set[tuple[Any, ...]] = set()
    out: list[tuple[Any, ...]] = []
    for row in sample_rows or []:
        if not isinstance(row, dict):
            continue
        tup = tuple(row.get(c) for c in columns)
        key = tuple(present_cell_text(v) for v in tup)
        if any(k is None for k in key) or key in seen:
            continue  # MATCH SIMPLE: a NULL in the key is unconstrained
        seen.add(key)
        out.append(tup)
        if len(out) >= _MAX_TUPLES:
            break
    return out


def assess_fk_parents(
    *,
    foreign_keys: list[dict[str, Any]] | None,
    destination_config: Any,
    source_table: str,
    sample_rows: list[dict[str, Any]] | None,
    validation_mode: str,
    fk_risk_acknowledged: bool,
) -> dict[str, Any]:
    """Report plus at most one finding for the route's FK parents."""
    report: dict[str, Any] = {"ran": False, "relations": [], "finding": None}
    table = (source_table or "").strip()
    fks = [
        fk
        for fk in (foreign_keys or [])
        if isinstance(fk, dict)
        and str(fk.get("referenced_table") or "").strip()
        and str(fk.get("referenced_table")).strip().lower() != table.lower()
    ]
    if not fks or not destination_config:
        return report
    try:
        policy = route_fk_policy(destination_config)
    except ValueError as exc:
        report["finding"] = {
            "code": "fk_route_option_invalid",
            "severity": "block",
            "columns": [],
            "message": str(exc),
        }
        return report
    report["policy"] = policy
    labels = [_label(table, fk) for fk in fks]
    if policy == "ignore":
        report["ran"] = True
        report["finding"] = {
            "code": "fk_route_ignore",
            "severity": "info",
            "columns": labels[:20],
            "message": (
                "foreign_keys=ignore: child rows are copied as read; no destination "
                "parent check and no constraint for " + "; ".join(labels[:5]) + "."
            ),
        }
        return report
    from src.transfer.stream_foreign_keys import FK_ROUTE_OPTION

    dest_cfg = {k: v for k, v in dict(destination_config).items() if k != FK_ROUTE_OPTION}
    known, detail = _destination_tables(dest_cfg)
    if known is None:
        report["note"] = f"destination catalog not listed ({detail}); parents unverified"
        return report
    report["ran"] = True
    absent: list[str] = []
    orphan_labels: list[str] = []
    orphan_total = 0
    for fk, label in zip(fks, labels):
        parent = str(fk.get("referenced_table")).strip()
        present = parent.lower() in known
        rel: dict[str, Any] = {"label": label, "parent": parent, "present": present}
        if not present:
            absent.append(parent)
        else:
            cols = [str(c) for c in fk.get("columns") or []]
            pcols = [str(c) for c in fk.get("referenced_columns") or []]
            tuples = _sample_tuples(sample_rows, cols)
            if tuples and len(cols) == len(pcols):
                try:
                    from services.fk_tuple_scan import sql_existing_parent_tuples
                    from services.value_serializer import present_cell_text

                    found = sql_existing_parent_tuples(
                        dest_cfg, parent_table=parent, parent_columns=pcols, values=tuples
                    )
                    have = {tuple(present_cell_text(v) for v in t) for t in found}
                    orphans = [
                        t for t in tuples if tuple(present_cell_text(v) for v in t) not in have
                    ]
                    rel["sample_tuples"] = len(tuples)
                    rel["sample_orphans"] = len(orphans)
                    if orphans:
                        orphan_total += len(orphans)
                        orphan_labels.append(label)
                except Exception as exc:  # noqa: BLE001 — reported, not hidden
                    rel["error"] = f"{type(exc).__name__}: {exc}"
        report["relations"].append(rel)
    if orphan_labels:
        strict = (validation_mode or "strict").strip().lower() in {"strict", "maximum"}
        report["finding"] = {
            "code": "fk_sample_orphans_at_destination",
            "severity": "block" if strict and not fk_risk_acknowledged else "warn",
            "columns": orphan_labels[:20],
            "coverage": "sample_orphan_probe",
            "population_proof": False,
            "message": (
                f"{orphan_total} distinct sample key(s) reference a parent row the "
                "destination does not hold (" + "; ".join(orphan_labels[:5]) + "). "
                + (
                    "Strict validation refuses orphan child rows — load the parent "
                    "rows first, acknowledge the FK risk to quarantine them, or set "
                    "foreign_keys=ignore."
                    if strict and not fk_risk_acknowledged
                    else "Orphan rows are quarantined at write time, not written."
                )
                + (
                    f" Not at the destination (not carried): {', '.join(sorted(set(absent)))}."
                    if absent
                    else ""
                )
            ),
        }
    elif absent:
        parents = sorted(set(absent))
        report["finding"] = {
            "code": "fk_parent_absent_at_destination",
            "severity": "warn",
            "columns": [l for l, fk in zip(labels, fks) if fk.get("referenced_table") in parents][:20],
            "parents": parents,
            "message": (
                "Parent table(s) " + ", ".join(parents) + " are not at the "
                "destination, so the foreign key is not carried and the child rows "
                "are not checked against a parent. Copy the parent first (or in the "
                "same run) to keep the constraint."
            ),
        }
    return report
