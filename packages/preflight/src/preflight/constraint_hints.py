"""Relational constraint findings — schema FK coverage honesty.

Core Validate remains G1–G9. This module assesses destination FK metadata
against write mappings. Findings are structured with severity:

* ``block`` — strict/maximum without acknowledgement (fail closed)
* ``ack_required`` — operator must acknowledge FK risk
* ``info`` — advisory only

Never claims population orphan detection from schema hints alone.
Do not market these as an additional numbered gate unless a GateId is allocated.
"""

from __future__ import annotations

from typing import Any, Mapping


def _as_mapping(ctx: Any) -> Mapping[str, Any]:
    """Normalize PreflightContext, TransferPlan-ish objects, or plain dicts."""
    if isinstance(ctx, Mapping):
        return ctx
    plan = getattr(ctx, "plan", None)
    if plan is not None:
        return {
            "source_columns": list(getattr(getattr(plan, "source", None), "columns", None) or []),
            "destination_columns": list(
                getattr(getattr(plan, "destination", None), "target_columns", None) or []
            ),
            "mappings": list(getattr(plan, "mappings", None) or []),
            "destination_foreign_keys": list(
                getattr(plan, "destination_foreign_keys", None) or []
            ),
            "destination_pk_columns": list(
                getattr(plan, "destination_pk_columns", None) or []
            ),
            "destination_unique_keys": list(
                getattr(plan, "destination_unique_keys", None) or []
            ),
            "table_exists": getattr(
                getattr(plan, "destination", None), "table_exists", None
            ),
            "validation_mode": getattr(plan, "validation_mode", None),
            "fk_risk_acknowledged": getattr(plan, "fk_risk_acknowledged", False),
        }
    return {
        "source_columns": list(getattr(ctx, "source_columns", None) or []),
        "destination_columns": list(getattr(ctx, "destination_columns", None) or []),
        "mappings": list(getattr(ctx, "mappings", None) or []),
        "destination_foreign_keys": list(
            getattr(ctx, "destination_foreign_keys", None) or []
        ),
        "destination_pk_columns": list(getattr(ctx, "destination_pk_columns", None) or []),
        "destination_unique_keys": list(
            getattr(ctx, "destination_unique_keys", None) or []
        ),
        "table_exists": getattr(ctx, "table_exists", None),
        "validation_mode": getattr(ctx, "validation_mode", None),
        "fk_risk_acknowledged": getattr(ctx, "fk_risk_acknowledged", False),
    }


def _col_name(item: Any) -> str:
    if item is None:
        return ""
    if isinstance(item, str):
        return item.strip()
    name = getattr(item, "name", None)
    if name is not None:
        return str(name).strip()
    if isinstance(item, Mapping):
        return str(item.get("name") or "").strip()
    return str(item).strip()


def _mapped_targets(mappings: list[Any]) -> set[str]:
    targets: set[str] = set()
    for m in mappings:
        if isinstance(m, Mapping):
            tgt = str(m.get("target") or "").strip()
            omitted = bool(m.get("intentional_omit") or m.get("intentionalOmit"))
        else:
            tgt = str(getattr(m, "target", "") or "").strip()
            omitted = bool(getattr(m, "intentional_omit", False))
        if tgt and not omitted:
            targets.add(tgt.lower())
    return targets


def _fk_columns(fk: Mapping[str, Any]) -> list[str]:
    raw = fk.get("columns") or fk.get("column") or fk.get("fk_columns") or []
    if isinstance(raw, str):
        raw = [raw]
    return [str(c).strip() for c in raw if str(c).strip()]


def _severity_for_unmapped_fk(
    *,
    validation_mode: str,
    fk_risk_acknowledged: bool,
    table_exists: bool | None,
) -> str:
    """Strict/maximum + live dest table → block unless operator acknowledged."""
    mode = (validation_mode or "strict").strip().lower()
    if fk_risk_acknowledged:
        return "info"
    if table_exists and mode in {"strict", "maximum"}:
        return "block"
    if table_exists:
        return "ack_required"
    return "info"


def assess_constraint_compatibility(
    ctx: Any,
    *,
    validation_mode: str | None = None,
    fk_risk_acknowledged: bool | None = None,
) -> list[dict[str, Any]]:
    """Return structured FK / constraint findings (never invents RI proven).

    Empty / unknown schemas yield no findings. Destination FK columns not covered
    by write mappings produce findings with severity based on validation mode.
    """
    data = _as_mapping(ctx)
    source_cols = [_col_name(c) for c in list(data.get("source_columns") or [])]
    dest_cols = [_col_name(c) for c in list(data.get("destination_columns") or [])]
    source_cols = [c for c in source_cols if c]
    dest_cols = [c for c in dest_cols if c]

    if not source_cols and not dest_cols:
        return []

    mode = (
        validation_mode
        if validation_mode is not None
        else str(data.get("validation_mode") or "strict")
    )
    ack = (
        bool(fk_risk_acknowledged)
        if fk_risk_acknowledged is not None
        else bool(data.get("fk_risk_acknowledged"))
    )
    table_exists = data.get("table_exists")

    mappings = list(data.get("mappings") or [])
    fks = [fk for fk in list(data.get("destination_foreign_keys") or []) if isinstance(fk, Mapping)]
    if not fks:
        return []

    mapped = _mapped_targets(mappings)
    dest_names = {c.lower() for c in dest_cols}
    findings: list[dict[str, Any]] = []

    for fk in fks:
        cols = _fk_columns(fk)
        if not cols:
            continue
        ref_table = str(fk.get("referenced_table") or fk.get("ref_table") or "").strip()
        ref_cols = fk.get("referenced_columns") or fk.get("ref_columns") or []
        if isinstance(ref_cols, str):
            ref_cols = [ref_cols]
        ref_cols = [str(c).strip() for c in ref_cols if str(c).strip()]

        missing = [c for c in cols if c.lower() not in mapped]
        if missing:
            col_list = ", ".join(missing)
            ref_bit = ""
            if ref_table:
                ref_target = ".".join(
                    [ref_table, ", ".join(ref_cols) if ref_cols else ""]
                ).rstrip(".")
                ref_bit = f" → {ref_target}" if ref_target else f" → {ref_table}"
            severity = _severity_for_unmapped_fk(
                validation_mode=mode,
                fk_risk_acknowledged=ack,
                table_exists=bool(table_exists),
            )
            findings.append(
                {
                    "code": "fk_column_unmapped",
                    "severity": severity,
                    "columns": missing,
                    "referenced_table": ref_table or None,
                    "referenced_columns": ref_cols,
                    "coverage": "destination_fk_metadata",
                    "message": (
                        f"Foreign key column(s) {col_list}{ref_bit} are not covered by "
                        "the current mapping; destination FK checks may reject rows "
                        f"(coverage=destination_fk_metadata · severity={severity} — "
                        "population orphan detection not claimed)."
                    ),
                }
            )
            continue

        if dest_names:
            unknown = [c for c in cols if c.lower() not in dest_names]
            if unknown:
                findings.append(
                    {
                        "code": "fk_column_missing_from_schema_snapshot",
                        "severity": "info",
                        "columns": unknown,
                        "coverage": "destination_fk_metadata",
                        "message": (
                            f"Mapped foreign key column(s) {', '.join(unknown)} are not "
                            "present on the destination schema snapshot "
                            "(informational — verify introspected FK metadata)."
                        ),
                    }
                )

    return findings


def constraint_findings_block_transfer(
    findings: list[dict[str, Any]] | None,
    *,
    validation_mode: str = "strict",
    fk_risk_acknowledged: bool = False,
) -> bool:
    """True when FK findings must refuse Execute unlock."""
    if fk_risk_acknowledged:
        return False
    mode = (validation_mode or "strict").strip().lower()
    for f in findings or []:
        if not isinstance(f, dict):
            continue
        sev = str(f.get("severity") or "").lower()
        if sev == "block":
            return True
        if sev == "ack_required" and mode in {"strict", "maximum"}:
            return True
    return False


def referential_integrity_posture(
    findings: list[dict[str, Any]] | None,
    *,
    population_orphan_probe_ran: bool = False,
    population_orphan_count: int | None = None,
    sample_orphan_probe_ran: bool = False,
    sample_orphan_count: int | None = None,
) -> dict[str, Any]:
    """Honesty stamp — schema FK hints / sample probes never equal population RI.

    ``proven`` is True only after a full-table population orphan probe reports
    zero orphans. Sample probes never set ``proven``.
    """
    findings = list(findings or [])
    has_schema = any(
        isinstance(f, dict) and f.get("coverage") == "destination_fk_metadata"
        for f in findings
    )
    pop_count = population_orphan_count
    # Sample findings never veto a completed clean population scan.
    # Incomplete population work must leave population_orphan_count=None.
    proven = bool(
        population_orphan_probe_ran
        and pop_count is not None
        and int(pop_count) == 0
    )
    if proven:
        coverage = "population_orphan_probe"
        note = "Population orphan detection proven for selected transfer."
    elif sample_orphan_probe_ran:
        coverage = "sample_orphan_probe"
        note = (
            "Sample orphan probe ran on Validate sample FK values only — "
            "population referential integrity is not proven."
        )
    elif has_schema:
        coverage = "destination_fk_metadata"
        note = (
            "FK metadata coverage only — population orphan / referential "
            "integrity is not proven from schema hints alone."
        )
    else:
        coverage = "none"
        note = (
            "No FK metadata or orphan probe for this run — referential "
            "integrity is not proven."
        )
    return {
        "proven": proven,
        "coverage": coverage,
        "population_orphan_probe_ran": bool(population_orphan_probe_ran),
        "population_orphan_count": pop_count,
        "sample_orphan_probe_ran": bool(sample_orphan_probe_ran),
        "sample_orphan_count": sample_orphan_count,
        "finding_count": len(findings),
        "note": note,
    }


def constraint_hint_messages(findings: list[dict[str, Any]] | None) -> list[str]:
    """Backward-compatible string list for Studio soft-hint surfaces."""
    out: list[str] = []
    for f in findings or []:
        if isinstance(f, dict):
            msg = str(f.get("message") or "").strip()
            if msg:
                out.append(msg)
        elif f:
            out.append(str(f))
    return out
