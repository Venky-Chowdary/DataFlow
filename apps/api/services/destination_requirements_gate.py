"""Gate G14 — a required destination column nothing fills must block at Validate.

A destination column that is ``NOT NULL``, has no default, and is neither an
identity nor a generated column has exactly one filler: a mapping. When none
exists the write is already doomed, and the operator used to learn that from
the engine mid-write (``ORA-01400``, MySQL ``1364``, PG ``not-null constraint``)
instead of from Validate. The outcome was safe — no rows land — but the failure
arrives as a driver error with no remediation, after the operator approved.

Honesty: nullability comes from the destination catalog. When the catalog was
not read, the gate reports *unmeasured* rather than pass — an unread catalog is
not evidence that every required column is filled.
"""

from __future__ import annotations

from typing import Any

from services.mapping_constraints import write_mappings
from services.source_coverage_gate import build_source_coverage_gate

GATE_ID = "g14_destination_requirements"
_NAMED_LIMIT = 8


def _lower_set(values: list[str] | None) -> set[str]:
    return {str(v).strip().lower() for v in (values or []) if str(v).strip()}


def unfilled_required_columns(
    *,
    column_nullability: dict[str, bool] | None,
    column_defaults: dict[str, str] | None,
    identity_columns: list[str] | None,
    generated_columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
) -> list[str]:
    """Destination columns that require a value no mapping and no engine supplies."""
    nulls = {str(k): bool(v) for k, v in (column_nullability or {}).items()}
    if not nulls:
        return []
    filled = _lower_set([
        str(m.get("target") or "") for m in write_mappings(mappings) if m.get("target")
    ])
    has_default = _lower_set(list((column_defaults or {}).keys()))
    engine_filled = _lower_set(identity_columns) | _lower_set(generated_columns)
    return [
        name
        for name, nullable in nulls.items()
        if not nullable
        and name.strip().lower() not in filled
        and name.strip().lower() not in has_default
        and name.strip().lower() not in engine_filled
    ]


def build_destination_requirements_gate(
    *,
    destination_table_exists: bool | None,
    column_nullability: dict[str, bool] | None,
    column_defaults: dict[str, str] | None,
    identity_columns: list[str] | None,
    generated_columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    """Return the G14 gate, or ``None`` when there is no existing table to check."""
    if destination_table_exists is not True:
        return None

    if not (column_nullability or {}):
        # Unmeasured ≠ proven. We could not read the destination's required-column
        # contract, so this gate must NOT be presented as a green/proven pass.
        # It is a SKIP carrying an explicit ``unmeasured`` flag: the NOT NULL
        # contract is still enforced fail-closed at write (a truly unfilled
        # required column is rejected at row 1, never silently dropped), so
        # hard-blocking every existing-table transfer whose caller did not wire
        # nullability metadata would be a false block, not added safety.
        return {
            "id": GATE_ID,
            "status": "skip",
            "message": (
                "Destination nullability catalog unreadable — required-column "
                "coverage is unmeasured (not proven). The NOT NULL contract is "
                "still enforced at write."
            ),
            "duration_ms": 0,
            "details": {
                "reason": "nullability_metadata_unavailable",
                "unmeasured": True,
                "rule_id": f"{GATE_ID}.unmeasured",
                "remediation_kind": "retry_validate",
            },
        }

    unfilled = unfilled_required_columns(
        column_nullability=column_nullability,
        column_defaults=column_defaults,
        identity_columns=identity_columns,
        generated_columns=generated_columns,
        mappings=mappings,
    )
    required = [
        str(k) for k, v in (column_nullability or {}).items() if not bool(v)
    ]
    if not unfilled:
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": (
                f"All {len(required)} required destination column(s) are filled by a "
                "mapping, a default, or the engine"
            ),
            "duration_ms": 0,
            "details": {
                "required_columns": required,
                "identity_columns": list(identity_columns or []),
                "generated_columns": list(generated_columns or []),
            },
        }

    named = ", ".join(unfilled[:_NAMED_LIMIT])
    more = (
        f" (+{len(unfilled) - _NAMED_LIMIT} more)"
        if len(unfilled) > _NAMED_LIMIT
        else ""
    )
    return {
        "id": GATE_ID,
        "status": "block",
        "message": (
            f"{len(unfilled)} destination column(s) are NOT NULL with no default and "
            f"no source mapping: {named}{more} — the write would be rejected row 1."
        ),
        "duration_ms": 0,
        "details": {
            "unfilled_required_columns": unfilled,
            "required_columns": required,
            "rule_id": f"{GATE_ID}.unfilled",
            "remediation_kind": "review_mappings",
        },
    }


def build_mapping_contract_gates(
    *,
    source_columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
    destination_table_exists: bool | None,
    column_nullability: dict[str, bool] | None,
    column_defaults: dict[str, str] | None,
    identity_columns: list[str] | None,
    generated_columns: list[str] | None,
    dest_columns: list[str] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Source coverage (G13), dest requirements (G14), dest-exists shape (G15).

    Returns ``(source_coverage, gates, blockers)``. G15 does not add blockers.
    """
    coverage, cov_gate = build_source_coverage_gate(
        source_columns=list(source_columns or []), mappings=list(mappings or [])
    )
    req_gate = build_destination_requirements_gate(
        destination_table_exists=destination_table_exists,
        column_nullability=column_nullability,
        column_defaults=column_defaults,
        identity_columns=identity_columns,
        generated_columns=generated_columns,
        mappings=list(mappings or []),
    )
    from services.shape_contract import build_shape_gate, classify_dest_exists_shape

    shape = classify_dest_exists_shape(
        destination_table_exists=destination_table_exists,
        source_columns=list(source_columns or []),
        dest_columns=list(dest_columns or (column_nullability or {}).keys()),
        mappings=list(mappings or []),
        column_nullability=column_nullability,
        column_defaults=column_defaults,
        identity_columns=identity_columns,
        generated_columns=generated_columns,
    )
    coverage = {**coverage, "shape_contract": shape}
    shape_gate = build_shape_gate(shape)
    gates = [g for g in (cov_gate, req_gate, shape_gate) if g]
    blockers = [
        {"id": g["id"], "message": g["message"], "details": g["details"]}
        for g in gates
        if g["status"] == "block"
    ]
    return coverage, gates, blockers
