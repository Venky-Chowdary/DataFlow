"""G19 — a full refresh may not silently redefine an existing destination column.

A ``full_refresh_overwrite`` run drops the destination table and recreates it
from the source shape. That is the documented overwrite semantic and it is what
makes a second overwrite tick reproducible: the carrier the rows land in is the
one this run creates, not the one standing there now
(:mod:`services.sync_cursor`).

It also means the type declared on an *existing* destination column stops being
a contract. When an operator has declared ``amt_dec INTEGER`` and the source
carries ``DECIMAL(20,9)``, every other route refuses the narrowing — append,
upsert and CDC all block — while overwrite accepts all rows, because the
INTEGER column is dropped and recreated as ``NUMERIC(20,9)`` before the write.
The destination the operator declared is gone and nothing said so.

This gate names that replacement. It fires only where the live declared carrier
would have truncated or rejected the source magnitude
(:func:`carrier_would_truncate`), which is the case where the two readings of
the run disagree materially:

* the operator's reading — "this column is an integer amount, round or refuse";
* the engine's reading — "this column is whatever the source is".

A widening replacement (``VARCHAR(32)`` recreated as ``VARCHAR(64)``, or an
identical recreate on the second tick of a schedule) is not reported: nothing
the operator declared is contradicted by it.

The escape is the product's existing one, a continue-policy Migration Risk
Contract on the mapping, which demotes the block to a warning and records the
decision in the proof pack. This gate does not decide which reading is right;
it refuses to pick one silently.
"""

from __future__ import annotations

from typing import Any

from services.mapping_constraints import write_mappings

GATE_ID = "g19_dest_schema_replacement"

#: Columns named in the operator-facing message before it elides the rest.
_NAMED_LIMIT = 6


def _norm(name: str) -> str:
    return str(name or "").strip().lower()


def _live_type(target: str, live_types: dict[str, str]) -> str:
    exact = live_types.get(target)
    if exact:
        return str(exact)
    wanted = _norm(target)
    for key, value in live_types.items():
        if _norm(key) == wanted:
            return str(value)
    return ""


def carrier_would_truncate(source_type: str, live_type: str, *, dest_db: str = "") -> bool:
    """True when the live declared carrier cannot hold the source *magnitude*.

    Deliberately narrower than :func:`is_lossy_coercion`, which also answers
    fidelity and domain questions (``CHAR(36) → UUID``). Those are judgements
    about meaning, and on a recreate they are usually Datawrap arguing with a
    carrier Datawrap itself created on the previous tick. This asks the one
    question a recreate silently reverses: would a row have been truncated or
    rejected by the column the operator declared?
    """
    from services.type_system import (
        LOGICAL_DECIMAL,
        binary_width_would_narrow,
        bitstring_width_would_narrow,
        decimal_params_would_narrow,
        integer_storage_bounds,
        integer_width_would_narrow,
        interval_precision_would_narrow,
        normalize_logical_type,
        parse_numeric_precision_scale,
        string_width_would_narrow,
        temporal_precision_would_narrow,
    )

    if integer_width_would_narrow(source_type, live_type, dest_db=dest_db):
        return True
    if decimal_params_would_narrow(source_type, live_type, dest_db=dest_db):
        return True
    if string_width_would_narrow(source_type, live_type):
        return True
    if binary_width_would_narrow(source_type, live_type):
        return True
    if bitstring_width_would_narrow(source_type, live_type):
        return True
    if temporal_precision_would_narrow(source_type, live_type, dest_db=dest_db):
        return True
    if interval_precision_would_narrow(source_type, live_type):
        return True

    # Decimal into an integer carrier: the fractional digits have nowhere to go,
    # and 20 integral digits do not fit an int32 even when the scale is zero.
    bounds = integer_storage_bounds(live_type, dest_db=dest_db)
    if bounds is None:
        return False
    if normalize_logical_type(source_type) != LOGICAL_DECIMAL:
        return False
    precision, scale = parse_numeric_precision_scale(source_type)
    if (scale or 0) > 0:
        return True
    if not precision:
        return False
    return 10**precision - 1 > bounds[1]


def find_silent_replacements(
    *,
    mappings: list[dict[str, Any]] | None,
    source_column_types: dict[str, str] | None,
    destination_column_types: dict[str, str] | None,
    destination_db_type: str = "",
    source_db_type: str = "",
) -> list[dict[str, Any]]:
    """Mapped columns whose live declared carrier this run would replace.

    Only replacements the live carrier could not have absorbed are returned:
    the operator declared a carrier narrower than the source, so recreating the
    table quietly answers a question the operator answered differently.
    """
    from services.decision_kernel.type_invent import create_new_mapping_target_type
    from services.migration_risk_contract import mapping_has_clearing_risk_contract

    src_types = dict(source_column_types or {})
    live_types = dict(destination_column_types or {})
    findings: list[dict[str, Any]] = []
    for mapping in write_mappings(mappings):
        target = str(mapping.get("target") or "").strip()
        source = str(mapping.get("source") or "").strip()
        if not target or not source:
            continue
        live = _live_type(target, live_types).strip()
        if not live:
            continue
        source_type = str(src_types.get(source) or "").strip()
        if not source_type:
            continue
        if not carrier_would_truncate(source_type, live, dest_db=destination_db_type):
            continue
        replacement = create_new_mapping_target_type(
            source_type, destination_db_type, source_db=source_db_type
        )
        findings.append(
            {
                "source": source,
                "target": target,
                "source_type": source_type,
                "declared_destination_type": live,
                "replacement_type": replacement,
                "risk_contract_cleared": mapping_has_clearing_risk_contract(mapping),
            }
        )
    return findings


def _named(findings: list[dict[str, Any]]) -> str:
    named = ", ".join(
        f"{f['target']} ({f['declared_destination_type']} → {f['replacement_type']}"
        f", source {f['source_type']})"
        for f in findings[:_NAMED_LIMIT]
    )
    if len(findings) > _NAMED_LIMIT:
        named += f" (+{len(findings) - _NAMED_LIMIT} more)"
    return named


def build_dest_schema_replacement_gate(
    *,
    mappings: list[dict[str, Any]] | None,
    source_column_types: dict[str, str] | None,
    destination_column_types: dict[str, str] | None,
    destination_table_exists: bool | None,
    dest_recreated: bool,
    destination_db_type: str = "",
    source_db_type: str = "",
) -> dict[str, Any]:
    """G19 for one run. ``skip`` unless the run recreates a live typed table."""
    if not dest_recreated or destination_table_exists is not True:
        return {
            "id": GATE_ID,
            "status": "skip",
            "message": (
                "This run does not recreate an existing destination table"
                if destination_table_exists is not True
                else "Sync mode writes into the destination as declared"
            ),
            "duration_ms": 0,
            "details": {
                "dest_recreated": bool(dest_recreated),
                "destination_table_exists": destination_table_exists,
            },
        }

    from services.db_type_utils import dest_declares_column_ddl

    if not dest_declares_column_ddl(destination_db_type):
        return {
            "id": GATE_ID,
            "status": "skip",
            "message": (
                "Destination declares no column DDL — there is no declared "
                "carrier for a recreate to replace"
            ),
            "duration_ms": 0,
            "details": {"destination_db_type": destination_db_type},
        }

    findings = find_silent_replacements(
        mappings=mappings,
        source_column_types=source_column_types,
        destination_column_types=destination_column_types,
        destination_db_type=destination_db_type,
        source_db_type=source_db_type,
    )
    blocking = [f for f in findings if not f["risk_contract_cleared"]]
    cleared = [f for f in findings if f["risk_contract_cleared"]]

    if blocking:
        return {
            "id": GATE_ID,
            "status": "block",
            "message": (
                f"{len(blocking)} existing destination column(s) declare a carrier "
                f"narrower than the source, and a full refresh would drop and "
                f"replace them without saying so: {_named(blocking)}"
            ),
            "duration_ms": 0,
            "details": {
                "replacements": blocking,
                "risk_contract_cleared": cleared,
                "rule_id": f"{GATE_ID}.narrower_carrier_replaced",
                "remediation_kind": "review_mappings",
            },
        }

    if cleared:
        return {
            "id": GATE_ID,
            "status": "warn",
            "message": (
                f"Execute is not blocked. {len(cleared)} destination column(s) "
                f"narrower than the source are replaced under a signed Migration "
                f"Risk Contract: {_named(cleared)}"
            ),
            "duration_ms": 0,
            "details": {
                "risk_contract_cleared": cleared,
                "blocks_execute": False,
                "rule_id": f"{GATE_ID}.narrower_carrier_replaced_contracted",
                "remediation_kind": "review_mappings",
            },
        }

    return {
        "id": GATE_ID,
        "status": "pass",
        "message": (
            "Full refresh recreates the destination and no existing column "
            "declares a carrier the source would overflow"
        ),
        "duration_ms": 0,
        "details": {"replacements": []},
    }
