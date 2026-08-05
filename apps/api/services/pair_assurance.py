"""Offline connector-pair migration assurance — fail-closed, non-mocked.

This module is the enterprise pair matrix for **datatype + DDL stamp + mapping
coercion + fixture value transforms**. It calls real ``type_system``,
``type_coercion_validator``, ``mapping_pipeline``, and ``apply_transform``.

Honesty (explicit non-claims):
  * mode = offline — not a live transfer / checksum / Gate-8 population proof
  * type inventory = fixture population of logical carriers (not customer rows)
  * value checks = fixture sample transforms (not population fidelity)
  * constraints / FK / orphans = not proven here
  * rollback / product undo = not claimed
  * CDC exactly-once = not claimed

Live execute + reconcile remains ``test_production_sku_matrix`` (separate claim).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

PROOF_DIR = (
    Path(__file__).resolve().parents[1] / "data" / "proofs" / "pair_assurance"
)

# Logical carriers exercised as the offline type inventory (fixture population).
# Every cell uses real ddl_type / create_new / lossy / risk / coercion APIs.
TYPE_INVENTORY: tuple[str, ...] = (
    "INTEGER",
    "BIGINT",
    "DECIMAL(18,4)",
    "FLOAT",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "VARCHAR(64)",
    "TEXT",
    "UUID",
    "OBJECTID",
    "JSON",
    "BINARY",
)

# Fixture sample values — transform round-trip scope = sample, not population.
VALUE_FIXTURES: tuple[dict[str, Any], ...] = (
    {"name": "integer", "source_type": "INTEGER", "transform": "integer", "raw": "42", "expect_ok": True},
    {"name": "decimal", "source_type": "DECIMAL(18,4)", "transform": "decimal", "raw": "12.3400", "expect_ok": True},
    {"name": "boolean", "source_type": "BOOLEAN", "transform": "boolean", "raw": "true", "expect_ok": True},
    {"name": "date", "source_type": "DATE", "transform": "date", "raw": "2024-01-15", "expect_ok": True},
    {"name": "bad_int", "source_type": "INTEGER", "transform": "integer", "raw": "not-a-number", "expect_ok": False},
)

# Name-mapping golden (small, typed samples) — separate from type inventory.
_MAPPING_CASES: tuple[dict[str, str], ...] = (
    {"source": "customer_id", "target": "customer_id", "source_type": "INTEGER", "target_type": "INTEGER"},
    {"source": "email_address", "target": "email", "source_type": "VARCHAR(255)", "target_type": "VARCHAR(255)"},
    {"source": "created_at", "target": "created_at", "source_type": "TIMESTAMPTZ", "target_type": "TIMESTAMPTZ"},
    {"source": "amount", "target": "amount", "source_type": "DECIMAL(18,4)", "target_type": "DECIMAL(18,4)"},
    {"source": "is_active", "target": "is_active", "source_type": "BOOLEAN", "target_type": "BOOLEAN"},
)


@dataclass
class TypeCellResult:
    source_type: str
    stamped_target: str
    ddl: str
    classification: str  # legacy: lossless | lossy_ack_required | blocked | error
    lossy: bool
    risk_kinds: list[str] = field(default_factory=list)
    coercion_issue_kinds: list[str] = field(default_factory=list)
    failure: str | None = None
    # Module 15 — charter ConversionClass (SSOT: conversion_contract).
    conversion_class: str = ""
    invents_capacity: bool = False
    requires_risk_contract: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "stamped_target": self.stamped_target,
            "ddl": self.ddl,
            "classification": self.classification,
            "conversion_class": self.conversion_class,
            "invents_capacity": self.invents_capacity,
            "requires_risk_contract": self.requires_risk_contract,
            "lossy": self.lossy,
            "risk_kinds": list(self.risk_kinds),
            "coercion_issue_kinds": list(self.coercion_issue_kinds),
            "failure": self.failure,
        }


def committed_offline_pairs() -> list[tuple[str, str]]:
    """PRODUCTION_SKU database→database pairs — committed offline assurance set.

    Does not invent catalog tiles. File→DB routes are covered by dedicated
    file-format matrices; this module focuses on dialect×dialect type/DDL.
    """
    from src.transfer.registry import PRODUCTION_SKU

    pairs: set[tuple[str, str]] = set()
    for sk, sf, dk, df in PRODUCTION_SKU:
        if sk == "database" and dk == "database":
            pairs.add((str(sf).lower(), str(df).lower()))
    return sorted(pairs)


def _scope_stamp() -> dict[str, Any]:
    return {
        "mode": "offline",
        "type_inventory": "fixture_population",
        "value_checks": "fixture_sample",
        "live_transfer": "not_run",
        "checksum": "not_applicable_offline",
        "constraints": "not_proven",
        "referential_integrity": "not_proven",
        "rollback": "not_claimed",
        "cdc_exactly_once": "not_claimed",
        "conversion_class": "charter_7_via_conversion_contract",
    }


def evaluate_type_cell(
    source_type: str,
    *,
    dest_db: str,
) -> TypeCellResult:
    """Evaluate one source carrier → dest create-new stamp using real engines."""
    from services.type_coercion_validator import validate_mapping_coercions
    from services.type_system import (
        assess_create_new_type_risk,
        create_new_mapping_target_type,
        ddl_type,
        is_lossy_coercion,
    )

    try:
        stamped = create_new_mapping_target_type(source_type, dest_db)
        ddl = ddl_type(dest_db, source_type)
    except Exception as exc:
        return TypeCellResult(
            source_type=source_type,
            stamped_target="",
            ddl="",
            classification="error",
            lossy=True,
            failure=f"DDL/stamp raised: {exc}",
            conversion_class="unsupported",
        )

    if not stamped or not str(stamped).strip():
        return TypeCellResult(
            source_type=source_type,
            stamped_target=str(stamped or ""),
            ddl=str(ddl or ""),
            classification="error",
            lossy=True,
            failure="create_new_mapping_target_type returned empty (invent refused)",
            conversion_class="unsupported",
        )

    lossy = bool(is_lossy_coercion(source_type, stamped, dest_db=dest_db))
    risks = assess_create_new_type_risk(
        source_type, stamped, destination_db_type=dest_db
    )
    risk_kinds = [str(r.get("kind") or "") for r in risks if isinstance(r, dict)]

    mapping = {
        "source": "col",
        "target": "col",
        "confidence": 1.0,
        "create_new": True,
        "target_type": stamped,
        "risk_acknowledged": False,
    }
    issues = validate_mapping_coercions(
        [mapping],
        source_types={"col": source_type},
        target_types={},
        validation_mode="strict",
        dest_db_type=dest_db,
    )
    issue_kinds = [
        str(i.get("kind") or i.get("code") or i.get("severity") or "issue")
        for i in issues
        if isinstance(i, dict)
    ]

    failure: str | None = None
    if lossy and not risks and not issues:
        failure = (
            "silent_green_lossy: is_lossy_coercion=True but no create_new risks "
            "and no coercion issues without risk_acknowledged"
        )
        classification = "blocked"
    elif lossy and (risks or issues):
        classification = "lossy_ack_required"
    elif issues and not lossy:
        # Hard block for non-lossy declared mismatch (type_locked-class) — surface.
        classification = "blocked"
        if not failure:
            failure = f"coercion_issues_without_lossy_flag: {issue_kinds}"
    else:
        classification = "lossless"

    # Module 15 — stamp charter ConversionClass (never invent green on invent/lossy).
    from services.conversion_contract import classify_conversion

    conv = classify_conversion(
        source_type,
        str(stamped),
        dest_db=dest_db,
        transform="none",
        risk_acknowledged=False,
    )
    # Silent-green lossy remains blocked even if conversion_class disagrees.
    conversion_class = str(conv.get("conversion_class") or "")
    if failure and classification == "blocked":
        conversion_class = conversion_class or "unsupported"

    return TypeCellResult(
        source_type=source_type,
        stamped_target=str(stamped),
        ddl=str(ddl or ""),
        classification=classification,
        lossy=lossy,
        risk_kinds=risk_kinds,
        coercion_issue_kinds=issue_kinds,
        failure=failure,
        conversion_class=conversion_class,
        invents_capacity=bool(conv.get("invents_capacity")),
        requires_risk_contract=bool(conv.get("requires_risk_contract")),
    )


def evaluate_mapping_contract(src_db: str, dest_db: str) -> dict[str, Any]:
    """Name+type mapping through real pipeline (fixture sample schemas)."""
    from services.mapping_pipeline import run_mapping_pipeline

    sources = [c["source"] for c in _MAPPING_CASES]
    targets = [c["target"] for c in _MAPPING_CASES]
    source_schemas = [
        {
            "name": c["source"],
            "inferred_type": c["source_type"],
            "samples": ["1"] if "INT" in c["source_type"] else ["x"],
        }
        for c in _MAPPING_CASES
    ]
    target_schemas = [
        {
            "name": c["target"],
            "inferred_type": c["target_type"],
            "samples": [],
        }
        for c in _MAPPING_CASES
    ]
    result = run_mapping_pipeline(
        source_columns=sources,
        target_columns=targets,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
        destination_db_type=dest_db,
        use_llm=False,
    )
    by = {m["source"]: m["target"] for m in result.get("mappings") or []}
    correct = sum(1 for c in _MAPPING_CASES if by.get(c["source"]) == c["target"])
    total = len(_MAPPING_CASES)
    score = correct / total if total else 0.0
    return {
        "score": round(score, 4),
        "correct": correct,
        "total": total,
        "engine": result.get("engine"),
        "scope": "fixture_sample",
        "passed": score >= 0.8,
        "source_dialect_label": src_db,
        "dest_dialect": dest_db,
    }


def evaluate_value_fixtures(dest_db: str) -> dict[str, Any]:
    """Fixture transform checks via real apply_transform (sample scope)."""
    from services.transform_engine import apply_transform

    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    for fx in VALUE_FIXTURES:
        value, err = apply_transform(fx["raw"], fx["transform"])
        ok = err is None
        expect = bool(fx["expect_ok"])
        passed = ok == expect
        rows.append(
            {
                "name": fx["name"],
                "source_type": fx["source_type"],
                "transform": fx["transform"],
                "raw": fx["raw"],
                "expect_ok": expect,
                "ok": ok,
                "error": err,
                "passed": passed,
                "dest_dialect": dest_db,
            }
        )
        if not passed:
            failures.append(
                f"{fx['name']}: expect_ok={expect} got ok={ok} err={err}"
            )
    return {
        "scope": "fixture_sample",
        "checked": len(rows),
        "failures": failures,
        "passed": len(failures) == 0,
        "rows": rows,
    }


def assess_pair(
    src_db: str,
    dest_db: str,
    *,
    write_proof: bool = True,
) -> dict[str, Any]:
    """Full offline assurance for one dialect pair. Fail closed on silent-green."""
    src_db = (src_db or "").strip().lower()
    dest_db = (dest_db or "").strip().lower()
    pair_id = f"{src_db}__{dest_db}"

    type_cells = [
        evaluate_type_cell(carrier, dest_db=dest_db) for carrier in TYPE_INVENTORY
    ]
    cell_failures = [c for c in type_cells if c.failure]
    lossless = sum(1 for c in type_cells if c.classification == "lossless")
    lossy = sum(1 for c in type_cells if c.classification == "lossy_ack_required")
    blocked = sum(1 for c in type_cells if c.classification in {"blocked", "error"})
    conversion_counts: dict[str, int] = {}
    for c in type_cells:
        key = c.conversion_class or "unclassified"
        conversion_counts[key] = conversion_counts.get(key, 0) + 1

    mapping = evaluate_mapping_contract(src_db, dest_db)
    values = evaluate_value_fixtures(dest_db)

    # Recovery / CDC honesty stamps (real modules — never invent guarantees).
    recovery: dict[str, Any] = {}
    cdc: dict[str, Any] = {}
    try:
        from services.recovery_honesty import honesty_dict as recovery_honesty_dict

        recovery = recovery_honesty_dict()
    except Exception as exc:
        recovery = {"available": False, "error": str(exc), "transfer_undo_claimed": False}
    try:
        from services.cdc_effectively_once import classify_sink_delivery
        from services.cdc_effectively_once import honesty_dict as cdc_honesty_dict

        cdc = {
            **cdc_honesty_dict(),
            "pair_sink_class": classify_sink_delivery(
                dest_type=dest_db,
                has_primary_key=True,
                write_mode="upsert",
                has_lsn_column=None,
            ),
        }
    except Exception as exc:
        cdc = {"available": False, "error": str(exc), "exactly_once_claimed": False}

    type_ok = len(cell_failures) == 0
    mapping_ok = bool(mapping.get("passed"))
    values_ok = bool(values.get("passed"))
    passed = type_ok and mapping_ok and values_ok

    proof: dict[str, Any] = {
        "pair": pair_id,
        "source": src_db,
        "dest": dest_db,
        "mode": "offline",
        "scope": _scope_stamp(),
        "type_cells": {
            "inventory_size": len(TYPE_INVENTORY),
            "lossless": lossless,
            "lossy_ack_required": lossy,
            "blocked_or_error": blocked,
            "conversion_class_counts": conversion_counts,
            "conversion_contract": "conversion_contract.v1",
            "failures": [c.to_dict() for c in cell_failures],
            "cells": [c.to_dict() for c in type_cells],
        },
        "mapping": mapping,
        "value_fixtures": {
            "scope": values.get("scope"),
            "checked": values.get("checked"),
            "passed": values_ok,
            "failures": values.get("failures") or [],
        },
        "recovery": recovery,
        "cdc": cdc,
        "passed": passed,
        "fail_closed_reasons": [
            *(["type_cell_silent_green_or_error"] if not type_ok else []),
            *(["mapping_score_below_floor"] if not mapping_ok else []),
            *(["value_fixture_mismatch"] if not values_ok else []),
        ],
    }

    if write_proof:
        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        path = PROOF_DIR / f"{pair_id}.json"
        # Omit full cell dump from on-disk proof to keep artifacts readable;
        # failures always retained.
        disk = dict(proof)
        disk["type_cells"] = {
            **proof["type_cells"],
            "cells": [
                c.to_dict()
                for c in type_cells
                if c.classification != "lossless" or c.failure
            ],
        }
        path.write_text(json.dumps(disk, indent=2, default=str), encoding="utf-8")
        proof["proof_path"] = str(path)

    return proof


def run_committed_pair_assurance(
    *,
    write_proof: bool = True,
    pairs: list[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Run offline assurance for every committed PRODUCTION_SKU DB pair."""
    selected = pairs if pairs is not None else committed_offline_pairs()
    results: list[dict[str, Any]] = []
    for src, dst in selected:
        try:
            results.append(assess_pair(src, dst, write_proof=write_proof))
        except Exception as exc:
            logger.exception("pair assurance crashed for %s→%s", src, dst)
            results.append(
                {
                    "pair": f"{src}__{dst}",
                    "source": src,
                    "dest": dst,
                    "mode": "offline",
                    "passed": False,
                    "fail_closed_reasons": [f"exception: {exc}"],
                    "scope": _scope_stamp(),
                }
            )

    passed_n = sum(1 for r in results if r.get("passed"))
    failed = [r["pair"] for r in results if not r.get("passed")]
    type_failures = sum(
        len((r.get("type_cells") or {}).get("failures") or []) for r in results
    )
    summary = {
        "mode": "offline",
        "scope": _scope_stamp(),
        "pair_count": len(results),
        "passed_count": passed_n,
        "failed_pairs": failed,
        "type_cell_failures": type_failures,
        "all_passed": passed_n == len(results) and len(results) > 0,
        "pairs": [
            {
                "pair": r.get("pair"),
                "passed": r.get("passed"),
                "fail_closed_reasons": r.get("fail_closed_reasons") or [],
                "mapping_score": (r.get("mapping") or {}).get("score"),
                "type_failures": len((r.get("type_cells") or {}).get("failures") or []),
            }
            for r in results
        ],
    }
    if write_proof:
        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        (PROOF_DIR / "_summary.json").write_text(
            json.dumps(summary, indent=2, default=str), encoding="utf-8"
        )
    return summary
