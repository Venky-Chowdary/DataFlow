"""QA T1 (RT-01 / ACC-03) — false ``rc-duplicate-identity`` on unique keys.

Every ``incremental_upsert`` was refused at Validate with
``rc-duplicate-identity-87d820160a90`` (single key) or ``-cdf71859cb7a``
(composite) while the full-selected uniqueness probe found zero duplicates.
Three independent faults combined:

1. The package G9 sync-contract gate evaluated an empty contract list, so the
   first preflight pass always said "Missing primary key — no stream contract
   carries one" for keyed modes, although the plan carried the contract.
2. The policy pass re-ran the root-cause engine over the first pass's collapsed
   ``rc-sync-contract-incomplete`` blocker and its ``proof_N`` echo; their prose
   ("identity key") matched the duplicate regex and minted a constant
   duplicate_identity root (the id hashes no data — only the absorbed ids).
3. The expectation suite asserted ``expect_column_unique`` on a single
   component of a composite key (``region`` / ``id``), which legitimately
   repeats; tuple uniqueness is ``_check_duplicate_keys``'s job.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
_PREFLIGHT_ROOT = _API_ROOT.parents[1] / "packages" / "preflight" / "src"
for _p in (_API_ROOT, _PREFLIGHT_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from preflight.gates import gate_g9_sync_contract  # noqa: E402
from preflight.models import (  # noqa: E402
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    GateStatus,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)
from services.data_integrity import run_integrity_audit  # noqa: E402
from services.expectations_engine import run_auto_expectations  # noqa: E402
from services.root_cause_engine import apply_root_causes_to_preflight  # noqa: E402

COMPOSITE_HEADERS = ["region", "id", "amount"]
COMPOSITE_ROWS = [
    {"region": r, "id": str(i), "amount": f"{i}.00"}
    for r in ("eu", "us", "ap")
    for i in range(1, 11)
]


def _ctx(stream_contracts, *, sync_mode="incremental_deduped", contract_pk=""):
    return PreflightContext(
        plan=TransferPlan(
            source=SourceConfig(
                kind="database",
                db_type="postgresql",
                connected=True,
                parseable=True,
                source_read_mode="table",
                columns=[
                    ColumnSchema(name="id", inferred_type="INTEGER"),
                    ColumnSchema(name="updated_at", inferred_type="TIMESTAMP"),
                ],
            ),
            destination=DestinationConfig(
                kind="database", db_type="postgresql", connected=True, can_write=True
            ),
            mappings=[
                ColumnMapping(source="id", target="id", confidence=0.99),
                ColumnMapping(source="updated_at", target="updated_at", confidence=0.99),
            ],
            sync_mode=sync_mode,
            contract_primary_key=contract_pk,
            stream_contracts=stream_contracts,
            dry_run_passed=True,
        )
    )


def test_package_g9_sync_contract_reads_the_plan_stream_contracts():
    contracts = [
        {
            "name": "orders",
            "selected": True,
            "primary_key": ["id"],
            "cursor_field": "updated_at",
            "cursor_semantics": "modification_timestamp",
            "sync_mode": "incremental_deduped",
        }
    ]
    result = gate_g9_sync_contract(_ctx(contracts, contract_pk="id"))
    assert result.status != GateStatus.BLOCK, result.message
    assert "Missing primary key" not in str(result.details)


def test_package_g9_sync_contract_still_blocks_a_keyless_upsert():
    contracts = [{"name": "orders", "selected": True, "cursor_field": "updated_at"}]
    result = gate_g9_sync_contract(_ctx(contracts))
    assert result.status == GateStatus.BLOCK
    assert "Missing primary key" in str(result.details)


def _sync_contract_preflight() -> dict:
    return {
        "passed": False,
        "gates": [
            {
                "id": "g9_sync_contract",
                "status": "block",
                "message": "Sync mode contract incomplete",
                "details": {"issues": ["Missing primary key — no stream contract carries one"]},
            }
        ],
        "blockers": [
            {
                "id": "g9_sync_contract",
                "message": "Sync mode contract incomplete",
                "severity": "block",
                "details": {"issues": ["Missing primary key — no stream contract carries one"]},
            }
        ],
        "proof_bundle": {"transfer_decision": {"decision": "block", "blockers": []}},
    }


def test_second_root_cause_pass_never_relabels_sync_contract_as_duplicates():
    first = apply_root_causes_to_preflight(_sync_contract_preflight())
    kinds_first = [r["kind"] for r in first["root_causes"]]
    assert kinds_first == ["sync_contract_incomplete"], kinds_first
    # The policy pass sees the collapsed root + its proof echo as blockers.
    echoed = {
        **first,
        "blockers": list(first["blockers"])
        + [
            {
                "id": "proof_0",
                "message": first["blockers"][0]["message"],
                "severity": "block",
            }
        ],
    }
    second = apply_root_causes_to_preflight(echoed)
    kinds = [r["kind"] for r in second["root_causes"]]
    assert "duplicate_identity" not in kinds, second["root_causes"]
    assert kinds == ["sync_contract_incomplete"], kinds
    assert "Duplicate identity" not in second["proof_bundle"]["transfer_decision"]["reason"]


def test_real_duplicate_finding_still_mints_duplicate_identity_root():
    preflight = {
        "passed": False,
        "gates": [
            {
                "id": "g9_data_integrity",
                "status": "block",
                "message": "Data integrity failed: Duplicate primary key values: 3",
                "details": {"duplicate_keys": [{"key": "7", "count": 2}], "column": "id"},
            }
        ],
        "blockers": [],
    }
    once = apply_root_causes_to_preflight(preflight)
    twice = apply_root_causes_to_preflight(once)
    for out in (once, twice):
        assert [r["kind"] for r in out["root_causes"]] == ["duplicate_identity"]


def test_expectation_suite_skips_component_uniqueness_for_composite_identity():
    out = run_auto_expectations(
        COMPOSITE_ROWS,
        COMPOSITE_HEADERS,
        {"region": "VARCHAR", "id": "INTEGER", "amount": "DECIMAL"},
        primary_key="id",
        primary_key_columns=["region", "id"],
        dest_kind="postgresql",
        sync_mode="incremental_deduped",
    )
    assert out["passed"], out["blocking_failures"]
    unique_cols = {
        r["column"] for r in out["results"] if r["expectation"] == "expect_column_unique"
    }
    assert not unique_cols & {"region", "id"}, unique_cols
    not_null = {
        r["column"]
        for r in out["results"]
        if r["expectation"] == "expect_column_not_null" and r["severity"] == "block"
    }
    assert {"region", "id"} <= not_null


def test_single_key_expectation_still_blocks_a_repeated_key():
    rows = [{"id": "1"}, {"id": "1"}, {"id": "2"}]
    out = run_auto_expectations(
        rows, ["id"], {"id": "INTEGER"}, primary_key="id",
        dest_kind="postgresql", sync_mode="incremental_deduped",
    )
    assert not out["passed"]
    assert out["blocking_failures"][0]["expectation"] == "expect_column_unique"


def _composite_audit(rows):
    return run_integrity_audit(
        mappings=[{"source": h, "target": h, "confidence": 1.0} for h in COMPOSITE_HEADERS],
        source_columns=COMPOSITE_HEADERS,
        target_columns=COMPOSITE_HEADERS,
        sample_rows=rows,
        destination_db_type="postgresql",
        sync_mode="incremental_deduped",
        validation_mode="strict",
        stream_contracts=[{"name": "t", "primary_key": ["region", "id"], "selected": True}],
    )


def test_validate_g9_composite_duplicate_tuple_still_blocks():
    out = _composite_audit(COMPOSITE_ROWS + [{"region": "eu", "id": "3", "amount": "9.00"}])
    dup = next(c for c in out["checks"] if c["check"] == "duplicate_keys")
    assert not dup["passed"] and dup["blocks_transfer"], dup
    assert "(eu, 3)" in " ".join(dup["issues"])


def test_validate_g9_composite_upsert_not_blocked_by_component_repeat():
    out = _composite_audit(COMPOSITE_ROWS)
    suite = next(c for c in out["checks"] if c["check"] == "expectations_suite")
    assert suite["passed"], suite["issues"]
    assert not out.get("blocks_transfer"), [c for c in out["checks"] if c.get("blocks_transfer")]
