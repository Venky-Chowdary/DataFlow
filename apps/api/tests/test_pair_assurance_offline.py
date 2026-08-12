"""Offline pair assurance — enterprise complete (non-mocked).

Exercises real type_system / coercion / mapping / transform engines for every
PRODUCTION_SKU database→database pair. Fail closed on silent-green lossy cells.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.pair_assurance import (
    TYPE_INVENTORY,
    assess_pair,
    committed_offline_pairs,
    evaluate_type_cell,
    evaluate_value_fixtures,
    run_committed_pair_assurance,
)


def test_committed_pairs_come_from_production_sku_not_catalog_tiles():
    from src.transfer.registry import PRODUCTION_SKU

    sku_db = {
        (sf.lower(), df.lower())
        for sk, sf, dk, df in PRODUCTION_SKU
        if sk == "database" and dk == "database"
    }
    pairs = committed_offline_pairs()
    assert pairs, "expected committed DB pairs"
    assert set(pairs) == sku_db
    # Honesty: far fewer than marketing "119 connectors"
    assert len(pairs) < 80


def test_type_cell_never_silent_green_on_core_dests():
    """Lossy create-new stamps must surface risks or coercion issues."""
    for dest in ("postgresql", "mysql", "sqlserver", "oracle", "snowflake", "sqlite"):
        for carrier in TYPE_INVENTORY:
            cell = evaluate_type_cell(carrier, dest_db=dest)
            assert cell.failure is None, (
                f"{dest} ← {carrier}: {cell.failure} "
                f"(stamped={cell.stamped_target!r} risks={cell.risk_kinds})"
            )
            assert cell.stamped_target, f"{dest} ← {carrier}: empty stamp"
            if cell.lossy:
                assert cell.classification == "lossy_ack_required", (
                    f"{dest} ← {carrier}: lossy must require ack, got {cell.classification}"
                )
                assert cell.risk_kinds or cell.coercion_issue_kinds


def test_value_fixtures_use_real_transforms():
    report = evaluate_value_fixtures("postgresql")
    assert report["passed"] is True
    assert report["scope"] == "fixture_sample"
    assert report["checked"] == 5


def test_assess_pair_proof_honesty_stamps(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "services.pair_assurance.PROOF_DIR",
        tmp_path / "pair_assurance",
    )
    proof = assess_pair("postgresql", "mysql", write_proof=True)
    assert proof["mode"] == "offline"
    assert proof["scope"]["checksum"] == "not_applicable_offline"
    assert proof["scope"]["rollback"] == "not_claimed"
    assert proof["scope"]["referential_integrity"] == "not_proven"
    assert proof["scope"]["cdc_exactly_once"] == "not_claimed"
    assert proof["passed"] is True, proof.get("fail_closed_reasons")
    path = tmp_path / "pair_assurance" / "postgresql__mysql.json"
    assert path.is_file()
    disk = json.loads(path.read_text(encoding="utf-8"))
    assert disk["passed"] is True
    assert disk["scope"]["live_transfer"] == "not_run"


def test_oracle_to_postgresql_pair_complete():
    proof = assess_pair("oracle", "postgresql", write_proof=False)
    assert proof["passed"] is True, proof.get("fail_closed_reasons")
    assert proof["type_cells"]["inventory_size"] == len(TYPE_INVENTORY)
    assert proof["mapping"]["passed"] is True
    assert proof["value_fixtures"]["passed"] is True


def test_sqlserver_to_postgresql_pair_complete():
    proof = assess_pair("sqlserver", "postgresql", write_proof=False)
    assert proof["passed"] is True, proof.get("fail_closed_reasons")


def test_mongodb_to_postgresql_objectid_cell():
    cell = evaluate_type_cell("OBJECTID", dest_db="postgresql")
    assert cell.failure is None
    assert cell.stamped_target
    # ObjectId→PG is specialty wire — must not silent-green if lossy domain
    if cell.lossy:
        assert cell.risk_kinds or cell.coercion_issue_kinds


def test_run_committed_pair_assurance_all_pass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "services.pair_assurance.PROOF_DIR",
        tmp_path / "pair_assurance",
    )
    # Full SKU set — this is the enterprise gate (offline, real engines).
    summary = run_committed_pair_assurance(write_proof=True)
    assert summary["pair_count"] == len(committed_offline_pairs())
    assert summary["type_cell_failures"] == 0, summary.get("failed_pairs")
    assert summary["all_passed"] is True, (
        f"failed pairs: {summary.get('failed_pairs')} "
        f"reasons sample: {[p for p in summary.get('pairs') or [] if not p.get('passed')][:5]}"
    )
    assert (tmp_path / "pair_assurance" / "_summary.json").is_file()


def test_silent_green_detector_flags_hypothetical_gap(monkeypatch: pytest.MonkeyPatch):
    """Property: if lossy and empty risks+issues, cell must fail closed.

    The lossiness is forced on the symbol ``evaluate_type_cell`` actually reads
    (``services.decision_kernel``). Patching the ``services.type_system``
    re-export left the real verdict in play, so the property silently rode on
    ``TIMESTAMPTZ → mysql`` happening to be lossy — and stopped being tested at
    all once that pair became a faithful instant-to-instant mapping.
    """
    import services.pair_assurance as pa

    monkeypatch.setattr(
        "services.decision_kernel.is_lossy_coercion",
        lambda *a, **k: True,
    )
    monkeypatch.setattr(
        "services.type_system.is_lossy_coercion",
        lambda *a, **k: True,
    )
    monkeypatch.setattr(
        "services.type_system.assess_create_new_type_risk",
        lambda *a, **k: [],
    )
    monkeypatch.setattr(
        "services.type_coercion_validator.validate_mapping_coercions",
        lambda *a, **k: [],
    )
    monkeypatch.setattr(
        "services.type_system.create_new_mapping_target_type",
        lambda src, dest: "VARCHAR",
    )
    monkeypatch.setattr(
        "services.type_system.ddl_type",
        lambda dest, src: "VARCHAR",
    )
    cell = pa.evaluate_type_cell("TIMESTAMPTZ", dest_db="mysql")
    assert cell.failure and "silent_green_lossy" in cell.failure
    assert cell.classification == "blocked"
