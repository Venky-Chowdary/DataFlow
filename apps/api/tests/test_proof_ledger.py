"""Proof ledger — customer-visible migration proofs (not connection tests)."""

from __future__ import annotations

import sys
from pathlib import Path


_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_build_proof_ledger_has_honest_metrics():
    from services.proof_ledger import build_proof_ledger

    ledger = build_proof_ledger()
    assert ledger["headline"]
    metrics = ledger["metrics"]
    assert metrics["unique_transfer_drivers"] >= 1
    assert metrics["production_sku_routes"] >= 1
    assert len(ledger["production_sku"]) == metrics["production_sku_routes"]
    assert metrics["production_sku_sold"] + metrics["production_sku_driver_missing"] + metrics["production_sku_refused"] == metrics["production_sku_routes"]
    assert metrics["production_sku_sold"] >= 1
    statuses = {row["status"] for row in ledger["production_sku"]}
    assert "sold" in statuses
    assert all(row["status"] in {"sold", "driver_missing", "refused"} for row in ledger["production_sku"])
    sold_rows = [row for row in ledger["production_sku"] if row["status"] == "sold"]
    assert all(row["sold"] is True and row["validate_ok"] is True for row in sold_rows)
    assert len(ledger["integrity_comparison"]) >= 4
    assert any("Silent data loss" in row["dimension"] for row in ledger["integrity_comparison"])
    assert all("industry_elt" in row for row in ledger["integrity_comparison"])
    assert ledger["how_to_verify"]


def test_run_fidelity_proof_writes_artifact(tmp_path, monkeypatch):
    from services import proof_ledger as pl

    monkeypatch.setattr(pl, "PROOF_DIR", tmp_path / "proofs")
    result = pl.run_fidelity_proof()
    assert result["success"] is True, result
    assert result["route"] == "csv→sqlite"
    assert result["rows"] == 5
    assert "unicode_jp" in (result.get("checks") or [])
    assert "null_note" in (result.get("checks") or [])
    proof_file = tmp_path / "proofs" / result["proof_file"]
    assert proof_file.exists()


def test_proof_ledger_endpoints(monkeypatch):
    from fastapi.testclient import TestClient

    from src.main import app
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "auth_required", lambda: False)
    with TestClient(app) as client:
        resp = client.get("/api/v1/workspace/proofs/ledger")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "metrics" in data
        assert "integrity_comparison" in data


def test_sold_route_without_test_evidence_is_not_certified(tmp_path, monkeypatch):
    """``sold`` is availability. Without a route matrix every route is unverified."""
    import services.proof_ledger as proof_ledger

    monkeypatch.setattr(proof_ledger, "PROOF_DIR", tmp_path)
    ledger = proof_ledger.build_proof_ledger()
    sold = [r for r in ledger["production_sku"] if r["status"] == "sold"]
    assert sold
    assert all(r["certification"]["status"] == "unverified" for r in sold)
    cert = ledger["metrics"]["route_certification"]
    assert cert["artifact"] is None
    assert cert["tests"] is None
    assert cert["routes_by_status"]["unverified"] == len(ledger["production_sku"])


def test_route_matrix_artifact_grades_routes_from_test_results(tmp_path, monkeypatch):
    import json

    import services.proof_ledger as proof_ledger
    from services.route_certification import REQUIRED_CHECKS, build_route_matrix
    from src.transfer.registry import PRODUCTION_SKU

    pg_mysql = ("database", "postgresql", "database", "mysql")
    pg_sqlite = ("database", "postgresql", "database", "sqlite")
    assert pg_mysql in PRODUCTION_SKU and pg_sqlite in PRODUCTION_SKU

    def rec(route, mode, checks=()):
        return {"route": list(route), "sync_mode": mode, "checks": list(checks), "cases": []}

    tests = [
        {"test": "t::ok", "outcome": "passed", "message": "",
         "records": [rec(pg_mysql, m, REQUIRED_CHECKS)
                     for m in ("full_refresh_overwrite", "full_refresh_append", "upsert")]},
        {"test": "t::bad", "outcome": "failed", "message": "boom",
         "records": [rec(pg_sqlite, "full_refresh_overwrite")]},
    ]
    matrix = build_route_matrix(tests)
    (tmp_path / proof_ledger.ROUTE_MATRIX_FILENAME).write_text(json.dumps(matrix), encoding="utf-8")
    monkeypatch.setattr(proof_ledger, "PROOF_DIR", tmp_path)

    ledger = proof_ledger.build_proof_ledger()
    by_route = {r["route"]: r for r in ledger["production_sku"]}
    assert by_route["database/postgresql → database/mysql"]["certification"]["status"] == "verified"
    assert by_route["database/postgresql → database/sqlite"]["certification"]["status"] == "failed"
    cert = ledger["metrics"]["route_certification"]
    assert cert["tests"] == {"tests_passed": 1, "tests_failed": 1, "tests_skipped": 0}
    assert cert["routes_by_status"]["verified"] == 1
