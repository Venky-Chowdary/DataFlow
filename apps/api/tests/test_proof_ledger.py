"""Proof ledger — customer-visible migration proofs (not connection tests)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

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
    assert len(ledger["vs_airbyte"]) >= 4
    assert any("Silent data loss" in row["dimension"] for row in ledger["vs_airbyte"])
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
        assert "vs_airbyte" in data
