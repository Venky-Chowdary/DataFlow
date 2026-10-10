"""QA MX3-02 — plan_transfer must not approve a contract bind Confirm refuses.

QA: plan_transfer(..., contract_id="dfc-000000000000bogus",
require_signed_contract=true) -> contract_status="not_found",
transfer_decision.decision="approve", safe_to_start=true; only start_transfer
then said "Contract dfc-000000000000bogus not found".
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import connector_store  # noqa: E402
from src.ai.copilot import tools as tools_mod  # noqa: E402


def _decision(out: dict) -> str:
    return str(
        ((out["preflight"].get("proof_bundle") or {}).get("transfer_decision") or {}).get("decision")
        or ""
    )


@pytest.fixture()
def sqlite_route(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    connector_store._backend_choice = None
    tools_mod._tools = None
    names = []
    for role in ("src", "dst"):
        db = tmp_path / f"mx302_{role}.db"
        conn = sqlite3.connect(db)
        if role == "src":
            conn.execute("CREATE TABLE QA_E2E_RT_pay (id INTEGER PRIMARY KEY, amount REAL)")
            conn.executemany("INSERT INTO QA_E2E_RT_pay VALUES (?, ?)", [(1, 1.5), (2, 2.5)])
            conn.commit()
        conn.close()
        saved = connector_store.create_connector({
            "name": f"MX302{role}", "type": "sqlite", "role": "both",
            "connection_string": f"sqlite:///{db.resolve().as_posix()}", "workspace_id": "",
        })
        names.append(saved.name)
    return names


def _plan(names, **extra):
    from src.ai.copilot.transfer_tools import plan_transfer

    res = plan_transfer(
        source_connector_name=names[0], source_table="QA_E2E_RT_pay",
        dest_connector_name=names[1], dest_table="QA_E2E_RT_plan2",
        sync_mode="full_refresh_overwrite", **extra,
    )
    assert res.success, res.error
    return res.output


def test_plan_with_missing_required_contract_blocks(sqlite_route):
    control = _plan(sqlite_route)
    assert control["safe_to_start"] is True, "precondition: the route itself approves"
    assert _decision(control) == "approve"

    out = _plan(sqlite_route, contract_id="dfc-000000000000bogus", require_signed_contract=True)
    assert out["contract_status"] == "not_found"
    assert out["safe_to_start"] is False
    assert _decision(out) == "block"
    assert "dfc-000000000000bogus not found" in out["contract_blocker"]
    assert any(b.get("id") == "contract_bind" for b in out["preflight"]["blockers"])


def test_plan_require_signed_without_contract_id_blocks(sqlite_route):
    out = _plan(sqlite_route, require_signed_contract=True)
    assert out["safe_to_start"] is False
    assert "no contract_id" in out["contract_blocker"]


def test_plan_with_draft_contract_under_require_signed_blocks(sqlite_route, monkeypatch):
    from services import contract_store as cstore
    from services.data_contract import ContractStatus, DataContract

    backend = cstore.InMemoryContractStore()
    monkeypatch.setattr(cstore, "get_contract_store", lambda: backend)
    draft = DataContract(name="draft-pay", status=ContractStatus.DRAFT)
    backend.save_contract(draft)

    out = _plan(sqlite_route, contract_id=draft.id, require_signed_contract=True)
    assert out["contract_status"] == "DRAFT"
    assert out["safe_to_start"] is False
    assert "must be SIGNED" in out["contract_blocker"]
