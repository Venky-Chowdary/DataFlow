"""QA MX3-18 — a signed contract for another route must be refused before Confirm.

QA: plan_transfer/start_transfer of a SQLite -> SQLite route bound to a SIGNED
contract recorded for a PostgreSQL source issued an ack (preflight passed);
only after Confirm did the job fail with "Data contract dfc-96a528050857490c
violated: Source format changed from postgresql to sqlite".
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_mx3_02_plan_blocks_missing_contract as _mx302  # noqa: E402

_decision = _mx302._decision
_plan = _mx302._plan
sqlite_route = _mx302.sqlite_route


@pytest.fixture()
def contracts(monkeypatch):
    from services import contract_store as cstore
    from services.data_contract import ContractStatus, DataContract

    import src.transfer.contract_engine as ce

    backend = cstore.InMemoryContractStore()
    monkeypatch.setattr(cstore, "get_contract_store", lambda: backend)
    monkeypatch.setattr(ce, "get_contract_store", lambda: backend)

    def _make(src_format: str, dst_format: str = "sqlite"):
        contract = DataContract(
            name=f"pay-{src_format}", status=ContractStatus.SIGNED,
            source={"format": src_format}, destination={"format": dst_format},
        )
        backend.save_contract(contract)
        return contract.id

    return _make


def test_plan_refuses_a_contract_signed_for_another_source(sqlite_route, contracts):
    cid = contracts("postgresql")
    out = _plan(sqlite_route, contract_id=cid, require_signed_contract=True)
    assert out["contract_status"] == "SIGNED"
    assert out["safe_to_start"] is False
    assert _decision(out) == "block"
    assert "Source format changed from postgresql to sqlite" in out["contract_blocker"]


def test_start_transfer_issues_no_ack_for_a_mismatched_contract(sqlite_route, contracts, monkeypatch):
    import src.ai.copilot.ack_ledger as ledger_mod
    from src.ai.copilot.transfer_tools import start_transfer

    puts: list = []
    real = ledger_mod.get_ack_ledger()
    monkeypatch.setattr(real, "put", lambda **kw: puts.append(kw) or "ack-x", raising=False)
    cid = contracts("postgresql")
    res = start_transfer(
        source_connector_name=sqlite_route[0], source_table="QA_E2E_RT_pay",
        dest_connector_name=sqlite_route[1], dest_table="QA_E2E_RT_ctr",
        sync_mode="full_refresh_overwrite", contract_id=cid, require_signed_contract=True,
    )
    assert not res.success
    assert "Source format changed from postgresql to sqlite" in (res.error or ""), res.error
    assert puts == []


def test_matching_signed_contract_still_approves(sqlite_route, contracts):
    cid = contracts("sqlite")
    out = _plan(sqlite_route, contract_id=cid, require_signed_contract=True)
    assert out["safe_to_start"] is True, out.get("contract_blocker")
    assert "contract_blocker" not in out
