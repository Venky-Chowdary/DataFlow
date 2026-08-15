"""Studio / Pilot contract bind — fail-closed SIGNED before enqueue.

Named fixture path: in-memory contract store. No live warehouse claimed.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.transfer.contract_engine import stamp_bound_contract


def _backend(monkeypatch):
    from services import contract_store as cstore
    from services.data_contract import ContractStatus, DataContract

    cstore.reset_contract_store()
    backend = cstore.InMemoryContractStore()
    monkeypatch.setattr(cstore, "get_contract_store", lambda: backend)
    return backend, DataContract, ContractStatus


def test_stamp_bound_contract_signed_sets_fields(monkeypatch):
    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="orders-v1", status=ContractStatus.SIGNED)
    backend.save_contract(signed)

    req = SimpleNamespace(contract_id="", enforce_contract=False, require_signed_contract=False)
    stamp_bound_contract(req, contract_id=signed.id, require_signed=True)
    assert req.contract_id == signed.id
    assert req.enforce_contract is True
    assert req.require_signed_contract is True


def test_stamp_bound_contract_draft_require_signed_raises(monkeypatch):
    backend, DataContract, ContractStatus = _backend(monkeypatch)
    draft = DataContract(name="draft-orders", status=ContractStatus.DRAFT)
    backend.save_contract(draft)

    req = SimpleNamespace(contract_id="", enforce_contract=True, require_signed_contract=False)
    with pytest.raises(ValueError, match="must be SIGNED"):
        stamp_bound_contract(req, contract_id=draft.id, require_signed=True)
    assert req.contract_id == ""
    assert req.require_signed_contract is False


def test_stamp_bound_contract_require_signed_without_id_raises():
    req = SimpleNamespace(contract_id="", enforce_contract=True, require_signed_contract=False)
    with pytest.raises(ValueError, match="no contract_id"):
        stamp_bound_contract(req, contract_id="", require_signed=True)
    assert req.contract_id == ""
    assert req.enforce_contract is True


def test_stamp_bound_contract_no_id_leaves_enforce_alone():
    req = SimpleNamespace(contract_id="", enforce_contract=True, require_signed_contract=False)
    stamp_bound_contract(req, contract_id="", require_signed=False)
    assert req.contract_id == ""
    assert req.enforce_contract is True
    assert req.require_signed_contract is False


def test_stage_bound_contract_defaults_require_signed_when_id_set(monkeypatch):
    from src.ai.copilot.transfer_tools import _stage_bound_contract

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="pilot-bind", status=ContractStatus.SIGNED)
    backend.save_contract(signed)

    bound = _stage_bound_contract(signed.id, None)
    assert bound["contract_id"] == signed.id
    assert bound["enforce_contract"] is True
    assert bound["require_signed_contract"] is True

    assert _stage_bound_contract("", None) == {}


def test_stage_bound_contract_refuses_draft(monkeypatch):
    from src.ai.copilot.transfer_tools import _stage_bound_contract

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    draft = DataContract(name="unsigned", status=ContractStatus.DRAFT)
    backend.save_contract(draft)

    with pytest.raises(ValueError, match="must be SIGNED"):
        _stage_bound_contract(draft.id, None)


def _approve_plan():
    return {
        "source": {
            "connector_id": "src-1",
            "connector_name": "Source",
            "table": "orders",
            "type": "postgres",
            "schema": "public",
            "source_read_mode": "table",
        },
        "destination": {
            "connector_id": "dst-1",
            "connector_name": "Dest",
            "table": "orders_wh",
            "type": "postgres",
            "schema": "public",
            "table_exists": True,
        },
        "engine_mappings": [{"source": "id", "target": "id"}],
        "column_types": {"id": "INTEGER"},
        "sync_mode": "full_refresh_append",
        "schema_policy": "manual_review",
        "validation_mode": "balanced",
        "mapped_count": 1,
        "unmapped_source_columns": [],
        "lossy_conversions": [],
        "preflight": {
            "run_id": "pf_contract_bind",
            "passed": True,
            "readiness_score": 100,
            "blockers": [],
            "proof_bundle": {"transfer_decision": {"decision": "approve"}},
        },
    }


def test_start_transfer_refuses_unsigned_bind(monkeypatch):
    from src.ai.copilot import transfer_tools as tt
    from src.ai.copilot.query_tools import _tool_result

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    draft = DataContract(name="draft-bind", status=ContractStatus.DRAFT)
    backend.save_contract(draft)

    monkeypatch.setattr(
        tt,
        "plan_transfer",
        lambda **_kw: _tool_result("plan_transfer", success=True, output=_approve_plan()),
    )

    res = tt.start_transfer(
        source_connector_name="Source",
        source_table="orders",
        dest_connector_name="Dest",
        dest_table="orders_wh",
        contract_id=draft.id,
    )
    assert not res.success
    assert "must be SIGNED" in (res.error or "")
    assert "ack_id" not in (res.output or {})


def test_start_transfer_stamps_signed_bind_on_ack(monkeypatch):
    from src.ai.copilot import transfer_tools as tt
    from src.ai.copilot.ack_ledger import get_ack_ledger
    from src.ai.copilot.query_tools import _tool_result

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="signed-bind", status=ContractStatus.SIGNED)
    backend.save_contract(signed)

    monkeypatch.setattr(
        tt,
        "plan_transfer",
        lambda **_kw: _tool_result("plan_transfer", success=True, output=_approve_plan()),
    )

    res = tt.start_transfer(
        source_connector_name="Source",
        source_table="orders",
        dest_connector_name="Dest",
        dest_table="orders_wh",
        contract_id=signed.id,
    )
    assert res.success, res.error
    ack_id = (res.output or {}).get("ack_id")
    assert ack_id
    payload, err = get_ack_ledger().get_pending_payload(ack_id)
    assert err == ""
    assert payload is not None
    assert payload["contract_id"] == signed.id
    assert payload["enforce_contract"] is True
    assert payload["require_signed_contract"] is True
    assert (res.output or {}).get("preview", {}).get("contract_id") == signed.id


def test_start_transfer_without_contract_does_not_set_enforce(monkeypatch):
    from src.ai.copilot import transfer_tools as tt
    from src.ai.copilot.ack_ledger import get_ack_ledger
    from src.ai.copilot.query_tools import _tool_result

    monkeypatch.setattr(
        tt,
        "plan_transfer",
        lambda **_kw: _tool_result("plan_transfer", success=True, output=_approve_plan()),
    )

    res = tt.start_transfer(
        source_connector_name="Source",
        source_table="orders",
        dest_connector_name="Dest",
        dest_table="orders_wh",
    )
    assert res.success, res.error
    payload, err = get_ack_ledger().get_pending_payload(res.output["ack_id"])
    assert err == ""
    assert payload is not None
    assert "contract_id" not in payload
    assert "enforce_contract" not in payload
    assert "require_signed_contract" not in payload


def test_start_transfer_tool_schema_and_wrapper_accept_contract():
    from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools

    start = next(t for t in TOOL_DEFINITIONS if t["name"] == "start_transfer")
    props = start["input_schema"]["properties"]
    assert "contract_id" in props
    assert "require_signed_contract" in props

    res = DataPilotTools().execute(
        "start_transfer",
        {"contract_id": "", "require_signed_contract": False},
    )
    assert "unexpected keyword" not in (res.error or "").lower()
