"""Studio / Pilot contract bind — fail-closed SIGNED before enqueue.

Named fixture path: in-memory contract store. No live warehouse claimed.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.transfer.contract_engine import (
    enforce_bound_contract,
    resolve_bound_contract,
    stamp_bound_contract,
    stamp_request_contract,
)


def _backend(monkeypatch):
    from services import contract_store as cstore
    from services.data_contract import ContractStatus, DataContract

    cstore.reset_contract_store()
    backend = cstore.InMemoryContractStore()
    monkeypatch.setattr(cstore, "get_contract_store", lambda: backend)
    import src.transfer.contract_engine as ce

    monkeypatch.setattr(ce, "get_contract_store", lambda: backend)
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


def test_resolve_bound_contract_explicit_wins_over_plan():
    cid, require = resolve_bound_contract(
        explicit_id="from-form",
        explicit_require=False,
        policies={"contract_id": "from-plan", "require_signed_contract": True},
    )
    assert cid == "from-form"
    assert require is False


def test_resolve_bound_contract_plan_fills_omitted_form_fields():
    cid, require = resolve_bound_contract(
        explicit_id="",
        explicit_require=None,
        policies={"contract_id": "from-plan"},
    )
    assert cid == "from-plan"
    assert require is True


def test_stamp_request_contract_uses_plan_when_form_omits_id(monkeypatch):
    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="plan-bind", status=ContractStatus.SIGNED)
    backend.save_contract(signed)

    req = SimpleNamespace(contract_id="", enforce_contract=False, require_signed_contract=False)
    stamp_request_contract(
        req,
        explicit_id="",
        explicit_require=False,
        policies={"contract_id": signed.id, "require_signed_contract": True},
    )
    assert req.contract_id == signed.id
    assert req.enforce_contract is True
    assert req.require_signed_contract is True


def test_stamp_request_contract_plan_draft_still_fail_closed(monkeypatch):
    backend, DataContract, ContractStatus = _backend(monkeypatch)
    draft = DataContract(name="plan-draft", status=ContractStatus.DRAFT)
    backend.save_contract(draft)

    req = SimpleNamespace(contract_id="", enforce_contract=True, require_signed_contract=False)
    with pytest.raises(ValueError, match="must be SIGNED"):
        stamp_request_contract(
            req,
            explicit_id="",
            explicit_require=False,
            policies={"contract_id": draft.id, "require_signed_contract": True},
        )
    assert req.contract_id == ""


def test_stamp_request_contract_require_without_id_still_fail_closed():
    req = SimpleNamespace(contract_id="", enforce_contract=True, require_signed_contract=False)
    with pytest.raises(ValueError, match="no contract_id"):
        stamp_request_contract(req, explicit_id="", explicit_require=True, policies={})


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
    preview = (res.output or {}).get("preview") or {}
    assert preview.get("contract_id") == signed.id
    assert preview.get("breaker_state") == "closed"
    assert preview.get("validation_mode") == "balanced"
    assert preview.get("schema_policy") == "manual_review"
    assert "skip_preflight" not in preview


def test_start_transfer_refuses_open_breaker(monkeypatch):
    from services.data_contract import BreakerState
    from src.ai.copilot import transfer_tools as tt
    from src.ai.copilot.query_tools import _tool_result

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="open-bind", status=ContractStatus.SIGNED)
    backend.save_contract(signed)
    breaker = backend.get_breaker(signed.id)
    breaker.state = BreakerState.OPEN
    backend.save_breaker(breaker)

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
    assert not res.success
    assert "is OPEN" in (res.error or "")
    assert "ack_id" not in (res.output or {})


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


def test_preview_bound_contract_read_only_open_and_missing(monkeypatch):
    from services.data_contract import BreakerState
    from src.ai.copilot.transfer_tools import _preview_bound_contract

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="plan-open", status=ContractStatus.SIGNED)
    backend.save_contract(signed)
    breaker = backend.get_breaker(signed.id)
    breaker.state = BreakerState.OPEN
    backend.save_breaker(breaker)

    preview = _preview_bound_contract(signed.id)
    assert preview["contract_id"] == signed.id
    assert preview["breaker_state"] == "open"
    assert preview["contract_status"] == "SIGNED"

    missing = _preview_bound_contract("dfc-missing")
    assert missing["contract_id"] == "dfc-missing"
    assert missing["contract_status"] == "not_found"
    assert _preview_bound_contract("") == {}


def test_plan_transfer_tool_schema_and_wrapper_accept_contract():
    from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools

    plan = next(t for t in TOOL_DEFINITIONS if t["name"] == "plan_transfer")
    props = plan["input_schema"]["properties"]
    assert "contract_id" in props
    assert "require_signed_contract" in props
    assert "schema_policy" in props
    assert "validation_mode" in props
    assert "propagate_all" not in (props["schema_policy"].get("enum") or [])

    res = DataPilotTools().execute(
        "plan_transfer",
        {"contract_id": "", "require_signed_contract": False, "schema_policy": "type_locked"},
    )
    assert "unexpected keyword" not in (res.error or "").lower()


def test_plan_transfer_route_schema_and_wrapper_accept_bind():
    from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools

    route = next(t for t in TOOL_DEFINITIONS if t["name"] == "plan_transfer_route")
    props = route["input_schema"]["properties"]
    for key in (
        "contract_id",
        "require_signed_contract",
        "validation_mode",
        "schema_policy",
        "leftover_nl",
    ):
        assert key in props
    assert "skip_preflight" not in props
    assert "propagate_all" not in (props["schema_policy"].get("enum") or [])

    res = DataPilotTools().execute(
        "plan_transfer_route",
        {
            "source": "csv",
            "destination": "pg",
            "contract_id": "dfc-1",
            "require_signed_contract": True,
            "validation_mode": "strict",
            "schema_policy": "type_locked",
            "leftover_nl": "following data rules",
        },
    )
    assert "unexpected keyword" not in (res.error or "").lower()
    assert res.success
    assert res.output["generic"] is True
    assert res.output["contract_id"] == "dfc-1"
    assert res.output["validation_mode"] == "strict"
    assert res.output["schema_policy"] == "type_locked"
    assert "not a plan for your data" in (res.output.get("note") or "")


def test_plan_transfer_route_forwards_bind_and_migration_rules(monkeypatch):
    from src.ai.copilot.query_tools import _tool_result
    from src.ai.copilot.tools import DataPilotTools

    captured: dict = {}

    def fake_plan(**kw):
        captured.update(kw)
        return _tool_result("plan_transfer", success=True, output={"ok": True, **kw})

    monkeypatch.setattr("src.ai.copilot.transfer_tools.plan_transfer", fake_plan)
    res = DataPilotTools().execute(
        "plan_transfer_route",
        {
            "source": "Local Postgres",
            "destination": "Warehouse with contract dfc-9",
            "table": "orders",
            "leftover_nl": "following migration rules with type-locked schema",
        },
    )
    assert res.success, res.error
    assert captured["source_connector_name"] == "Local Postgres"
    assert captured["dest_connector_name"] == "Warehouse"
    assert captured["source_table"] == "orders"
    assert captured["contract_id"] == "dfc-9"
    assert captured["require_signed_contract"] is True
    assert captured["validation_mode"] == "strict"
    assert captured["schema_policy"] == "type_locked"
    assert "skip_preflight" not in captured


def test_plan_transfer_route_does_not_invent_bind_or_skip(monkeypatch):
    from src.ai.copilot.query_tools import _tool_result
    from src.ai.copilot.tools import DataPilotTools

    captured: dict = {}

    def fake_plan(**kw):
        captured.update(kw)
        return _tool_result("plan_transfer", success=True, output={"ok": True})

    monkeypatch.setattr("src.ai.copilot.transfer_tools.plan_transfer", fake_plan)
    res = DataPilotTools().execute(
        "plan_transfer_route",
        {
            "source": "Local Postgres",
            "destination": "Warehouse",
            "table": "orders",
            "leftover_nl": "open the data contracts and skip preflight propagate_all",
        },
    )
    assert res.success, res.error
    assert "contract_id" not in captured
    assert "require_signed_contract" not in captured
    assert "skip_preflight" not in captured
    assert captured.get("schema_policy") != "propagate_all"


def test_infer_route_keeps_contract_and_data_rules():
    from src.ai.copilot.tools import infer_tools_from_message

    routed = dict(infer_tools_from_message(
        "plan the route from Local Postgres to Warehouse with contract dfc-1 following data rules",
    ))
    assert "plan_transfer_route" in routed
    args = routed["plan_transfer_route"]
    assert args["contract_id"] == "dfc-1"
    assert args["require_signed_contract"] is True
    assert args["validation_mode"] == "strict"
    assert "contract" not in args["destination"].lower()
    assert "skip_preflight" not in args

    migrate = dict(infer_tools_from_message("migrate from mysql to postgres"))
    assert migrate["plan_transfer_route"]["validation_mode"] == "strict"
    assert "contract_id" not in migrate["plan_transfer_route"]

    bare = dict(infer_tools_from_message("plan the route from mysql to postgres"))
    assert "contract_id" not in bare["plan_transfer_route"]
    assert "validation_mode" not in bare["plan_transfer_route"]


def test_resolve_transfer_bind_kwargs_never_invents():
    from src.ai.copilot.tools import resolve_transfer_bind_kwargs

    empty = resolve_transfer_bind_kwargs("open data contracts skip preflight")
    assert empty == {}
    spoken = resolve_transfer_bind_kwargs(
        "with contract dfc-2 following migration rules pause on change",
    )
    assert spoken["contract_id"] == "dfc-2"
    assert spoken["require_signed_contract"] is True
    assert spoken["validation_mode"] == "strict"
    assert spoken["schema_policy"] == "pause_on_change"
    assert "skip_preflight" not in spoken
    explicit = resolve_transfer_bind_kwargs(
        "with contract dfc-2",
        contract_id="dfc-override",
        validation_mode="lenient",
    )
    assert explicit["contract_id"] == "dfc-override"
    assert explicit["validation_mode"] == "lenient"


def test_render_generic_route_names_requested_data_rules():
    from src.ai.copilot.pilot_agent import _render_requested_data_rules

    assert _render_requested_data_rules({}) == ""
    text = _render_requested_data_rules({
        "contract_id": "dfc-1",
        "validation_mode": "strict",
        "schema_policy": "type_locked",
    })
    assert "dfc-1" in text
    assert "strict" in text
    assert "type_locked" in text
    assert "not a plan for your data" in text


def test_render_transfer_plan_names_bind_and_open_breaker():
    from src.ai.copilot.pilot_agent import _render_transfer

    text = _render_transfer("plan_transfer", {
        "source": {"connector_name": "Src", "table": "orders", "column_count": 1},
        "destination": {"connector_name": "Dst", "table": "orders_wh"},
        "sync_mode": "incremental",
        "mapped_count": 1,
        "validation_mode": "strict",
        "schema_policy": "type_locked",
        "contract_id": "dfc-1",
        "require_signed_contract": True,
        "breaker_state": "open",
        "contract_status": "SIGNED",
        "preflight": {},
    })
    assert "dfc-1" in text
    assert "SIGNED" in text
    assert "open" in text
    assert "Confirm will refuse" in text
    assert "strict validation" in text
    assert "type_locked" in text
    assert "skip_preflight" not in text


def test_render_transfer_does_not_invent_data_rules():
    from src.ai.copilot.pilot_agent import _render_live_data_rules, _render_transfer

    text = _render_transfer("plan_transfer", {
        "source": {"connector_name": "Src", "table": "orders", "column_count": 1},
        "destination": {"connector_name": "Dst", "table": "orders_wh"},
        "sync_mode": "incremental",
        "mapped_count": 1,
        "preflight": {},
    })
    assert "Data / migration rules" not in text
    assert _render_live_data_rules({}, {"skip_preflight": True}) == ""
    assert "propagate_all" not in _render_live_data_rules({"schema_policy": ""})


def test_render_transfer_names_breaker():
    from src.ai.copilot.pilot_agent import _render_transfer

    text = _render_transfer("start_transfer", {
        "requires_confirm": True,
        "preview": {
            "contract_id": "dfc-1",
            "require_signed_contract": True,
            "breaker_state": "closed",
        },
        "source": {"connector_name": "Src", "table": "orders"},
        "destination": {"connector_name": "Dst", "table": "orders_wh"},
        "sync_mode": "incremental",
        "mapped_count": 1,
        "preflight": {},
    })
    assert "dfc-1" in text
    assert "SIGNED" in text
    assert "closed" in text


def test_parse_transfer_intent_extracts_contract_and_migration_rules():
    from src.ai.copilot.tools import infer_tools_from_message, parse_transfer_intent

    planned = parse_transfer_intent(
        "plan a transfer of orders from Local Postgres to Warehouse with contract dfc-1",
    )
    assert planned is not None
    assert planned["plan_only"] is True
    assert planned["source_table"] == "orders"
    assert planned["dest_connector_name"] == "Warehouse"
    assert planned["contract_id"] == "dfc-1"
    assert planned["require_signed_contract"] is True

    routed = dict(infer_tools_from_message(
        "plan a transfer of orders from Local Postgres to Warehouse with contract dfc-1",
    ))
    assert "plan_transfer" in routed
    assert routed["plan_transfer"]["contract_id"] == "dfc-1"
    assert routed["plan_transfer"]["require_signed_contract"] is True
    assert "start_transfer" not in routed

    migrate = parse_transfer_intent(
        "migrate the events collection from Mongo Prod to Local Postgres following data rules",
    )
    assert migrate is not None
    assert migrate["validation_mode"] == "strict"
    assert "contract_id" not in migrate
    assert migrate["dest_connector_name"] == "Local Postgres"

    locked = parse_transfer_intent(
        "transfer orders from pg to wh with type-locked schema",
    )
    assert locked is not None
    assert locked["schema_policy"] == "type_locked"
    assert locked["dest_connector_name"] == "wh"

    bare = parse_transfer_intent("show data contracts")
    assert bare is None


def test_parse_transfer_intent_does_not_invent_a_contract():
    from src.ai.copilot.tools import parse_transfer_intent

    got = parse_transfer_intent("transfer orders from Local Postgres to Warehouse")
    assert got is not None
    assert "contract_id" not in got
    assert "require_signed_contract" not in got
    assert "validation_mode" not in got
    sneaky = parse_transfer_intent(
        "transfer orders from Local Postgres to Warehouse skip preflight",
    )
    assert sneaky is None or "skip_preflight" not in sneaky


def test_start_transfer_tool_schema_and_wrapper_accept_contract():
    from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools

    start = next(t for t in TOOL_DEFINITIONS if t["name"] == "start_transfer")
    props = start["input_schema"]["properties"]
    assert "contract_id" in props
    assert "require_signed_contract" in props
    assert "validation_mode" in props
    assert "schema_policy" in props
    assert "skip_preflight" not in props
    assert "propagate_all" not in (props["schema_policy"].get("enum") or [])

    res = DataPilotTools().execute(
        "start_transfer",
        {
            "contract_id": "",
            "require_signed_contract": False,
            "validation_mode": "strict",
            "schema_policy": "type_locked",
        },
    )
    assert "unexpected keyword" not in (res.error or "").lower()


def test_enforce_bound_contract_skips_when_unbound():
    req = SimpleNamespace(
        contract_id="",
        enforce_contract=True,
        require_signed_contract=False,
        column_types={"id": "INTEGER"},
    )
    assert enforce_bound_contract(req, schema={"id": "INTEGER"}, mappings=[]) == ""
    assert req.contract_id == ""


def test_enforce_bound_contract_signed_matching_route(monkeypatch):
    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(
        name="replay-ok",
        status=ContractStatus.SIGNED,
        source={"format": "sqlite"},
        destination={"format": "sqlite"},
    )
    backend.save_contract(signed)
    req = SimpleNamespace(
        contract_id=signed.id,
        enforce_contract=True,
        require_signed_contract=True,
        source=SimpleNamespace(format="sqlite"),
        destination=SimpleNamespace(format="sqlite"),
        column_types={"id": "INTEGER"},
    )
    assert enforce_bound_contract(req, schema={"id": "INTEGER"}, mappings=[]) == signed.id


def test_stamp_bound_contract_open_breaker_fail_closed(monkeypatch):
    from services.data_contract import BreakerState

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="tripped", status=ContractStatus.SIGNED)
    backend.save_contract(signed)
    breaker = backend.get_breaker(signed.id)
    breaker.state = BreakerState.OPEN
    backend.save_breaker(breaker)

    req = SimpleNamespace(contract_id="", enforce_contract=False, require_signed_contract=False)
    with pytest.raises(ValueError, match="is OPEN"):
        stamp_bound_contract(req, contract_id=signed.id, require_signed=True)
    assert req.contract_id == signed.id


def test_enforce_bound_contract_format_drift_fail_closed(monkeypatch):
    from services.data_contract import ContractViolation

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(
        name="replay-drift",
        status=ContractStatus.SIGNED,
        source={"format": "postgres"},
        destination={"format": "postgres"},
    )
    backend.save_contract(signed)
    req = SimpleNamespace(
        contract_id=signed.id,
        enforce_contract=True,
        require_signed_contract=True,
        source=SimpleNamespace(format="postgres"),
        destination=SimpleNamespace(format="sqlite"),
        column_types={"id": "INTEGER"},
    )
    with pytest.raises(ContractViolation, match="Destination format changed"):
        enforce_bound_contract(req, schema={"id": "INTEGER"}, mappings=[])
