"""Pilot run_schedule Confirm — bind + breaker on preview, fail-closed staging.

Named fixture: in-memory contract store. No live warehouse claimed.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest


def _backend(monkeypatch):
    from services import contract_store as cstore
    from services.data_contract import ContractStatus, DataContract

    cstore.reset_contract_store()
    backend = cstore.InMemoryContractStore()
    monkeypatch.setattr(cstore, "get_contract_store", lambda: backend)
    return backend, DataContract, ContractStatus


def test_assert_schedule_run_allowed_unbound_leaves_enforce_unset():
    from services.schedule_store import assert_schedule_run_allowed

    preview = assert_schedule_run_allowed(
        SimpleNamespace(contract_id="", require_signed_contract=False),
    )
    assert preview == {}


def test_assert_schedule_run_allowed_signed_preview(monkeypatch):
    from services.schedule_store import assert_schedule_run_allowed

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="nightly-bind", status=ContractStatus.SIGNED)
    backend.save_contract(signed)

    preview = assert_schedule_run_allowed(
        SimpleNamespace(contract_id=signed.id, require_signed_contract=True),
    )
    assert preview["contract_id"] == signed.id
    assert preview["require_signed_contract"] is True
    assert preview["enforce_contract"] is True
    assert preview["breaker_state"] == "closed"


def test_assert_schedule_run_allowed_draft_require_signed_raises(monkeypatch):
    from services.schedule_store import assert_schedule_run_allowed

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    draft = DataContract(name="draft-bind", status=ContractStatus.DRAFT)
    backend.save_contract(draft)

    with pytest.raises(ValueError, match="must be SIGNED"):
        assert_schedule_run_allowed(
            SimpleNamespace(contract_id=draft.id, require_signed_contract=True),
        )


def test_assert_schedule_run_allowed_open_breaker_raises(monkeypatch):
    from services.data_contract import BreakerState
    from services.schedule_store import assert_schedule_run_allowed

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="tripped", status=ContractStatus.SIGNED)
    backend.save_contract(signed)
    breaker = backend.get_breaker(signed.id)
    breaker.state = BreakerState.OPEN
    backend.save_breaker(breaker)

    with pytest.raises(ValueError, match="is OPEN"):
        assert_schedule_run_allowed(
            SimpleNamespace(contract_id=signed.id, require_signed_contract=True),
        )


def test_run_schedule_now_preview_includes_signed_bind(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    import src.ai.copilot.ack_ledger as ack_mod
    from src.ai.copilot.ack_ledger import PilotAckLedger
    from src.ai.copilot.tools import DataPilotTools

    ack_mod._ledger = None
    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="sched-signed", status=ContractStatus.SIGNED)
    backend.save_contract(signed)

    tools = DataPilotTools()

    class _Sched:
        id = "sched_bind_1"
        name = "Nightly Bound"
        source_connector_id = "c1"
        dest_connector_id = "c2"
        source_table = "orders"
        dest_table = "orders_wh"
        sync_mode = "incremental"
        contract_id = signed.id
        require_signed_contract = True

    monkeypatch.setattr(tools, "_resolve_schedule", lambda sid="", name="": (_Sched(), None))
    result = tools.execute("run_schedule_now", {"name": "Nightly Bound"})
    assert result.success is True, result.error
    preview = (result.output or {}).get("preview") or {}
    assert preview["contract_id"] == signed.id
    assert preview["require_signed_contract"] is True
    assert preview["breaker_state"] == "closed"
    peek = PilotAckLedger(path=tmp_path / "acks.json").peek(result.output["ack_id"])
    assert peek is not None
    assert peek.get("kind") == "run_schedule"
    assert (peek.get("preview") or {}).get("contract_id") == signed.id


def test_run_schedule_now_refuses_open_breaker(tmp_path, monkeypatch):
    from services.data_contract import BreakerState
    from src.ai.copilot.tools import DataPilotTools

    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    import src.ai.copilot.ack_ledger as ack_mod

    ack_mod._ledger = None
    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="sched-open", status=ContractStatus.SIGNED)
    backend.save_contract(signed)
    breaker = backend.get_breaker(signed.id)
    breaker.state = BreakerState.OPEN
    backend.save_breaker(breaker)

    tools = DataPilotTools()

    class _Sched:
        id = "sched_open_1"
        name = "Nightly Open"
        source_connector_id = "c1"
        dest_connector_id = "c2"
        source_table = "orders"
        dest_table = "orders_wh"
        sync_mode = "full_refresh_overwrite"
        contract_id = signed.id
        require_signed_contract = True

    monkeypatch.setattr(tools, "_resolve_schedule", lambda sid="", name="": (_Sched(), None))
    result = tools.execute("run_schedule_now", {"name": "Nightly Open"})
    assert result.success is False
    assert "is OPEN" in (result.error or "")
    assert not (result.output or {}).get("ack_id")


def test_render_schedule_run_names_bind_and_overwrite():
    from src.ai.copilot.pilot_agent import _render_schedule_run

    text = _render_schedule_run({
        "name": "Nightly Bound",
        "destructive": True,
        "preview": {
            "source_table": "orders",
            "dest_table": "orders_wh",
            "sync_mode": "full_refresh_overwrite",
            "contract_id": "dfc-1",
            "require_signed_contract": True,
            "breaker_state": "closed",
        },
    })
    assert "Nightly Bound" in text
    assert "`orders` → `orders_wh`" in text
    assert "dfc-1" in text
    assert "SIGNED" in text
    assert "closed" in text
    assert "overwrites" in text.lower()


def test_schedule_bind_summary_lists_open_breaker_without_raising(monkeypatch):
    from services.data_contract import BreakerState
    from services.schedule_store import schedule_bind_summary

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="listed-open", status=ContractStatus.SIGNED)
    backend.save_contract(signed)
    breaker = backend.get_breaker(signed.id)
    breaker.state = BreakerState.OPEN
    backend.save_breaker(breaker)

    preview = schedule_bind_summary(
        SimpleNamespace(contract_id=signed.id, require_signed_contract=True),
    )
    assert preview["contract_id"] == signed.id
    assert preview["breaker_state"] == "open"


def test_schedule_bind_summary_unbound_is_empty():
    from services.schedule_store import schedule_bind_summary

    assert schedule_bind_summary(
        SimpleNamespace(contract_id="", require_signed_contract=False),
    ) == {}


def test_schedule_summary_includes_bind_and_sync(monkeypatch):
    from src.ai.copilot.tools import DataPilotTools

    backend, DataContract, ContractStatus = _backend(monkeypatch)
    signed = DataContract(name="nightly-summary", status=ContractStatus.SIGNED)
    backend.save_contract(signed)

    class _Sched:
        id = "sched_sum_1"
        name = "Nightly"
        enabled = True
        interval = "daily"
        cron = ""
        timezone = "UTC"
        source_table = "orders"
        dest_table = "orders_wh"
        sync_mode = "incremental"
        next_run_at = "2026-08-16T00:00:00+00:00"
        last_run_at = None
        last_status = "success"
        run_count = 3
        contract_id = signed.id
        require_signed_contract = True

    row = DataPilotTools()._schedule_summary(_Sched())
    assert row["sync_mode"] == "incremental"
    assert row["contract_id"] == signed.id
    assert row["require_signed_contract"] is True
    assert row["breaker_state"] == "closed"


def test_schedule_summary_omits_bind_when_unbound():
    from src.ai.copilot.tools import DataPilotTools

    class _Sched:
        id = "sched_sum_2"
        name = "Adhoc"
        enabled = True
        interval = "hourly"
        cron = ""
        timezone = "UTC"
        source_table = "t"
        dest_table = "t"
        sync_mode = "full_refresh_append"
        next_run_at = None
        last_run_at = None
        last_status = None
        run_count = 0
        contract_id = ""
        require_signed_contract = False

    row = DataPilotTools()._schedule_summary(_Sched())
    assert "contract_id" not in row
    assert "enforce_contract" not in row
    assert "breaker_state" not in row


def test_render_schedule_detail_names_route_and_bind():
    from src.ai.copilot.pilot_agent import _render_schedule_detail, _schedule_bind_phrase

    text = _render_schedule_detail({
        "id": "s1",
        "name": "Nightly",
        "interval": "daily",
        "enabled": True,
        "source_table": "orders",
        "dest_table": "orders_wh",
        "sync_mode": "incremental",
        "contract_id": "dfc-1",
        "require_signed_contract": True,
        "breaker_state": "open",
        "next_run_at": "2026-08-16T00:00:00+00:00",
        "last_status": "failed",
        "run_count": 2,
    })
    assert "Nightly" in text
    assert "`orders` → `orders_wh`" in text
    assert "incremental" in text
    assert "dfc-1" in text
    assert "SIGNED" in text
    assert "open" in text
    assert _schedule_bind_phrase({"name": "x"}) == ""
    assert "no contract" in _schedule_bind_phrase({"require_signed_contract": True})


def test_render_schedule_detail_unbound_does_not_invent_enforce():
    from src.ai.copilot.pilot_agent import _render_schedule_detail

    text = _render_schedule_detail({
        "name": "Adhoc",
        "interval": "hourly",
        "enabled": False,
        "source_table": "t",
        "dest_table": "t",
    })
    assert "No data contract bound" in text
    assert "enforce stays unset" in text
