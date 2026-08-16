"""Pilot plan_transfer must peek CALL/SELECT — never introspect a colliding table."""

from __future__ import annotations

import pytest

from services.procedure_source import ProcedureSourceError
from src.ai.copilot.transfer_tools import plan_transfer, resolve_callable_plan_source


def test_resolve_callable_from_procedure_call() -> None:
    plan = resolve_callable_plan_source(
        source_read_mode="procedure",
        procedure_call="CALL get_orders(:since)",
        procedure_params={"since": "2024-01-01"},
        dialect="postgresql",
    )
    assert plan is not None
    assert plan["mode"] == "procedure"
    assert plan["stream_name"] == "get_orders"
    assert plan["params"]["since"] == "2024-01-01"


def test_resolve_callable_from_table_slot_call() -> None:
    plan = resolve_callable_plan_source(source_table="CALL get_orders()")
    assert plan is not None
    assert plan["mode"] == "procedure"
    assert plan["stream_name"] == "get_orders"


def test_resolve_callable_from_procedure_call_without_mode() -> None:
    plan = resolve_callable_plan_source(procedure_call="CALL get_orders()")
    assert plan is not None
    assert plan["mode"] == "procedure"
    assert plan["stream_name"] == "get_orders"


def test_resolve_callable_from_table_slot_select() -> None:
    plan = resolve_callable_plan_source(source_table="SELECT * FROM get_orders(:since)")
    assert plan is not None
    assert plan["mode"] == "query"
    assert plan["stream_name"] == "get_orders"


def test_bare_procedure_mode_without_sql_fails_closed() -> None:
    with pytest.raises(ProcedureSourceError, match="CALL/SELECT text"):
        resolve_callable_plan_source(
            source_read_mode="procedure",
            source_table="get_orders",
        )


def test_table_plan_is_none_for_ordinary_table() -> None:
    assert resolve_callable_plan_source(source_table="orders") is None
    assert resolve_callable_plan_source(source_read_mode="table", source_table="orders") is None


def test_plan_transfer_still_requires_a_table_when_not_callable() -> None:
    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("plan_transfer", {"source_connector_name": "pg"})
    assert not res.success
    assert "table" in (res.error or "").lower()


def test_plan_transfer_refuses_cdc_before_introspect_or_peek(monkeypatch: pytest.MonkeyPatch) -> None:
    """CDC on a CALL is a snapshot refusal — do not open connectors or peek."""
    calls: list[str] = []

    def _boom(*_a, **_k):
        calls.append("safe_connector")
        raise AssertionError("CDC refusal must not look up connectors")

    monkeypatch.setattr("src.ai.copilot.transfer_tools._safe_connector", _boom)
    monkeypatch.setattr(
        "src.ai.copilot.transfer_tools._peek_callable_source",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("must not peek")),
    )
    monkeypatch.setattr(
        "src.ai.copilot.transfer_tools._introspect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("must not introspect")),
    )

    res = plan_transfer(
        source_connector_name="pg",
        dest_connector_name="wh",
        procedure_call="CALL get_orders()",
        sync_mode="cdc",
    )
    assert not res.success
    assert "snapshot" in (res.error or "").lower() or "cdc" in (res.error or "").lower()
    assert calls == []


def test_plan_transfer_refuses_scd2_and_mirror_on_query() -> None:
    for mode in ("scd2", "mirror", "full_refresh_mirror"):
        res = plan_transfer(
            source_table="SELECT * FROM get_orders()",
            dest_table="orders_out",
            sync_mode=mode,
        )
        assert not res.success, mode
        assert "snapshot" in (res.error or "").lower() or "identity" in (res.error or "").lower()


def test_plan_transfer_peeks_callable_and_skips_source_introspect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    introspect_purposes: list[str] = []

    def _fake_connector(cid, name, tool):
        label = (name or cid or "pg").strip() or "pg"
        return (
            {"id": label, "name": label, "type": "postgresql", "schema": "public"},
            None,
        )

    def _fake_introspect(conn, table, purpose="source"):
        introspect_purposes.append(f"{purpose}:{table}")
        if purpose == "source":
            raise AssertionError("callable plan must not introspect the stream as a table")
        return {
            "ok": True,
            "columns": [
                {"name": "order_id", "inferred_type": "INTEGER", "nullable": False},
                {"name": "amount", "inferred_type": "NUMERIC", "nullable": True},
            ],
            "db_type": "postgresql",
            "schema": {"order_id": "INTEGER", "amount": "NUMERIC"},
            "cfg": {},
            "endpoint": None,
            "error": "",
            "raw": {},
            "table_exists": True,
        }

    def _fake_peek(conn, plan):
        assert plan["mode"] == "procedure"
        assert plan["stream_name"] == "get_orders"
        return {
            "ok": True,
            "columns": [
                {"name": "order_id", "inferred_type": "INTEGER", "nullable": True},
                {"name": "amount", "inferred_type": "NUMERIC", "nullable": True},
            ],
            "schema": {"order_id": "INTEGER", "amount": "NUMERIC"},
            "db_type": "postgresql",
            "cfg": {},
            "endpoint": None,
            "sample_rows": [{"order_id": 1, "amount": "9.50"}],
            "error": "",
            "raw": {},
        }

    def _fake_preflight(**kwargs):
        assert kwargs.get("source_read_mode") == "procedure"
        cfg = kwargs.get("source_config") or {}
        assert cfg.get("source_read_mode") == "procedure"
        assert "CALL get_orders()" in str(cfg.get("procedure_call") or "")
        return {
            "run_id": "pf_test_callable",
            "passed": True,
            "readiness_score": 1,
            "passed_count": 9,
            "total_gates": 9,
            "gates": [],
            "blockers": [],
            "warnings": [],
            "proof_bundle": {"transfer_decision": {"decision": "approve"}},
        }

    monkeypatch.setattr("src.ai.copilot.transfer_tools._safe_connector", _fake_connector)
    monkeypatch.setattr("src.ai.copilot.transfer_tools._introspect", _fake_introspect)
    monkeypatch.setattr("src.ai.copilot.transfer_tools._peek_callable_source", _fake_peek)
    monkeypatch.setattr("src.ai.copilot.transfer_tools._run_preflight", _fake_preflight)

    res = plan_transfer(
        source_connector_name="pg",
        dest_connector_name="wh",
        dest_table="orders_out",
        procedure_call="CALL get_orders()",
        sync_mode="full_refresh_append",
    )
    assert res.success, res.error
    out = res.output or {}
    assert out["source"]["source_read_mode"] == "procedure"
    assert out["source"]["table"] == "get_orders"
    assert out["source"]["procedure_call"] == "CALL get_orders()"
    assert introspect_purposes == ["destination:orders_out"]


def test_merge_callable_source_extra_form_wins_over_plan() -> None:
    from services.procedure_source import merge_callable_source_extra

    merged = merge_callable_source_extra(
        {"source_read_mode": "procedure", "procedure_call": "CALL form_orders()"},
        {
            "source_read_mode": "query",
            "source_query": "SELECT 1",
            "extra": {"procedure_call": "CALL plan_orders()"},
        },
    )
    assert merged["source_read_mode"] == "procedure"
    assert merged["procedure_call"] == "CALL form_orders()"
    assert merged["source_query"] == "SELECT 1"

    filled = merge_callable_source_extra(
        {},
        {
            "table": "get_orders",
            "extra": {
                "source_read_mode": "procedure",
                "procedure_call": "CALL get_orders()",
            },
        },
    )
    assert filled["source_read_mode"] == "procedure"
    assert filled["procedure_call"] == "CALL get_orders()"


def test_confirm_payload_keeps_callable_on_endpoint() -> None:
    """Confirm builds EndpointConfig from the staged payload — extra must keep CALL."""
    from services.procedure_source import is_callable_source, procedure_text_of, source_read_mode_of
    from src.transfer.models import EndpointConfig

    ep = EndpointConfig.from_dict(
        "database",
        {
            "kind": "database",
            "format": "postgresql",
            "connector_id": "c1",
            "table": "get_orders",
            "source_read_mode": "procedure",
            "procedure_call": "CALL get_orders()",
            "source_query": "",
            "procedure_params": {"since": "2024-01-01"},
        },
    )
    assert source_read_mode_of(ep) == "procedure"
    assert is_callable_source(ep)
    assert procedure_text_of(ep) == "CALL get_orders()"
    assert ep.extra.get("procedure_params") == {"since": "2024-01-01"}


def test_pilot_keeps_scd2_and_mirror_tokens() -> None:
    from src.ai.copilot.transfer_tools import normalize_sync_mode

    assert normalize_sync_mode("scd2") == "scd2"
    assert normalize_sync_mode("full_refresh_mirror") == "mirror"


def test_render_transfer_names_callable_snapshot() -> None:
    from src.ai.copilot.pilot_agent import _render_transfer

    text = _render_transfer(
        "plan_transfer",
        {
            "source": {
                "connector_name": "pg",
                "table": "get_orders",
                "column_count": 2,
                "source_read_mode": "procedure",
                "procedure_call": "CALL get_orders()",
            },
            "destination": {"connector_name": "wh", "table": "orders_out", "table_exists": True},
            "sync_mode": "full_refresh_append",
            "mapped_count": 2,
            "preflight": {},
        },
    )
    assert "procedure" in text
    assert "CALL get_orders()" in text
    assert "snapshot" in text.lower()


def test_recommend_sync_mode_refuses_cdc_for_procedure() -> None:
    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute(
        "recommend_sync_mode",
        {"workload": "cdc", "source_read_mode": "procedure"},
    )
    assert res.success
    assert res.output["recommended_mode"] == "Full Refresh Append"
    assert "snapshot" in res.output["reason"].lower() or "cdc" in res.output["reason"].lower()
    assert res.output["requires"]["cdc_log_access"] is False
