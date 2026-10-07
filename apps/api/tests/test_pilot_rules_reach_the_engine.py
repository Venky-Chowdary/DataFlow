"""A rule spoken in chat must reach the engine, or the run must be refused.

Parsing "only rows where status = active, upsert on id" correctly is worth
nothing if the confirmed run then copies the whole table under a green proof.
These tests pin the two hand-offs where that could silently happen:

* ``_ground_data_rules`` — the parsed rule is bound to a real source column, or
  the plan fails closed;
* ``_start_confirmed_transfer`` — the staged filter and upsert contract are put
  on the ``TransferRequest`` the engine actually runs, with preflight still on.
"""

from __future__ import annotations

import asyncio

import pytest

from src.ai.copilot.transfer_tools import (
    _ground_data_rules,
    _identity_stream_contract,
    _unapplied_rules_error,
)

COLUMNS = ["id", "Email", "status", "signup_date"]


def _ground(**kwargs):
    return _ground_data_rules(
        source_filter=kwargs.get("source_filter"),
        upsert_key=kwargs.get("upsert_key", ""),
        dedupe_key=kwargs.get("dedupe_key", ""),
        source_columns=kwargs.get("source_columns", COLUMNS),
        source_label="Prod PG.users",
        mode=kwargs.get("mode", "full_refresh_append"),
    )


def test_filter_column_is_rebound_to_the_sources_own_spelling():
    out, err = _ground(source_filter={"column": "email", "operator": "is_not_null"})
    assert err == ""
    # Read rows carry the DDL's case; "email" would match nothing.
    assert out["source_filter"] == {"column": "Email", "operator": "is_not_null"}


def test_nested_filter_columns_are_rebound():
    out, err = _ground(
        source_filter={
            "and": [
                {"column": "status", "operator": "eq", "value": "active"},
                {"column": "email", "operator": "is_not_null"},
            ]
        }
    )
    assert err == ""
    assert [c["column"] for c in out["source_filter"]["and"]] == ["status", "Email"]


def test_unknown_filter_column_fails_closed():
    out, err = _ground(source_filter={"column": "is_deleted", "operator": "eq", "value": "0"})
    assert out["source_filter"] == {}
    assert "no column `is_deleted`" in err
    # The operator is told what they can filter on instead of being guessed at.
    assert "signup_date" in err


def test_unknown_upsert_key_fails_closed():
    out, err = _ground(upsert_key="customer_id")
    assert out["upsert_key"] == ""
    assert "no column `customer_id`" in err


def test_upsert_key_becomes_a_stream_contract_and_switches_the_mode():
    out, err = _ground(upsert_key="id")
    assert err == ""
    # Grounding binds the column and switches append to upsert. The stream
    # contract is built after the mapping exists, so a key that is not mapped
    # cannot become a merge key.
    assert out["stream_contracts"] == []
    assert out["upsert_key"] == "id"
    assert "upsert" in out["sync_mode"]
    contracts, identity_error = _identity_stream_contract(
        mode=out["sync_mode"],
        source_table="users",
        source_columns=COLUMNS,
        mappings=[{"source": "id", "target": "id"}],
        operator_key=out["upsert_key"],
        catalog_key="",
    )
    assert identity_error == ""
    assert contracts == [
        {"name": "users", "selected": True, "primary_key": ["id"]}
    ]


def test_dedupe_key_is_honoured_as_the_upsert_identity():
    out, err = _ground(dedupe_key="Email")
    assert err == ""
    assert out["upsert_key"] == "Email"
    contracts, identity_error = _identity_stream_contract(
        mode=out["sync_mode"],
        source_table="users",
        source_columns=COLUMNS,
        mappings=[{"source": "Email", "target": "email"}],
        operator_key=out["upsert_key"],
        catalog_key="",
    )
    assert identity_error == ""
    assert contracts[0]["primary_key"] == ["Email"]


def test_cdc_keeps_its_mode_and_uses_the_log_as_the_cursor():
    out, err = _ground(
        upsert_key="order_id",
        mode="cdc",
        source_columns=["order_id", "status"],
    )
    assert err == ""
    assert out["sync_mode"] == "cdc"
    contracts, identity_error = _identity_stream_contract(
        mode="cdc",
        source_table="orders",
        source_columns=["order_id", "status"],
        mappings=[{"source": "order_id", "target": "order_id"}],
        operator_key=out["upsert_key"],
        catalog_key="",
    )
    assert identity_error == ""
    assert contracts == [{
        "name": "orders",
        "selected": True,
        "primary_key": ["order_id"],
        "cursor_semantics": "cdc_position",
    }]


def test_catalog_primary_key_is_used_when_every_column_is_mapped():
    contracts, identity_error = _identity_stream_contract(
        mode="cdc",
        source_table="orders",
        source_columns=["id", "status"],
        mappings=[{"source": "id", "target": "id"}, {"source": "status", "target": "status"}],
        operator_key="",
        catalog_key="id",
    )
    assert identity_error == ""
    assert contracts[0]["primary_key"] == ["id"]
    assert contracts[0]["cursor_semantics"] == "cdc_position"


def test_unmapped_catalog_key_does_not_invent_id():
    contracts, identity_error = _identity_stream_contract(
        mode="upsert",
        source_table="orders",
        source_columns=["id", "sku"],
        mappings=[{"source": "sku", "target": "sku"}],
        operator_key="",
        catalog_key="id",
    )
    assert identity_error == ""
    assert contracts == []


def test_unmapped_operator_key_is_refused():
    contracts, identity_error = _identity_stream_contract(
        mode="cdc",
        source_table="orders",
        source_columns=["order_id", "sku"],
        mappings=[{"source": "sku", "target": "sku"}],
        operator_key="order_id",
        catalog_key="order_id",
    )
    assert contracts == []
    assert "not in the column mapping" in identity_error


def test_composite_operator_key_stays_a_list():
    contracts, identity_error = _identity_stream_contract(
        mode="upsert",
        source_table="orders",
        source_columns=["id", "tenant_id"],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "tenant_id", "target": "tenant_id"},
        ],
        operator_key="id,tenant_id",
        catalog_key="",
    )
    assert identity_error == ""
    assert contracts[0]["primary_key"] == ["id", "tenant_id"]


def test_mcp_start_transfer_accepts_primary_key():
    import inspect

    from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools, get_pilot_tools

    schema = next(t for t in TOOL_DEFINITIONS if t["name"] == "start_transfer")
    props = schema["input_schema"]["properties"]
    assert "upsert_key" in props
    assert "primary_key" in props
    for method in (
        DataPilotTools._plan_transfer,
        DataPilotTools._start_transfer,
        DataPilotTools._create_schedule,
    ):
        assert "primary_key" in inspect.signature(method).parameters
    result = get_pilot_tools().execute(
        "start_transfer",
        {
            "source_connector_name": "missing-src-for-pk",
            "dest_connector_name": "missing-dst-for-pk",
            "source_table": "orders",
            "sync_mode": "cdc",
            "primary_key": "order_id",
        },
    )
    assert "unexpected keyword" not in (result.error or "").lower()


def test_no_rules_leaves_the_requested_mode_untouched():
    out, err = _ground(mode="incremental_append")
    assert err == ""
    assert out["sync_mode"] == "incremental_append"
    assert out["stream_contracts"] == []


def test_unapplied_rules_error_refuses_rather_than_degrades():
    text = _unapplied_rules_error(["Name the column, e.g. “skip nulls in email”."])
    assert "will not run it" in text
    assert "skip nulls in email" in text


# --------------------------------------------------------------------------
# Confirmed run: the rules ride the TransferRequest, preflight stays on
# --------------------------------------------------------------------------


@pytest.fixture
def captured_request(monkeypatch):
    from src.routers import copilot_router
    from src.transfer import background, engine

    captured: dict = {}

    class _Engine:
        def _create_pending_job(self, request_obj):
            captured["request"] = request_obj
            return "job-test-1"

    monkeypatch.setattr(engine, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr(background, "run_transfer_async", lambda job_id, req: None)
    monkeypatch.setattr(copilot_router, "_ack_audit", lambda *a, **k: None, raising=False)
    return captured


def _payload(**over) -> dict:
    payload = {
        "source": {"connector_id": "src-1", "table": "users"},
        "destination": {"connector_id": "dst-1", "table": "users"},
        "sync_mode": "upsert",
        "source_filter": {"column": "status", "operator": "eq", "value": "active"},
        "stream_contracts": [{"name": "stream", "primary_key": "id", "selected": True}],
        "limit": 100,
    }
    payload.update(over)
    return payload


def test_confirmed_transfer_carries_the_filter_and_the_upsert_contract(captured_request):
    from src.routers.copilot_router import _start_confirmed_transfer

    out = asyncio.run(_start_confirmed_transfer(_payload()))
    assert out["job_id"] == "job-test-1"
    req = captured_request["request"]
    assert req.source_filter == {"column": "status", "operator": "eq", "value": "active"}
    assert req.stream_contracts == [
        {"name": "stream", "primary_key": "id", "selected": True}
    ]
    assert req.limit == 100
    assert req.triggered_by == "data-pilot"


def test_confirmed_transfer_can_never_skip_preflight(captured_request):
    from src.routers.copilot_router import _start_confirmed_transfer

    asyncio.run(_start_confirmed_transfer(_payload(skip_preflight=True)))
    assert captured_request["request"].skip_preflight is False


def test_confirmed_transfer_without_endpoints_is_refused(captured_request):
    from fastapi import HTTPException

    from src.routers.copilot_router import _start_confirmed_transfer

    with pytest.raises(HTTPException):
        asyncio.run(_start_confirmed_transfer(_payload(source={"table": "users"})))
    assert "request" not in captured_request
