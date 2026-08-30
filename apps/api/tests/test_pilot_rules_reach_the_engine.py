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

from src.ai.copilot.transfer_tools import _ground_data_rules, _unapplied_rules_error

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
    # The engine reads merge keys off the contract; without this the write
    # inserts and duplicates the key instead of upserting.
    assert out["stream_contracts"] == [
        {"name": "stream", "primary_key": "id", "selected": True}
    ]
    assert "upsert" in out["sync_mode"]


def test_dedupe_key_is_honoured_as_the_upsert_identity():
    out, err = _ground(dedupe_key="Email")
    assert err == ""
    assert out["stream_contracts"][0]["primary_key"] == "Email"


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
