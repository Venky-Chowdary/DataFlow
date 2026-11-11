"""Pilot ground truth — stored results and staged approvals are bound and sealed (audit H5, L4).

A ``result_id`` was a bearer credential: any caller who had one could read the
rows it named, from any workspace, and the rows sat in plaintext in
``pilot_results.json`` for an hour. A staged approval held connector secrets in
plaintext in ``pilot_acks.json`` and could be confirmed from another workspace.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest

from tests.helpers import pilot_truth as pt

SALARY = "182000"
PASSWORD = "Hunter2-FAKE-xyz"


@contextmanager
def acting_as(workspace: str, actor: str, role: str = "admin"):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tool_permissions import caller_role

    with bound_workspace_filter(WorkspaceFilter.only(workspace), stamp=workspace, actor=actor), caller_role(role):
        yield


@pytest.fixture
def stores(monkeypatch, tmp_path):
    from src.ai.copilot import ack_ledger, result_store

    pt.reset_workspace(monkeypatch)
    monkeypatch.setenv("DATAFLOW_PILOT_RESULTS_PATH", str(tmp_path / "pilot_results.json"))
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "pilot_acks.json"))
    monkeypatch.setattr(result_store, "_store", None)
    monkeypatch.setattr(ack_ledger, "_ledger", None)
    return tmp_path


@pytest.fixture
def alices_result(stores):
    """Alice (workspace A) samples a payroll table; returns the result id."""
    from services.connector_store import create_connector
    from src.ai.copilot.tools import get_pilot_tools

    db = Path(stores) / "payroll.db"
    with sqlite3.connect(db) as conn:
        conn.execute("create table salaries (id integer, name text, salary real)")
        conn.executemany("insert into salaries values (?,?,?)", [(1, "Alice", 182000.0), (2, "Carl", 75000.0)])
        conn.commit()
    with acting_as("ws-A", "alice@a.example"):
        create_connector({"name": "A Payroll DB", "workspace_id": "ws-A", "type": "sqlite", "role": "source",
                          "database": str(db), "connection_string": str(db)})
        sampled = get_pilot_tools().execute(
            "sample_connector_object",
            {"connector_name": "A Payroll DB", "table": "salaries", "limit": 5, "session_id": "alice@a.example|s1"},
        )
    assert sampled.success, sampled.error
    return sampled.output["result_id"]


def _filter(result_id: str):
    from src.ai.copilot.tools import get_pilot_tools

    return get_pilot_tools().execute(
        "filter_result", {"result_id": result_id, "column": "salary", "op": "gt", "value": "0"}
    )


def test_the_owner_can_still_use_their_result(alices_result):
    with acting_as("ws-A", "alice@a.example"):
        result = _filter(alices_result)
    assert result.success, result.error
    assert result.output["match_count"] == 2


def test_another_workspace_cannot_read_a_result_by_id(alices_result):
    with acting_as("ws-B", "bob@b.example", role="viewer"):
        result = _filter(alices_result)
        response = pt.ask(pt.agent(), "analyze that", last_result_id=alices_result)
    assert result.success is False
    assert SALARY not in str(result.output) + str(result.error)
    for leaked in (SALARY, "Alice", "Carl", "75000"):
        assert leaked not in response.answer, response.answer


def test_another_actor_in_the_same_workspace_cannot_read_it(alices_result):
    with acting_as("ws-A", "carol@a.example"):
        result = _filter(alices_result)
    assert result.success is False
    assert SALARY not in str(result.output) + str(result.error)


def test_stored_rows_are_not_plaintext_on_disk_and_survive_a_restart(alices_result, stores):
    from src.ai.copilot.result_store import PilotResultStore

    path = Path(stores) / "pilot_results.json"
    on_disk = path.read_text(encoding="utf-8")
    assert alices_result in on_disk, "the reference itself is metadata and stays readable"
    for cell in (SALARY, "Alice", "Carl"):
        assert cell not in on_disk, f"cell value {cell!r} is in plaintext in {path.name}"

    reopened = PilotResultStore(path=path)  # another worker / the API after a restart
    with acting_as("ws-A", "alice@a.example"):
        doc = reopened.get(alices_result)
    assert doc and [row["name"] for row in doc["rows"]] == ["Alice", "Carl"]
    with acting_as("ws-B", "bob@b.example"):
        assert reopened.get(alices_result) is None


def test_latest_result_of_a_session_is_not_shared_across_workspaces(alices_result):
    from src.ai.copilot.result_store import get_result_store

    with acting_as("ws-B", "bob@b.example"):
        assert get_result_store().resolve(session_id="alice@a.example|s1") is None
    with acting_as("ws-A", "alice@a.example"):
        assert get_result_store().resolve(session_id="alice@a.example|s1") is not None


# ---- the ack ledger ---------------------------------------------------------


def _stage_connector_ack() -> str:
    from src.ai.copilot.ack_ledger import get_ack_ledger

    return get_ack_ledger().put(
        kind="create_connector",
        payload={"name": "Orders PG", "type": "postgresql", "host": "db.acme.example", "password": PASSWORD,
                 "connection_string": f"postgresql://svc:{PASSWORD}@db.acme.example/orders"},
    )


def test_staged_connector_secret_is_not_plaintext_on_disk_and_still_confirms(stores):
    from src.ai.copilot.ack_ledger import PilotAckLedger

    with acting_as("ws-A", "alice@a.example"):
        ack_id = _stage_connector_ack()
    path = Path(stores) / "pilot_acks.json"
    assert ack_id in path.read_text(encoding="utf-8")
    assert PASSWORD not in path.read_text(encoding="utf-8")

    reopened = PilotAckLedger(path=path)  # the API after a restart
    with acting_as("ws-A", "alice@a.example"):
        payload, error = reopened.claim(ack_id, actor="alice@a.example")
    assert not error and payload["password"] == PASSWORD


def test_an_ack_records_who_staged_it_and_where(stores):
    from src.ai.copilot.ack_ledger import get_ack_ledger

    with acting_as("ws-A", "alice@a.example"):
        ack_id = _stage_connector_ack()
        peek = get_ack_ledger().peek(ack_id)
    assert peek["workspace_id"] == "ws-A"
    assert peek["staged_by"] == "alice@a.example"
    assert PASSWORD not in str(peek)


def test_an_ack_cannot_be_seen_or_claimed_from_another_workspace(stores):
    from src.ai.copilot.ack_ledger import get_ack_ledger

    with acting_as("ws-A", "alice@a.example"):
        ack_id = _stage_connector_ack()
    ledger = get_ack_ledger()
    with acting_as("ws-B", "bob@b.example"):
        assert ledger.peek(ack_id) is None
        payload, error = ledger.claim(ack_id, actor="bob@b.example")
        pending, pending_error = ledger.get_pending_payload(ack_id)
    assert payload is None and error
    assert pending is None and pending_error
    with acting_as("ws-A", "alice@a.example"):
        assert ledger.peek(ack_id)["consumed"] is False, "the refused claim must not consume the approval"


def test_confirm_route_refuses_an_ack_staged_in_another_workspace(stores, monkeypatch):
    from fastapi.testclient import TestClient

    from src.ai.copilot.ack_ledger import get_ack_ledger
    from src.main import app
    from src.routers import copilot_router

    monkeypatch.setattr(copilot_router, "_caller", lambda _request: ("admin", "bob@b.example"))
    with acting_as("ws-A", "alice@a.example"):
        ack_id = get_ack_ledger().put(kind="create_transform_recipe", payload={"template_key": "dedupe-keep-latest"})
    response = TestClient(app).post(
        "/api/v1/copilot/confirm", headers={"X-Workspace-Id": "ws-B"}, json={"ack_id": ack_id}
    )
    assert response.status_code == 404
    with acting_as("ws-A", "alice@a.example"):
        assert get_ack_ledger().peek(ack_id)["consumed"] is False


def test_shared_mongo_ledger_seals_secrets_and_keeps_other_values_typed(stores):
    from decimal import Decimal

    from src.ai.copilot.ack_ledger import MongoAckLedger

    class _Coll:
        def __init__(self):
            self.docs: dict[str, dict] = {}

        def insert_one(self, doc):
            self.docs[doc["_id"]] = dict(doc)

        def find_one(self, query):
            return self.docs.get(query["_id"])

        def delete_many(self, _query):
            return None

    coll = _Coll()
    ledger = MongoAckLedger(collection=coll)
    with acting_as("ws-A", "alice@a.example"):
        ack_id = ledger.put(
            kind="start_transfer",
            payload={"source": {"host": "db", "password": PASSWORD}, "mappings": [{"statistics": {"min": Decimal("1")}}]},
        )
        assert PASSWORD not in str(coll.docs[ack_id]), "a staged secret is in plaintext in the shared collection"
        assert coll.docs[ack_id]["payload"]["mappings"][0]["statistics"]["min"] == Decimal("1")
        payload, error = ledger.get_pending_payload(ack_id)
    assert not error and payload["source"]["password"] == PASSWORD
    with acting_as("ws-B", "bob@b.example"):
        assert ledger.peek(ack_id) is None
        assert ledger.get_pending_payload(ack_id)[0] is None
