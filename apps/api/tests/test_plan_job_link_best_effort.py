"""Linking a started run to its plan is best effort.

``/transfer/execute`` and ``/transfer/run`` create the pending job (claiming
the idempotency key and the destination) before they record the job on the
plan. A busy or damaged plan store used to fail that link with 503, leaving
the claimed job orphaned: the caller's retry then answered 409
duplicate_transfer for a run that never started. The run does not need the
link, so a store refusal is logged and the run goes on.
"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from services import transfer_plan_store as plans

JSON_EXECUTE = {
    "source": {"kind": "database", "format": "sqlite", "database": "source", "table": "source_table"},
    "destination": {"kind": "database", "format": "sqlite", "database": "destination", "table": "dest_table"},
    "skip_preflight": True,
    "async_mode": True,
    "enforce_contract": False,
    "plan_id": "plan-link",
}
FORM_EXECUTE = {
    "source_kind": "database",
    "source_format": "sqlite",
    "source_database": "source",
    "source_table": "source_table",
    "dest_kind": "database",
    "dest_format": "sqlite",
    "dest_database": "destination",
    "dest_table": "dest_table",
    "skip_preflight": "true",
    "async_mode": "true",
    "mappings_json": "[]",
    "plan_id": "plan-link",
}


class _Engine:
    created: list[str] = []

    def _create_pending_job(self, _request):
        self.created.append("job-link")
        return "job-link"


@pytest.fixture
def started_runs(monkeypatch):
    from services import body_id_ownership, transfer_plan_service
    from src.routers import transfer_router
    from src.transfer import engine

    _Engine.created = []
    monkeypatch.setattr(engine, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr(transfer_router, "get_transfer_engine", lambda: _Engine(), raising=False)
    monkeypatch.setattr(body_id_ownership, "plan_owner", lambda _plan_id: (True, ""))
    monkeypatch.setattr(
        transfer_plan_service,
        "merge_plan_into_run",
        lambda *_a, **_k: {"mappings": [{"source": "id", "target": "id"}], "column_types": {}, "policies": {}},
    )
    monkeypatch.setattr(transfer_router, "_stamp_job_mapping_artifacts", lambda *_a, **_k: None)
    monkeypatch.setattr("src.transfer.background.run_transfer_async", lambda *_a, **_k: None)
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    return _Engine


def _post(route: str):
    from src.main import app

    client = TestClient(app, raise_server_exceptions=False)
    if route == "execute":
        return client.post("/api/v1/transfer/execute", json=JSON_EXECUTE)
    return client.post("/api/v1/transfer/run", data=FORM_EXECUTE)


@pytest.mark.parametrize("route", ["execute", "run"])
@pytest.mark.parametrize(
    "refusal",
    [
        lambda: plans.PlanStoreBusy(plans.STORE_PATH),
        lambda: plans.PlanStoreUnreadable(plans.STORE_PATH, ValueError("bad json")),
        lambda: OSError(28, "No space left on device", "/srv/secret-volume/transfer_plans.json"),
        lambda: plans.PlanStoreReentry("nested"),
        lambda: RuntimeError("unexpected at /srv/secret-volume/x"),
    ],
    ids=["busy", "unreadable", "enospc", "reentry", "runtime"],
)
def test_a_plan_store_refusal_does_not_fail_a_run_that_was_already_created(
    started_runs, monkeypatch, caplog, route, refusal
):
    def refuse(*_args, **_kwargs):
        raise refusal()

    monkeypatch.setattr(plans, "attach_job", refuse)
    caplog.set_level(logging.WARNING)
    res = _post(route)
    assert res.status_code == 200, res.text
    assert res.json()["job_id"] == "job-link"
    assert started_runs.created == ["job-link"]
    logged = [r for r in caplog.records if "job-link" in r.getMessage() and "plan-link" in r.getMessage()]
    assert logged, "the skipped link is logged with the job and plan ids"
    assert all("/srv/secret-volume" not in r.getMessage() and not r.exc_info for r in logged)


@pytest.mark.parametrize("route", ["execute", "run"])
def test_the_run_is_linked_to_its_plan_when_the_store_answers(started_runs, monkeypatch, route):
    linked: list[tuple] = []
    monkeypatch.setattr(plans, "attach_job", lambda plan_id, job_id, **kw: linked.append((plan_id, job_id, kw)))
    res = _post(route)
    assert res.status_code == 200, res.text
    assert linked == [("plan-link", "job-link", {"status": "running"})]
