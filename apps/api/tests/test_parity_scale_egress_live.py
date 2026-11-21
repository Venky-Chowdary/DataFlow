"""Parity at Scale phase 3: verification and the explorer dial under the job's workspace (review defect 2).

The test suite's conftest admits loopback for the whole process
(``DATAFLOW_EGRESS_ALLOWLIST``), which hid that the background worker and the
explorer bound no workspace: in production every allowlisted database ended
``error`` (EgressRefused) and the Values tab answered 500. Here the process
allowlist is removed; only the job's workspace allows loopback, as a
deployment's workspace egress allowlist does.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from test_parity_scale_verification_live import _env, _job, _sql, tables  # noqa: E402,F401

pytestmark = pytest.mark.skipif(not parity_engines.available("postgresql"),
                                reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")

ALLOWED = "ws-egress-allowed"


@pytest.fixture(autouse=True)
def _workspace_allowlist(monkeypatch):
    import services.tenant_store as tenant_store

    monkeypatch.delenv("DATAFLOW_EGRESS_ALLOWLIST", raising=False)
    monkeypatch.delenv("DATAWRAP_EGRESS_ALLOWLIST", raising=False)
    monkeypatch.setattr(tenant_store, "get_tenant_for_workspace",
                        lambda wid: SimpleNamespace(egress_allowlist=["127.0.0.0/8", "::1/128"])
                        if wid == ALLOWED else None)


def _svc():
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def _in_workspace(job_id: str, workspace_id: str) -> str:
    _svc().update_job_fields(job_id, {"workspace_id": workspace_id})
    return job_id


def test_the_background_worker_dials_under_the_jobs_workspace(tables) -> None:
    from services.parity_scale import verification

    job_id = _in_workspace(_job(*tables), ALLOWED)
    verification.start_for_job(job_id)  # the real background thread
    deadline = time.monotonic() + 120
    while verification.is_running(_svc().get_job(job_id)) and time.monotonic() < deadline:
        time.sleep(0.2)
    block = _svc().get_job(job_id)["parity_scale"]
    assert block["state"] == "passed", block


def test_the_explorer_dials_under_the_jobs_workspace(tables) -> None:
    from services.parity_scale import explorer, verification

    src, dst = tables
    _sql(f"UPDATE \"{dst}\" SET name = 'changed' WHERE id = 11")
    job_id = _in_workspace(_job(src, dst), ALLOWED)
    assert verification.start_for_job(job_id, background=False)["state"] == "failed"
    rows = explorer.values(_svc().get_job(job_id), dst)["rows"]
    assert [r["key"] for r in rows] == [["11"]]
    assert explorer.repair_sql(_svc().get_job(job_id), dst)["available"] is True


def test_a_workspace_without_the_allowlist_is_refused_with_a_plain_reason(tables) -> None:
    """The control: the guard really is consulted, and the report never shows the host."""
    from services.parity_scale import verification

    job_id = _in_workspace(_job(*tables), "ws-egress-none")
    block = verification.start_for_job(job_id, background=False)
    assert block["state"] == "error", block
    error = block["tables"][0]["error"]
    assert "EgressRefused" in error or "ValueError" in error
    assert "127.0.0.1" not in error and "loopback" not in error


def test_sql_server_dials_through_the_connectors_builder(monkeypatch) -> None:
    """Not a bare pymssql.connect: the egress guard, the workspace and the TLS options come along."""
    import connectors.generic_sql as generic_sql

    from services.egress_guard import egress_workspace
    from services.parity_scale import verification

    seen: dict = {}
    monkeypatch.setattr(generic_sql, "get_connection", lambda **kw: seen.update(kw) or "conn")
    cfg = {"host": "db.example.com", "port": 1433, "database": "sales", "username": "u", "password": "p",
           "extra": {"trust_server_certificate": True}}
    with egress_workspace(ALLOWED):
        assert verification.connect("sqlserver", cfg) == "conn"
    assert seen["db_type"] == "sqlserver" and seen["workspace_id"] == ALLOWED
    assert seen["trust_server_certificate"] is True


def test_the_explorer_closes_the_source_when_the_destination_fails(monkeypatch) -> None:
    from services.parity_scale import explorer, verification

    closed: list[str] = []

    class Side:
        def close(self) -> None:
            closed.append("source")

    def opener(_spec):
        def open_(role, _plan):
            if role == "destination":
                raise ConnectionError("destination refused")
            return Side()
        return open_

    monkeypatch.setattr(verification, "table_spec", lambda job: ({"name": "t"}, ""))
    monkeypatch.setattr(verification, "plan_for", lambda spec: object())
    monkeypatch.setattr(verification, "side_opener", opener)
    with pytest.raises(ConnectionError):
        explorer._sides({"workspace_id": ALLOWED})
    assert closed == ["source"]
