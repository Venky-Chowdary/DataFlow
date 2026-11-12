"""Security review round 2 (M4): CDC drift repair judges residency before any write.

The Pilot confirm action ``repair_cdc_drift`` runs ``run_fenced_drift_repair``,
which wrote to the job's destination with ``write_destination_database``
directly. A job created before its workspace was fenced could still be
written to after it. Now the resolved destination is judged with
``residency_refusal`` first, and an out-of-region destination is refused
(``PermissionError``, a 403 at the confirm route) before the lease, the
source scan or any write.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from tests.test_cdc_drift_repair import EVIDENCE, _job


@pytest.fixture
def fenced(tmp_path, monkeypatch):
    from services import tenant_store

    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    tenant_store.create_tenant(workspace_id="workspace-a", name="EU", data_region="eu-west-1")
    return tmp_path


def _arm(monkeypatch, destination):
    from src.transfer.models import EndpointConfig

    job = _job()
    source = EndpointConfig(kind="database", format="postgresql", host="x.eu-west-1.rds.amazonaws.com",
                            database="source", table="orders")

    class Store:
        def get_job(self, _job_id):
            return job

    calls: list[str] = []
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: Store())
    monkeypatch.setattr(
        "src.transfer.models.transfer_request_from_dict",
        lambda _saved: SimpleNamespace(source=source, destination=destination),
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_connector_config",
        lambda endpoint, workspace_id=None: calls.append("resolve") or {"database": "x"},
    )
    monkeypatch.setattr(
        "src.transfer.adapters.write_destination_database",
        lambda *a, **k: calls.append("write") or (1, [], {}),
    )
    monkeypatch.setattr("src.transfer.cdc_transfer._single_stream_cursor_key", lambda **_k: EVIDENCE["cursor_key"])
    from connectors import cdc_eos_sql

    monkeypatch.setattr(cdc_eos_sql, "dest_watermark_view", lambda *a, **k: SimpleNamespace(fence_epoch=4))
    monkeypatch.setattr("connectors.cdc_eos_sa.sa_dest_watermark_view", lambda *a, **k: SimpleNamespace(fence_epoch=4))
    monkeypatch.setattr("services.cdc_lease.lease_view", lambda _key: None)
    monkeypatch.setattr("services.cdc_lease.new_holder_id", lambda _job: "repair-holder")

    def stop_at_lease(*_a, **_k):
        calls.append("lease")
        raise _ReachedLease()

    monkeypatch.setattr("services.cdc_lease.acquire_lease", stop_at_lease)
    monkeypatch.setattr("services.audit_log.append_audit_event", lambda **_kwargs: None)
    return calls


class _ReachedLease(Exception):
    """The run got past residency to the lease: the harness stops it there."""


def test_an_out_of_region_destination_is_refused_before_any_write(fenced, monkeypatch):
    from services.cdc_drift_repair import run_fenced_drift_repair
    from src.transfer.models import EndpointConfig

    destination = EndpointConfig(kind="database", format="postgresql", host="52.1.2.3", database="d", table="orders_copy")
    calls = _arm(monkeypatch, destination)
    with pytest.raises(PermissionError, match="(?i)residency"):
        run_fenced_drift_repair("job-drift-12345678", [], actor="operator", workspace_id="workspace-a")
    assert "lease" not in calls and "write" not in calls, calls


def test_the_confirm_route_answers_403(fenced, monkeypatch):
    import asyncio

    from fastapi import HTTPException

    from src.routers import copilot_router
    from src.transfer.models import EndpointConfig

    destination = EndpointConfig(kind="database", format="postgresql", host="52.1.2.3", database="d", table="orders_copy")
    calls = _arm(monkeypatch, destination)
    monkeypatch.setattr(copilot_router, "_caller", lambda _request: ("operator", "actor"))
    request = type("Request", (), {"headers": {"x-workspace-id": "workspace-a"}})()
    with pytest.raises(HTTPException) as caught:
        asyncio.run(copilot_router._run_lifecycle_confirm(
            "repair_cdc_drift", {"job_id": "job-drift-12345678", "key_ids": []}, request, None))
    assert caught.value.status_code == 403
    assert "residency" in str(caught.value.detail).lower()
    assert "lease" not in calls and "write" not in calls, calls


def test_an_in_region_destination_is_not_refused_for_residency(fenced, monkeypatch):
    """Control: an eu-west-1 destination passes the residency check and the run
    goes on to take the lease (where this harness stops it)."""
    from services.cdc_drift_repair import run_fenced_drift_repair
    from src.transfer.models import EndpointConfig

    destination = EndpointConfig(kind="database", format="postgresql", host="y.eu-west-1.rds.amazonaws.com",
                                 database="d", table="orders_copy")
    calls = _arm(monkeypatch, destination)
    with pytest.raises(_ReachedLease):
        run_fenced_drift_repair("job-drift-12345678", [], actor="operator", workspace_id="workspace-a")
    assert calls[-1] == "lease" and "write" not in calls
