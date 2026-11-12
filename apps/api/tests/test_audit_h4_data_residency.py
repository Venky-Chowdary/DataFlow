"""Audit H-4: the workspace's configured data region is what residency enforces.

For a workspace whose region is ``eu-west-1`` the old check refused 3 of 12
out-of-region destinations: it looked at S3 and DynamoDB only, mapped
``us-east-1`` to "unknown" (so the default US region always passed), ignored
``endpoint_url``, and compared against a ``data_region`` the request body could
supply, so naming the destination's own region passed. Schedules, MCP and the
Pilot never reached the check at all.

Now: the region comes from the workspace's tenant record only; ``us-east-1`` is
a region like any other; every destination whose endpoint shows or declares a
region is judged; and a destination whose region cannot be established is
refused, with the reason, while a region is enforced. The check also runs
where every job is created, so a schedule, an MCP tool or the Pilot cannot
route around the REST handler.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401


def _endpoint(fmt: str, kind: str = "database", **fields):
    from src.transfer.models import EndpointConfig

    return EndpointConfig.from_dict(kind, {"format": fmt, **fields})


@pytest.fixture
def eu_workspace(tenancy_world, monkeypatch):  # noqa: F811
    """Workspace A is an ``eu-west-1`` tenant; workspace B has no region."""
    from services.tenant_store import create_tenant

    monkeypatch.delenv("DATAWRAP_RESIDENCY_STRICT", raising=False)
    monkeypatch.delenv("DATAFLOW_RESIDENCY_STRICT", raising=False)
    monkeypatch.delenv("DATAWRAP_DEFAULT_REGION", raising=False)
    monkeypatch.delenv("DATAFLOW_DEFAULT_REGION", raising=False)
    create_tenant(workspace_id=tenancy_world.workspace_a, name="Acme EU", data_region="eu-west-1")
    return tenancy_world


EU_WS = "11111111-1111-4111-8111-111111111111"
NO_REGION_WS = "22222222-2222-4222-8222-222222222222"


@pytest.fixture
def eu_tenant(tmp_path, monkeypatch):
    """The tenant store alone (no HTTP): ``EU_WS`` is ``eu-west-1``, ``NO_REGION_WS`` unset."""
    from services import connector_store, tenant_store
    from services.tenant_store import create_tenant

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    # The destination is judged as the engine resolves it, saved connectors included.
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    create_tenant(workspace_id=EU_WS, name="Acme EU", data_region="eu-west-1")
    create_tenant(workspace_id=NO_REGION_WS, name="No region")


# The audit's twelve destinations, each outside eu-west-1 (or of no provable region).
OUT_OF_REGION = [
    ("s3 host names another region", _endpoint("s3", host="us-west-2")),
    ("s3 host names us-east-1", _endpoint("s3", host="us-east-1")),
    ("s3 regional endpoint URL as host", _endpoint("s3", host="https://s3.us-west-2.amazonaws.com")),
    ("s3 global endpoint", _endpoint("s3", host="s3.amazonaws.com")),
    ("s3 identified by endpoint_url", _endpoint("s3", endpoint_url="https://s3.us-west-2.amazonaws.com")),
    ("dynamodb host names another region", _endpoint("dynamodb", host="ap-south-1")),
    ("snowflake account in us-east-1", _endpoint("snowflake", host="xy12345.us-east-1.snowflakecomputing.com")),
    ("bigquery multi-region US", _endpoint("bigquery", host="US", database="proj")),
    ("gcs bucket, no region shown", _endpoint("gcs", host="us-central1", database="bucket")),
    ("adls account, no region shown", _endpoint("adls", host="acct.dfs.core.windows.net")),
    ("postgresql on RDS in another region", _endpoint("postgresql", host="db.abc.us-west-2.rds.amazonaws.com")),
    ("redshift cluster in another region", _endpoint("redshift", host="c1.abc.us-east-2.redshift.amazonaws.com")),
    ("mongodb atlas, no region shown", _endpoint("mongodb", connection_string="mongodb+srv://u:p@c0.ab1cd.mongodb.net/db")),
    ("snowflake org-account, no region shown", _endpoint("snowflake", host="acme-prod.snowflakecomputing.com")),
    ("confluent cloud in another region", _endpoint("kafka", host="pkc-1234.us-east-1.aws.confluent.cloud:9092")),
    ("declared region contradicts the hostname", _endpoint("s3", host="s3.us-west-2.amazonaws.com", region="eu-west-1")),
    ("declared region elsewhere", _endpoint("gcs", database="bucket", region="us-central1")),
    ("s3-compatible endpoint, no region shown", _endpoint("s3", endpoint_url="https://minio.internal:9000")),
]

IN_REGION = [
    ("s3 host names the region", _endpoint("s3", host="eu-west-1")),
    ("s3 regional endpoint", _endpoint("s3", endpoint_url="https://s3.eu-west-1.amazonaws.com")),
    ("dynamodb regional endpoint", _endpoint("dynamodb", host="https://dynamodb.eu-west-1.amazonaws.com")),
    ("snowflake account in the region", _endpoint("snowflake", host="xy12345.eu-west-1.snowflakecomputing.com")),
    ("snowflake account, cloud-qualified", _endpoint("snowflake", host="xy12345.eu-west-1.aws")),
    ("postgresql on RDS in the region", _endpoint("postgresql", host="db.abc.eu-west-1.rds.amazonaws.com")),
]
# Security review H1: a region typed on the destination is not evidence (the
# three "declared region" rows that stood here are refused now; a platform
# administrator's verified connector region is what speaks for such a place:
# tests/test_review_h1_residency_region_evidence.py).
DECLARED_ONLY = [
    ("on-prem database with a declared region", _endpoint("postgresql", host="db.internal", region="EU-West-1")),
    ("gcs with a declared region", _endpoint("gcs", database="bucket", region="eu-west-1")),
    ("s3-compatible endpoint with a declared region", _endpoint("s3", endpoint_url="https://minio.internal:9000", region="eu-west-1")),
]
OUT_OF_REGION += DECLARED_ONLY


@pytest.mark.parametrize("label,endpoint", OUT_OF_REGION, ids=[c[0] for c in OUT_OF_REGION])
def test_out_of_region_destination_is_refused(eu_tenant, label, endpoint):
    from services.data_residency import residency_refusal

    refusal = residency_refusal(EU_WS, endpoint)
    assert refusal, f"{label}: allowed for an eu-west-1 workspace"
    assert refusal.startswith("Data residency violation:")
    assert "eu-west-1" in refusal
    # A workspace that configured no region (or has no tenant): nothing is enforced.
    assert residency_refusal(NO_REGION_WS, endpoint) is None
    assert residency_refusal("33333333-3333-4333-8333-333333333333", endpoint) is None


@pytest.mark.parametrize("label,endpoint", IN_REGION, ids=[c[0] for c in IN_REGION])
def test_in_region_destination_is_allowed(eu_tenant, label, endpoint):
    from services.data_residency import residency_refusal

    assert residency_refusal(EU_WS, endpoint) is None, label


def test_undetermined_region_says_what_to_do(eu_tenant):
    from services.data_residency import residency_refusal

    refusal = residency_refusal(EU_WS, _endpoint("adls", host="acct.dfs.core.windows.net"))
    assert "cannot be determined" in refusal and "region" in refusal


def test_file_export_needs_the_deployment_region_to_be_declared(eu_tenant, monkeypatch):
    """A file export lands on this deployment: in-region only when the operator says where that is."""
    from services.data_residency import residency_refusal

    export = _endpoint("csv", kind="file_export")
    assert "cannot be determined" in residency_refusal(EU_WS, export)
    monkeypatch.setenv("DATAWRAP_DEFAULT_REGION", "us-east-1")
    assert "'us-east-1'" in residency_refusal(EU_WS, export)
    monkeypatch.setenv("DATAWRAP_DEFAULT_REGION", "eu-west-1")
    assert residency_refusal(EU_WS, export) is None


def test_us_east_1_is_a_region_like_any_other(eu_tenant):
    from services.data_residency import residency_refusal
    from services.tenant_store import create_tenant

    ws = "44444444-4444-4444-8444-444444444444"
    create_tenant(workspace_id=ws, name="Acme US", data_region="us-east-1")
    assert residency_refusal(ws, _endpoint("s3", host="us-east-1")) is None
    assert residency_refusal(ws, _endpoint("s3", host="eu-west-1"))
    assert residency_refusal(ws, _endpoint("postgresql", host="db.abc.us-east-1.rds.amazonaws.com")) is None


def test_strict_mode_enforces_the_deployment_region_without_a_tenant_region(eu_tenant, monkeypatch):
    from services.data_residency import residency_refusal

    monkeypatch.setenv("DATAWRAP_RESIDENCY_STRICT", "1")
    monkeypatch.setenv("DATAWRAP_DEFAULT_REGION", "eu-central-1")
    ws = NO_REGION_WS
    assert residency_refusal(ws, _endpoint("s3", host="us-east-1"))
    assert residency_refusal(ws, _endpoint("s3", host="eu-central-1")) is None
    # A file export lands on this deployment, whose region the operator declared.
    assert residency_refusal(ws, _endpoint("csv", kind="file_export")) is None


def _run_body(dest: dict, **extra) -> dict:
    return {
        "source": {"kind": "database", "format": "sqlite", "database": "/nonexistent/src.sqlite", "table": "t"},
        "destination": dest,
        "mappings": [{"source": "id", "destination": "id"}],
        "sync_mode": "full_refresh_overwrite",
        **extra,
    }


def test_rest_execute_refuses_out_of_region_and_ignores_the_bodys_data_region(eu_workspace):
    us_bucket = {"kind": "database", "format": "s3", "host": "us-east-1", "database": "bucket", "table": "t"}
    refused = eu_workspace.a.post("/api/v1/transfer/execute", json=_run_body(us_bucket))
    assert refused.status_code == 403, refused.text
    assert refused.json()["detail"].startswith("Data residency violation:")
    assert "'us-east-1'" in refused.json()["detail"] and "'eu-west-1'" in refused.json()["detail"]

    # Naming the destination's own region in the body no longer moves the fence.
    overridden = eu_workspace.a.post(
        "/api/v1/transfer/execute", json=_run_body(us_bucket, data_region="us-east-1")
    )
    assert overridden.status_code == 403, overridden.text
    assert overridden.json()["detail"].startswith("Data residency violation:")

    # The same destination from workspace B (no region configured) is not a
    # residency refusal; whatever else happens to it is not this check.
    other = eu_workspace.b.post("/api/v1/transfer/execute", json=_run_body(us_bucket))
    assert "Data residency violation" not in other.text


def test_rest_form_run_refuses_out_of_region_and_ignores_the_forms_data_region(eu_workspace):
    form = {
        "source_kind": "database", "source_format": "sqlite",
        "source_database": "/nonexistent/src.sqlite", "source_table": "t",
        "dest_kind": "database", "dest_format": "s3", "dest_host": "us-west-2",
        "dest_database": "bucket", "dest_table": "t",
        "mappings_json": '[{"source": "id", "destination": "id"}]',
    }
    for extra in ({}, {"data_region": "us-west-2"}):
        refused = eu_workspace.a.post("/api/v1/transfer/run", data={**form, **extra})
        assert refused.status_code == 403, refused.text
        assert refused.json()["detail"].startswith("Data residency violation:")
        assert "'us-west-2'" in refused.json()["detail"]
    other = eu_workspace.b.post("/api/v1/transfer/run", data=form)
    assert "Data residency violation" not in other.text


def test_job_creation_refuses_for_every_submit_path(eu_workspace):
    """Schedules, sync-now, MCP and the Pilot create jobs through the engine,
    not the REST handler: the engine's own job creation refuses too."""
    from services.data_residency import ResidencyRefused
    from src.transfer.engine import get_transfer_engine
    from src.transfer.models import TransferRequest

    def request(dest, workspace_id, data_region=""):
        return TransferRequest(
            source=_endpoint("sqlite", database="/nonexistent/src.sqlite", table="t"),
            destination=dest,
            mappings=[{"source": "id", "destination": "id"}],
            workspace_id=workspace_id,
            data_region=data_region,
        )

    engine = get_transfer_engine()
    out = request(_endpoint("s3", host="us-west-2", database="bucket", table="t"), eu_workspace.workspace_a, "us-west-2")
    with pytest.raises(ResidencyRefused) as refused:
        engine._create_pending_job(out)
    assert "us-west-2" in str(refused.value) and "eu-west-1" in str(refused.value)

    inside = request(_endpoint("s3", host="eu-west-1", database="bucket", table="t"), eu_workspace.workspace_a, "us-west-2")
    job_id = engine._create_pending_job(inside)
    from services.mongodb_service import get_mongodb_service

    job = get_mongodb_service().get_job(job_id)
    # The job is tagged with the workspace's region, not the one the caller sent.
    assert job["data_region"] == "eu-west-1"
    assert inside.data_region == "eu-west-1"


def test_legacy_router_helper_no_longer_trusts_a_caller_region(eu_workspace):
    """The audit probe's own call: the router helper with the caller's region."""
    from fastapi import HTTPException
    from types import SimpleNamespace

    from src.routers import transfer_router

    request = SimpleNamespace(state=SimpleNamespace(data_region="us-west-2"), headers={})
    with pytest.raises(HTTPException) as refused:
        transfer_router._residency_check(
            request, _endpoint("s3", host="us-west-2"), eu_workspace.workspace_a
        )
    assert refused.value.status_code == 403


def test_schedule_into_another_region_is_held_before_a_job_exists(eu_workspace, tmp_path, monkeypatch):
    """A schedule never passes through the REST handler: its run's preflight
    holds it on the residency reason, and nothing is dispatched."""
    import sqlite3
    from concurrent.futures import Future

    import services.schedule_runner as runner
    import services.schedule_store as schedules
    from services.connector_store import create_connector

    dispatched: list = []

    def fake_run(job_id, request, **_kw):
        dispatched.append(request)
        done: Future = Future()
        done.set_result(None)
        return done

    monkeypatch.setattr("src.transfer.background.run_transfer_async", fake_run)
    ws = eu_workspace.workspace_a
    (tmp_path / "sqlite").mkdir()
    src = tmp_path / "sqlite" / "src.sqlite"
    with sqlite3.connect(str(src)) as conn:
        conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO customers VALUES (1, 'a')")
    source = create_connector(
        {"name": "h4-src", "type": "sqlite", "role": "source", "workspace_id": ws, "database": str(src)}
    )

    def schedule_into(host: str):
        dest = create_connector({
            "name": f"h4-dst-{host}", "type": "s3", "role": "destination", "workspace_id": ws,
            "host": host, "database": "bucket", "username": "k", "password": "s",
        })
        return schedules.create_schedule({
            "name": f"h4-{host}", "source_connector_id": source.id, "source_table": "customers",
            "dest_connector_id": dest.id, "dest_table": "customers_copy", "interval": "daily",
            "sync_mode": "full_refresh_overwrite", "workspace_id": ws,
            "mappings": [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        })

    out = schedule_into("us-west-2")
    assert runner._dispatch_transfer(out.id, allow_paused=True) is None
    assert dispatched == []
    last = (schedules.get_schedule(out.id).run_history or [{}])[-1]
    assert last.get("reason_code") == "preflight_refused", last
    assert "Data residency violation" in str(last.get("reason")), last
    assert "us-west-2" in str(last.get("reason")) and "eu-west-1" in str(last.get("reason"))

    inside = schedule_into("eu-west-1")
    runner._dispatch_transfer(inside.id, allow_paused=True)
    last_inside = (schedules.get_schedule(inside.id).run_history or [{}])[-1]
    assert "Data residency violation" not in str(last_inside.get("reason")), last_inside
