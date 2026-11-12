"""Security review H2 + H3: residency is enforced where data is written.

H2: ``POST /query/export`` wrote a fenced workspace's rows to an out-of-region
connector, and to a local file with no deployment region declared; neither
branch reached the residency check.

H3: the check ran only where a job is *created*. A resumed job (and any job a
worker claims later) reuses its record, so a connector moved to another region
after the job was created was never judged again. And a tenant record that
belongs to no workspace fenced the REST routes but not the engine or a
schedule, because only the routes knew which tenant the request belonged to.

Now the engine judges the resolved request at the top of every tracked run,
the query export judges its destination before the query is read, and a tenant
record with no workspace speaks for every workspace that has none of its own.
"""

from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401


@pytest.fixture
def eu_workspace(tenancy_world, monkeypatch, tmp_path):  # noqa: F811
    """Workspace A is an ``eu-west-1`` tenant with a small SQLite source connector."""
    from services.connector_store import create_connector
    from services.tenant_store import create_tenant

    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    world = tenancy_world
    create_tenant(workspace_id=world.workspace_a, name="Acme EU", data_region="eu-west-1")
    from services.embedded_volume import volume_workspace_dir

    # The workspace's own area of the sqlite volume (review 3, H3).
    folder = volume_workspace_dir(tmp_path.resolve() / "sqlite", world.workspace_a)
    folder.mkdir(parents=True)
    db = folder / "src.sqlite"
    with sqlite3.connect(str(db)) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'EU-PERSONAL-DATA')")
    world.source = create_connector(
        {"name": "eu-src", "type": "sqlite", "role": "source", "database": str(db), "workspace_id": world.workspace_a}
    )
    world.source_db = db
    return world


def _s3_connector(world, host: str, name: str = "bucket"):
    from services.connector_store import create_connector

    return create_connector({
        "name": name, "type": "s3", "role": "destination", "host": host, "database": "acme-bucket",
        "username": "k", "password": "s", "workspace_id": world.workspace_a,
    })


@pytest.fixture
def writes(monkeypatch):
    """Every attempt to reach a destination or to read the query is recorded, none is made."""
    seen: list[str] = []
    import src.transfer.adapters as adapters
    from src.routers import query_router

    def write(*_a, **_k):
        seen.append("write_destination_database")
        return 1, [], {}

    def boto(service, _cfg):
        seen.append(f"boto3:{service}")
        raise AssertionError("the destination was dialled")

    real_run = query_router._run_query

    def run_query(*a, **k):
        seen.append("query")
        return real_run(*a, **k)

    monkeypatch.setattr(adapters, "write_destination_database", write)
    monkeypatch.setattr("connectors.aws_common.boto3_client", boto)
    monkeypatch.setattr(query_router, "_run_query", run_query)
    return seen


# --------------------------------------------------------------------------- H2: query export


def _export(world, **body):
    return world.a.post("/api/v1/query/export", json={
        "connector_id": world.source.id, "query": "select * from t", "language": "sql", "format": "csv", **body,
    })


def test_query_export_to_an_out_of_region_connector_is_refused_before_anything_is_read(eu_workspace, writes):
    world = eu_workspace
    us_bucket = _s3_connector(world, "us-east-1")
    refused = _export(world, destination_connector_id=us_bucket.id, destination="leak/out.csv")
    assert refused.status_code == 403, refused.text
    assert refused.json()["detail"].startswith("Data residency violation:")
    assert "'us-east-1'" in refused.json()["detail"] and "'eu-west-1'" in refused.json()["detail"]
    assert writes == [], writes
    assert "EU-PERSONAL-DATA" not in refused.text

    # The same export to a connector in the workspace's region is not a residency refusal.
    eu_bucket = _s3_connector(world, "eu-west-1", name="eu-bucket")
    allowed = _export(world, destination_connector_id=eu_bucket.id, destination="ok/out.csv")
    assert allowed.status_code == 200, allowed.text
    assert writes == ["query", "write_destination_database"], writes


@pytest.mark.parametrize("extra", [{}, {"full_population": True}], ids=["preview", "full population"])
def test_query_export_to_a_local_file_needs_the_deployment_region(eu_workspace, writes, monkeypatch, extra):
    world = eu_workspace
    refused = _export(world, **extra)
    assert refused.status_code == 403, refused.text
    assert "cannot be determined" in refused.json()["detail"]
    assert writes == [], writes
    assert not list((world.data_dir / "exports").rglob("*.csv"))

    monkeypatch.setenv("DATAWRAP_DEFAULT_REGION", "us-east-1")
    elsewhere = _export(world, **extra)
    assert elsewhere.status_code == 403 and "'us-east-1'" in elsewhere.json()["detail"], elsewhere.text

    monkeypatch.setenv("DATAWRAP_DEFAULT_REGION", "eu-west-1")
    allowed = _export(world, **extra)
    assert allowed.status_code == 200 and allowed.json()["success"] is True, allowed.text
    assert allowed.json()["row_count"] == 1


def test_query_export_is_unaffected_for_a_workspace_with_no_region(eu_workspace, writes):
    from services.connector_store import create_connector

    import shutil

    from services.embedded_volume import volume_workspace_dir

    world = eu_workspace
    # B's own copy in B's area: B may not open A's file (review 3, H3).
    b_db = volume_workspace_dir(world.source_db.parent.parent.parent, world.workspace_b) / "src.sqlite"
    b_db.parent.mkdir(parents=True)
    shutil.copy(world.source_db, b_db)
    source_b = create_connector({
        "name": "b-src", "type": "sqlite", "role": "source", "database": str(b_db),
        "workspace_id": world.workspace_b,
    })
    ok = world.b.post("/api/v1/query/export", json={
        "connector_id": source_b.id, "query": "select * from t", "language": "sql", "format": "csv",
    })
    assert ok.status_code == 200 and ok.json()["success"] is True, ok.text


# --------------------------------------------------------------------------- H3: the engine re-judges


def _job_into(world, destination_connector) -> tuple[str, dict]:
    """A real pending job A created while its destination was in region."""
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import get_transfer_engine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", connector_id=world.source.id, table="t"),
        destination=EndpointConfig(
            kind="database", format="s3", connector_id=destination_connector.id, table="out/t.csv"
        ),
        mappings=[{"source": "id", "destination": "id"}, {"source": "v", "destination": "v"}],
        workspace_id=world.workspace_a,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    job_id = get_transfer_engine()._create_pending_job(request)
    return job_id, get_mongodb_service().get_job(job_id)


def _move(world, connector, host: str) -> None:
    moved = world.member("admin-a@tenancy.example.com", world.workspace_a, "admin").patch(
        f"/api/v1/connectors/saved/{connector.id}", json={"host": host}
    )
    assert moved.status_code == 200, moved.text


def test_engine_refuses_a_run_whose_connector_moved_out_of_region_after_the_job_was_created(eu_workspace, writes):
    """What resume, a worker claim and the orphan sweep all do: run the stored request."""
    from services.mongodb_service import get_mongodb_service
    from src.transfer.adapters import refresh_saved_connectors
    from src.transfer.engine import get_transfer_engine
    from src.transfer.models import transfer_request_from_dict

    world = eu_workspace
    bucket = _s3_connector(world, "eu-west-1")
    job_id, job = _job_into(world, bucket)  # accepted: eu-west-1
    _move(world, bucket, "us-east-1")

    request = transfer_request_from_dict(job["transfer_request"])
    refresh_saved_connectors(request)  # the resume route's replay against current settings
    assert request.destination.host == "us-east-1"
    for resume in (True, False):
        result = get_transfer_engine().execute_tracked(request, job_id, resume=resume)
        assert result.success is False
        assert str(result.error).startswith("Data residency violation:"), result.error
        assert "'us-east-1'" in result.error and "'eu-west-1'" in result.error
    assert not [w for w in writes if w.startswith("boto3")], writes
    stored = get_mongodb_service().get_job(job_id)
    assert stored["status"] == "failed"
    assert str(stored.get("error")).startswith("Data residency violation:")


def test_resume_route_refuses_and_a_resumed_run_never_writes(eu_workspace, writes, monkeypatch):
    from services import checkpoint_service
    from services.mongodb_service import get_mongodb_service

    world = eu_workspace
    bucket = _s3_connector(world, "eu-west-1")
    job_id, _job = _job_into(world, bucket)
    get_mongodb_service().update_job_status(job_id, "failed", error="interrupted")
    _move(world, bucket, "us-east-1")
    monkeypatch.setattr(checkpoint_service, "evaluate_resume_safety", lambda *_a, **_k: {"ok": True})

    refused = world.a.post(f"/api/v1/connectors/jobs/{job_id}/resume")
    assert refused.status_code == 403, refused.text
    assert refused.json()["detail"].startswith("Data residency violation:")
    assert get_mongodb_service().get_job(job_id)["status"] == "failed"

    # With the route's own early answer taken away, the run the route starts
    # is refused by the engine: nothing is written.
    import importlib

    connectors_router = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(connectors_router, "enforce_request_residency", lambda _request: None)
    started = world.a.post(f"/api/v1/connectors/jobs/{job_id}/resume")
    assert started.status_code == 200, started.text
    deadline = time.time() + 30
    while time.time() < deadline:
        stored = get_mongodb_service().get_job(job_id)
        if stored["status"] in ("failed", "completed", "completed_with_quarantine"):
            break
        time.sleep(0.1)
    assert stored["status"] == "failed", stored
    assert str(stored.get("error")).startswith("Data residency violation:"), stored.get("error")
    assert not [w for w in writes if w.startswith("boto3")], writes


def test_engine_run_in_region_is_not_refused_for_residency(eu_workspace, monkeypatch):
    """The control: the same run with the connector left where it was is not a residency refusal."""
    from src.transfer.engine import get_transfer_engine
    from src.transfer.models import transfer_request_from_dict

    world = eu_workspace
    dialled: list[str] = []

    def boto(service, cfg):
        dialled.append(str(cfg.get("host")))
        raise RuntimeError("no network in this test")

    # s3_writer first: importing it while aws_common is already patched bound
    # the stand-in into it, and undo then "restored" the stand-in for every
    # later test in the session.
    monkeypatch.setattr("connectors.s3_writer.boto3_client", boto, raising=False)
    monkeypatch.setattr("connectors.aws_common.boto3_client", boto)
    bucket = _s3_connector(world, "eu-west-1")
    job_id, job = _job_into(world, bucket)
    result = get_transfer_engine().execute_tracked(transfer_request_from_dict(job["transfer_request"]), job_id)
    assert "Data residency violation" not in str(result.error or "")


# --------------------------------------------------------------------------- H3: a tenant with no workspace


def test_tenant_record_with_no_workspace_fences_the_engine_and_the_schedule_check(tenancy_world, monkeypatch):  # noqa: F811
    from services import data_residency as residency
    from services.tenant_store import create_tenant
    from src.transfer.models import EndpointConfig, TransferRequest

    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    world = tenancy_world
    us_bucket = EndpointConfig.from_dict("database", {"format": "s3", "host": "us-east-1", "database": "b"})
    eu_bucket = EndpointConfig.from_dict("database", {"format": "s3", "host": "eu-west-1", "database": "b"})
    assert residency.residency_refusal(world.workspace_a, us_bucket) is None  # nothing configured yet

    create_tenant(workspace_id="", name="Single-tenant deployment", data_region="eu-west-1")
    for workspace_id in (world.workspace_a, ""):
        # The schedule runner's call: no request, so no host tenant.
        assert residency.residency_refusal(workspace_id, us_bucket), workspace_id
        assert residency.residency_refusal(workspace_id, eu_bucket) is None
        request = TransferRequest(
            source=EndpointConfig.from_dict("database", {"format": "sqlite"}),
            destination=us_bucket, workspace_id=workspace_id,
        )
        with pytest.raises(residency.ResidencyRefused):
            residency.enforce_request_residency(request)

    # A workspace with its own tenant record keeps its own region.
    create_tenant(workspace_id=world.workspace_b, name="Acme US", data_region="us-east-1")
    assert residency.residency_refusal(world.workspace_b, us_bucket) is None
    assert residency.residency_refusal(world.workspace_b, eu_bucket)

    # Two such records naming different regions cannot both be the fence:
    # nothing is provably inside it.
    create_tenant(workspace_id="", name="Second unscoped record", data_region="ap-south-1")
    for endpoint in (us_bucket, eu_bucket):
        refusal = residency.residency_refusal(world.workspace_a, endpoint)
        assert refusal and "more than one region" in refusal, refusal
