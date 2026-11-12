"""Security review round 3 (H4): the S3 bucket-region guard knows whose write it is.

``install_bucket_region_guard`` worked out the fence only from the ambient job
or egress workspace. Outside a job nothing was bound, so on ``/query/export``
to an S3 connector (``host=eu-west-1``, bucket in ``us-east-1``) no
``GetBucketLocation`` was sent and botocore's redirector re-sent the
PutObject to us-east-1.

Now:

- the saved connector's owner workspace rides in the resolved ``cfg``
  (``workspace_id``), and ``boto3_client`` passes it to the guard explicitly;
  the guard also honours the ambient job/egress workspace, so a value in
  ``cfg`` can add a fence but never remove one;
- when any fence exists on the deployment and the workspace cannot be
  determined, the write is refused (fail closed);
- ``/query/export``, quarantine replay, Pilot tools and connector Test bind
  the egress workspace around their work.
"""

from __future__ import annotations

import sqlite3

import pytest

from tenancy_world import tenancy_world  # noqa: F401
from test_review2_h3_s3_bucket_region import FakeS3


@pytest.fixture
def fenced(tenancy_world, monkeypatch):  # noqa: F811
    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "review3-h4-secrets-key-distinct-from-auth-0001")
    from services.tenant_store import create_tenant

    create_tenant(workspace_id=tenancy_world.workspace_a, name="EU", data_region="eu-west-1")
    return tenancy_world


def _client(fake, cfg, **kwargs):
    from connectors.aws_common import boto3_client

    client = boto3_client("s3", cfg, **kwargs)
    client.meta.events.register_first("before-send.s3", fake)
    return client


def _cfg(**extra):
    return {"host": "eu-west-1", "username": "AKIAEXAMPLE", "password": "x", **extra}


def _put(client, bucket="bucket-in-us-east-1"):
    client.put_object(Bucket=bucket, Key="rows.csv", Body=b"id\n1\n")


def _puts(fake):
    return [url for op, url in fake.sent if op == "PutObject"]


def test_the_connectors_workspace_in_cfg_arms_the_guard_with_nothing_bound(fenced):
    fake = FakeS3(location="us-east-1", put_redirect_to="us-east-1")
    client = _client(fake, _cfg(workspace_id=fenced.workspace_a))
    with pytest.raises(Exception, match="(?i)residency"):
        _put(client)
    assert _puts(fake) == []


def test_an_explicit_workspace_arms_the_guard(fenced):
    fake = FakeS3(location="us-east-1", put_redirect_to="us-east-1")
    client = _client(fake, _cfg(), workspace_id=fenced.workspace_a)
    with pytest.raises(Exception, match="(?i)residency"):
        _put(client)
    assert _puts(fake) == []


def test_an_unknown_workspace_is_refused_while_any_fence_exists(fenced):
    fake = FakeS3(location="eu-west-1")
    client = _client(fake, _cfg())
    with pytest.raises(Exception, match="(?i)residency.*cannot be determined"):
        _put(client, bucket="b")
    assert _puts(fake) == []


def test_an_unknown_workspace_writes_when_no_fence_exists(tenancy_world, monkeypatch):  # noqa: F811
    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    fake = FakeS3(location="us-east-1", put_redirect_to="us-east-1")
    _put(_client(fake, _cfg()))
    assert len(_puts(fake)) == 2  # unchanged: the redirector still follows S3's 301


def test_a_cfg_workspace_cannot_remove_the_bound_fence(fenced):
    """An inline ``extra`` can carry ``workspace_id``; naming an unfenced one changes nothing."""
    from services.egress_guard import egress_workspace

    fake = FakeS3(location="us-east-1", put_redirect_to="us-east-1")
    with egress_workspace(fenced.workspace_a):
        client = _client(fake, _cfg(workspace_id=fenced.workspace_b))
        with pytest.raises(Exception, match="(?i)residency"):
            _put(client)
    assert _puts(fake) == []


def test_an_unfenced_workspace_is_unchanged(fenced):
    fake = FakeS3(location="us-east-1", put_redirect_to="us-east-1")
    _put(_client(fake, _cfg(workspace_id=fenced.workspace_b)))
    ops = [op for op, _ in fake.sent]
    assert "GetBucketLocation" not in ops
    assert ops.count("PutObject") == 2


def test_a_saved_connector_resolves_with_its_owner_workspace(fenced):
    from services import connector_store
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.models import EndpointConfig

    dst = connector_store.create_connector({
        "name": "s3", "type": "s3", "host": "eu-west-1", "database": "bucket-in-us-east-1",
        "username": "AKIAEXAMPLE", "password": "x", "workspace_id": fenced.workspace_a,
    })
    cfg = resolve_connector_config(EndpointConfig(kind="database", format="s3", connector_id=dst.id,
                                                  extra={"workspace_id": fenced.workspace_b}))
    assert cfg["workspace_id"] == fenced.workspace_a


def test_query_export_to_an_s3_bucket_in_another_region_sends_no_put(fenced, monkeypatch, tmp_path_factory):
    """The reviewer's probe: no PutObject reaches either region."""
    import boto3

    from services import connector_store

    fake = FakeS3(location="us-east-1", head_region="us-east-1", put_redirect_to="us-east-1")
    real = boto3.client

    def hooked(service, *a, **k):
        # Hooked at boto3 itself: every module's boto3_client reaches it.
        client = real(service, *a, **k)
        if service == "s3":
            client.meta.events.register_first("before-send.s3", fake)
        return client

    monkeypatch.setattr(boto3, "client", hooked)
    db = tmp_path_factory.mktemp("h4src") / "src.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("create table t(id integer, v text)")
    conn.execute("insert into t values (1, 'EU-PERSONAL-DATA')")
    conn.commit()
    conn.close()
    src = connector_store.create_connector({"name": "src", "type": "sqlite", "database": str(db),
                                            "workspace_id": fenced.workspace_a})
    dst = connector_store.create_connector({"name": "s3", "type": "s3", "host": "eu-west-1",
                                            "database": "bucket-in-us-east-1", "username": "AKIAEXAMPLE",
                                            "password": "x", "workspace_id": fenced.workspace_a})
    r = fenced.a.post("/api/v1/query/export", json={
        "connector_id": src.id, "query": "select * from t", "language": "sql",
        "destination_connector_id": dst.id, "destination": "out/rows.csv", "format": "csv",
    })
    assert r.status_code in (200, 403), r.text
    assert r.status_code == 403 or r.json().get("success") is False
    assert _puts(fake) == []
    assert "GetBucketLocation" in [op for op, _ in fake.sent], (r.text, fake.sent)


def test_query_export_binds_the_egress_workspace(fenced, monkeypatch):
    from services.egress_guard import current_egress_workspace
    from src.routers import query_router

    seen: list[str] = []

    def fake_write(*_a, **_k):
        seen.append(current_egress_workspace())
        return 1, [], {}

    monkeypatch.setattr("src.transfer.adapters.write_destination_database", fake_write)
    from services import connector_store

    dst = connector_store.create_connector({"name": "pg", "type": "postgresql", "host": "x.eu-west-1.rds.amazonaws.com",
                                            "database": "d", "username": "u", "password": "p",
                                            "workspace_id": fenced.workspace_a})

    class _Req:
        class state:  # noqa: N801 - a request.state stand-in
            pass

    monkeypatch.setattr(query_router, "_check_workspace_write", lambda *_a, **_k: None)
    monkeypatch.setattr(query_router, "_check_connector_write", lambda *_a, **_k: None)
    body = query_router.QueryExportRequest(connector_id="x", query="select 1", destination_connector_id=dst.id,
                                           destination="t", format="csv")
    query_router._export_to_connector(body, [{"a": 1}], ["a"], {"a": "integer"}, fenced.workspace_a, _Req())
    assert seen == [fenced.workspace_a]


def test_quarantine_replay_writes_with_the_egress_workspace_bound(fenced, monkeypatch):
    from services.egress_guard import current_egress_workspace
    import src.transfer.adapters as adapters

    seen: list[str] = []

    def fake_write(*_a, **_k):
        seen.append(current_egress_workspace())
        return 1, [], {"rows_written": 1}

    monkeypatch.setattr(adapters, "write_destination_database", fake_write)
    monkeypatch.setattr("services.dest_physical_types.refresh_mapping_target_types", lambda m, *_a, **_k: m)
    monkeypatch.setattr("src.transfer.reconcile_step.run_reconciliation", lambda *_a, **_k: {"passed": True})
    monkeypatch.setattr("services.dest_quarantine.mark_dlq_promoted", lambda *_a, **_k: 0)
    detail = {"row": 1, "column": "c", "value": "x", "reason": "bad", "values": {"id": 1, "c": "v"},
              "record": {"id": 1, "c": "v"}, "row_data": {"id": 1, "c": "v"}}
    job_id = fenced.job(fenced.workspace_a, name="q", status="completed", rows_written=1, rejected_details=[detail],
                        transfer_request={"source": {"kind": "database", "format": "postgresql", "host": "x.eu-west-1.rds.amazonaws.com", "table": "t"},
                                          "destination": {"kind": "database", "format": "postgresql", "host": "x.eu-west-1.rds.amazonaws.com", "database": "d", "table": "t"},
                                          "mappings": [{"source": "id", "target": "id"}, {"source": "c", "target": "c"}]})
    r = fenced.a.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={})
    assert r.status_code == 200, r.text
    assert seen == [fenced.workspace_a]


def test_pilot_tools_run_with_the_callers_egress_workspace(fenced, monkeypatch):
    from services.egress_guard import current_egress_workspace
    from src.ai.copilot import tools as copilot_tools

    seen: list[str] = []
    monkeypatch.setattr(copilot_tools, "current_caller_role", lambda: "admin", raising=False)
    monkeypatch.setattr(copilot_tools, "is_tool_allowed", lambda *_a: True, raising=False)
    monkeypatch.setattr("services.workspace_filter.current_workspace_stamp", lambda **_k: fenced.workspace_a)
    executor = copilot_tools.DataPilotTools.__new__(copilot_tools.DataPilotTools)

    def record(**_k):
        seen.append(current_egress_workspace())
        return copilot_tools.ToolResult(name="list_connectors", success=True, output=None)

    monkeypatch.setattr(executor, "_list_connectors", record, raising=False)
    executor.execute("list_connectors", {})
    assert seen == [fenced.workspace_a]


def test_connector_test_runs_with_the_callers_egress_workspace(fenced, monkeypatch):
    import importlib

    from services.egress_guard import current_egress_workspace

    connectors_router = importlib.import_module("src.routers.connectors_router")
    seen: list[str] = []

    def record(_request, _op):
        seen.append(current_egress_workspace())
        return {"success": True, "message": "ok"}

    monkeypatch.setattr(connectors_router, "_test_connection_blocking", record)
    r = fenced.a.post("/api/v1/connectors/test", json={"type": "postgresql", "host": "x.eu-west-1.rds.amazonaws.com",
                                                        "port": 5432, "database": "d", "username": "u", "password": "p"})
    assert r.status_code == 200, r.text
    assert seen == [fenced.workspace_a]
