"""Security review round 2 (H4): a region verification is bound to every addressing field.

The stamp a platform administrator sets on a connector whose region no
hostname proves was bound to ``type`` plus its hosts. It survived edits and
per-request overrides that move the data: an Oracle ``dsn=`` or TNS
descriptor, the ADLS storage account (``username`` / ``extra.account_name``),
the bucket behind an S3-compatible endpoint, the BigQuery dataset or project,
a port on a shared TCP proxy, and the API key of an account-scoped SaaS.

Now the stamp is a keyed hash of every field the driver addresses with (per
driver family, credential identity for account-scoped APIs); any change to
them clears it, an address field that cannot be read counts as moved, a
per-request override of an address field on a verified connector is refused,
and the verification audit event records the verified address hash and the
resolved address.
"""

from __future__ import annotations

import pytest
from tenancy_world import ADMIN_EMAIL, tenancy_world  # noqa: F401

TNS_US = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=52.1.2.3)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=x)))"


@pytest.fixture
def fenced(tenancy_world, monkeypatch):
    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "h4-test-secrets-key-distinct-from-auth-0001")
    from services.tenant_store import create_tenant

    create_tenant(workspace_id=tenancy_world.workspace_a, name="EU", data_region="eu-west-1")
    return tenancy_world


def _saved(world, **fields):
    from services import connector_store

    return connector_store.create_connector({"workspace_id": world.workspace_a, "role": "destination", **fields})


def _verify(world, cid):
    from services import connector_store

    return connector_store.set_region_verification(cid, "eu-west-1", verified_by=ADMIN_EMAIL, workspace_id=world.workspace_a)


def _allowed(world, fmt, **fields):
    from services import data_residency as dr
    from src.transfer.models import EndpointConfig

    ep = EndpointConfig.from_dict("database", {"format": fmt, "table": "t", **fields})
    return dr.residency_refusal(world.workspace_a, ep) is None


def _edit(world, cid, patch):
    from services import connector_store

    connector_store.update_connector(cid, patch, workspace_id=world.workspace_a)
    return bool(connector_store.get_connector(cid, workspace_id=world.workspace_a).region_verification)


CASES = [
    # (label, connector fields, format, per-request overrides, edits that move it)
    ("oracle", {"type": "oracle", "host": "10.20.30.40", "port": 1521, "database": "ORCLPDB1", "username": "u", "password": "p"},
     "oracle",
     [{"connection_string": "oracle+oracledb://u:p@/?dsn=" + TNS_US.replace("(", "%28").replace(")", "%29").replace("=", "%3D")},
      {"port": 1522}, {"database": "OTHERPDB"}],
     [{"connection_string": TNS_US}, {"port": 1522}, {"database": "OTHERPDB"}]),
    ("adls", {"type": "adls", "host": "", "database": "container", "username": "euaccount", "password": "k"},
     "adls",
     [{"username": "usaccount"}, {"extra": {"account_name": "usaccount"}}],
     [{"username": "usaccount"}, {"extra": {"account_name": "usaccount"}}]),
    ("s3-compatible", {"type": "s3", "host": "", "endpoint_url": "https://storage.googleapis.com", "database": "eu-bucket", "username": "k", "password": "s"},
     "s3",
     [{"database": "us-bucket", "destination_override_acknowledged": True}],
     [{"database": "us-bucket"}, {"username": "other-account-key"}]),
    ("bigquery", {"type": "bigquery", "database": "proj", "schema": "eu_dataset", "service_account": "{\"client_email\": \"a@proj.iam\"}"},
     "bigquery",
     [{"schema": "us_dataset"}, {"extra": {"project_id": "other-proj"}}],
     [{"schema": "us_dataset"}, {"extra": {"project_id": "other-proj"}}, {"service_account": "{\"client_email\": \"b@other.iam\"}"}]),
    ("shared-proxy", {"type": "postgresql", "host": "roundhouse.proxy.rlwy.net", "port": 41234, "database": "d", "username": "u", "password": "p"},
     "postgresql",
     [{"port": 61000}],
     [{"port": 50999}]),
    ("hubspot", {"type": "hubspot", "host": "", "api_key": "eu-portal-key"},
     "hubspot",
     [{"api_key": "us-portal-key"}],
     [{"api_key": "us-portal-key"}]),
]


@pytest.mark.parametrize("label,fields,fmt,overrides,edits", CASES, ids=[c[0] for c in CASES])
def test_verified_connector_is_allowed_until_an_address_field_moves(fenced, label, fields, fmt, overrides, edits):
    conn = _saved(fenced, name=label, **fields)
    assert _verify(fenced, conn.id).region_verification
    assert _allowed(fenced, fmt, connector_id=conn.id), "control: the verified connector as saved"
    for override in overrides:
        assert not _allowed(fenced, fmt, connector_id=conn.id, **override), f"per-request override kept: {override}"
    for patch in edits:
        _verify(fenced, conn.id)
        assert not _edit(fenced, conn.id, patch), f"edit kept the stamp: {patch}"
        assert not _allowed(fenced, fmt, connector_id=conn.id)


def test_edits_that_do_not_address_keep_the_stamp(fenced):
    conn = _saved(fenced, name="pg", type="postgresql", host="10.1.1.1", port=5432, database="d", username="u", password="p")
    _verify(fenced, conn.id)
    assert _edit(fenced, conn.id, {"name": "renamed", "password": "rotated"})
    assert _edit(fenced, conn.id, {"username": "other_login"})
    assert _allowed(fenced, "postgresql", connector_id=conn.id)
    assert not _edit(fenced, conn.id, {"host": "10.9.9.9"})


def test_an_unreadable_address_cannot_be_verified_or_honoured(fenced):
    from services import connector_store

    conn = _saved(fenced, name="ora-alias", type="oracle", host="", connection_string="oracle+oracledb://u:p@/?dsn=PRODALIAS")
    with pytest.raises(ValueError, match="cannot be read"):
        _verify(fenced, conn.id)
    assert not connector_store.get_connector(conn.id, workspace_id=fenced.workspace_a).region_verification
    assert not _allowed(fenced, "oracle", connector_id=conn.id)


def test_the_stamp_never_stores_a_credential_in_the_clear(fenced):
    conn = _saved(fenced, name="hub", type="hubspot", host="", api_key="eu-portal-key-123456")
    stamp = _verify(fenced, conn.id).region_verification
    assert "eu-portal-key-123456" not in repr(stamp)
    import hashlib

    assert hashlib.sha256(b"eu-portal-key-123456").hexdigest()[:32] not in repr(stamp)


def test_http_override_on_a_verified_connector_is_refused_and_audit_records_the_address(fenced, monkeypatch):
    w = fenced
    import src.transfer.background as bg

    queued = []
    monkeypatch.setattr(bg, "run_transfer_async", lambda *a, **k: queued.append(a))
    conn = _saved(w, name="gcs-interop", type="s3", host="", endpoint_url="https://storage.googleapis.com",
                  database="eu-bucket", username="AKIA-H4-ACCESS-KEY", password="h4-secret-access-value")
    r = w.admin.post(f"/api/v1/connectors/saved/{conn.id}/region-verification", json={"region": "eu-west-1"},
                     headers={"X-Workspace-Id": w.workspace_a})
    assert r.status_code == 200, r.text
    from services import audit_log, connector_store

    stamp = connector_store.get_connector(conn.id, workspace_id=w.workspace_a).region_verification
    events = [e for e in audit_log.list_audit_events(limit=50) if e.get("action") == "connector.region_verified"]
    assert events, "verification is audited"
    details = events[0].get("details") or {}
    assert details.get("verified_address") == stamp["address"]
    assert "storage.googleapis.com" in str(details.get("resolved_address"))
    assert "eu-bucket" in str(details.get("resolved_address"))
    assert "h4-secret-access-value" not in str(details) and "AKIA-H4-ACCESS-KEY" not in str(details)
    src = {"kind": "database", "format": "sqlite", "database": ":memory:", "table": "t"}

    def run(dst):
        return w.a.post("/api/v1/transfer/execute", json={
            "source": src, "destination": dst, "mappings": [{"source": "id", "target": "id"}], "async_mode": True})

    ok = run({"kind": "database", "format": "s3", "connector_id": conn.id, "table": "k"})
    assert ok.status_code == 200, ok.text
    moved = run({"kind": "database", "format": "s3", "connector_id": conn.id, "table": "k",
                 "database": "us-bucket", "destination_override_acknowledged": True})
    assert moved.status_code == 403, moved.text
