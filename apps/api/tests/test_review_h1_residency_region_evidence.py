"""Security review H1: the region residency judges is the one the driver uses.

The first residency check (audit H-4) accepted evidence the destination's own
editor controls, or a region the driver never uses:

* S3 / DynamoDB with an empty ``host`` and ``region: eu-west-1``: judged
  ``eu-west-1``; boto3 signs for ``us-east-1`` (``aws_common.resolve_region``
  reads ``host`` only).
* a ``region`` typed next to an IP address, a custom domain, a Snowflake
  locator, a local file or ``localhost``: judged as declared.
* any hostname label shaped like a region: ``db.eu-west-1.attacker.example``,
  or the first label of ``eu-west-1.x.us-east-1.rds.amazonaws.com``.
* a decoy ``host`` in the region next to a connection string that is dialled.

Now: S3 / DynamoDB are judged on exactly what the driver resolves; a hostname
proves a region only under a known provider suffix, by the label next to that
suffix; every address field present must prove the same region; a declared
region counts for nothing unless a platform administrator verified the saved
connector's region (and the connector has not moved since); and local or
loopback destinations are in-region only through ``DATAWRAP_DEFAULT_REGION``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401

EU_WS = "11111111-1111-4111-8111-111111111111"


def _endpoint(fmt: str, kind: str = "database", **fields):
    from src.transfer.models import EndpointConfig

    return EndpointConfig.from_dict(kind, {"format": fmt, **fields})


@pytest.fixture
def eu_tenant(tmp_path, monkeypatch):
    """Stores only (no HTTP): workspace ``EU_WS`` is an ``eu-west-1`` tenant."""
    from services import connector_store, tenant_store

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    tenant_store.create_tenant(workspace_id=EU_WS, name="Acme EU", data_region="eu-west-1")


# Every spoof in the reviewer's probe (sec_review/p_res.py), plus the ones the
# same reasoning finds. None of these is provably in eu-west-1.
SPOOFS = [
    ("s3: empty host + declared region (boto3 signs us-east-1)",
     _endpoint("s3", host="", region="eu-west-1", database="us-bucket")),
    ("dynamodb: empty host + extra.region", _endpoint("dynamodb", host="", extra={"region": "eu-west-1"})),
    ("s3: custom endpoint_url + in-region host and declared region",
     _endpoint("s3", host="eu-west-1", endpoint_url="https://minio.us.example.com", region="eu-west-1")),
    ("s3: endpoint_url with a region-shaped label", _endpoint("s3", endpoint_url="https://eu-west-1.attacker.example")),
    ("s3: regional endpoint_url in another region than host",
     _endpoint("s3", host="eu-west-1", endpoint_url="https://s3.us-west-2.amazonaws.com")),
    ("s3: global endpoint + declared region", _endpoint("s3", host="s3.amazonaws.com", region="eu-west-1")),
    ("s3: host with a port is a custom endpoint", _endpoint("s3", host="eu-west-1:9000")),
    ("s3: host and service port is a custom endpoint", _endpoint("s3", host="eu-west-1", port=9000)),
    ("s3: extra.host overrides the host the driver reads",
     _endpoint("s3", host="eu-west-1", extra={"host": "us-east-1"})),
    ("s3: not a region name", _endpoint("s3", host="eu-west-1/")),
    ("postgresql: region-shaped label on an unknown domain", _endpoint("postgresql", host="db.eu-west-1.attacker.example")),
    ("postgresql: IP literal + declared region", _endpoint("postgresql", host="52.1.2.3", region="eu-west-1")),
    ("postgresql: IP literal + extra.location", _endpoint("postgresql", host="52.1.2.3", extra={"location": "eu-west-1"})),
    ("postgresql: custom domain + declared region", _endpoint("postgresql", host="db.corp.example", region="eu-west-1")),
    ("postgresql: decoy in-region host, connection string dials an IP",
     _endpoint("postgresql", host="x.eu-west-1.rds.amazonaws.com", connection_string="postgresql://u:p@52.1.2.3:5432/db")),
    ("postgresql: connection string host= parameter",
     _endpoint("postgresql", connection_string="postgresql://u:p@52.1.2.3:5432/db?host=eu-west-1")),
    ("postgresql: in-region netloc, libpq host= parameter dials elsewhere",
     _endpoint("postgresql", connection_string="postgresql://u:p@x.eu-west-1.rds.amazonaws.com/db?host=52.1.2.3")),
    ("postgresql: host and connection string in two regions",
     _endpoint("postgresql", host="x.eu-west-1.rds.amazonaws.com", connection_string="postgresql://u:p@y.us-east-1.rds.amazonaws.com/db")),
    ("postgresql: US RDS + declared EU", _endpoint("postgresql", host="x.abc.us-east-1.rds.amazonaws.com", region="eu-west-1")),
    ("postgresql: region as first label of a US RDS name", _endpoint("postgresql", host="eu-west-1.x.us-east-1.rds.amazonaws.com")),
    ("postgresql: provider suffix followed by another domain",
     _endpoint("postgresql", host="x.eu-west-1.rds.amazonaws.com.attacker.example")),
    ("postgresql: userinfo-style host", _endpoint("postgresql", host="x.eu-west-1.rds.amazonaws.com@attacker.example")),
    ("postgresql: ideographic full stop", _endpoint("postgresql", host="eu-west-1。attacker.example")),
    ("postgresql: seed list with one host elsewhere",
     _endpoint("postgresql", connection_string="postgresql://u:p@a.eu-west-1.rds.amazonaws.com,52.1.2.3/db")),
    ("postgresql: key=value connection string is not proof next to an in-region host",
     _endpoint("postgresql", host="x.eu-west-1.rds.amazonaws.com", connection_string="dbname=db user=u")),
    ("s3 bucket name shaped like a region on the global endpoint", _endpoint("s3", host="eu-west-1.s3.amazonaws.com")),
    ("snowflake: bare locator + declared region", _endpoint("snowflake", host="xy12345", region="eu-west-1")),
    ("snowflake: org-account + declared region", _endpoint("snowflake", host="myorg-myacct.snowflakecomputing.com", region="eu-west-1")),
    ("snowflake: region-shaped account name", _endpoint("snowflake", host="eu-west-1.snowflakecomputing.com")),
    ("bigquery: declared location", _endpoint("bigquery", database="proj", extra={"location": "eu-west-1"})),
    ("gcs: declared region", _endpoint("gcs", database="bucket", region="eu-west-1")),
    ("mongodb: Atlas SRV + declared region",
     _endpoint("mongodb", connection_string="mongodb+srv://u:p@cluster0.ab1cd.mongodb.net/db", region="eu-west-1")),
    ("kafka: US Confluent + declared EU", _endpoint("kafka", host="pkc-1.us-east-1.aws.confluent.cloud:9092", region="eu-west-1")),
    ("kafka: cloud label on an unknown domain", _endpoint("kafka", host="pkc-1.eu-west-1.aws.attacker.example:9092")),
    ("adls: account + declared region", _endpoint("adls", username="acct", host="acct", region="eu-west-1")),
    ("file export + declared region", _endpoint("csv", kind="file_export", region="eu-west-1")),
    ("sqlite file + declared region", _endpoint("sqlite", database="/tmp/x.db", extra={"region": "eu-west-1"})),
    ("localhost + declared region", _endpoint("postgresql", host="localhost", region="eu-west-1")),
    ("loopback address + declared region", _endpoint("postgresql", host="127.0.0.1", region="eu-west-1")),
]

PROVEN_IN_REGION = [
    ("s3: host is the region the driver uses", _endpoint("s3", host="eu-west-1")),
    ("s3: upper case and padded host", _endpoint("s3", host=" EU-WEST-1 ")),
    ("s3: regional endpoint URL", _endpoint("s3", endpoint_url="https://s3.eu-west-1.amazonaws.com")),
    ("s3: virtual-hosted bucket whose name looks like another region",
     _endpoint("s3", host="https://us-east-1.s3.eu-west-1.amazonaws.com")),
    ("dynamodb: regional endpoint as host", _endpoint("dynamodb", host="https://dynamodb.eu-west-1.amazonaws.com")),
    ("postgresql on RDS", _endpoint("postgresql", host="db.abc.eu-west-1.rds.amazonaws.com")),
    ("postgresql on RDS, host and connection string agree",
     _endpoint("postgresql", host="db.abc.eu-west-1.rds.amazonaws.com",
               connection_string="postgresql://u:p@db.abc.eu-west-1.rds.amazonaws.com:5432/db")),
    ("redshift", _endpoint("redshift", host="c1.abc.eu-west-1.redshift.amazonaws.com")),
    ("snowflake legacy regional account", _endpoint("snowflake", host="xy12345.eu-west-1.snowflakecomputing.com")),
    ("snowflake cloud-qualified account", _endpoint("snowflake", host="xy12345.eu-west-1.aws")),
    ("snowflake privatelink", _endpoint("snowflake", host="xy12345.eu-west-1.privatelink.snowflakecomputing.com")),
    ("confluent cloud", _endpoint("kafka", host="pkc-1.eu-west-1.aws.confluent.cloud:9092")),
    ("clickhouse cloud", _endpoint("clickhouse", host="abc.eu-west-1.aws.clickhouse.cloud")),
]


@pytest.mark.parametrize("label,endpoint", SPOOFS, ids=[c[0] for c in SPOOFS])
def test_region_spoof_is_refused(eu_tenant, label, endpoint):
    from services.data_residency import destination_region, residency_refusal

    refusal = residency_refusal(EU_WS, endpoint)
    where = destination_region(endpoint, workspace_id=EU_WS)
    assert refusal, f"{label}: allowed for an eu-west-1 workspace (judged {where.region!r} from {where.evidence})"
    assert refusal.startswith("Data residency violation:")
    assert where.region != "eu-west-1", f"{label}: judged in-region from {where.evidence}"


@pytest.mark.parametrize("label,endpoint", PROVEN_IN_REGION, ids=[c[0] for c in PROVEN_IN_REGION])
def test_address_that_proves_the_region_is_allowed(eu_tenant, label, endpoint):
    from services.data_residency import residency_refusal

    assert residency_refusal(EU_WS, endpoint) is None, label


def test_s3_and_dynamodb_are_judged_on_the_region_the_driver_resolves(eu_tenant):
    """Whatever ``aws_common.resolve_region`` hands boto3 is what is judged."""
    from connectors.aws_common import resolve_region
    from services.data_residency import destination_region

    for fmt in ("s3", "dynamodb"):
        for host in ("us-east-1", "eu-west-1", "ap-south-1", " us-west-2 ", "https://s3.eu-central-1.amazonaws.com"):
            endpoint = _endpoint(fmt, host=host, region="eu-west-1", extra={"region": "eu-west-1"})
            judged = destination_region(endpoint, workspace_id=EU_WS)
            assert judged.region == resolve_region({"host": host}).lower(), (fmt, host, judged)
        # No host at all: nothing names a region, whatever the destination declares.
        assert destination_region(_endpoint(fmt, region="eu-west-1"), workspace_id=EU_WS).region == ""


def test_region_label_is_read_next_to_the_provider_suffix_only():
    from services.data_residency import provider_region

    assert provider_region("x.abc.us-east-1.rds.amazonaws.com") == "us-east-1"
    assert provider_region("eu-west-1.x.us-east-1.rds.amazonaws.com") == "us-east-1"
    assert provider_region("s3.eu-west-1.amazonaws.com") == "eu-west-1"
    assert provider_region("us-east-1.s3.eu-west-1.amazonaws.com") == "eu-west-1"
    assert provider_region("s3.cn-north-1.amazonaws.com.cn") == "cn-north-1"
    assert provider_region("acct.eu-west-1.snowflakecomputing.com") == "eu-west-1"
    assert provider_region("acct.west-europe.azure.snowflakecomputing.com") == "west-europe"
    assert provider_region("pkc-1.us-east-1.aws.confluent.cloud") == "us-east-1"
    for unproven in (
        "s3.amazonaws.com", "bucket.s3.amazonaws.com", "eu-west-1.s3.amazonaws.com",
        "eu-west-1.attacker.example", "db.eu-west-1.attacker.example", "eu-west-1.aws.attacker.example",
        "x.eu-west-1.rds.amazonaws.com.attacker.example", "eu-west-1.snowflakecomputing.com",
        "myorg-myacct.snowflakecomputing.com", "cluster0.ab1cd.mongodb.net", "52.1.2.3", "eu-west-1", "",
    ):
        assert provider_region(unproven) == "", unproven


def test_local_destinations_take_the_deployment_region_only(eu_tenant, monkeypatch):
    from services.data_residency import residency_refusal

    local = [
        _endpoint("csv", kind="file_export", region="eu-west-1"),
        _endpoint("sqlite", database="/tmp/x.db", extra={"region": "eu-west-1"}),
        _endpoint("postgresql", host="localhost", region="eu-west-1"),
    ]
    for endpoint in local:
        assert "cannot be determined" in residency_refusal(EU_WS, endpoint)
    monkeypatch.setenv("DATAWRAP_DEFAULT_REGION", "us-east-1")
    for endpoint in local:
        assert "'us-east-1'" in residency_refusal(EU_WS, endpoint)
    monkeypatch.setenv("DATAWRAP_DEFAULT_REGION", "eu-west-1")
    for endpoint in local:
        assert residency_refusal(EU_WS, endpoint) is None
    # A cloud API destination is not "this deployment" because resolution
    # filled its unused host with a placeholder.
    assert residency_refusal(EU_WS, _endpoint("bigquery", database="proj", host="localhost"))
    assert residency_refusal(EU_WS, _endpoint("gcs", database="bucket"))
    assert residency_refusal(EU_WS, _endpoint("snowflake", host="localhost"))


# --------------------------------------------------------------------------- verified connector region


def _gcs_connector(world, **fields):
    from services.connector_store import create_connector

    return create_connector({
        "name": "eu-bucket", "type": "gcs", "role": "destination", "database": "acme-eu-bucket",
        "workspace_id": world.workspace_a, **fields,
    })


@pytest.fixture
def eu_workspace(tenancy_world, monkeypatch):  # noqa: F811
    from services.tenant_store import create_tenant

    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    create_tenant(workspace_id=tenancy_world.workspace_a, name="Acme EU", data_region="eu-west-1")
    tenancy_world.admin_a = tenancy_world.member("admin-a@tenancy.example.com", tenancy_world.workspace_a, "admin")
    return tenancy_world


def _verify(client, connector_id: str, region: str):
    return client.post(f"/api/v1/connectors/saved/{connector_id}/region-verification", json={"region": region})


def _refusal(world, connector, **inline):
    from services.data_residency import residency_refusal

    return residency_refusal(world.workspace_a, _endpoint(connector.type, connector_id=connector.id, **inline))


def test_declared_region_counts_only_once_a_platform_administrator_verified_it(eu_workspace):
    world = eu_workspace
    connector = _gcs_connector(world, extra={"region": "eu-west-1"})
    # The editor's own statement is not evidence.
    assert "cannot be determined" in _refusal(world, connector)

    # Neither the workspace's editor nor its administrator can verify it.
    for client in (world.a, world.admin_a):
        refused = _verify(client, connector.id, "eu-west-1")
        assert refused.status_code == 403, refused.text
    assert _refusal(world, connector)

    verified = _verify(world.admin, connector.id, "eu-west-1")
    assert verified.status_code == 200, verified.text
    body = verified.json()
    assert body["region_verification"]["region"] == "eu-west-1"
    assert body["region_verification"]["by"] == "root-tenancy@example.com"
    assert _refusal(world, connector) is None

    # The verification is audited, with who and which region.
    from services.audit_log import list_audit_events

    events = [e for e in list_audit_events(limit=200) if e.get("action") == "connector.region_verified"]
    assert events and events[0]["resource"] == f"connector:{connector.id}"
    assert events[0]["details"]["region"] == "eu-west-1"

    # Verified for another region than the one the connector declares: the
    # two statements contradict each other, so neither is taken.
    assert _verify(world.admin, connector.id, "us-central1").status_code == 200
    assert "cannot be determined" in _refusal(world, connector)
    # A connector that declares nothing is judged where it was verified to be.
    plain = _gcs_connector(world, name="us-bucket", database="acme-us-bucket")
    assert _verify(world.admin, plain.id, "us-central1").status_code == 200
    assert "'us-central1'" in _refusal(world, plain)

    # Withdrawn: unknown again.
    assert _verify(world.admin, connector.id, "").status_code == 200
    assert "cannot be determined" in _refusal(world, connector)


def test_verification_is_cleared_when_the_connector_moves(eu_workspace):
    from services.connector_store import get_connector, update_connector

    world = eu_workspace
    connector = _gcs_connector(world)
    assert _verify(world.admin, connector.id, "eu-west-1").status_code == 200
    assert _refusal(world, connector) is None

    # A rename is not a move.
    renamed = world.admin_a.patch(f"/api/v1/connectors/saved/{connector.id}", json={"name": "renamed"})
    assert renamed.status_code == 200, renamed.text
    assert _refusal(world, connector) is None

    # Another bucket is another place: the verification goes with the edit.
    moved = world.admin_a.patch(f"/api/v1/connectors/saved/{connector.id}", json={"database": "acme-us-bucket"})
    assert moved.status_code == 200, moved.text
    assert not get_connector(connector.id, workspace_id=world.workspace_a).region_verification
    assert "cannot be determined" in _refusal(world, connector)
    assert not moved.json().get("region_verification")

    # A stored verification whose address no longer matches the connector is
    # not honoured either (a write path that forgot to clear it).
    assert _verify(world.admin, connector.id, "eu-west-1").status_code == 200
    stale = dict(get_connector(connector.id, workspace_id=world.workspace_a).region_verification)
    update_connector(connector.id, {"database": "third-bucket"}, workspace_id=world.workspace_a)
    from services import connector_store

    connector_store._patch_file_connector(connector.id, {"region_verification": stale})
    assert get_connector(connector.id, workspace_id=world.workspace_a).region_verification == stale
    assert "cannot be determined" in _refusal(world, connector)


def test_a_workspace_member_cannot_write_the_verification_through_a_connector_payload(eu_workspace):
    from services.connector_store import get_connector

    world = eu_workspace
    forged = {"region": "eu-west-1", "by": "root-tenancy@example.com", "at": "2026-01-01T00:00:00+00:00", "address": "x"}
    created = world.admin_a.post("/api/v1/connectors/saved", json={
        "name": "forged", "type": "gcs", "role": "destination", "database": "b",
        "region_verification": forged, "extra": {"region_verification": forged},
    })
    assert created.status_code in (200, 201), created.text
    connector_id = (created.json().get("connector") or created.json())["id"]
    assert not get_connector(connector_id, workspace_id=world.workspace_a).region_verification

    connector = _gcs_connector(world, region_verification=forged)
    assert not get_connector(connector.id, workspace_id=world.workspace_a).region_verification
    world.admin_a.patch(f"/api/v1/connectors/saved/{connector.id}", json={"name": "x", "region_verification": forged})
    from services.connector_store import update_connector

    update_connector(connector.id, {"region_verification": forged}, workspace_id=world.workspace_a)
    assert not get_connector(connector.id, workspace_id=world.workspace_a).region_verification
    assert _refusal(world, connector)


def test_verification_does_not_cover_an_address_the_job_overrides(eu_workspace):
    world = eu_workspace
    connector = _gcs_connector(world)
    assert _verify(world.admin, connector.id, "eu-west-1").status_code == 200
    assert _refusal(world, connector) is None
    # The same connector id with another bucket named inline is another place.
    assert _refusal(world, connector, database="some-other-bucket")
    # A verification never outranks an address that proves a region.
    from services.connector_store import create_connector

    rds = create_connector({
        "name": "us-rds", "type": "postgresql", "host": "db.abc.us-east-1.rds.amazonaws.com",
        "database": "d", "workspace_id": world.workspace_a,
    })
    assert _verify(world.admin, rds.id, "eu-west-1").status_code == 200
    assert "'us-east-1'" in _refusal(world, rds)
    # Nor does it make a local destination in-region.
    local = create_connector({
        "name": "local-pg", "type": "postgresql", "host": "localhost", "database": "d",
        "workspace_id": world.workspace_a,
    })
    assert _verify(world.admin, local.id, "eu-west-1").status_code == 200
    assert "cannot be determined" in _refusal(world, local)


def test_another_workspaces_verified_connector_proves_nothing(eu_workspace):
    from services.connector_store import create_connector
    from services.data_residency import residency_refusal

    world = eu_workspace
    foreign = create_connector({
        "name": "b-bucket", "type": "gcs", "role": "destination", "database": "b-bucket",
        "workspace_id": world.workspace_b,
    })
    assert _verify(world.admin, foreign.id, "eu-west-1").status_code == 200
    endpoint = _endpoint("gcs", connector_id=foreign.id, database="b-bucket")
    assert residency_refusal(world.workspace_a, endpoint)


# --------------------------------------------------------------------------- HTTP (the reviewer's calls)


def _run_body(dest: dict) -> dict:
    return {
        "source": {"kind": "database", "format": "sqlite", "database": ":memory:", "table": "t"},
        "destination": dest, "mappings": [{"source": "id", "target": "id"}], "async_mode": True,
    }


@pytest.mark.parametrize("label,dest", [
    ("s3 empty host + declared region",
     {"kind": "database", "format": "s3", "host": "", "region": "eu-west-1", "database": "b", "table": "k"}),
    ("postgresql IP + declared region",
     {"kind": "database", "format": "postgresql", "host": "203.0.113.9", "region": "eu-west-1", "database": "d", "table": "t"}),
    ("postgresql attacker label",
     {"kind": "database", "format": "postgresql", "host": "db.eu-west-1.attacker.example", "database": "d", "table": "t"}),
    ("decoy host + connection string",
     {"kind": "database", "format": "postgresql", "host": "x.eu-west-1.rds.amazonaws.com",
      "connection_string": "postgresql://u:p@52.1.2.3:5432/db", "table": "t"}),
    ("file export + declared region", {"kind": "file_export", "format": "csv", "region": "eu-west-1"}),
])
def test_rest_execute_refuses_spoofed_region_evidence(eu_workspace, label, dest):
    refused = eu_workspace.a.post("/api/v1/transfer/execute", json=_run_body(dest))
    assert refused.status_code == 403, f"{label}: {refused.status_code} {refused.text[:200]}"
    assert refused.json()["detail"].startswith("Data residency violation:")
