"""Security review round 3 (H6): the region stamp binds every addressing and identity field.

A platform administrator's region verification survived edits and
per-request overrides of fields that move the data: a SaaS token kept in
``password`` (``saas_common.token`` falls back to it), ``private_key``,
``extra.token`` / ``oauth_token`` / ``credential``, the Iceberg ``warehouse``
and its ``s3.*`` / ``client.*`` FileIO properties, Redshift's ``staging_*``
bucket and any ``*_endpoint``. The stamp hash now covers them (an edit clears
the stamp) and a request that overrides one is not covered by it.
"""

from __future__ import annotations

import pytest

from tenancy_world import tenancy_world  # noqa: F401


@pytest.fixture
def fenced(tenancy_world, monkeypatch):  # noqa: F811
    for n in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{n}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{n}", raising=False)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "review3-h6-secrets-key-distinct-from-auth-0001")
    from services import tenant_store

    tenant_store.create_tenant(workspace_id=tenancy_world.workspace_a, name="A", data_region="eu-west-1")
    return tenancy_world


def _saved(ws, **fields):
    from services import connector_store

    return connector_store.create_connector({"workspace_id": ws, **fields})


def _verify(cid, ws):
    from services import connector_store

    return connector_store.set_region_verification(cid, "eu-west-1", verified_by="root@example.com", workspace_id=ws)


def _stamped(cid, ws):
    from services import connector_store

    return bool(connector_store.get_connector(cid, workspace_id=ws).region_verification)


def _allowed(ws, **dest):
    from services.data_residency import residency_refusal
    from src.transfer.models import EndpointConfig

    return residency_refusal(ws, EndpointConfig.from_dict("database", {"table": "t", **dest})) is None


def _edit(cid, ws, change):
    from services import connector_store

    connector_store.update_connector(cid, change, workspace_id=ws)


@pytest.mark.parametrize("typ", ["hubspot", "notion", "airtable"])
def test_a_saas_token_in_password_is_bound(fenced, typ):
    ws = fenced.workspace_a
    c = _saved(ws, name=f"{typ}1", type=typ, host="", password="eu-account-token")
    _verify(c.id, ws)
    assert _allowed(ws, format=typ, connector_id=c.id)
    assert not _allowed(ws, format=typ, connector_id=c.id, password="other-account-token")
    _edit(c.id, ws, {"password": "us-account-token"})
    assert not _stamped(c.id, ws)
    assert not _allowed(ws, format=typ, connector_id=c.id)


@pytest.mark.parametrize("field,value", [
    ("private_key", "-----BEGIN PRIVATE KEY-----\nOTHER\n-----END PRIVATE KEY-----"),
    ("extra", {"token": "other"}),
    ("extra", {"oauth_token": "other"}),
    ("extra", {"credential": "client:secret"}),
])
def test_identity_fields_are_bound_for_a_driver_that_dials_no_host(fenced, field, value):
    ws = fenced.workspace_a
    c = _saved(ws, name="gcs1", type="gcs", host="", database="eu-bucket", service_account="{}")
    _verify(c.id, ws)
    assert _allowed(ws, format="gcs", connector_id=c.id)
    _edit(c.id, ws, {field: value})
    assert not _stamped(c.id, ws), field


def test_iceberg_warehouse_and_fileio_are_bound(fenced):
    ws = fenced.workspace_a
    ice = _saved(ws, name="ice", type="iceberg", connection_string="https://catalog.example-corp.com/api/catalog",
                 warehouse="s3://eu-bucket/wh", extra={"catalog_type": "rest"})
    _verify(ice.id, ws)
    assert _allowed(ws, format="iceberg", connector_id=ice.id)
    assert not _allowed(ws, format="iceberg", connector_id=ice.id, warehouse="s3://us-bucket/wh")
    assert not _allowed(ws, format="iceberg", connector_id=ice.id,
                        extra={"catalog_type": "rest", "client.region": "us-east-1"})
    _edit(ice.id, ws, {"warehouse": "s3://us-bucket/wh"})
    assert not _stamped(ice.id, ws)
    _verify(ice.id, ws)
    _edit(ice.id, ws, {"extra": {"catalog_type": "rest", "s3.endpoint": "https://203.0.113.9",
                                 "s3.region": "us-east-1"}})
    assert not _stamped(ice.id, ws)
    assert not _allowed(ws, format="iceberg", connector_id=ice.id)


@pytest.mark.parametrize("extra", [
    {"staging_bucket": "us-bucket"},
    {"s3_endpoint": "https://s3.us-east-1.amazonaws.com"},
    {"staging_prefix": "elsewhere/"},
    {"webhdfs_endpoint": "http://203.0.113.9:9870"},
])
def test_staging_and_endpoint_extras_are_bound(fenced, extra):
    ws = fenced.workspace_a
    c = _saved(ws, name="mini", type="s3", host="", endpoint_url="https://minio.example-corp.com",
               database="eu-bucket", username="k", password="s")
    _verify(c.id, ws)
    _edit(c.id, ws, {"extra": extra})
    assert not _stamped(c.id, ws), extra


def test_an_unchanged_connector_keeps_its_stamp(fenced):
    ws = fenced.workspace_a
    c = _saved(ws, name="mini", type="s3", host="", endpoint_url="https://minio.example-corp.com",
               database="eu-bucket", username="k", password="s", extra={"prefix": "eu/"})
    _verify(c.id, ws)
    _edit(c.id, ws, {"name": "renamed"})
    assert _stamped(c.id, ws)
    assert _allowed(ws, format="s3", connector_id=c.id)


def test_the_bound_fields_cover_every_family_address_extra():
    from services.region_verification import is_bound_extra
    from services.residency_addressing import FAMILIES

    for name, family in FAMILIES.items():
        for key in family.address_extra:
            assert is_bound_extra(key), (name, key)
