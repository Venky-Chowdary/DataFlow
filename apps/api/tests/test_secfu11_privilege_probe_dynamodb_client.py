"""Follow-up review 1, M1: the DynamoDB privilege probe builds the connector's own client.

``destination_privilege_probe._probe_dynamodb`` built a raw ``boto3.client``.
For a real-AWS destination (a region label in ``host``) it passed only
``region_name='us-east-1'``: the tenant's keys were dropped, so preflight's
ListTables / DescribeTable(<tenant's table>) ran with the platform's own
credential chain (a table-existence oracle in the platform's account). It also
honoured ``AWS_ENDPOINT_URL_DYNAMODB`` and skipped the egress check for a
custom endpoint. It now builds the client the writer builds:
``aws_common.boto3_client("dynamodb", dynamodb_client_cfg(...))``.
"""

from __future__ import annotations

import pytest

from services.destination_privilege_probe import probe_destination_privileges


class _Stop(Exception):
    pass


@pytest.fixture
def built(monkeypatch):
    """Every boto3 client the probe builds (kwargs). Nothing is sent."""
    import boto3

    seen: list[dict] = []

    def fake_client(service, **kwargs):
        seen.append({"service": service, **kwargs})
        raise _Stop()

    monkeypatch.setattr(boto3, "client", fake_client)
    monkeypatch.setenv("AWS_ENDPOINT_URL_DYNAMODB", "http://127.0.0.1:9")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "PLATFORMKEY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "platform-secret")
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_EGRESS_ALLOWLIST", raising=False)
    return seen


def _probe(**fields):
    return probe_destination_privileges(
        "dynamodb", table="orders", username="AKIATENANT", password="tenant-secret", **fields
    )


def test_a_real_aws_destination_is_probed_with_the_tenants_keys(built):
    result = _probe(host="us-west-2")
    assert built, result
    kwargs = built[0]
    assert kwargs["aws_access_key_id"] == "AKIATENANT"
    assert kwargs["aws_secret_access_key"] == "tenant-secret"
    assert kwargs["region_name"] == "us-west-2"
    assert "endpoint_url" not in kwargs
    assert kwargs["config"].ignore_configured_endpoint_urls is True


def test_a_custom_endpoint_is_judged_by_the_egress_guard(built, monkeypatch):
    monkeypatch.setattr("socket.socket.connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("dial")))
    result = _probe(host="169.254.169.254", port=8000)
    assert built == [], "the client was built for a metadata endpoint"
    assert result.status != "ok"


def test_tls_on_the_destination_reaches_the_probe_endpoint(built):
    _probe(host="ddb.example.com", port=8443, ssl=True)
    assert built and built[0]["endpoint_url"] == "https://ddb.example.com:8443"
    assert built[0]["aws_access_key_id"] == "AKIATENANT"
