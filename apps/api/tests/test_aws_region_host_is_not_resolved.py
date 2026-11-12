"""An AWS connector's region label is not a dial target: never DNS-resolve it.

ConnectorModal saves an S3 or DynamoDB connector with ``host`` set to the
region (``"us-east-1"``) and the custom store in ``endpoint_url``. Every
``boto3_client()`` call, probe, introspect, batch read and save passed that
``host`` to the egress guard, which resolved ``us-east-1`` with
``socket.getaddrinfo``: 126 lookups for one Studio prefix test. Nothing ever
dials it: the SDK only signs with the region and connects to ``endpoint_url``.

CI run 43151557376 (PR 410, api-tests 6/6): the runner's resolver stopped
answering for about ten minutes. Each lookup of ``us-east-1`` then took about
10 s, on the event loop inside ``POST /transfer/execute``.
``test_r7_s3_compatible_studio_rest_e2e`` went from 3 s to over 600 s and timed
out; the portal thread sat in ``boto3_client -> egress_guard.classify_host ->
socket.getaddrinfo``.
"""
from __future__ import annotations

import socket

import pytest

pytest.importorskip("boto3")

_REAL_GETADDRINFO = socket.getaddrinfo


@pytest.fixture
def lookups(monkeypatch):
    """Record every name resolved. Names other than localhost do not resolve."""
    seen: list[str] = []

    def recording(host, *args, **kwargs):
        name = host.decode() if isinstance(host, bytes) else str(host)
        seen.append(name)
        if name in ("localhost", "127.0.0.1"):
            return _REAL_GETADDRINFO(host, *args, **kwargs)
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    monkeypatch.setattr(socket, "getaddrinfo", recording)
    # The local store is on loopback, as in the CI test (r4_route_harness).
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.delenv("DATAWRAP_EGRESS_ALLOWLIST", raising=False)
    return seen


def _studio_s3_cfg(port: int | str) -> dict:
    """What ConnectorModal saves for S3 with a custom endpoint (the CI test's connector)."""
    return {
        "type": "s3", "host": "us-east-1", "port": port,
        "database": "r7-demo-bucket", "username": "test", "password": "test-secret",
        "ssl": False, "endpoint_url": "http://localhost:42869", "path_style": True,
    }


@pytest.mark.parametrize("port", [42869, 443])
@pytest.mark.parametrize("service", ["s3", "dynamodb"])
def test_region_label_in_host_is_not_resolved(lookups, service, port):
    from connectors.aws_common import boto3_client

    boto3_client(service, _studio_s3_cfg(port))

    assert "us-east-1" not in lookups, lookups
    # The endpoint the client really dials is still checked.
    assert "localhost" in lookups, lookups


def test_region_label_with_a_hung_resolver_does_not_stall_the_client(monkeypatch):
    """The CI symptom: a resolver that takes 10 s per name costs nothing here."""
    import time

    from connectors.aws_common import boto3_client

    slow: list[str] = []

    def hung(host, *args, **kwargs):
        name = host.decode() if isinstance(host, bytes) else str(host)
        if name not in ("localhost", "127.0.0.1"):
            slow.append(name)
            time.sleep(10)
            raise socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution")
        return _REAL_GETADDRINFO(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", hung)
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.delenv("DATAWRAP_EGRESS_ALLOWLIST", raising=False)
    started = time.monotonic()
    boto3_client("s3", _studio_s3_cfg(42869))
    elapsed = time.monotonic() - started
    assert not slow, slow
    assert elapsed < 5.0, elapsed


def test_endpoint_on_a_metadata_address_is_still_refused(lookups):
    from connectors.aws_common import boto3_client
    from services.egress_guard import EgressRefused

    cfg = _studio_s3_cfg(80)
    cfg["endpoint_url"] = "http://169.254.169.254:80"
    with pytest.raises(EgressRefused):
        boto3_client("s3", cfg)


@pytest.mark.parametrize("host", ["169.254.169.254", "metadata.google.internal:80"])
def test_a_host_that_names_an_address_is_still_checked(lookups, host):
    """Only a bare region label is skipped; a host that names an address is still checked."""
    from connectors.aws_common import boto3_client
    from services.egress_guard import EgressRefused

    cfg = _studio_s3_cfg(42869)
    cfg["host"] = host
    with pytest.raises(EgressRefused):
        boto3_client("s3", cfg)


def test_a_host_derived_endpoint_is_still_resolved(lookups):
    """``minio`` + port 9000 is the Docker endpoint ``http://minio:9000``: resolve it."""
    from connectors.aws_common import boto3_client

    cfg = {"type": "s3", "host": "minio", "port": 9000, "username": "k", "password": "s"}
    boto3_client("s3", cfg)
    assert "minio" in lookups, lookups


# The other S3 sites (probe, introspect, batch reads, connector save) reach the
# guard with the connector's ``type``; the rule lives in ``iter_refusals``.
@pytest.mark.parametrize("db_type", ["s3", "amazon_s3", "dynamodb", "kinesis"])
@pytest.mark.parametrize("region", ["us-east-1", "eu-central-1", "ap-southeast-2", "us-gov-west-1"])
def test_guard_skips_the_region_label_of_an_aws_connector(lookups, db_type, region):
    from services.egress_guard import metadata_endpoint_refusal

    assert metadata_endpoint_refusal(region, "http://localhost:42869", db_type=db_type) == ""
    assert region not in lookups, lookups


def test_connector_scope_on_an_s3_cfg_does_not_resolve_the_region(lookups):
    """The batch-read / introspect path: ``connector_dial_scope`` with the saved cfg."""
    from services.egress_guard import connector_dial_scope

    with connector_dial_scope(_studio_s3_cfg(42869), point="transfer"):
        pass
    assert "us-east-1" not in lookups, lookups


@pytest.mark.parametrize("db_type", ["postgresql", "redshift", "mysql", ""])
def test_a_region_spelled_host_of_another_engine_is_still_resolved(lookups, db_type):
    """Postgres or Redshift dial ``host``: a name spelled like a region is still a host."""
    from services.egress_guard import metadata_endpoint_refusal

    metadata_endpoint_refusal("us-east-1", db_type=db_type)
    assert "us-east-1" in lookups, lookups


@pytest.mark.parametrize("host", ["minio", "us-east-1.corp.example", "us-east-1x", "s3.us-east-1"])
def test_only_an_exact_region_spelling_is_skipped(lookups, host):
    from services.egress_guard import metadata_endpoint_refusal

    metadata_endpoint_refusal(host, db_type="s3")
    assert host in lookups, lookups
