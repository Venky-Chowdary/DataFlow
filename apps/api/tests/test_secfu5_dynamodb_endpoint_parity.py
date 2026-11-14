"""Security follow-up 5: DynamoDB is judged, written and read back at one endpoint.

* The egress and residency pre-checks judge the endpoint
  ``connectors.aws_common.resolve_endpoint_url`` derives from the resolved
  connector config (``egress_guard.aws_sdk_endpoint``). The DynamoDB writer
  rebuilt its client config without ``ssl``, so a TLS endpoint the checks
  judged (``https://host:port``) was written over plain ``http``.
* Gate-8's DynamoDB read-back dialled ``connection_string or
  http://localhost:8000`` with the destination's keys, always signing for
  us-east-1, so a real-AWS table (no endpoint) was "verified" against
  whatever answered on localhost:8000 (and another region's table was read in
  us-east-1). It now reads where the write went, through the same client
  builder, or reports that it cannot verify.
"""

from __future__ import annotations

import pytest

from connectors.aws_common import resolve_endpoint_url
from services.egress_guard import aws_sdk_endpoint
from src.transfer.adapters import resolve_connector_config
from src.transfer.models import EndpointConfig


class _Stop(Exception):
    pass


@pytest.fixture
def dialled(monkeypatch):
    """Every boto3 client built: (service, endpoint_url or "", region). Nothing is sent."""
    import boto3

    built: list[tuple[str, str, str]] = []

    def fake_client(service, **kwargs):
        built.append((service, str(kwargs.get("endpoint_url") or ""), str(kwargs.get("region_name") or "")))
        raise _Stop()

    monkeypatch.setattr(boto3, "client", fake_client)
    for name in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_DYNAMODB"):
        monkeypatch.delenv(name, raising=False)
    return built


_CASES = {
    "tls_custom_endpoint": dict(host="ddb.internal.example", port=9000, ssl=True),
    "plain_custom_endpoint": dict(host="ddb.internal.example", port=9000, ssl=False),
    "real_aws_region": dict(host="eu-west-1"),
    "aws_endpoint_host": dict(host="dynamodb.eu-west-1.amazonaws.com", ssl=True),
    "extra_endpoint_url": dict(host="us-east-1", extra={"endpoint_url": "https://ddb.internal.example:8443"}),
}


def _endpoint(**fields) -> EndpointConfig:
    return EndpointConfig(kind="database", format="dynamodb", table="t", username="AKIATENANT", password="s", **fields)


@pytest.mark.parametrize("case", sorted(_CASES))
def test_the_writer_dials_the_endpoint_the_pre_checks_judged(dialled, case):
    from src.transfer.adapters import write_destination_database

    endpoint = _endpoint(**_CASES[case])
    judged = aws_sdk_endpoint(resolve_connector_config(endpoint))
    with pytest.raises(Exception):
        write_destination_database(endpoint, [{"id": "1"}], ["id"], {"id": "string"}, [])
    assert dialled, "no DynamoDB client was built"
    assert {url for _service, url, _region in dialled} == {judged}, (case, judged, dialled)


def _verify(dest: dict) -> tuple[int, str]:
    from services.reconciliation import verify_target

    # -1: verify_target's "the read-back did not answer" (it returns the fallback).
    return verify_target("dynamodb", dest, schema="", table_name="t", fallback_rows=-1, fallback_checksum="")


def test_read_back_of_a_real_aws_table_never_dials_localhost(dialled):
    dest = resolve_connector_config(_endpoint(host="us-east-1"))
    rows, _checksum = _verify(dest)
    assert dialled, "the read-back built no client"
    assert all("localhost" not in url and "127.0.0.1" not in url for _s, url, _r in dialled), dialled
    assert {(url, region) for _s, url, region in dialled} == {("", "us-east-1")}
    assert rows == -1  # the stubbed client failed: reported as not verified, never as a match


def test_read_back_signs_for_the_tables_region(dialled):
    dest = resolve_connector_config(_endpoint(host="eu-west-1"))
    _verify(dest)
    assert {(url, region) for _s, url, region in dialled} == {("", "eu-west-1")}


@pytest.mark.parametrize("case", sorted(_CASES))
def test_read_back_reads_where_the_write_went(dialled, case):
    from src.transfer.adapters import write_destination_database

    endpoint = _endpoint(**_CASES[case])
    with pytest.raises(Exception):
        write_destination_database(endpoint, [{"id": "1"}], ["id"], {"id": "string"}, [])
    written = set(dialled)
    dialled.clear()
    _verify(resolve_connector_config(endpoint))
    assert set(dialled) == written


def test_read_back_without_a_destination_cannot_verify(dialled):
    rows, checksum = _verify({"type": "dynamodb"})
    assert rows == -1 and checksum == ""
    assert all("localhost" not in url for _s, url, _r in dialled)


def test_derivation_helper_matches_the_client_builder():
    """The endpoint the pre-checks compute is resolve_endpoint_url of the client cfg."""
    cfg = resolve_connector_config(_endpoint(host="ddb.internal.example", port=9000, ssl=True))
    assert aws_sdk_endpoint(cfg) == resolve_endpoint_url(cfg) == "https://ddb.internal.example:9000"
