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

    endpoint = "http://localhost:42869"
    assert metadata_endpoint_refusal(region, endpoint, db_type=db_type, aws_endpoint=endpoint) == ""
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


# --- PR 410 review round 2, H-E1: the skip must never cover the dialled name -
#
# ``resolve_endpoint_url`` turns a dot-less host plus a service port into
# ``http://<host>:<port>``, so the label itself can be the dial target. A
# region-shaped single-label name (``us-east-1``, ``us-db-01``) that resolves
# inside the cluster (k8s Service, search domain, wildcard DNS) then reached a
# private, loopback or metadata address with the pre-dial check skipped.

_TARGETS = ["169.254.169.254", "10.1.2.3", "127.0.0.1"]


@pytest.fixture
def resolves(monkeypatch):
    """``answers[name] = ip`` makes ``name`` resolve there; no allowlist is set."""
    answers: dict[str, str] = {}

    def fake(host, port=None, *args, **kwargs):
        name = host.decode() if isinstance(host, bytes) else str(host)
        key = name.lower().rstrip(".")
        if key in answers:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (answers[key], port or 0))]
        if name in ("localhost", "127.0.0.1"):
            return _REAL_GETADDRINFO(host, port, *args, **kwargs)
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    monkeypatch.setattr(socket, "getaddrinfo", fake)
    for name in (
        "DATAFLOW_EGRESS_ALLOWLIST", "DATAWRAP_EGRESS_ALLOWLIST",
        "DATAFLOW_EGRESS_ENFORCE_NEW", "DATAWRAP_EGRESS_ENFORCE_NEW",
    ):
        monkeypatch.delenv(name, raising=False)
    return answers


def _refused_client(service: str, cfg: dict) -> str:
    from connectors.aws_common import boto3_client
    from services.egress_guard import EgressRefused

    with pytest.raises(EgressRefused) as refused:
        boto3_client(service, cfg)
    return str(refused.value)


@pytest.mark.parametrize("ip", _TARGETS)
@pytest.mark.parametrize(
    ("service", "cfg"),
    [
        pytest.param("s3", {"type": "s3", "host": "us-east-1", "port": 9000}, id="s3-host-port"),
        pytest.param(
            "s3", {"type": "s3", "host": "us-east-1", "endpoint_url": "http://us-east-1:80"},
            id="s3-endpoint-url",
        ),
        pytest.param(
            "s3", {"type": "s3", "host": "eu-west-1", "endpoint_url": "http://US-EAST-1."},
            id="s3-endpoint-upper-trailing-dot",
        ),
        pytest.param(
            "dynamodb", {"type": "dynamodb", "host": "us-east-1", "port": 8000},
            id="dynamodb-host-port",
        ),
    ],
)
def test_a_region_label_the_client_dials_is_checked(resolves, ip, service, cfg):
    resolves["us-east-1"] = ip
    assert "us-east-1" in _refused_client(service, dict(cfg))


@pytest.mark.parametrize("ip", _TARGETS)
@pytest.mark.parametrize("name", ["us-db-01", "eu-prod-1", "ap-cache-3", "ca-web-02", "us-gov-anything-9"])
def test_a_region_shaped_host_name_is_not_a_region(resolves, ip, name):
    """``-[a-z]+-\\d`` took any word: only AWS direction words name a region."""
    from services.egress_guard import metadata_endpoint_refusal

    resolves[name] = ip
    assert name in _refused_client("s3", {"type": "s3", "host": name, "port": 9000})
    # Even where the SDK would sign with it and dial AWS, it is resolved.
    assert metadata_endpoint_refusal(name, db_type="s3", aws_endpoint="")


@pytest.mark.parametrize("ip", _TARGETS)
@pytest.mark.parametrize("label", ["us-east-١", "us-east-１", "us-east-\U0001d7cf"])
def test_unicode_digits_are_not_a_region(resolves, ip, label):
    """``\\d`` matched Arabic-Indic and fullwidth digits; the pattern is ASCII only."""
    from services.egress_guard import is_aws_region_label, metadata_endpoint_refusal

    resolves[label] = ip
    assert not is_aws_region_label(label, "s3")
    assert metadata_endpoint_refusal(label, db_type="s3", aws_endpoint="")


@pytest.mark.parametrize("ip", _TARGETS)
def test_metadata_endpoint_refusal_checks_a_url_on_the_label(resolves, ip):
    from services.egress_guard import metadata_endpoint_refusal

    resolves["us-east-1"] = ip
    assert metadata_endpoint_refusal("http://us-east-1:80", db_type="s3")
    assert metadata_endpoint_refusal(
        "us-east-1", "http://us-east-1:9000", db_type="s3", aws_endpoint="http://us-east-1:9000"
    )


@pytest.mark.parametrize("ip", _TARGETS)
def test_a_bare_label_is_checked_when_the_dialled_endpoint_is_not_known(resolves, ip):
    """A caller that cannot say what the SDK dials (no port at hand) gets no skip."""
    from services.egress_guard import metadata_endpoint_refusal

    resolves["us-east-1"] = ip
    assert metadata_endpoint_refusal("us-east-1", db_type="s3")


@pytest.mark.parametrize("ip", _TARGETS)
def test_cfg_sites_check_the_endpoint_derived_from_host_and_port(resolves, monkeypatch, ip):
    """Save, scoped dials and the probe build the endpoint from ``host`` + ``port``."""
    from services.egress_guard import EgressRefused, connector_dial_scope, connector_save_refusal
    from src.transfer.connector_registry import _run_probe_impl

    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    resolves["us-east-1"] = ip
    cfg = {"type": "s3", "host": "us-east-1", "port": 9000, "username": "k", "password": "s"}
    assert "us-east-1" in connector_save_refusal(dict(cfg, name="store"))
    with pytest.raises(EgressRefused):
        with connector_dial_scope(dict(cfg), point="transfer"):
            pass
    ok, message = _run_probe_impl("s3", dict(cfg))
    assert not ok and "us-east-1" in message, message


def test_ci_case_still_does_not_resolve_the_region(lookups):
    """The Studio S3 connector: the label is skipped at every site that knows the endpoint."""
    from services.egress_guard import connector_save_refusal

    assert connector_save_refusal(dict(_studio_s3_cfg(42869), name="store")) == ""
    assert connector_save_refusal(dict(_studio_s3_cfg(42869), name="store", port=443)) == ""
    # Real AWS: no endpoint, no service port. The SDK dials the region's AWS host.
    assert connector_save_refusal({"type": "s3", "host": "us-east-1", "name": "aws"}) == ""
    assert "us-east-1" not in lookups, lookups


def _botocore_regions() -> list[str]:
    import json
    import os

    import botocore

    data = os.path.join(os.path.dirname(botocore.__file__), "data")
    names: set[str] = set()
    for file_name in ("partitions.json", "endpoints.json"):
        with open(os.path.join(data, file_name), encoding="utf-8") as handle:
            for partition in json.load(handle).get("partitions", []):
                names.update(partition.get("regions", {}))
    # ``aws-global`` and friends are partition pseudo-regions, never typed in.
    return sorted(name for name in names if not name.endswith("-global"))


def test_every_region_botocore_ships_is_a_region_label():
    from services.egress_guard import is_aws_region_label

    regions = _botocore_regions()
    assert "us-east-1" in regions and "us-gov-west-1" in regions and "cn-north-1" in regions
    assert [r for r in regions if not is_aws_region_label(r, "s3")] == []


@pytest.mark.parametrize(
    "host",
    ["us-east-1\n", "us-east-1x", "us-east-100", "us-middle-1", "xx-east-1", "us-east-1.svc", "us-iso-db-1"],
)
def test_the_region_pattern_is_exact(host):
    from services.egress_guard import is_aws_region_label

    assert not is_aws_region_label(host, "s3")


# --- review round 2, L-E3: ``extra.type`` must not pick the guard's rule ----
#
# ``POST /connectors/test`` merged ``extra`` into the probe cfg after the
# request's own fields, so ``{"type": "postgresql", "extra": {"type": "s3"}}``
# probed Postgres while the guard applied the S3 rule (``cfg.type`` first).


def _capture_route_probe(monkeypatch, tmp_path) -> dict:
    import importlib

    import tests.r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    seen: dict = {}

    def capture(driver, cfg):
        seen.update(cfg, _driver=driver)
        return True, "ok"

    router_mod = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(router_mod, "run_probe", capture)
    return seen


def test_extra_cannot_override_the_type_or_dial_fields_of_a_probe(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    from src.main import app

    seen = _capture_route_probe(monkeypatch, tmp_path)
    response = TestClient(app).post(
        "/api/v1/connectors/test",
        json={
            "type": "postgresql", "host": "us-db-01",
            "extra": {
                "type": "s3", "db_type": "s3", "host": "us-east-1", "port": 9000,
                "endpoint_url": "http://us-east-1:9000", "connection_string": "http://us-east-1",
                "sslrootcert": "/certs/ca.pem",
            },
        },
    )
    assert response.status_code == 200, response.text
    assert seen["_driver"] == "postgresql"
    assert seen["type"] == "postgresql"
    assert "db_type" not in seen
    assert seen["host"] == "us-db-01"
    assert seen["port"] in (0, None, "")
    assert seen["endpoint_url"] == ""
    assert "us-east-1" not in str(seen.get("connection_string") or "")
    assert seen["sslrootcert"] == "/certs/ca.pem", "other extra fields still reach the probe"


@pytest.mark.parametrize("ip", _TARGETS)
def test_postgres_probe_with_extra_type_s3_is_refused(resolves, monkeypatch, tmp_path, ip):
    """The review's request, end to end through the real probe."""
    import tests.r4_route_harness as h
    from fastapi.testclient import TestClient

    from src.main import app

    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    monkeypatch.delenv("DATAFLOW_EGRESS_ALLOWLIST", raising=False)  # the harness allows loopback
    resolves["us-db-01"] = ip
    response = TestClient(app).post(
        "/api/v1/connectors/test",
        json={
            "type": "postgresql", "host": "us-db-01", "port": 5432, "database": "app",
            "username": "probe", "password": "probe-pass", "extra": {"type": "s3"},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["success"] is False
    assert "Refused connector host us-db-01" in body["message"], body


@pytest.mark.parametrize("ip", _TARGETS)
def test_probe_guard_uses_the_driver_not_a_cfg_type(resolves, monkeypatch, ip):
    """``_run_probe_impl`` checks with the driver it is about to run."""
    import src.transfer.connector_registry as registry

    resolves["us-east-1"] = ip
    monkeypatch.setattr(registry, "CONNECTOR_MODULES", {})  # nothing past the guard
    ok, message = registry._run_probe_impl("postgresql", {"type": "s3", "host": "us-east-1"})
    assert not ok
    assert "Refused connector host us-east-1" in message, message
