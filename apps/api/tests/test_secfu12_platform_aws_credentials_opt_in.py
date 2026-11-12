"""Follow-up review 1, H2: tenant settings never make a client sign with the platform's AWS chain.

Security follow-up 4 closed it for the S3 and DynamoDB connector clients, but
the review found the same fallback in Iceberg: REST SigV4 signing
(``rest.sigv4-enabled``, possibly switched on by the REST server's own config
overrides), a Glue catalog with no keys, ``*.profile-name`` and
``botocore_session`` properties, pyarrow's S3 FileIO with no keys, and
``dest_precount``'s Iceberg S3 COUNT, which asked for the chain on purpose.

Off (the default) every one of them is refused with the two remedies (the
connector's own keys, or the operator opt-in) and no secret in the message.
``DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS=1`` is for a self-hosted,
single-tenant deployment whose connectors use the instance's IAM role: those
paths are then allowed, the API warns at startup, and the security-settings
snapshot records it.
"""

from __future__ import annotations

import logging

import pytest

FLAG = "DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS"

_SECRET = "tenant-secret-value"


class _Stop(Exception):
    pass


@pytest.fixture(params=["off", "on"])
def mode(request, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_ALLOW_PLATFORM_AWS_CREDENTIALS", raising=False)
    if request.param == "on":
        monkeypatch.setenv(FLAG, "1")
    # Never reach the metadata service or any AWS endpoint.
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "PLATFORMKEY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "platform-secret")
    for name in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL_DYNAMODB"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        "socket.socket.connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled"))
    )
    return request.param


@pytest.fixture
def sessions(monkeypatch):
    """boto3 sessions pyiceberg builds (kwargs); their clients do nothing."""
    import boto3

    seen: list[dict] = []

    class _Session:
        def __init__(self, **kwargs):
            seen.append(kwargs)
            self.region_name = kwargs.get("region_name") or "us-east-1"

        def client(self, *_a, **_k):
            return object()

    monkeypatch.setattr(boto3, "Session", _Session)
    return seen


def _refused(message: str) -> None:
    assert FLAG in message and "access key" in message, message
    assert _SECRET not in message


# --- S3 / DynamoDB connector clients ------------------------------------------


@pytest.mark.parametrize("service", ["s3", "dynamodb"])
def test_a_tenant_request_for_the_instance_role(mode, service):
    from connectors.aws_common import boto3_client
    from services.egress_guard import connector_save_refusal

    cfg = {"type": service, "host": "us-east-1", "database": "b", "use_default_aws_credentials": True}
    if mode == "off":
        _refused(connector_save_refusal({"name": "c", **cfg}))
        with pytest.raises(ValueError) as caught:
            boto3_client(service, dict(cfg))
        _refused(str(caught.value))
    else:
        assert connector_save_refusal({"name": "c", **cfg}) == ""
        client = boto3_client(service, dict(cfg))
        assert client._request_signer._credentials.access_key == "PLATFORMKEY"


def test_keys_on_the_connector_always_sign_with_those_keys(mode):
    from connectors.aws_common import boto3_client

    client = boto3_client("s3", {"type": "s3", "host": "us-east-1", "username": "AKIATENANT", "password": _SECRET})
    assert client._request_signer._credentials.access_key == "AKIATENANT"


# --- Iceberg catalogs ----------------------------------------------------------


def _iceberg(extra: dict, **fields) -> dict:
    return {"type": "iceberg", "name": "ice", "table": "t", "extra": extra, **fields}


_REST = "https://catalog.example.com/v1"
_SIGV4 = {"rest.sigv4-enabled": "true", "rest.signing-name": "s3", "rest.signing-region": "us-east-1"}


@pytest.fixture
def rest_config(monkeypatch):
    """The REST server's ``/v1/config`` answer, without HTTP."""
    from pyiceberg.catalog.rest import RestCatalog

    overrides: dict = {}

    def fetch(self):
        self.properties = {**self.properties, **overrides}
        self.uri = self.properties["uri"]
        self._supported_endpoints = set()

    monkeypatch.setattr(RestCatalog, "_fetch_config", fetch)
    return overrides


@pytest.mark.parametrize(
    "extra",
    [
        {"catalog_type": "rest", **_SIGV4},
        {"catalog_type": "rest", "client.profile-name": "platform"},
        {"catalog_type": "rest", "s3.profile-name": "platform"},
        {"catalog_type": "rest", **_SIGV4, "client.access-key-id": "AKIA", "client.secret-access-key": _SECRET,
         "client.profile-name": "platform"},
        {"catalog_type": "glue", "glue.region": "us-east-1"},
        {"catalog_type": "glue", "glue.profile-name": "platform", "glue.access-key-id": "AKIA",
         "glue.secret-access-key": _SECRET},
    ],
)
def test_iceberg_settings_that_sign_with_the_platform_chain(mode, extra, rest_config, sessions):
    from connectors.iceberg_catalog import load_catalog
    from services.egress_guard import connector_save_refusal

    endpoint = _iceberg(dict(extra), connection_string=_REST if extra["catalog_type"] == "rest" else "")
    if mode == "off":
        _refused(connector_save_refusal(dict(endpoint)))
        with pytest.raises(ValueError) as caught:
            load_catalog(dict(endpoint))
        _refused(str(caught.value))
        assert sessions == []
    else:
        assert connector_save_refusal(dict(endpoint)) == ""
        load_catalog(dict(endpoint))


@pytest.mark.parametrize(
    "extra",
    [
        {"catalog_type": "rest", **_SIGV4, "client.access-key-id": "AKIATENANT", "client.secret-access-key": _SECRET},
        {"catalog_type": "glue", "glue.region": "us-east-1", "glue.access-key-id": "AKIATENANT",
         "glue.secret-access-key": _SECRET},
    ],
)
def test_iceberg_signers_with_the_connectors_own_keys_are_allowed(mode, extra, rest_config, sessions):
    from connectors.iceberg_catalog import load_catalog
    from services.egress_guard import connector_save_refusal

    endpoint = _iceberg(dict(extra), connection_string=_REST if extra["catalog_type"] == "rest" else "")
    assert connector_save_refusal(dict(endpoint)) == ""
    load_catalog(dict(endpoint))
    assert sessions and sessions[-1]["aws_access_key_id"] == "AKIATENANT"


def test_a_rest_servers_overrides_cannot_switch_on_sigv4(mode, rest_config, sessions):
    from connectors.iceberg_catalog import load_catalog

    rest_config.update(_SIGV4)
    endpoint = _iceberg({"catalog_type": "rest"}, connection_string=_REST)
    if mode == "off":
        with pytest.raises(ValueError) as caught:
            load_catalog(endpoint)
        _refused(str(caught.value))
        assert sessions == []
    else:
        load_catalog(endpoint)
        assert sessions and sessions[-1]["aws_access_key_id"] is None  # the instance role


# --- Iceberg FileIO -----------------------------------------------------------


@pytest.fixture
def s3_filesystems(monkeypatch):
    import pyarrow.fs

    seen: list[dict] = []

    def fake(**kwargs):
        seen.append(kwargs)
        return object()

    monkeypatch.setattr(pyarrow.fs, "S3FileSystem", fake)
    return seen


def _sql_catalog(tmp_path, **props):
    from pyiceberg.catalog.sql import SqlCatalog

    from connectors.iceberg_credential_guard import guarded_catalog

    return guarded_catalog(SqlCatalog)(
        "c", uri=f"sqlite:///{tmp_path / 'c.db'}", warehouse="s3://bucket/wh", **{"s3.region": "us-east-1", **props}
    )


def test_a_catalogs_s3_fileio_without_keys(mode, tmp_path, s3_filesystems):
    catalog = _sql_catalog(tmp_path)
    # A REST server may name another FileIO with the table; it is not used while off.
    io = catalog._load_file_io({"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}, location="s3://bucket/wh/t")
    if mode == "off":
        assert type(io).__name__ == "KeyedS3PyArrowFileIO"
        with pytest.raises(ValueError) as caught:
            io.new_input("s3://bucket/wh/t/data/f.parquet")
        _refused(str(caught.value))
        assert s3_filesystems == []
    else:
        assert type(io).__name__ == "FsspecFileIO"


def test_a_fileio_with_vended_or_own_keys_opens_s3(mode, tmp_path, s3_filesystems):
    catalog = _sql_catalog(tmp_path)
    vended = {"s3.access-key-id": "ASIAVENDED", "s3.secret-access-key": _SECRET, "s3.session-token": "t"}
    io = catalog._load_file_io(vended, location="s3://bucket/wh/t")
    io.new_input("s3://bucket/wh/t/data/f.parquet")
    assert s3_filesystems and s3_filesystems[-1]["access_key"] == "ASIAVENDED"


def test_a_fileio_built_outside_a_catalog_is_keyed_too(mode, s3_filesystems):
    from connectors.iceberg_credential_guard import guarded_file_io

    io = guarded_file_io({"s3.region": "us-east-1", "s3.endpoint": "https://s3.example.com"}, "s3://b/k")
    if mode == "off":
        with pytest.raises(ValueError):
            io.new_input("s3://b/k")
        assert s3_filesystems == []
    else:
        io.new_input("s3://b/k")
        assert s3_filesystems and s3_filesystems[-1]["access_key"] is None


# --- dest_precount and the privilege probe ------------------------------------


def test_dest_precounts_iceberg_s3_count_without_keys(mode):
    from connectors.aws_common import PLATFORM_DEFAULT_CREDENTIALS
    from services.dest_precount import _iceberg_object_store_cfg
    from services.object_streaming import parse_object_store_uri

    endpoint = _iceberg({"catalog_type": "rest", "s3.region": "us-east-1"}, connection_string=_REST)
    cfg = _iceberg_object_store_cfg(endpoint, parse_object_store_uri("s3://bucket/wh/t/data/f.parquet"))
    if mode == "off":
        assert cfg is None
    else:
        assert cfg["use_default_aws_credentials"] is PLATFORM_DEFAULT_CREDENTIALS


def test_the_dynamodb_privilege_probe_never_uses_the_platform_chain(mode, monkeypatch):
    import boto3

    from services.destination_privilege_probe import probe_destination_privileges

    built: list[dict] = []

    def fake_client(service, **kwargs):
        built.append(kwargs)
        raise _Stop()

    monkeypatch.setattr(boto3, "client", fake_client)
    probe_destination_privileges("dynamodb", host="us-west-2", table="t")
    assert built and built[0].get("aws_access_key_id") not in (None, "PLATFORMKEY")


# --- startup ------------------------------------------------------------------


def test_startup_warning_and_snapshot(mode, caplog):
    from services.platform_aws_credentials import (
        platform_aws_settings,
        warn_if_platform_aws_credentials_allowed,
    )

    logger = logging.getLogger("datawrap.security.test")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        warn_if_platform_aws_credentials_allowed(logger)
    warned = [r.getMessage() for r in caplog.records if r.name == logger.name]
    if mode == "off":
        assert warned == []
        assert platform_aws_settings() == {"platform_aws_chain": "refused"}
    else:
        assert len(warned) == 1 and FLAG in warned[0]
        assert platform_aws_settings() == {"platform_aws_chain": "allowed"}
