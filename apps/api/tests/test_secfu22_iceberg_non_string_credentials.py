"""Follow-up review 2, H1: a credential that is not text never counts as the table's own.

The guards counted a credential as present when ``str(value).strip()`` was
non-empty, so ``0``, ``False``, ``[]`` or ``{}`` (``"0"``, ``"False"``,
``"[]"``, ``"{}"``) passed every "has its own key" check. pyiceberg reads the
same properties by truthiness (``if token := props.get(...)``,
``get_first_property_value``), so it passed no credential at all and pyarrow
(GCS, Azure, S3, OSS) or boto3 (REST SigV4, Glue) fell back to the API host's
own identity. A tenant controls the connector's ``extra`` (a ``dict[str,
Any]``) and runs its own REST catalog, whose ``/v1/config`` and load responses
are JSON, so every one of those values is reachable.

A credential property now counts only as a non-empty string after strip, and
a credential property whose value is not text (and not null, which is no
value) is refused outright, in every mode, with a message that names the
property and never the value. A JSON ``null`` is the same as an absent key.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

GCP_FLAG = "DATAFLOW_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS"
AWS_FLAG = "DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS"
_REST = "https://catalog.example.com/v1"
_SECRET = "tenant-secret-value"
_GCS = "gs://bucket/wh/ns/t"
_AZURE = "abfss://container@tenantacct.dfs.core.windows.net/wh/ns/t"
_S3 = "s3://bucket/wh/ns/t"
_OSS = "oss://bucket/wh/ns/t"
_ACCOUNT = {"adls.account-name": "tenantacct"}

#: Values a JSON body or ``extra`` may carry that are not text. ``None`` (JSON null) is no value.
_NOT_TEXT = [
    pytest.param(0, id="zero"),
    pytest.param(False, id="false"),
    pytest.param([], id="empty-list"),
    pytest.param({}, id="empty-dict"),
    pytest.param(1, id="one"),
    pytest.param(True, id="true"),
    pytest.param(["AKIA"], id="list"),
    pytest.param({"k": "v"}, id="dict"),
]
_FALSY_OR_NULL = [
    pytest.param(0, id="zero"),
    pytest.param(False, id="false"),
    pytest.param([], id="empty-list"),
    pytest.param({}, id="empty-dict"),
    pytest.param(None, id="null"),
]


@pytest.fixture(params=["off", "on"])
def mode(request, monkeypatch):
    """Both operator opt-ins off, or both on: a credential that is not text is refused either way."""
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "ALLOW_PLATFORM_AWS_CREDENTIALS"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    if request.param == "on":
        monkeypatch.setenv(GCP_FLAG, "1")
        monkeypatch.setenv(AWS_FLAG, "1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )
    return request.param


@pytest.fixture
def off(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "ALLOW_PLATFORM_AWS_CREDENTIALS"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )


@pytest.fixture
def filesystems(monkeypatch):
    """pyarrow filesystems pyiceberg builds: (class, kwargs). pyarrow's credential lookup is C++."""
    import pyarrow.fs

    seen: list[tuple[str, dict]] = []
    for name in ("GcsFileSystem", "AzureFileSystem", "S3FileSystem"):

        def fake(*_args, _name=name, **kwargs):
            seen.append((_name, kwargs))
            return object()

        monkeypatch.setattr(pyarrow.fs, name, fake)
    return seen


@pytest.fixture
def sessions(monkeypatch):
    """boto3 sessions REST SigV4 and Glue build; one without keys signs with the host's chain."""
    import boto3

    built: list[dict] = []

    class FakeSession:
        def __init__(self, **kwargs):
            built.append(kwargs)
            self.region_name = kwargs.get("region_name")

        def client(self, *_a, **_k):
            raise RuntimeError("intercepted: a boto3 client was built")

        def get_credentials(self):
            raise RuntimeError("intercepted: credentials were looked up")

    monkeypatch.setattr(boto3, "Session", FakeSession)
    return built


def _keyless(sessions: list[dict]) -> list[dict]:
    """Sessions that sign with the host's chain: no text access key of the connector's own."""
    return [kw for kw in sessions if not (isinstance(kw.get("aws_access_key_id"), str) and kw["aws_access_key_id"])]


def _table_response(location: str, config: dict) -> dict:
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
    from pyiceberg.types import LongType, NestedField

    metadata = new_table_metadata(
        Schema(NestedField(1, "id", LongType(), required=False)),
        UNPARTITIONED_PARTITION_SPEC,
        UNSORTED_SORT_ORDER,
        location,
        {},
    )
    return {
        "metadata-location": f"{location}/metadata/v1.metadata.json",
        "metadata": json.loads(metadata.model_dump_json()),
        "config": config,
    }


@pytest.fixture
def server(monkeypatch):
    """The tenant's REST catalog server, answered in-process (no socket)."""
    import requests

    state = SimpleNamespace(defaults={}, overrides={}, location=_GCS, table_config={})

    def request(self, method, url, *args, **kwargs):
        path = urlsplit(url).path
        if path.endswith("/config"):
            body = {"defaults": state.defaults, "overrides": state.overrides}
        elif path.endswith("/tables/t") and method == "GET":
            body = _table_response(state.location, state.table_config)
        else:
            raise AssertionError(f"unexpected catalog call {method} {path}")
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(body).encode()
        response.headers["Content-Type"] = "application/json"
        response.url = url
        return response

    monkeypatch.setattr(requests.Session, "request", request)
    return state


def _rest_endpoint(**extra) -> dict:
    return {
        "type": "iceberg",
        "name": "ice",
        "schema": "ns",
        "table": "t",
        "connection_string": _REST,
        "extra": {"catalog_type": "rest", **extra},
    }


def _open_data_file(server, **extra) -> None:
    from connectors.iceberg_catalog import load_table

    table = load_table(_rest_endpoint(**extra))
    table.io.new_input(f"{server.location}/data/f.parquet")


def _names_only(message: str, prop: str) -> None:
    assert prop in message, message
    for value in (_SECRET, "AKIA", "tenantacct", "bucket", "'k'", "[]", "{}"):
        assert value not in message, message


# --- a REST table's vended config ---------------------------------------------

_TABLE_CREDENTIALS = [
    pytest.param(_GCS, {"gcs.oauth2.token": "V"}, "gcs.oauth2.token", id="gcs-token"),
    pytest.param(_AZURE, {**_ACCOUNT, "adls.account-key": "V"}, "adls.account-key", id="adls-account-key"),
    pytest.param(_AZURE, {**_ACCOUNT, "adls.sas-token": "V"}, "adls.sas-token", id="adls-sas"),
    pytest.param(
        _AZURE,
        {**_ACCOUNT, "adls.client-id": "V", "adls.client-secret": "V", "adls.tenant-id": "V"},
        "adls.client-id",
        id="adls-client",
    ),
    pytest.param(
        _S3, {"s3.region": "us-east-1", "s3.access-key-id": "V", "s3.secret-access-key": "V"},
        "s3.access-key-id", id="s3-keys",
    ),
    pytest.param(
        _S3, {"s3.region": "us-east-1", "client.access-key-id": "V", "client.secret-access-key": "V"},
        "client.access-key-id", id="client-keys",
    ),
    pytest.param(_OSS, {"s3.access-key-id": "V", "s3.secret-access-key": "V"}, "s3.access-key-id", id="oss-keys"),
]


def _with(config: dict, value) -> dict:
    return {key: (value if val == "V" else val) for key, val in config.items()}


@pytest.mark.parametrize("value", _NOT_TEXT)
@pytest.mark.parametrize(("location", "config", "prop"), _TABLE_CREDENTIALS)
def test_a_vended_credential_that_is_not_text_is_refused(mode, server, filesystems, location, config, prop, value):
    server.location = location
    server.table_config = _with(config, value)
    with pytest.raises(ValueError) as caught:
        _open_data_file(server)
    _names_only(str(caught.value), prop)
    assert filesystems == [], "a filesystem was built with no credential of the table's own"


@pytest.mark.parametrize(("location", "config", "prop"), _TABLE_CREDENTIALS)
def test_a_null_vended_credential_is_no_credential(off, server, filesystems, location, config, prop):
    server.location = location
    server.table_config = _with(config, None)
    with pytest.raises(ValueError) as caught:
        _open_data_file(server)
    assert GCP_FLAG in str(caught.value) or AWS_FLAG in str(caught.value)
    assert filesystems == []


@pytest.mark.parametrize("value", ["", "   ", "\t"])
@pytest.mark.parametrize(("location", "config", "prop"), _TABLE_CREDENTIALS)
def test_a_blank_vended_credential_is_no_credential(off, server, filesystems, location, config, prop, value):
    server.location = location
    server.table_config = _with(config, value)
    with pytest.raises(ValueError):
        _open_data_file(server)
    assert filesystems == []


@pytest.mark.parametrize(("location", "config", "prop"), _TABLE_CREDENTIALS)
def test_a_text_credential_still_opens_storage(off, server, filesystems, location, config, prop):
    server.location = location
    server.table_config = _with(config, _SECRET)
    _open_data_file(server)
    assert len(filesystems) == 1


@pytest.mark.parametrize("value", _FALSY_OR_NULL)
@pytest.mark.parametrize("where", ["overrides", "defaults"])
def test_s3_keys_from_the_servers_config_that_are_not_text(off, server, filesystems, where, value):
    server.location = _S3
    server.table_config = {"s3.region": "us-east-1"}
    getattr(server, where).update({"s3.access-key-id": value, "s3.secret-access-key": value})
    with pytest.raises(ValueError):
        _open_data_file(server)
    assert filesystems == []


@pytest.mark.parametrize("value", _NOT_TEXT)
def test_s3_keys_in_the_connectors_settings_that_are_not_text(mode, server, filesystems, value):
    server.location = _S3
    server.table_config = {"s3.region": "us-east-1"}
    with pytest.raises(ValueError) as caught:
        _open_data_file(server, **{"s3.access-key-id": value, "s3.secret-access-key": value})
    _names_only(str(caught.value), "s3.access-key-id")
    assert filesystems == []


# --- a FileIO outside a catalog -----------------------------------------------


@pytest.mark.parametrize("value", _NOT_TEXT)
@pytest.mark.parametrize(("location", "config", "prop"), _TABLE_CREDENTIALS)
def test_a_fileio_outside_a_catalog(mode, filesystems, location, config, prop, value):
    from connectors.iceberg_credential_guard import guarded_file_io

    with pytest.raises(ValueError) as caught:
        guarded_file_io(_with(config, value), location).new_input(f"{location}/data/f.parquet")
    _names_only(str(caught.value), prop)
    assert filesystems == []


# --- REST SigV4 and Glue sign with boto3 --------------------------------------


@pytest.mark.parametrize("value", _FALSY_OR_NULL + [pytest.param(1, id="one")])
def test_rest_sigv4_with_client_keys_that_are_not_text(off, server, sessions, value):
    from connectors.iceberg_catalog import load_catalog
    from services.platform_aws_credentials import iceberg_connector_refusal

    endpoint = _rest_endpoint(**{
        "rest.sigv4-enabled": "true",
        "client.access-key-id": value,
        "client.secret-access-key": value,
        "client.region": "us-east-1",
    })
    assert iceberg_connector_refusal(endpoint)
    with pytest.raises(ValueError) as caught:
        load_catalog(endpoint)
    assert "client." in str(caught.value)
    assert _keyless(sessions) == [], "a boto3 session was built without the connector's keys"


@pytest.mark.parametrize("value", _FALSY_OR_NULL)
def test_a_rest_servers_override_cannot_blank_the_sigv4_keys(off, server, sessions, value):
    from connectors.iceberg_catalog import load_catalog

    server.overrides.update({"client.access-key-id": value, "client.secret-access-key": value})
    endpoint = _rest_endpoint(**{
        "rest.sigv4-enabled": "true",
        "client.access-key-id": "AKIATENANT",
        "client.secret-access-key": _SECRET,
        "client.region": "us-east-1",
    })
    with pytest.raises(ValueError):
        load_catalog(endpoint)
    assert _keyless(sessions) == []


@pytest.mark.parametrize("value", _FALSY_OR_NULL + [pytest.param(1, id="one")])
@pytest.mark.parametrize("prefix", ["glue", "client"])
def test_glue_with_keys_that_are_not_text(off, sessions, prefix, value):
    from connectors.iceberg_catalog import load_catalog
    from services.platform_aws_credentials import iceberg_connector_refusal

    endpoint = {
        "type": "iceberg", "name": "ice", "table": "t", "region": "us-east-1",
        "warehouse": "s3://bucket/wh",
        "extra": {
            "catalog_type": "glue",
            f"{prefix}.access-key-id": value,
            f"{prefix}.secret-access-key": value,
            "glue.endpoint": "https://glue.tenant.example",
        },
    }
    assert iceberg_connector_refusal(endpoint)
    with pytest.raises(ValueError):
        load_catalog(endpoint)
    assert _keyless(sessions) == []


def test_glue_with_text_keys_builds_its_client_with_them(off, sessions):
    from connectors.iceberg_catalog import load_catalog

    endpoint = {
        "type": "iceberg", "name": "ice", "table": "t", "region": "us-east-1",
        "warehouse": "s3://bucket/wh",
        "extra": {"catalog_type": "glue", "glue.access-key-id": "AKIATENANT", "glue.secret-access-key": _SECRET},
    }
    with pytest.raises(RuntimeError, match="intercepted"):
        load_catalog(endpoint)
    assert sessions and sessions[0]["aws_access_key_id"] == "AKIATENANT"


# --- the helpers themselves, and save -----------------------------------------

_HELPER_CASES = [
    pytest.param("s3_fileio_refusal", {"s3.access-key-id": "V", "s3.secret-access-key": "V"}, id="s3"),
    pytest.param("s3_fileio_refusal", {"client.access-key-id": "V", "client.secret-access-key": "V"}, id="s3-client"),
    pytest.param("s3_fileio_refusal", {"s3.access-key-id": "AKIA", "s3.secret-access-key": "x", "s3.session-token": "V"}, id="s3-session-token"),
    pytest.param("gcs_fileio_refusal", {"gcs.oauth2.token": "V"}, id="gcs"),
    pytest.param("azure_fileio_refusal", {"adls.account-key": "V"}, id="adls-key"),
    pytest.param("azure_fileio_refusal", {"adls.sas-token": "V"}, id="adls-sas"),
    pytest.param("azure_fileio_refusal", {"adls.client-id": "V", "adls.client-secret": "x", "adls.tenant-id": "t"}, id="adls-client-id"),
    pytest.param("rest_sigv4", {"client.access-key-id": "V", "client.secret-access-key": "V"}, id="sigv4"),
    pytest.param("glue", {"glue.access-key-id": "V", "glue.secret-access-key": "V"}, id="glue"),
    pytest.param("glue", {"glue.access-key-id": "AKIA", "glue.secret-access-key": "x", "glue.session-token": "V"}, id="glue-session-token"),
    pytest.param("rest", {"glue.profile-name": "V"}, id="profile-name"),
]


def _helper(name: str):
    from services import platform_aws_credentials as aws
    from services import platform_gcp_azure_credentials as gcp

    if name == "rest_sigv4":
        return lambda props: aws.iceberg_credentials_refusal("rest", {**props, "rest.sigv4-enabled": "true"})
    if name in {"glue", "rest"}:
        return lambda props: aws.iceberg_credentials_refusal(name, props)
    return getattr(aws, name, None) or getattr(gcp, name)


@pytest.mark.parametrize("value", _NOT_TEXT)
@pytest.mark.parametrize(("helper", "props"), _HELPER_CASES)
def test_every_helper_refuses_a_credential_that_is_not_text(mode, helper, props, value):
    refusal = _helper(helper)(_with(props, value))
    prop = next(key for key, val in props.items() if val == "V")
    _names_only(refusal, prop)


@pytest.mark.parametrize(("helper", "props"), _HELPER_CASES)
def test_every_helper_accepts_text(off, helper, props):
    assert _helper(helper)(_with(props, _SECRET)) == "" or helper == "rest"


@pytest.mark.parametrize("value", _NOT_TEXT)
@pytest.mark.parametrize(
    "extra",
    [
        {"s3.access-key-id": "V", "s3.secret-access-key": "V"},
        {"gcs.oauth2.token": "V"},
        {"adls.sas-token": "V"},
        {"rest.sigv4-enabled": "true", "client.access-key-id": "V", "client.secret-access-key": "V"},
    ],
    ids=["s3", "gcs", "adls", "sigv4"],
)
def test_save_refuses_a_credential_that_is_not_text(mode, extra, value):
    from services.egress_guard import connector_save_refusal

    endpoint = _rest_endpoint(**_with(extra, value))
    refusal = connector_save_refusal(endpoint)
    prop = next(key for key, val in extra.items() if val == "V")
    _names_only(refusal, prop)


@pytest.mark.parametrize("value", _NOT_TEXT)
def test_save_refuses_a_top_level_credential_that_is_not_text(mode, value):
    from services.egress_guard import connector_save_refusal

    endpoint = {**_rest_endpoint(), "s3.access-key-id": value, "s3.secret-access-key": "x"}
    _names_only(connector_save_refusal(endpoint), "s3.access-key-id")


def test_save_accepts_text_credentials(off):
    from services.egress_guard import connector_save_refusal

    endpoint = _rest_endpoint(**{"s3.access-key-id": "AKIATENANT", "s3.secret-access-key": _SECRET})
    assert connector_save_refusal(endpoint) == ""
