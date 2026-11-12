"""Follow-up review 1: an Iceberg FileIO never opens Google Cloud or Azure storage with the platform's credentials.

pyiceberg 0.12's ``PyArrowFileIO`` opens ``gs://`` / ``gcs://`` with
``pyarrow.fs.GcsFileSystem``, passing only ``gcs.oauth2.token`` (+ expiry) and
``gcs.service.host``, and ``abfs[s]://`` / ``wasb[s]://`` with
``pyarrow.fs.AzureFileSystem``, passing ``adls.account-key``,
``adls.sas-token``, ``adls.client-id`` / ``-secret`` / ``tenant-id`` and the
``adls.*-storage-authority`` / ``-scheme`` endpoint overrides. With no
credential, pyarrow's C++ clients use the API host's own: Google Application
Default Credentials and Azure ``DefaultAzureCredential`` (pyarrow's docstrings
say so). Neither lookup is a Python call a test can intercept, so the tests
record the options each filesystem is constructed with: a ``GcsFileSystem``
without ``access_token`` (pyiceberg never passes ``anonymous``) or an
``AzureFileSystem`` without a key, SAS or client secret is one that signs with
the host's identity. The ``oss://`` scheme goes through pyarrow's S3 client the
same way, and ``KeyedS3PyArrowFileIO`` guarded only ``s3://``. A tenant runs
its own REST catalog, so a table's location, its config and the server's
``/v1/config`` are the tenant's; a SQL catalog builds the IO for a table's
metadata file from its own properties, outside ``_load_file_io``.

Off (the default) such a filesystem is refused with the remedies and no value
from the config. ``DATAFLOW_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS=1`` (a
single-tenant deployment whose connectors use the instance's Google or Azure
identity) allows it, except toward an endpoint a property names. FsspecFileIO's
``gs`` / ``abfs`` paths (gcsfs / adlfs) are not installed, so not reachable.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

FLAG = "DATAFLOW_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS"
AWS_FLAG = "DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS"
_REST = "https://catalog.example.com/v1"
_SECRET = "tenant-secret-value"
_KEYS = {"s3.access-key-id": "AKIATENANT", "s3.secret-access-key": _SECRET, "s3.region": "us-east-1"}
_TENANT_HOST = "storage.tenant.example"
_ARROW = "pyiceberg.io.pyarrow.PyArrowFileIO"
_LOCAL_URI = "connectors.iceberg_pyarrow_io.LocalUriPyArrowFileIO"
_FSSPEC = "pyiceberg.io.fsspec.FsspecFileIO"

_GCS = "gs://bucket/wh/ns/t"
_AZURE = "abfss://container@tenantacct.dfs.core.windows.net/wh/ns/t"
_ACCOUNT = {"adls.account-name": "tenantacct"}

#: Table configs a REST catalog may return that leave the FileIO without a GCS / Azure credential.
_NO_CREDENTIAL = [
    pytest.param({}, id="keyless"),
    pytest.param(dict(_KEYS), id="s3-keys"),
    pytest.param({**_KEYS, "py-io-impl": _ARROW}, id="names-pyarrow"),
    pytest.param({**_KEYS, "py-io-impl": _LOCAL_URI}, id="names-local-uri"),
]


@pytest.fixture(params=["off", "on"])
def mode(request, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "ALLOW_PLATFORM_AWS_CREDENTIALS"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    if request.param == "on":
        monkeypatch.setenv(FLAG, "1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )
    return request.param


@pytest.fixture(params=["aws-off", "aws-on"])
def aws(request, monkeypatch):
    """The AWS opt-in decides which FileIO a keyless catalog gets; neither may open GCS / Azure."""
    if request.param == "aws-on":
        monkeypatch.setenv(AWS_FLAG, "1")
    return request.param


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


def _ambient(seen: list[tuple[str, dict]]) -> list[str]:
    """The filesystems built with no credential of their own: they sign with the API host's identity."""
    own = {
        "GcsFileSystem": ("access_token",),
        "AzureFileSystem": ("account_key", "sas_token", "client_secret"),
        "S3FileSystem": ("access_key", "anonymous"),
    }
    return [name for name, kwargs in seen if not any(kwargs.get(k) for k in own[name])]


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


def _open_data_file(server) -> None:
    from connectors.iceberg_catalog import load_table

    table = load_table(_rest_endpoint())
    table.io.new_input(f"{server.location}/data/f.parquet")


def _refused(message: str, *names: str) -> None:
    for name in names:
        assert name in message, message
    for value in (_SECRET, _TENANT_HOST, "tenantacct", "bucket", "ya29"):
        assert value not in message, message


# --- a REST table's GCS / Azure files with no credential ----------------------


@pytest.mark.parametrize("config", _NO_CREDENTIAL)
@pytest.mark.parametrize("scheme", ["gs", "gcs"])
def test_a_tables_gcs_files_without_a_token(mode, aws, server, filesystems, config, scheme):
    server.location = _GCS.replace("gs://", f"{scheme}://")
    server.table_config = dict(config)
    if mode == "off":
        with pytest.raises(ValueError) as caught:
            _open_data_file(server)
        _refused(str(caught.value), FLAG, "gcs.oauth2.token")
        assert filesystems == []
    else:
        _open_data_file(server)
        assert _ambient(filesystems) == ["GcsFileSystem"]


@pytest.mark.parametrize("config", _NO_CREDENTIAL)
@pytest.mark.parametrize("scheme", ["abfss", "wasbs"])
def test_a_tables_azure_files_without_a_credential(mode, aws, server, filesystems, config, scheme):
    server.location = _AZURE.replace("abfss://", f"{scheme}://")
    server.table_config = {**_ACCOUNT, **config}
    if mode == "off":
        with pytest.raises(ValueError) as caught:
            _open_data_file(server)
        _refused(str(caught.value), FLAG, "adls.sas-token")
        assert filesystems == []
    else:
        _open_data_file(server)
        assert _ambient(filesystems) == ["AzureFileSystem"]


# --- an endpoint a property names never gets the platform's identity ----------


@pytest.mark.parametrize("where", ["table_config", "overrides", "defaults"])
def test_a_gcs_service_host_without_a_token_is_refused_in_both_modes(mode, server, filesystems, where):
    getattr(server, where).update({"gcs.service.host": f"https://{_TENANT_HOST}"})
    with pytest.raises(ValueError) as caught:
        _open_data_file(server)
    _refused(str(caught.value), "gcs.service.host")
    assert filesystems == []


@pytest.mark.parametrize(
    "prop",
    [
        "adls.blob-storage-authority",
        "adls.dfs-storage-authority",
        "adls.blob-storage-scheme",
        "adls.dfs-storage-scheme",
    ],
)
def test_an_azure_endpoint_override_without_a_credential_is_refused_in_both_modes(
    mode, server, filesystems, prop
):
    server.location = _AZURE
    value = "http" if prop.endswith("scheme") else f"{_TENANT_HOST}:443"
    server.table_config = {**_ACCOUNT, prop: value}
    with pytest.raises(ValueError) as caught:
        _open_data_file(server)
    _refused(str(caught.value), prop)
    assert filesystems == []


# --- the table's own (vended) credential is used ------------------------------


def test_a_vended_gcs_token_opens_gcs(mode, aws, server, filesystems):
    server.table_config = {
        "gcs.oauth2.token": "ya29.vended",
        "gcs.oauth2.token-expires-at": "4102444800000",
        "gcs.service.host": f"https://{_TENANT_HOST}",
    }
    _open_data_file(server)
    assert filesystems[-1][0] == "GcsFileSystem"
    assert filesystems[-1][1]["access_token"] == "ya29.vended"
    assert _ambient(filesystems) == []


@pytest.mark.parametrize(
    "own",
    [
        {"adls.sas-token": "sv=vended"},
        {"adls.account-key": _SECRET},
        {"adls.client-id": "c", "adls.client-secret": _SECRET, "adls.tenant-id": "t"},
    ],
    ids=["sas", "account-key", "client-secret"],
)
def test_an_azure_credential_of_its_own_opens_azure(mode, aws, server, filesystems, own):
    server.location = _AZURE
    server.table_config = {**_ACCOUNT, **own, "adls.dfs-storage-authority": f"{_TENANT_HOST}:443"}
    _open_data_file(server)
    assert filesystems[-1][0] == "AzureFileSystem"
    assert _ambient(filesystems) == []


# --- outside a REST table -----------------------------------------------------


@pytest.mark.parametrize("io_impl", [None, _ARROW, _LOCAL_URI])
def test_a_fileio_built_outside_a_catalog(mode, aws, filesystems, io_impl):
    from connectors.iceberg_credential_guard import guarded_file_io

    props = {**_KEYS, **({"py-io-impl": io_impl} if io_impl else {})}
    io = guarded_file_io(props, _GCS)
    if mode == "off":
        with pytest.raises(ValueError):
            io.new_input(f"{_GCS}/data/f.parquet")
        assert filesystems == []
    else:
        io.new_input(f"{_GCS}/data/f.parquet")
        assert _ambient(filesystems) == ["GcsFileSystem"]


def _sql_catalog(tmp_path, warehouse: str, **props):
    from pyiceberg.catalog.sql import SqlCatalog

    from connectors.iceberg_credential_guard import guarded_catalog

    return guarded_catalog(SqlCatalog)(
        "c", uri=f"sqlite:///{tmp_path / 'c.db'}", warehouse=warehouse, **props
    )


def _schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    return Schema(NestedField(1, "id", LongType(), required=False))


@pytest.mark.parametrize(
    ("warehouse", "props"),
    [("gs://bucket/wh", dict(_KEYS)), ("abfss://c@tenantacct.dfs.core.windows.net/wh", {**_KEYS, **_ACCOUNT})],
    ids=["gcs", "azure"],
)
def test_a_sql_catalogs_own_metadata_io(mode, tmp_path, filesystems, warehouse, props):
    """SqlCatalog writes and reads a table's metadata file with load_file_io(self.properties)."""
    from pyiceberg.io import load_file_io

    catalog = _sql_catalog(tmp_path, warehouse, **props)
    catalog.create_namespace("ns")
    if mode == "off":
        with pytest.raises(ValueError) as caught:
            catalog.create_table(("ns", "t"), schema=_schema())
        _refused(str(caught.value), FLAG)
        assert filesystems == []
    else:
        io = load_file_io(catalog.properties, f"{warehouse}/ns/t/metadata/v1.metadata.json")
        io.new_output(f"{warehouse}/ns/t/metadata/v1.metadata.json")
        assert len(_ambient(filesystems)) == 1


def test_a_sql_catalogs_own_metadata_io_without_s3_keys(mode, tmp_path, filesystems):
    """The same path for S3: the AWS opt-in is off, so the keyless S3 metadata write is refused."""
    catalog = _sql_catalog(tmp_path, "s3://bucket/wh", **{"s3.region": "us-east-1"})
    catalog.create_namespace("ns")
    with pytest.raises(ValueError) as caught:
        catalog.create_table(("ns", "t"), schema=_schema())
    _refused(str(caught.value), AWS_FLAG)
    assert filesystems == []


def test_an_oss_location_without_s3_keys(mode, aws, filesystems):
    """pyiceberg opens oss:// with pyarrow's S3 client too; it is the AWS opt-in's to allow."""
    from connectors.iceberg_credential_guard import guarded_file_io

    io = guarded_file_io({"s3.region": "us-east-1"}, "oss://bucket/wh/t")
    if aws == "aws-off":
        with pytest.raises(ValueError) as caught:
            io.new_input("oss://bucket/wh/t/data/f.parquet")
        _refused(str(caught.value), AWS_FLAG)
        assert filesystems == []
    else:
        io.new_input("oss://bucket/wh/t/data/f.parquet")
        assert _ambient(filesystems) == ["S3FileSystem"]


def test_a_local_warehouse_needs_no_cloud_credential(mode, tmp_path):
    from connectors.iceberg_credential_guard import guarded_file_io

    location = (tmp_path / "f.bin").as_uri()
    io = guarded_file_io({}, location)
    with io.new_output(location).create() as out:
        out.write(b"ok")
    with io.new_input(location).open() as handle:
        assert handle.read() == b"ok"


@pytest.mark.parametrize("location", [_GCS, _AZURE])
def test_fsspec_gcs_and_azure_are_not_installed(location):
    """FsspecFileIO's gs / abfs filesystems (gcsfs / adlfs) fall back to ADC / DefaultAzureCredential.

    Neither package is a dependency (requirements: pyiceberg[pyarrow,...,s3fs]),
    so those paths cannot be reached. If this fails, one was installed: guard
    FsspecFileIO's gs / abfs paths as LocalUriPyArrowFileIO guards pyarrow's.
    """
    from pyiceberg.io.fsspec import FsspecFileIO

    with pytest.raises(ModuleNotFoundError):
        FsspecFileIO({**_ACCOUNT}).new_input(f"{location}/data/f.parquet")


# --- save ---------------------------------------------------------------------


def _sql_connector(warehouse: str, field: str = "warehouse") -> dict:
    return {
        "type": "iceberg",
        "name": "ice",
        "schema": "ns",
        "table": "t",
        "connection_string": "postgresql://catalog.example.com/iceberg",
        field: warehouse,
        "extra": {"catalog_type": "sql"},
    }


@pytest.mark.parametrize(
    "warehouse",
    ["gs://bucket/wh", "gcs://bucket/wh", "abfss://c@tenantacct.dfs.core.windows.net/wh", "wasbs://c@tenantacct.blob.core.windows.net/wh"],
)
@pytest.mark.parametrize("field", ["warehouse", "database"])
def test_save_refuses_a_non_rest_catalog_on_gcs_or_azure(mode, warehouse, field):
    from services.egress_guard import connector_save_refusal

    refusal = connector_save_refusal(_sql_connector(warehouse, field))
    if mode == "off":
        _refused(refusal, FLAG, warehouse.split("://")[0] + "://")
    else:
        assert FLAG not in refusal


def test_save_accepts_a_rest_catalog_that_may_vend_credentials(mode):
    from services.egress_guard import connector_save_refusal

    endpoint = _rest_endpoint()
    endpoint["warehouse"] = "gs://bucket/wh"
    assert FLAG not in connector_save_refusal(endpoint)


# --- startup ------------------------------------------------------------------


def test_startup_warning_and_snapshot(mode, caplog):
    from services.platform_gcp_azure_credentials import (
        platform_gcp_azure_settings,
        warn_if_platform_gcp_azure_credentials_allowed,
    )

    logger = logging.getLogger("datawrap.security.test")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        warn_if_platform_gcp_azure_credentials_allowed(logger)
    warned = [r.getMessage() for r in caplog.records if r.name == logger.name]
    if mode == "off":
        assert warned == []
        assert platform_gcp_azure_settings() == {"platform_gcp_azure_chain": "refused"}
    else:
        assert len(warned) == 1 and FLAG in warned[0]
        assert platform_gcp_azure_settings() == {"platform_gcp_azure_chain": "allowed"}
