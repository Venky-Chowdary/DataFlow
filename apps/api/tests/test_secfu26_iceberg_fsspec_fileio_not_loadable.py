"""Follow-up review 2, L1: no Iceberg setting can select pyiceberg's FsspecFileIO.

``pyiceberg.io.fsspec.FsspecFileIO`` was in the FileIO allowlist, but none of
its ``gs`` / ``gcs`` / ``abfs`` / ``abfss`` / ``hf`` filesystems is guarded:
gcsfs and adlfs fall back to Google Application Default Credentials and
``DefaultAzureCredential`` (the API host's identity), and huggingface_hub to
the host's Hugging Face token. It was safe only because none of those packages
is installed. Datawrap never selects FsspecFileIO itself (every guarded FileIO
is a PyArrow one: ``connectors.iceberg_pyarrow_io``), so it is no longer
accepted from any source: the connector's settings, the REST server's
configuration, a table's config, or a FileIO built outside a catalog.

The last test fails if FsspecFileIO is ever allowed again while gcsfs, adlfs or
huggingface_hub is installed.
"""

from __future__ import annotations

import importlib.util
import json
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

_FSSPEC = "pyiceberg.io.fsspec.FsspecFileIO"
_REST = "https://catalog.example.com/v1"
_KEYS = {"s3.access-key-id": "AKIATENANT", "s3.secret-access-key": "tenant-secret", "s3.region": "us-east-1"}
_UNGUARDED_FSSPEC_PACKAGES = ("gcsfs", "adlfs", "huggingface_hub")


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "ALLOW_PLATFORM_AWS_CREDENTIALS"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )


@pytest.fixture(params=["off", "on"])
def mode(request, monkeypatch):
    if request.param == "on":
        monkeypatch.setenv("DATAFLOW_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "1")
        monkeypatch.setenv("DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS", "1")
    return request.param


def _table_response(location: str, config: dict) -> dict:
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
    from pyiceberg.types import LongType, NestedField

    md = new_table_metadata(
        Schema(NestedField(1, "id", LongType(), required=False)),
        UNPARTITIONED_PARTITION_SPEC, UNSORTED_SORT_ORDER, location, {},
    )
    return {
        "metadata-location": f"{location}/metadata/v1.metadata.json",
        "metadata": json.loads(md.model_dump_json()),
        "config": config,
    }


@pytest.fixture
def server(monkeypatch):
    import requests

    state = SimpleNamespace(defaults={}, overrides={}, table_config={})

    def request(self, method, url, *args, **kwargs):
        path = urlsplit(url).path
        if path.endswith("/config"):
            body = {"defaults": state.defaults, "overrides": state.overrides}
        elif path.endswith("/tables/t"):
            body = _table_response("s3://bucket/wh/ns/t", state.table_config)
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


def _endpoint(**extra) -> dict:
    return {
        "type": "iceberg", "name": "ice", "schema": "ns", "table": "t",
        "connection_string": _REST, "extra": {"catalog_type": "rest", **_KEYS, **extra},
    }


def test_fsspec_is_not_an_allowed_fileio():
    from services.iceberg_class_properties import ALLOWED_FILE_IO

    assert _FSSPEC not in ALLOWED_FILE_IO
    assert all(name.startswith(("pyiceberg.io.pyarrow.", "connectors.iceberg_pyarrow_io.")) for name in ALLOWED_FILE_IO)


@pytest.mark.parametrize("where", ["table_config", "overrides", "defaults"])
def test_a_catalog_server_cannot_select_it(mode, server, where):
    from connectors.iceberg_catalog import load_table

    getattr(server, where)["py-io-impl"] = _FSSPEC
    with pytest.raises(ValueError) as caught:
        load_table(_endpoint())
    assert "py-io-impl" in str(caught.value)


def test_the_connectors_settings_cannot_select_it(mode, server):
    from connectors.iceberg_catalog import load_catalog
    from services.egress_guard import connector_save_refusal

    assert "py-io-impl" in connector_save_refusal(_endpoint(**{"py-io-impl": _FSSPEC}))
    with pytest.raises(ValueError):
        load_catalog(_endpoint(**{"py-io-impl": _FSSPEC}))


@pytest.mark.parametrize("location", ["gs://b/wh/t", "abfss://c@acct.dfs.core.windows.net/wh/t", "hf://datasets/x/y", "s3://b/wh/t"])
def test_a_fileio_outside_a_catalog_cannot_select_it(mode, location):
    from connectors.iceberg_credential_guard import guarded_file_io

    with pytest.raises(ValueError):
        guarded_file_io({**_KEYS, "py-io-impl": _FSSPEC}, location)


def test_the_pyarrow_fileios_are_still_accepted(server):
    from connectors.iceberg_catalog import load_table

    server.table_config = {"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"}
    assert type(load_table(_endpoint()).io).__name__ == "LocalUriPyArrowFileIO"


@pytest.mark.parametrize("package", _UNGUARDED_FSSPEC_PACKAGES)
def test_fsspec_stays_refused_while_an_unguarded_filesystem_is_installed(package):
    """If this fails, FsspecFileIO came back while gcsfs / adlfs / huggingface_hub is installed:
    guard its gs / abfs / hf paths as ``LocalUriPyArrowFileIO`` guards pyarrow's first."""
    from services.iceberg_class_properties import ALLOWED_FILE_IO

    installed = importlib.util.find_spec(package) is not None
    assert not (installed and _FSSPEC in ALLOWED_FILE_IO), package
    assert _FSSPEC not in ALLOWED_FILE_IO
