"""Follow-up review 2 (to verify): a tenant's Iceberg catalog cannot make the API read or write files on its host.

A REST catalog's table response is the tenant's: the metadata names the
manifest list, the manifest list names manifests, the manifests name data and
delete files. pyiceberg opens each through the table's FileIO
(``new_input`` / ``new_output`` / ``delete``), and the guarded FileIO
(``LocalUriPyArrowFileIO`` / ``KeyedS3PyArrowFileIO``) opened a ``file://`` or
bare path on the API host as it was given. 5576b354c confined only the
destination COUNT (``dest_precount``); ``table.scan().to_arrow()`` (the Iceberg
reader), ``plan_files``, ``inspect`` and an append went straight to the host's
files: a catalog could return another workspace's Parquet export, or any
Parquet file the API process can read, as the tenant's rows, or have the API
write into a host directory.

Now the guarded FileIO refuses every local location a remote catalog (REST,
Glue, Hive) listed, and a local catalog's (SQL / Hadoop) local location must pass
the Iceberg jail (``iceberg_catalog._jail_local_path``: under
``DATAFLOW_ICEBERG_ROOT``, else the data folder's ``iceberg`` area while
confinement is on). Messages name no path.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest

_REST = "https://catalog.example.com/v1"
_KEYS = {"s3.access-key-id": "AKIATENANT", "s3.secret-access-key": "tenant-secret", "s3.region": "us-east-1"}


class Dialled(Exception):
    """A remote filesystem was about to dial (recorded)."""


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "ALLOW_PLATFORM_AWS_CREDENTIALS", "ICEBERG_ROOT",
                     "CONFINE_LOCAL_FILES"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )


@pytest.fixture
def host_table(tmp_path):
    """A real Iceberg table in a folder on the API host (another workspace's, say)."""
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog

    host = tmp_path / "host"
    host.mkdir()
    catalog = SqlCatalog("host", uri=f"sqlite:///{host}/c.db", warehouse=f"file://{host}/wh")
    catalog.create_namespace("ns")
    table = catalog.create_table("ns.secret", schema=pa.schema([("v", pa.string())]))
    table.append(pa.table({"v": ["host-secret-1", "host-secret-2"]}))
    return catalog.load_table("ns.secret")


@pytest.fixture
def opened(monkeypatch):
    """Every local file pyarrow opens or creates for a FileIO."""
    import pyarrow.fs

    paths: list[str] = []
    real_in, real_out = pyarrow.fs.LocalFileSystem.open_input_file, pyarrow.fs.LocalFileSystem.open_output_stream

    class Recording(pyarrow.fs.LocalFileSystem):
        pass

    def open_input_file(self, path, *a, **k):
        paths.append(str(path))
        return real_in(self, path, *a, **k)

    def open_output_stream(self, path, *a, **k):
        paths.append(str(path))
        return real_out(self, path, *a, **k)

    from pyiceberg.io import pyarrow as io_pyarrow

    monkeypatch.setattr(io_pyarrow.PyArrowFileIO, "_initialize_local_fs",
                        lambda self: Recording())
    monkeypatch.setattr(Recording, "open_input_file", open_input_file, raising=False)
    monkeypatch.setattr(Recording, "open_output_stream", open_output_stream, raising=False)
    return paths


@pytest.fixture
def rest(monkeypatch, host_table):
    """A tenant REST catalog that answers every table load with the host table's metadata."""
    import requests

    metadata = json.loads(host_table.metadata.model_dump_json(by_alias=True))

    def request(self, method, url, *args, **kwargs):
        path = urlsplit(url).path
        if path.endswith("/config"):
            body = {"defaults": {}, "overrides": {}}
        elif "/tables/" in path and method.upper() == "GET":
            body = {"metadata-location": host_table.metadata_location, "metadata": metadata, "config": {}}
        else:
            raise AssertionError(f"unexpected catalog call {method} {path}")
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(body).encode()
        response.headers["Content-Type"] = "application/json"
        response.url = url
        return response

    monkeypatch.setattr(requests.Session, "request", request)
    return host_table


def _endpoint() -> dict:
    return {"type": "iceberg", "name": "ice", "schema": "ns", "table": "secret",
            "connection_string": _REST, "extra": {"catalog_type": "rest", **_KEYS}}


def _rest_table():
    from connectors.iceberg_catalog import load_catalog

    return load_catalog(_endpoint()).load_table(("ns", "secret"))


def _refused(message: str, tmp: Path) -> None:
    assert "local" in message.lower(), message
    assert str(tmp) not in message and "host-secret" not in message, message


@pytest.mark.parametrize("confine", ["", "1"])
def test_a_rest_scan_never_reads_the_hosts_files(rest, opened, tmp_path, monkeypatch, confine):
    if confine:
        monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", confine)
    with pytest.raises(ValueError) as caught:
        _rest_table().scan().to_arrow()
    _refused(str(caught.value), tmp_path)
    assert opened == []


def test_rest_plan_files_never_reads_the_hosts_manifest_list(rest, opened, tmp_path):
    with pytest.raises(ValueError) as caught:
        list(_rest_table().scan().plan_files())
    _refused(str(caught.value), tmp_path)
    assert opened == []


def test_rest_inspect_never_reads_the_hosts_manifests(rest, opened, tmp_path):
    with pytest.raises(ValueError):
        _rest_table().inspect.entries()
    assert opened == []


def test_the_iceberg_reader_never_returns_the_hosts_rows(rest, opened, tmp_path):
    from connectors.iceberg_reader import read_table_batch

    with pytest.raises(ValueError) as caught:
        read_table_batch(cfg=_endpoint(), table="secret", limit=10)
    _refused(str(caught.value), tmp_path)
    assert opened == []


def test_a_rest_append_never_writes_into_a_host_folder(rest, opened, tmp_path):
    import pyarrow as pa

    with pytest.raises(ValueError):
        _rest_table().append(pa.table({"v": ["tenant"]}))
    assert opened == []
    assert not [p for p in (tmp_path / "host" / "wh").rglob("*.parquet") if "host-secret" in p.name]


@pytest.mark.parametrize("location", ["file:///etc/hostname", "/etc/hostname", "file:/etc/hostname"])
def test_a_rest_fileio_refuses_any_local_location(rest, opened, location):
    from connectors.iceberg_catalog import load_catalog

    io = load_catalog(_endpoint())._load_file_io({}, location="s3://bucket/t")
    for action in (lambda: io.new_input(location).open(), lambda: io.new_output(location).create(),
                   lambda: io.delete(location)):
        with pytest.raises(ValueError):
            action()
    assert opened == []


def test_a_server_cannot_turn_the_refusal_off(rest, opened):
    from connectors.iceberg_catalog import load_catalog

    io = load_catalog(_endpoint())._load_file_io({"dataflow.local-files": "jail"}, location="s3://bucket/t")
    with pytest.raises(ValueError):
        io.new_input("file:///etc/hostname").open()


def test_a_rest_s3_location_still_reaches_s3(rest, monkeypatch):
    import pyarrow.fs

    from connectors.iceberg_catalog import load_catalog

    def s3(*_a, **_k):
        raise Dialled("s3")

    monkeypatch.setattr(pyarrow.fs, "S3FileSystem", s3)
    io = load_catalog(_endpoint())._load_file_io({}, location="s3://bucket/t")
    with pytest.raises(Dialled):
        io.new_input("s3://bucket/t/data/f.parquet").open()


# ------------------------------------------------------------------ local catalogs: the Iceberg jail


def test_a_local_fileio_opens_only_inside_the_iceberg_root(tmp_path, monkeypatch, opened):
    from connectors.iceberg_credential_guard import guarded_file_io

    root = tmp_path / "jail"
    root.mkdir()
    inside = root / "f.txt"
    inside.write_text("ok")
    outside = tmp_path / "outside.txt"
    outside.write_text("host")
    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(root))
    io = guarded_file_io({})
    with io.new_input(inside.as_uri()).open() as f:
        assert f.read() == b"ok"
    with pytest.raises(ValueError) as caught:
        io.new_input(outside.as_uri()).open()
    assert str(outside) not in str(caught.value)
    assert str(outside) not in opened
    with pytest.raises(ValueError):
        io.new_input(f"{root}/../outside.txt").open()


def test_a_local_sql_catalog_table_in_the_jail_still_reads(tmp_path, monkeypatch):
    import pyarrow as pa

    from connectors.iceberg_catalog import load_catalog

    root = tmp_path / "jail"
    root.mkdir()
    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(root))
    endpoint = {"type": "iceberg", "name": "local", "schema": "ns", "table": "t",
                "connection_string": str(root / "wh"), "extra": {"catalog_type": "sql"}}
    catalog = load_catalog(endpoint)
    catalog.create_namespace("ns")
    table = catalog.create_table("ns.t", schema=pa.schema([("v", pa.string())]))
    table.append(pa.table({"v": ["a", "b"]}))
    assert sorted(catalog.load_table("ns.t").scan().to_arrow().column("v").to_pylist()) == ["a", "b"]
