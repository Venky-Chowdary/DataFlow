"""Follow-up review 4 (M): an Iceberg table location cannot make pyarrow dial an HDFS namenode it chooses.

pyiceberg 0.12's ``PyArrowFileIO`` opens ``hdfs://`` / ``viewfs://`` with
``pyarrow.fs.HadoopFileSystem.from_uri(f"{scheme}://{netloc}")`` (or, with no
netloc, ``HadoopFileSystem(host=hdfs.host, kerb_ticket=hdfs.kerberos_ticket,
...)``). The netloc is the catalog's (a tenant-run REST, Glue, Hive or SQL
catalog writes every metadata, manifest and data location), and nothing judged
it: probe rev33/p16 reached ``from_uri("hdfs://169.254.169.254:80")``. Only
``hdfs.host`` from a REST server's config was checked.

Now every PyArrow FileIO here (``LocalUriPyArrowFileIO`` and
``KeyedS3PyArrowFileIO``) refuses HDFS / viewfs unless the connector itself
named that namenode (its own warehouse location; the guarded catalog records
it after any server's properties are merged, so neither a server nor a table
can add one), and the namenode passes the connector egress check
(``egress_guard.new_point_refusal``: metadata and link-local always refused).
``hdfs.kerberos_ticket`` (a ticket cache file on the API host) is refused
outside the areas a connector may open. Local and remote catalogs alike.
"""

from __future__ import annotations

import json
import socket
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

_KEYS = {"s3.access-key-id": "AKIATENANT", "s3.secret-access-key": "tenant-secret", "s3.region": "us-east-1"}
_OWN = "hdfs://nn.example.com:8020"


@pytest.fixture(autouse=True)
def made(tmp_path, monkeypatch):
    """Every HadoopFileSystem pyarrow would build (recorded instead of dialled)."""
    import pyarrow.fs

    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "ALLOW_PLATFORM_AWS_CREDENTIALS", "EGRESS_ENFORCE_NEW",
                     "EGRESS_ALLOWLIST", "SOURCE_PATH_ROOTS", "REQUIRE_AUTH"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(tmp_path / "iceroot"))
    (tmp_path / "iceroot").mkdir()
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(
        "socket.socket.connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled"))
    )
    monkeypatch.setattr("socket.getaddrinfo", lambda *_a, **_k: (_ for _ in ()).throw(socket.gaierror("no DNS here")))
    calls: list = []

    class Recorder:
        @staticmethod
        def from_uri(uri):
            calls.append(("from_uri", uri))
            raise RuntimeError("recorded")

        def __init__(self, **kwargs):
            calls.append(("init", kwargs))
            raise RuntimeError("recorded")

    monkeypatch.setattr(pyarrow.fs, "HadoopFileSystem", Recorder)
    return calls


def _open(io, location):
    try:
        io.new_input(location).open()
    except RuntimeError as exc:
        assert str(exc) == "recorded"
        return "dialled"
    return "opened"


def _bare_file_io(**props):
    from connectors.iceberg_credential_guard import file_io_properties
    from pyiceberg.io import load_file_io

    return load_file_io(file_io_properties({**_KEYS, **props}), "s3://bucket/t")


@pytest.mark.parametrize(
    "location",
    ["hdfs://169.254.169.254:80/t/metadata/v1.metadata.json", "viewfs://10.0.0.7:8020/x",
     "hdfs://nn.example.com:8020/wh/t/data/f.parquet", "VIEWFS://nn.example.com:8020/x"],
)
def test_the_reviewer_probe_a_namenode_nobody_named_is_refused(made, location):
    io = _bare_file_io()
    with pytest.raises(ValueError, match="(?i)hdfs"):
        _open(io, location)
    assert made == []


def test_no_netloc_and_a_supplied_host_or_ticket_is_refused(made):
    io = _bare_file_io(**{"hdfs.host": "169.254.169.254", "hdfs.port": "8020"})
    with pytest.raises(ValueError, match="(?i)hdfs"):
        _open(io, "hdfs:///t/metadata/v1.metadata.json")
    io = _bare_file_io(**{"hdfs.kerberos_ticket": "/tmp/krb5cc_0"})
    with pytest.raises(ValueError, match="(?i)hdfs"):
        _open(io, "hdfs:///t/metadata/v1.metadata.json")
    assert made == []


def test_the_property_that_names_namenodes_cannot_come_from_the_table(made):
    """A table's metadata properties are merged into the FileIO's; the guard writes the key last."""
    from connectors.iceberg_catalog import load_catalog

    catalog = load_catalog({"connection_string": "sqlite:///cat.db", "extra": {"catalog_type": "sql"},
                            "table": "t", "warehouse": "wh"})
    io = catalog._load_file_io({"dataflow.hdfs-namenodes": "hdfs://169.254.169.254:80"}, "file:///x")
    with pytest.raises(ValueError, match="(?i)hdfs"):
        _open(io, "hdfs://169.254.169.254:80/t/metadata/v1.metadata.json")
    assert made == []


def _local_catalog(warehouse: str):
    from connectors.iceberg_catalog import load_catalog

    return load_catalog({"connection_string": "sqlite:///cat.db", "extra": {"catalog_type": "sql"},
                         "table": "t", "warehouse": warehouse})


def test_a_local_catalog_reaches_only_the_namenode_its_connector_named(made):
    catalog = _local_catalog(f"{_OWN}/wh")
    io = catalog._load_file_io({}, f"{_OWN}/wh/ns/t")
    assert _open(io, f"{_OWN}/wh/ns/t/metadata/v1.metadata.json") == "dialled"
    assert made == [("from_uri", _OWN)]
    with pytest.raises(ValueError, match="(?i)hdfs"):
        _open(io, "hdfs://other.example.com:8020/wh/ns/t/data/f.parquet")
    assert made == [("from_uri", _OWN)]


def test_a_named_namenode_still_passes_the_egress_check(made):
    catalog = _local_catalog("hdfs://169.254.169.254:8020/wh")
    io = catalog._load_file_io({}, "hdfs://169.254.169.254:8020/wh/ns/t")
    with pytest.raises(ValueError, match="(?i)egress|metadata|link-local"):
        _open(io, "hdfs://169.254.169.254:8020/wh/ns/t/metadata/v1.metadata.json")
    assert made == []


@pytest.fixture
def rest(monkeypatch):
    """A tenant REST catalog whose /v1/config can name a warehouse of its own."""
    import requests

    state = SimpleNamespace(defaults={}, overrides={})

    def request(self, method, url, *args, **kwargs):
        parts = urlsplit(url)
        if not parts.path.endswith("/config"):
            raise AssertionError(f"unexpected catalog call {method} {parts.path}")
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({"defaults": state.defaults, "overrides": state.overrides}).encode()
        response.headers["Content-Type"] = "application/json"
        response.url = url
        response.request = requests.Request(method, url).prepare()
        return response

    monkeypatch.setattr(requests.Session, "request", request)
    return state


def _rest_catalog(warehouse: str = ""):
    from connectors.iceberg_catalog import load_catalog

    endpoint = {"connection_string": "https://catalog.example.com/v1", "extra": {"catalog_type": "rest", **_KEYS},
                "table": "t"}
    if warehouse:
        endpoint["warehouse"] = warehouse
    return load_catalog(endpoint)


def test_a_rest_server_cannot_add_a_namenode(made, rest):
    rest.overrides = {"warehouse": "hdfs://10.0.0.7:8020/wh"}
    catalog = _rest_catalog()
    io = catalog._load_file_io({"warehouse": "hdfs://10.0.0.7:8020/wh"}, "hdfs://10.0.0.7:8020/wh/ns/t")
    with pytest.raises(ValueError, match="(?i)hdfs"):
        _open(io, "hdfs://10.0.0.7:8020/wh/ns/t/metadata/v1.metadata.json")
    assert made == []


def test_a_rest_connector_reaches_the_namenode_it_named(made, rest):
    catalog = _rest_catalog(f"{_OWN}/wh")
    io = catalog._load_file_io({}, f"{_OWN}/wh/ns/t")
    assert _open(io, f"{_OWN}/wh/ns/t/metadata/v1.metadata.json") == "dialled"
    assert made == [("from_uri", _OWN)]
