"""Follow-up review 1: an Iceberg catalog never loads a Python class a tenant or its catalog server names.

pyiceberg imports and instantiates the class a property names by dotted path:
the FileIO (``py-io-impl``), the catalog (``py-catalog-impl``), the S3 retry
strategy (``s3.retry-strategy-impl``), the location provider
(``write.py-location-provider.impl``, a table property) and a REST catalog's
auth manager (``auth: {type: custom, impl: ...}``, or a dotted ``type``). A
tenant runs its own REST catalog server, so those properties may come from the
connector's settings, from the server's ``/v1/config`` defaults and overrides,
from a table's load response (its config, its metadata properties, its
storage credentials) and from a commit response.

Only the FileIO classes Datawrap ships are accepted (pyiceberg's
``PyArrowFileIO`` and ``FsspecFileIO`` and Datawrap's two PyArrow subclasses).
Every other class-name property is refused before anything imports it, with a
message that names the property and repeats no value. The operator's
platform-AWS-credentials opt-in does not change this.
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

EVIL = "secfu18_evil.Evil"
_SECRET = "tenant-secret-value"
_REST = "https://catalog.example.com/v1"
_KEYS = {"s3.access-key-id": "AKIATENANT", "s3.secret-access-key": _SECRET}
_LOCATION = "s3://bucket/wh/ns/t"


@pytest.fixture(params=["off", "on"])
def mode(request, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_ALLOW_PLATFORM_AWS_CREDENTIALS", raising=False)
    if request.param == "on":
        monkeypatch.setenv("DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS", "1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setattr(
        "socket.socket.connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled"))
    )
    return request.param


@pytest.fixture
def evil(monkeypatch):
    """A class that records being imported or built; the module exists, so an import succeeds."""
    seen = SimpleNamespace(imported=[], made=[])
    module = types.ModuleType("secfu18_evil")

    class Evil:
        def __init__(self, *args, **kwargs):
            seen.made.append((args, kwargs))

        def __getattr__(self, name):  # whatever pyiceberg calls on it
            return lambda *_a, **_k: None

    module.Evil = Evil
    monkeypatch.setitem(sys.modules, "secfu18_evil", module)
    real = importlib.import_module

    def spy(name, *args, **kwargs):
        seen.imported.append(name)
        return real(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", spy)
    return seen


def _untouched(evil) -> None:
    assert "secfu18_evil" not in evil.imported
    assert evil.made == []


def _refused(message: str, prop: str) -> None:
    assert prop in message, message
    assert "class" in message, message
    assert EVIL not in message and "secfu18" not in message and _SECRET not in message, message


def _table_response(properties: dict, config: dict, credentials: list | None = None) -> dict:
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
    from pyiceberg.types import LongType, NestedField

    metadata = new_table_metadata(
        Schema(NestedField(1, "id", LongType(), required=False)),
        UNPARTITIONED_PARTITION_SPEC,
        UNSORTED_SORT_ORDER,
        _LOCATION,
        properties,
    )
    body = {
        "metadata-location": f"{_LOCATION}/metadata/v1.metadata.json",
        "metadata": json.loads(metadata.model_dump_json()),
        "config": config,
    }
    if credentials is not None:
        body["storage-credentials"] = credentials
    return body


@pytest.fixture
def server(monkeypatch):
    """The tenant's REST catalog server, answered in-process (no socket)."""
    import requests

    state = SimpleNamespace(
        defaults={}, overrides={}, table_props={}, table_config={}, credentials=None, commit_props={}, calls=[]
    )

    def request(self, method, url, *args, **kwargs):
        path = urlsplit(url).path
        state.calls.append((method, path))
        if path.endswith("/config"):
            body = {"defaults": state.defaults, "overrides": state.overrides}
        elif path.endswith("/tables/t") and method == "GET":
            body = _table_response(state.table_props, state.table_config, state.credentials)
        elif path.endswith("/tables/t") and method == "POST":
            body = _table_response(state.commit_props, {})
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
        "extra": {"catalog_type": "rest", **_KEYS, **extra},
    }


_CLASS_PROPERTIES = [
    "py-io-impl",
    "py-catalog-impl",
    "s3.retry-strategy-impl",
    "write.py-location-provider.impl",
]


# --- the connector's own settings ---------------------------------------------


@pytest.mark.parametrize("prop", _CLASS_PROPERTIES)
def test_a_tenants_settings_cannot_name_a_class(mode, evil, server, prop):
    from connectors.iceberg_catalog import load_catalog
    from services.egress_guard import connector_save_refusal

    endpoint = _rest_endpoint(**{prop: EVIL})
    _refused(connector_save_refusal(dict(endpoint)), prop)
    with pytest.raises(ValueError) as caught:
        load_catalog(dict(endpoint))
    _refused(str(caught.value), prop)
    assert server.calls == []  # refused before the catalog was even asked
    _untouched(evil)


def test_a_dotted_top_level_setting_cannot_name_a_class(mode, evil, server):
    from connectors.iceberg_catalog import load_catalog
    from services.egress_guard import connector_save_refusal

    endpoint = {**_rest_endpoint(), "s3.retry-strategy-impl": EVIL}
    _refused(connector_save_refusal(dict(endpoint)), "s3.retry-strategy-impl")
    with pytest.raises(ValueError) as caught:
        load_catalog(dict(endpoint))
    _refused(str(caught.value), "s3.retry-strategy-impl")
    _untouched(evil)


# --- the REST server's /v1/config ---------------------------------------------


@pytest.mark.parametrize("where", ["defaults", "overrides"])
@pytest.mark.parametrize(
    ("prop", "value", "named"),
    [
        ("py-io-impl", EVIL, "py-io-impl"),
        ("s3.retry-strategy-impl", EVIL, "s3.retry-strategy-impl"),
        ("auth", {"type": "custom", "impl": EVIL}, "auth"),
        ("auth", {"type": EVIL}, "auth"),
    ],
)
def test_a_rest_servers_config_cannot_name_a_class(mode, evil, server, where, prop, value, named):
    from connectors.iceberg_catalog import load_catalog

    getattr(server, where)[prop] = value
    with pytest.raises(ValueError) as caught:
        load_catalog(_rest_endpoint())
    _refused(str(caught.value), named)
    _untouched(evil)


# --- a table's load response and a commit response ---------------------------


@pytest.mark.parametrize("prop", ["py-io-impl", "s3.retry-strategy-impl", "write.py-location-provider.impl"])
@pytest.mark.parametrize("where", ["config", "metadata", "storage-credentials"])
def test_a_table_load_response_cannot_name_a_class(mode, evil, server, where, prop):
    from connectors.iceberg_catalog import load_table

    if where == "config":
        server.table_config[prop] = EVIL
    elif where == "metadata":
        server.table_props[prop] = EVIL
    else:
        server.credentials = [{"prefix": "s3://bucket/wh", "config": {**_KEYS, prop: EVIL}}]
    with pytest.raises(ValueError) as caught:
        load_table(_rest_endpoint())
    _refused(str(caught.value), prop)
    _untouched(evil)


def test_a_commit_response_cannot_name_a_class(mode, evil, server):
    from connectors.iceberg_catalog import load_table

    table = load_table(_rest_endpoint())
    server.commit_props["write.py-location-provider.impl"] = EVIL
    with pytest.raises(ValueError) as caught:
        table.catalog.commit_table(table, (), ())
    _refused(str(caught.value), "write.py-location-provider.impl")
    _untouched(evil)


def test_a_fileio_built_outside_a_catalog_cannot_name_a_class(mode, evil):
    from connectors.iceberg_credential_guard import guarded_file_io

    with pytest.raises(ValueError) as caught:
        guarded_file_io({**_KEYS, "py-io-impl": EVIL}, location="s3://bucket/k")
    _refused(str(caught.value), "py-io-impl")
    _untouched(evil)


# --- the FileIO classes Datawrap ships still work ------------------------------


# pyiceberg's plain PyArrowFileIO (named or inferred) is its local-URI subclass, which
# refuses GCS / Azure without the table's own credential (secfu21).
@pytest.mark.parametrize(
    ("server_io", "table_io", "expected"),
    [
        ("pyiceberg.io.pyarrow.PyArrowFileIO", None, "LocalUriPyArrowFileIO"),
        ("pyiceberg.io.pyarrow.PyArrowFileIO", "pyiceberg.io.fsspec.FsspecFileIO", "FsspecFileIO"),
        (None, "connectors.iceberg_pyarrow_io.LocalUriPyArrowFileIO", "LocalUriPyArrowFileIO"),
        (None, None, "LocalUriPyArrowFileIO"),
    ],
)
def test_the_fileios_datawrap_ships_are_accepted(mode, evil, server, server_io, table_io, expected):
    from connectors.iceberg_catalog import load_table

    server.table_config.update(_KEYS)  # vended keys: the guard keeps the named FileIO
    if server_io:
        server.overrides["py-io-impl"] = server_io
    if table_io:
        server.table_config["py-io-impl"] = table_io
    table = load_table(_rest_endpoint())
    assert type(table.io).__name__ == expected
    _untouched(evil)


def test_a_local_catalog_with_its_uri_fileio_still_round_trips(mode, evil, tmp_path, monkeypatch):
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    from connectors.iceberg_catalog import load_table
    from services.egress_guard import connector_save_refusal

    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(tmp_path))
    endpoint = {
        "type": "iceberg",
        "name": "ice",
        "schema": "ns",
        "table": "t",
        "connection_string": f"sqlite:///{tmp_path / 'catalog.db'}",
        "warehouse": str(tmp_path / "wh"),
        "extra": {"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"},
    }
    assert connector_save_refusal(dict(endpoint)) == ""
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    created = load_table(dict(endpoint), create=True, schema=schema)
    loaded = load_table(dict(endpoint))
    from connectors.iceberg_pyarrow_io import LocalUriPyArrowFileIO

    # Off, the keyless local IO is its keyed subclass (secfu12), which opens local files.
    assert isinstance(created.io, LocalUriPyArrowFileIO) and isinstance(loaded.io, LocalUriPyArrowFileIO)
    assert loaded.scan().to_arrow().num_rows == 0
    _untouched(evil)
