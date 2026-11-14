"""Follow-up review 2, H2: a REST load response cannot hide a class-name property.

pyiceberg's REST catalog builds a table from a load response
(``_response_to_table`` / ``_response_to_staged_table``) and hands the FileIO
``{**metadata.properties, **config, **credential_config}``: the table config
and the best-matching ``storage-credentials[].config`` win over the metadata.
The class-name check saw only that merge, so a tenant's catalog server could
put ``write.py-location-provider.impl`` in the table's metadata properties and
blank it with ``""`` (or null) in ``config`` or in a storage credential. The
check saw ``""`` and let it through, while the ``Table`` kept the raw
metadata, and ``Table.location_provider()`` (every write) imported and
instantiated the class. ``Table.refresh()`` loads the table again the same way.

Now the metadata properties, the config and every storage credential's config
are each checked before a table (or a staged table) is built, on load, create,
register and refresh, and the metadata a commit returns is checked too.
"""

from __future__ import annotations

import json
import sys
import types
import uuid
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

_REST = "https://catalog.example.com/v1"
_KEY = "write.py-location-provider.impl"
_EVIL = "secfu23_tenant_module.Evil"
_LOCATION = "s3://bucket/wh/ns/t"
_UUID = uuid.UUID("5e1f0c3a-7d1b-4c9e-9a51-0b2f4d6c8e10")
_KEYS = {"s3.access-key-id": "AKIATENANT", "s3.secret-access-key": "tenant-secret", "s3.region": "us-east-1"}


@pytest.fixture
def evil(monkeypatch):
    """A tenant-named location provider: records that pyiceberg imported and built it."""
    from pyiceberg.table.locations import SimpleLocationProvider

    module = types.ModuleType("secfu23_tenant_module")
    module.HITS = []

    class Evil(SimpleLocationProvider):
        def __init__(self, *args, **kwargs):
            module.HITS.append("instantiated")
            super().__init__(*args, **kwargs)

    module.Evil = Evil
    monkeypatch.setitem(sys.modules, "secfu23_tenant_module", module)
    return module


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


def _metadata(properties: dict) -> dict:
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
        dict(properties),
        table_uuid=_UUID,
    )
    return json.loads(metadata.model_dump_json())


@pytest.fixture
def server(monkeypatch):
    """The tenant's REST catalog server, answered in-process (no socket)."""
    import requests

    state = SimpleNamespace(
        overrides={}, responses=[], commit_properties={}, calls=[],
    )

    def table_body() -> dict:
        md_props, config, credentials = state.responses.pop(0) if len(state.responses) > 1 else state.responses[0]
        body = {
            "metadata-location": f"{_LOCATION}/metadata/v1.metadata.json",
            "metadata": _metadata(md_props),
            "config": config,
        }
        if credentials is not None:
            body["storage-credentials"] = credentials
        return body

    def request(self, method, url, *args, **kwargs):
        path = urlsplit(url).path
        state.calls.append((method, path))
        if path.endswith("/config"):
            body = {"defaults": {}, "overrides": state.overrides}
        elif path.endswith("/tables/t") and method == "GET":
            body = table_body()
        elif path.endswith("/tables/t") and method == "POST":  # commit
            body = {
                "metadata-location": f"{_LOCATION}/metadata/v2.metadata.json",
                "metadata": _metadata(state.commit_properties),
            }
        elif path.endswith("/tables") and method == "POST":  # create (staged or not)
            body = table_body()
        elif path.endswith("/register") and method == "POST":
            body = table_body()
        elif path.endswith("/namespaces/ns") and method in {"GET", "HEAD"}:
            body = {"namespace": ["ns"], "properties": {}}
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


def _endpoint() -> dict:
    return {
        "type": "iceberg",
        "name": "ice",
        "schema": "ns",
        "table": "t",
        "connection_string": _REST,
        "extra": {"catalog_type": "rest", **_KEYS},
    }


def _credential(config: dict) -> list[dict]:
    return [{"prefix": "s3://bucket/wh", "config": config}]


#: (metadata properties, table config, storage credentials): the class is named in the metadata
#: and blanked (empty or null) where the merge for the FileIO would let the blank win.
_HIDDEN = [
    pytest.param({_KEY: _EVIL}, {_KEY: ""}, None, id="config-empty"),
    pytest.param({_KEY: _EVIL}, {_KEY: None}, None, id="config-null"),
    pytest.param({_KEY: _EVIL}, {}, _credential({_KEY: ""}), id="credential-empty"),
    pytest.param({_KEY: _EVIL}, {_KEY: "x"}, _credential({_KEY: ""}), id="config-named-credential-blank"),
    pytest.param({"py-io-impl": _EVIL}, {"py-io-impl": ""}, None, id="py-io-impl-config-empty"),
    pytest.param({_KEY: _EVIL}, {}, None, id="metadata-only"),
    pytest.param({}, {_KEY: _EVIL}, None, id="config-only"),
    pytest.param({}, {}, _credential({_KEY: _EVIL}), id="credential-only"),
    pytest.param({}, {}, [{"prefix": "s3://other", "config": {_KEY: _EVIL}}], id="unmatched-credential"),
]


def _refused(message: str, *names: str) -> None:
    for name in names:
        assert name in message, message
    assert _EVIL not in message and "secfu23_tenant_module" not in message, message


@pytest.mark.parametrize(("md_props", "config", "credentials"), _HIDDEN)
def test_load_table_refuses_a_class_hidden_from_the_fileio_merge(server, evil, md_props, config, credentials):
    from connectors.iceberg_catalog import load_table

    server.responses = [(md_props, config, credentials)]
    with pytest.raises(ValueError) as caught:
        table = load_table(_endpoint())
        table.location_provider()  # what every write does
    _refused(str(caught.value), "names a")
    assert evil.HITS == []


@pytest.mark.parametrize(("md_props", "config", "credentials"), _HIDDEN[:6])
def test_refresh_refuses_it_too(server, evil, md_props, config, credentials):
    from connectors.iceberg_catalog import load_table

    server.responses = [({}, {}, None), (md_props, config, credentials)]
    table = load_table(_endpoint())
    with pytest.raises(ValueError):
        table.refresh()
        table.location_provider()
    assert evil.HITS == []


@pytest.mark.parametrize(("md_props", "config", "credentials"), _HIDDEN[:6])
def test_create_and_register_refuse_it_too(server, evil, md_props, config, credentials):
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    from connectors.iceberg_catalog import load_catalog

    server.responses = [(md_props, config, credentials)]
    catalog = load_catalog(_endpoint())
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    for build in (
        lambda: catalog.create_table(("ns", "t"), schema=schema),
        lambda: catalog.register_table(("ns", "t"), f"{_LOCATION}/metadata/v1.metadata.json"),
        lambda: catalog.create_table_transaction(("ns", "t"), schema=schema),
    ):
        with pytest.raises(ValueError):
            made = build()
            getattr(made, "_table", made).location_provider()
    assert evil.HITS == []


def test_a_clean_response_still_loads(server, evil):
    from connectors.iceberg_catalog import load_table

    server.responses = [({"write.format.default": "parquet"}, {"s3.region": "us-east-1"}, _credential({}))]
    table = load_table(_endpoint())
    assert type(table.location_provider()).__name__ == "SimpleLocationProvider"
    assert table.metadata.properties["write.format.default"] == "parquet"


@pytest.mark.parametrize("value", [_EVIL, ""])
def test_a_commit_response_is_checked_on_its_raw_metadata(server, evil, value):
    """The commit response carries no config, so its metadata properties are what the table keeps."""
    from connectors.iceberg_catalog import load_table

    server.responses = [({}, {}, None)]
    server.commit_properties = {_KEY: value}
    table = load_table(_endpoint())
    if value:
        with pytest.raises(ValueError):
            with table.transaction() as tx:
                tx.set_properties({"k": "v"})
        assert evil.HITS == []
    else:
        with table.transaction() as tx:
            tx.set_properties({"k": "v"})
