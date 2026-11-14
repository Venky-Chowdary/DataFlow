"""Follow-up review 3 (H2): a SQL catalog on the tenant's database server names no file on the API host.

``iceberg_catalog._sql_props`` accepts ``postgresql://`` / ``mysql://`` /
``mssql://`` catalog URIs: the catalog database then runs on the tenant's
server and its ``iceberg_tables.metadata_location`` rows are the tenant's to
write. The guarded catalog treated every ``SqlCatalog`` as local and kept only
the shared Iceberg jail (the same folder for every workspace), so a row naming
another workspace's ``metadata.json`` in that folder read the other
workspace's table. Now a ``SqlCatalog`` whose URI is not a SQLite file is
remote: its FileIO refuses every local location, as REST / Glue / Hive do.
"""

from __future__ import annotations

import socket

import pytest

pa = pytest.importorskip("pyarrow")
pytest.importorskip("pyiceberg.catalog.sql")


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ICEBERG_ROOT", "EGRESS_ENFORCE_NEW", "EGRESS_ALLOWLIST"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )
    monkeypatch.setattr(
        socket, "getaddrinfo",
        lambda host, port, *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.215.14", port or 0))],
    )
    root = tmp_path / "iceroot"
    (root / "victim").mkdir(parents=True)
    (root / "attacker").mkdir()
    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(root))
    return root


@pytest.fixture
def victim(_isolated):
    from pyiceberg.catalog.sql import SqlCatalog

    from connectors.iceberg_credential_guard import LOCAL_URI_FILE_IO

    root = _isolated
    catalog = SqlCatalog("v", uri=f"sqlite:///{root}/victim/cat.db", warehouse=f"file://{root}/victim",
                         **{"py-io-impl": LOCAL_URI_FILE_IO})
    catalog.create_namespace("ns")
    table = catalog.create_table("ns.t", schema=pa.schema([("s", pa.string())]))
    table.append(pa.table({"s": ["victim-secret-1", "victim-secret-2"]}))
    return catalog.load_table("ns.t").metadata_location


@pytest.fixture
def tenant_server(monkeypatch, tmp_path):
    """The tenant's catalog database: any non-SQLite URI opens this SQLite file instead."""
    import pyiceberg.catalog.sql as sql_catalog
    from pyiceberg.catalog.sql import SqlCatalog

    standin = tmp_path / "tenant_server.db"
    real = sql_catalog.create_engine
    monkeypatch.setattr(sql_catalog, "create_engine", lambda _uri, **kw: real(f"sqlite:///{standin}", **kw))

    def write_row(metadata_location: str, catalog_name: str = "a") -> None:  # the tenant's own row
        owner = SqlCatalog(catalog_name, uri=f"sqlite:///{standin}")
        owner.create_namespace("ns")
        owner.register_table("ns.stolen", metadata_location)

    return write_row


_URIS = [
    "postgresql://u:p@tenant-db.example.com:5432/cat",
    "postgresql+psycopg2://u:p@tenant-db.example.com:5432/cat",
    "mysql+pymysql://u:p@tenant-db.example.com:3306/cat",
    "mssql+pyodbc://u:p@tenant-db.example.com:1433/cat",
]


@pytest.mark.parametrize("uri", _URIS)
def test_a_tenant_server_catalog_cannot_read_another_workspaces_table(_isolated, victim, tenant_server, uri):
    from pyiceberg.catalog.sql import SqlCatalog

    from connectors.iceberg_credential_guard import LOCAL_URI_FILE_IO, guarded_catalog

    root = _isolated
    tenant_server(victim)
    attacker = guarded_catalog(SqlCatalog)("a", uri=uri, warehouse=f"file://{root}/attacker",
                                           **{"py-io-impl": LOCAL_URI_FILE_IO})
    with pytest.raises(ValueError) as caught:
        attacker.load_table("ns.stolen").scan().to_arrow()
    assert "victim" not in str(caught.value) and str(root) not in str(caught.value)


def test_load_catalog_with_a_tenant_server_uri_refuses_local_files(_isolated, victim, tenant_server):
    from connectors.iceberg_catalog import load_catalog

    root = _isolated
    tenant_server(victim, "dataflow")  # load_catalog's default catalog name
    catalog = load_catalog({"type": "iceberg", "connection_string": _URIS[0], "warehouse": f"file://{root}/attacker",
                            "table": "ns.stolen", "extra": {"catalog_type": "sql"}})
    with pytest.raises(ValueError):
        catalog.load_table("ns.stolen").scan().to_arrow()


def test_a_local_sqlite_catalog_still_reads_its_own_table(_isolated, victim):
    from pyiceberg.catalog.sql import SqlCatalog

    from connectors.iceberg_credential_guard import LOCAL_URI_FILE_IO, guarded_catalog

    root = _isolated
    own = guarded_catalog(SqlCatalog)("v", uri=f"sqlite:///{root}/victim/cat.db", warehouse=f"file://{root}/victim",
                                      **{"py-io-impl": LOCAL_URI_FILE_IO})
    assert own.load_table("ns.t").scan().to_arrow().column("s").to_pylist() == ["victim-secret-1", "victim-secret-2"]
