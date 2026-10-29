"""D4 — DuckDB tables were not listed or queryable through the MCP tools.

The saved connector has a file path in ``database`` and a label in ``host``
("localhost"). ``test_connector``, ``introspect_connector_schema`` and
``sample_connector_object`` answered, but ``list_connector_objects`` returned
``connected: false`` with no objects, and ``run_query`` then refused every SELECT
because its identifier allow-list is built from that listing.

``endpoint_intelligence._list_sqlalchemy_tables`` resolved the driver to
``generic_sql`` and did not hand the catalog type ``duckdb`` on, so
``connectors.generic_sql._build_engine`` lost the file-engine exemption it
applies by that type and refused "localhost" as a loopback dial. The table
introspection path forwards ``catalog_type`` and was unaffected.
"""

from __future__ import annotations

import uuid

import pytest

duckdb = pytest.importorskip("duckdb")


@pytest.fixture()
def duck_connector(tmp_path, monkeypatch):
    from services.connector_store import create_connector, delete_connector

    # Production has no loopback allowlist; the exemption must come from the
    # engine being a file, not from the rig's env.
    monkeypatch.delenv("DATAFLOW_EGRESS_ALLOWLIST", raising=False)
    path = str(tmp_path / "lab.duckdb")
    db = duckdb.connect(path)
    db.execute("CREATE TABLE orders (id INTEGER, amt DECIMAL(10,2))")
    db.execute("INSERT INTO orders VALUES (1, 1.50), (2, 2.25)")
    db.close()
    created = create_connector(
        {
            "name": "duck" + uuid.uuid4().hex[:6],
            "type": "duckdb",
            "host": "localhost",
            "port": 0,
            "database": path,
        }
    )
    cid = getattr(created, "id", None) or created["id"]
    yield cid
    delete_connector(cid)


def test_list_connector_objects_lists_the_duckdb_tables(duck_connector):
    from src.ai.copilot.schema_tools import list_connector_objects

    listed = list_connector_objects(connector_id=duck_connector)
    assert listed.success, listed.error
    assert listed.output["connected"] is True, listed.output
    assert "orders" in listed.output["objects"], listed.output


def test_run_query_reads_a_duckdb_table(duck_connector):
    from src.ai.copilot.query_tools import run_connector_query

    res = run_connector_query(
        connector_id=duck_connector, query="SELECT COUNT(*) AS n, SUM(amt) AS total FROM orders"
    )
    assert res.success, res.error
    assert [int(r["n"]) for r in res.output["rows"]] == [2]
