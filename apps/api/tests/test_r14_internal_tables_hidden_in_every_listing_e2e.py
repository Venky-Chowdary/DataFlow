"""QA browser 10: the product's own tables never appear in a listing an operator reads.

The Query schema browser listed the internal write ledger. Round 7 filtered
``introspect_endpoint`` (``/transfer/introspect``, guarded by
``test_r7_sql_query_source_rest_e2e.py``); the schema browser's own route
(``POST /api/v1/query/schema``) and Pilot's ``introspect_connector_schema``
("Known tables include …") were not under test, and Pilot read the raw
``introspect_schema`` table list. One predicate (``services.brand.
is_internal_object_name``) now filters both entry points; live PostgreSQL.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h

INTERNAL = ["dataflow_signal", "datawrap_signal", "orders_df_quarantine", "_df_stg_orders",
            "_dataflow_write_ledger", "_datawrap_write_ledger", "orders__df_bak_ab12cd",
            "_df_cdc_eos_watermarks", "orders_df_staging"]
USER = ["orders", "orders_dfq_notes", "ledger"]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def catalog():
    token = uuid.uuid4().hex[:8]
    schema = f"r14q_{token}"
    h.execute("postgresql", f"CREATE SCHEMA {schema}")
    try:
        for name in [*USER, *INTERNAL]:
            h.execute("postgresql", f'CREATE TABLE {schema}."{name}" (id int)')  # nosec B608
        from services.connector_store import create_connector

        conn = create_connector({
            "name": f"r14q-{token}", "role": "source", "workspace_id": "", "ssl": False,
            **h.dest_config("postgresql"), "schema": schema,
        })
        yield conn, schema
    finally:
        h.execute("postgresql", f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_query_schema_browser_lists_no_internal_table(client, catalog):
    conn, schema = catalog
    resp = client.post("/api/v1/query/schema", json={"connector_id": conn.id, "schema_name": schema})
    assert resp.status_code == 200, resp.text
    names = [o["name"].rsplit(".", 1)[-1] for o in resp.json()["objects"]]
    assert set(USER) <= set(names), names
    assert not set(INTERNAL) & set(names), names


def test_pilot_known_tables_hint_lists_no_internal_table(client, catalog):
    conn, _schema = catalog
    from src.ai.copilot.schema_tools import introspect_connector_schema

    out = introspect_connector_schema(connector_id=conn.id, table="no_such_table_r14")
    listed = [str(t).rsplit(".", 1)[-1] for t in (out.output or {}).get("tables") or []]
    text = f"{out.error} {listed}"
    assert "orders" in listed, text
    for name in INTERNAL:
        assert name not in listed and f"{name}," not in str(out.error), (name, text)


def test_one_predicate_filters_introspect_schema_tables():
    from services.schema_introspect import _hide_internal_tables

    result = {"tables": [*USER, *INTERNAL, {"name": "_df_stg_x"}, {"name": "customers"}]}
    _hide_internal_tables(result)
    assert result["tables"] == [*USER, {"name": "customers"}]
