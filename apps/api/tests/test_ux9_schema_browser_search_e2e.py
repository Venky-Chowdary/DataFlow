"""UX-9: the schema browser can find any table in a large schema.

The connection workbench showed the first 40 of 8,730 tables with no search,
no schema selector and no count, and Studio's picker said "No tables match"
for a table in another schema. ``POST /api/v1/query/schema`` now takes a
``search`` (name contains, case-insensitive, LIKE wildcards literal) that
narrows the engine's own listing, and reports ``objects_total`` so the browser
can say "N of M"; ``POST /api/v1/query/schemas`` lists PostgreSQL schemas.
Live PostgreSQL and MySQL.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _names(resp) -> list[str]:
    assert resp.status_code == 200, resp.text
    return sorted(o["name"].rsplit(".", 1)[-1] for o in resp.json()["objects"])


@pytest.fixture
def pg_catalog():
    token = uuid.uuid4().hex[:8]
    schema = f"w4ux_s9_{token}"
    h.execute("postgresql", f"CREATE SCHEMA {schema}")
    try:
        # 205 tables: one more than a listing page, so the page is truncated.
        for i in range(205):
            h.execute("postgresql", f"CREATE TABLE {schema}.t{i:03d} (id int)")  # nosec B608
        for name in ("orders_2024", "orders_2025", "order%x", "order_y"):
            h.execute("postgresql", f'CREATE TABLE {schema}."{name}" (id int, note text)')  # nosec B608
        from services.connector_store import create_connector

        conn = create_connector({
            "name": f"w4ux-s9-{token}", "role": "source", "workspace_id": "", "ssl": False,
            **h.dest_config("postgresql"), "schema": schema,
        })
        yield conn, schema
    finally:
        h.execute("postgresql", f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_unfiltered_listing_reports_the_whole_count(client, pg_catalog):
    conn, schema = pg_catalog
    resp = client.post("/api/v1/query/schema", json={"connector_id": conn.id, "schema_name": schema})
    body = resp.json()
    assert body["objects_truncated"] is True
    assert body["objects_total"] == 209
    assert len(body["objects"]) == 200


def test_search_finds_tables_beyond_the_first_page(client, pg_catalog):
    conn, schema = pg_catalog
    resp = client.post(
        "/api/v1/query/schema", json={"connector_id": conn.id, "schema_name": schema, "search": "ORDERS_"}
    )
    assert _names(resp) == ["orders_2024", "orders_2025"]
    assert resp.json()["objects_total"] == 2
    assert resp.json()["objects_truncated"] is False


def test_search_wildcards_are_literal(client, pg_catalog):
    conn, schema = pg_catalog
    pct = client.post("/api/v1/query/schema", json={"connector_id": conn.id, "schema_name": schema, "search": "order%"})
    assert _names(pct) == ["order%x"]
    under = client.post("/api/v1/query/schema", json={"connector_id": conn.id, "schema_name": schema, "search": "order_"})
    assert _names(under) == ["order_y"]


def test_columns_still_load_for_one_object(client, pg_catalog):
    conn, schema = pg_catalog
    resp = client.post(
        "/api/v1/query/schema",
        json={"connector_id": conn.id, "schema_name": schema, "object_name": "orders_2025", "search": "zzz"},
    )
    assert resp.status_code == 200, resp.text
    cols = [c["name"] for o in resp.json()["objects"] for c in o["columns"]]
    assert cols == ["id", "note"]


def test_postgres_schemas_are_listed(client, pg_catalog):
    conn, schema = pg_catalog
    resp = client.post("/api/v1/query/schemas", json={"connector_id": conn.id})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    names = [c["name"] for n in body["nodes"] for c in n.get("children", [])]
    assert body["listed_live"] is True
    assert schema in names and "public" in names


@pytest.fixture
def mysql_catalog():
    token = uuid.uuid4().hex[:8]
    names = [f"w4ux_s9_{token}_alpha", f"w4ux_s9_{token}_beta"]
    for name in names:
        h.execute("mysql", f"CREATE TABLE {name} (id int)")  # nosec B608
    try:
        from services.connector_store import create_connector

        conn = create_connector({
            "name": f"w4ux-s9m-{token}", "role": "source", "workspace_id": "", "ssl": False,
            **h.dest_config("mysql"),
        })
        yield conn, token
    finally:
        for name in names:
            h.execute("mysql", f"DROP TABLE IF EXISTS {name}")  # nosec B608


def test_mysql_search(client, mysql_catalog):
    conn, token = mysql_catalog
    resp = client.post("/api/v1/query/schema", json={"connector_id": conn.id, "search": f"{token}_BET"})
    assert _names(resp) == [f"w4ux_s9_{token}_beta"]
    assert resp.json()["objects_total"] == 1
