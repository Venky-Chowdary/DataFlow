"""Catalog tree honesty: one configured node, or a live Snowflake list."""

from services.query_catalog import build_catalog_tree, live_schema_names


def test_postgres_returns_the_configured_database_and_schema():
    tree = build_catalog_tree(
        connector_type="postgresql",
        database="studio_src",
        schema="sales",
    )
    assert tree["listed_live"] is False
    assert tree["nodes"] == [{
        "kind": "database",
        "name": "studio_src",
        "children": [{"kind": "schema", "name": "sales", "children": []}],
    }]
    assert "does not publish" in tree["message"]


def test_unknown_connector_is_one_node_not_an_empty_list():
    tree = build_catalog_tree(connector_type="", database="", schema="")
    assert tree["nodes"]
    assert tree["nodes"][0]["kind"] == "database"
    assert tree["nodes"][0]["name"] == "connector"


def test_live_schemas_replace_the_configured_name():
    tree = build_catalog_tree(
        connector_type="snowflake",
        database="ANALYTICS",
        schema="PUBLIC",
        live_schemas=["PUBLIC", "SALES", "PUBLIC"],
    )
    assert tree["listed_live"] is True
    names = [node["name"] for node in tree["nodes"][0]["children"]]
    assert names == ["PUBLIC", "SALES"]


def test_empty_live_list_keeps_the_configured_schema():
    tree = build_catalog_tree(
        connector_type="snowflake",
        database="ANALYTICS",
        schema="PUBLIC",
        live_schemas=[],
    )
    assert tree["listed_live"] is False
    assert tree["nodes"][0]["children"][0]["name"] == "PUBLIC"
    assert "no schema names" in tree["message"]


def test_non_snowflake_does_not_pretend_to_list():
    class Endpoint:
        format = "postgresql"

    assert live_schema_names(Endpoint()) is None


def test_postgres_lists_its_schemas_live():
    """UX-9: a PostgreSQL browser can leave ``public``; the schemas come from the database."""
    import os

    import pytest

    if not os.environ.get("PGHOST"):
        pytest.skip("needs a live PostgreSQL (PGHOST)")
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ.get("PGPORT") or 5432),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS w4ux_catalog_probe")
    try:

        class Endpoint:
            format = "postgresql"
            host = os.environ["PGHOST"]
            port = int(os.environ.get("PGPORT") or 5432)
            database = os.environ.get("PGDATABASE", "postgres")
            username = os.environ.get("PGUSER", "postgres")
            password = os.environ.get("PGPASSWORD", "")
            connection_string = ""
            ssl = False

        names = live_schema_names(Endpoint())
        assert names is not None
        assert "public" in names and "w4ux_catalog_probe" in names
        assert not any(n.startswith("pg_") or n == "information_schema" for n in names)
        tree = build_catalog_tree(connector_type="postgresql", database=Endpoint.database, schema="public", live_schemas=names)
        assert tree["listed_live"] is True
        assert "w4ux_catalog_probe" in [c["name"] for c in tree["nodes"][0]["children"]]
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS w4ux_catalog_probe")
        conn.close()
