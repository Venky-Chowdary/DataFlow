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
