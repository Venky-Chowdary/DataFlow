"""Studio schema honesty: Mongo create-new, Validate table_exists, Execute None."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_mongo_missing_collection_is_create_new_not_existing():
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database",
        format="mongodb",
        host="localhost",
        port=27017,
        database="demo",
        collection="brand_new_airports",
        extra={"introspect_purpose": "destination"},
    )
    db = MagicMock()
    db.list_collection_names.return_value = ["users", "orders"]
    client = MagicMock()
    client.__getitem__.return_value = db

    with (
        patch("src.transfer.endpoint_intelligence.resolve_connector_config", return_value={
            "type": "mongodb",
            "host": "localhost",
            "port": 27017,
            "database": "demo",
            "auth_source": "admin",
        }),
        patch("src.transfer.connector_registry.run_probe", return_value=(True, "ok")),
        patch("src.transfer.endpoint_intelligence._mongo_client", return_value=client),
        patch("src.transfer.endpoint_intelligence.mongodb_connection_string", return_value="mongodb://x"),
    ):
        out = introspect_endpoint(endpoint)

    assert out.get("connected") is True
    assert out.get("table_exists") is False, out
    assert out.get("columns") == []
    assert "created" in (out.get("message") or "").lower() or "not found" in (out.get("message") or "").lower()


def test_inspect_destination_prefers_introspect_table_exists():
    from services.preflight_service import inspect_destination_for_preflight

    with (
        patch("services.connector_probe.probe_saved_connector", return_value=(True, "ok", {"type": "postgresql"})),
        patch("services.connector_probe.endpoint_from_saved_connector") as ep_fn,
        patch("src.transfer.endpoint_intelligence.introspect_endpoint") as intro,
    ):
        ep = MagicMock()
        ep.collection = ""
        ep.table = "jobs"
        ep.schema = "public"
        ep.host = "h"
        ep.port = 5432
        ep.database = "db"
        ep.username = "u"
        ep.password = "p"
        ep.connection_string = ""
        ep.warehouse = ""
        ep.auth_role = ""
        ep.service_account = ""
        ep.ssl = False
        ep_fn.return_value = ep
        intro.return_value = {
            "connected": True,
            "table_exists": False,
            "columns": [],
            "schema": {},
            "objects": [{"name": "other"}],
            "message": "not found",
            "db_type": "postgresql",
        }
        out = inspect_destination_for_preflight(
            connector_id="c1",
            dest_type="postgresql",
            dest_table="jobs",
        )
    assert out["table_exists"] is False


def test_destination_schema_probe_preserves_none():
    from src.transfer.engine import _destination_schema_probe
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="postgresql", table="jobs")
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint", return_value={
        "schema": {},
        "table_exists": None,
        "columns": [],
    }):
        _schema, exists = _destination_schema_probe(dest, sync_mode="full_refresh_append")
    assert exists is None
