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
    from src.transfer.models import EndpointConfig

    with (
        patch("services.connector_probe.probe_saved_connector", return_value=(True, "ok", {"type": "postgresql"})),
        patch("services.connector_probe.endpoint_from_saved_connector") as ep_fn,
        patch("src.transfer.endpoint_intelligence.introspect_endpoint") as intro,
    ):
        ep = EndpointConfig(
            kind="database",
            format="postgresql",
            host="h",
            port=5432,
            database="db",
            schema="public",
            table="jobs",
        )
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
    assert ep.extra.get("introspect_purpose") == "destination"
    assert intro.called


def test_execute_destination_schema_probe_stamps_destination_purpose():
    from src.transfer.engine import _destination_schema_probe
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="mysql", database="railway", table="users")
    with patch(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        return_value={"schema": {}, "table_exists": False, "columns": []},
    ) as intro:
        schema, exists = _destination_schema_probe(dest, sync_mode="full_refresh_append")
    assert schema == {}
    assert exists is False
    assert dest.extra.get("introspect_purpose") == "destination"
    assert intro.called


def test_destination_schema_probe_preserves_none():
    from src.transfer.engine import _destination_schema_probe
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="postgresql", table="jobs")
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint", return_value={
        "schema": {},
        "table_exists": None,
        "columns": [],
        "message": "permission denied for relation jobs",
    }):
        _schema, exists = _destination_schema_probe(dest, sync_mode="full_refresh_append")
    assert exists is None
    assert "permission denied" in (dest.extra or {}).get("schema_probe_error", "")


def test_destination_schema_probe_stamps_exception_error():
    from src.transfer.engine import _destination_schema_probe
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="postgresql", table="jobs")
    with patch(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        side_effect=RuntimeError("connection refused"),
    ):
        _schema, exists = _destination_schema_probe(dest, sync_mode="full_refresh_append")
    assert exists is None
    assert "connection refused" in (dest.extra or {}).get("schema_probe_error", "")


def test_destination_schema_probe_overwrite_keeps_existence_clears_types():
    """Create-new overwrite must not invent unknown existence — still probe, drop stale types."""
    from src.transfer.engine import _destination_schema_probe
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="sqlite", database="/tmp/x.db", table="out")
    with patch(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        return_value={"schema": {"id": "INTEGER", "legacy": "TEXT"}, "table_exists": False},
    ) as intro:
        schema, exists = _destination_schema_probe(dest, sync_mode="full_refresh_overwrite")
    assert schema == {}
    assert exists is False
    assert intro.called
    assert (dest.extra or {}).get("schema_nullability") == {}


def test_destination_schema_probe_overwrite_dest_exists_keeps_nullability():
    """Dest-exists overwrite keeps live NOT NULL so G14 can block dest-only columns."""
    from src.transfer.engine import _destination_schema_probe
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="postgresql", table="users")
    with patch(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        return_value={
            "schema": {"id": "INTEGER", "tenant_id": "TEXT"},
            "schema_nullability": {"id": False, "tenant_id": False},
            "table_exists": True,
        },
    ):
        schema, exists = _destination_schema_probe(dest, sync_mode="full_refresh_overwrite")
    assert exists is True
    assert schema["tenant_id"] == "TEXT"
    assert (dest.extra or {}).get("schema_nullability") == {"id": False, "tenant_id": False}


def test_destination_schema_probe_stamps_nullability_for_g3():
    """Append/upsert must pass live NOT NULL into preflight via destination.extra."""
    from src.transfer.engine import _destination_schema_probe
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="postgresql", table="users")
    with patch(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        return_value={
            "schema": {"id": "INTEGER", "email": "VARCHAR(255)"},
            "schema_nullability": {"id": False, "email": True},
            "table_exists": True,
        },
    ):
        schema, exists = _destination_schema_probe(dest, sync_mode="full_refresh_append")
    assert exists is True
    assert schema["email"] == "VARCHAR(255)"
    assert (dest.extra or {}).get("schema_nullability") == {"id": False, "email": True}
