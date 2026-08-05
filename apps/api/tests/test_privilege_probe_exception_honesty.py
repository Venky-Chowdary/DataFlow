"""Privilege probe exceptions must not invent can_create=True."""

from __future__ import annotations

from unittest.mock import patch


def test_inspect_privilege_exception_refuses_create_soft_pass() -> None:
    from services.preflight_service import inspect_destination_for_preflight

    with (
        patch(
            "services.connector_probe.probe_saved_connector",
            return_value=(True, "ok", {"type": "postgresql", "host": "h", "port": 5432}),
        ),
        patch(
            "services.connector_probe.endpoint_from_saved_connector",
            side_effect=lambda *a, **k: __import__(
                "src.transfer.models", fromlist=["EndpointConfig"]
            ).EndpointConfig(
                kind="database",
                format="postgresql",
                connector_id="dst-1",
                host="h",
                port=5432,
                database="db",
                schema="railway",
                table="users",
            ),
        ),
        patch(
            "src.transfer.endpoint_intelligence.introspect_endpoint",
            return_value={
                "ok": True,
                "connected": True,
                "columns": [{"name": "id", "type": "text"}],
                "table_exists": True,
                "objects": [{"name": "users"}],
            },
        ),
        patch(
            "services.destination_privilege_probe.probe_destination_privileges",
            side_effect=RuntimeError("catalog timeout"),
        ),
    ):
        out = inspect_destination_for_preflight(
            connector_id="dst-1",
            dest_type="postgresql",
            dest_table="users",
            dest_schema="railway",
            dest_kind="database",
        )

    assert out.get("connected") is True
    assert out.get("can_create_table") is False
    assert out.get("can_write") is True
    probe = out.get("privilege_probe") or {}
    assert probe.get("status") == "unavailable" or probe.get("privilege_verified") is False
