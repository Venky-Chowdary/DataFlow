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


def test_run_file_preflight_unknown_create_flag_is_false() -> None:
    """Pilot omitting destination_can_create must not soft-pass create-new."""
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["id"],
        column_types={"id": "INTEGER"},
        row_count=1,
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        destination_connected=True,
        destination_error=None,
        source_connected=True,
        source_error=None,
        source_kind="database",
        source_format="postgresql",
        sync_mode="full_refresh_overwrite",
        sample_rows=[{"id": "1"}],
        destination_can_create=None,
        destination_can_write=True,
        destination_table_exists=False,
        destination_db_type="postgresql",
    )
    # Table missing + unknown create ⇒ must not pass as Execute-ready.
    assert result.get("passed") is False
    blockers = " ".join(
        str(b.get("message") or b.get("id") or b)
        for b in (result.get("blockers") or [])
    ).lower()
    gate_blob = " ".join(
        f"{g.get('id')} {g.get('status')} {g.get('message') or g.get('details') or ''}"
        for g in (result.get("gates") or [])
    ).lower()
    assert (
        "creat" in blockers
        or "creat" in gate_blob
        or "privilege" in gate_blob
        or "ddl" in gate_blob
        or any(g.get("status") == "block" for g in (result.get("gates") or []))
    )
