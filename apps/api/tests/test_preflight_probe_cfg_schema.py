"""The destination probe reads the schema the operator chose, not the connector default.

Audit repro: Studio picked qa_s2_audit_1108.ddl_target on a saved PostgreSQL
connector whose default schema is public. Validate disclosed
CREATE UNIQUE INDEX ... ON "public"."ddl_target" and the append key-collision
probe failed on public.keyed (relation does not exist), so an enforced-key
collision approved instead of blocking.
"""

from __future__ import annotations

from unittest.mock import patch


def _inspect(dest_schema: str | None):
    from services.preflight_service import inspect_destination_for_preflight
    from src.transfer.models import EndpointConfig

    with (
        patch(
            "services.connector_probe.probe_saved_connector",
            return_value=(True, "ok", {"type": "postgresql", "host": "h", "port": 5432, "schema": "public"}),
        ),
        patch(
            "services.connector_probe.endpoint_from_saved_connector",
            side_effect=lambda *a, **k: EndpointConfig(
                kind="database", format="postgresql", connector_id="dst-1", host="h", port=5432,
                database="db", schema=k.get("schema") or "public", table="ddl_target",
            ),
        ),
        patch(
            "src.transfer.endpoint_intelligence.introspect_endpoint",
            return_value={"ok": True, "connected": True, "columns": [{"name": "id", "type": "int4"}],
                          "table_exists": True, "objects": [{"name": "ddl_target"}]},
        ),
    ):
        return inspect_destination_for_preflight(
            connector_id="dst-1", dest_type="postgresql", dest_table="ddl_target",
            dest_schema=dest_schema,
        )


def test_probe_cfg_carries_the_chosen_schema() -> None:
    out = _inspect("qa_s2_audit_1108")
    assert out["_probe_cfg"]["schema"] == "qa_s2_audit_1108"


def test_probe_cfg_keeps_the_connector_schema_when_none_is_chosen() -> None:
    out = _inspect(None)
    assert out["_probe_cfg"]["schema"] == "public"
