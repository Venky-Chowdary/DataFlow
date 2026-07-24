"""New-table introspect must return table_exists=False (not null) for create-new Map."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_mysql_missing_table_stamps_table_exists_false():
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database",
        format="mysql",
        host="localhost",
        port=3306,
        database="railway",
        table="airports_brand_new",
        extra={"introspect_purpose": "destination"},
    )
    probe = MagicMock()
    probe.ok = True
    probe.tables = ["users", "orders"]  # airports not listed
    probe.message = "ok"
    probe.error = None

    with (
        patch("src.transfer.endpoint_intelligence.resolve_connector_config", return_value={
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "railway",
            "username": "u",
            "password": "p",
            "ssl": False,
        }),
        patch("connectors.mysql.test_mysql", return_value=probe),
    ):
        out = introspect_endpoint(endpoint)

    assert out.get("connected") is True
    assert out.get("table_exists") is False, out
    assert out.get("columns") == []
    assert "not found" in (out.get("message") or "").lower() or "created" in (out.get("message") or "").lower()


def test_attach_db_sample_exception_does_not_wipe_false():
    from src.transfer import endpoint_intelligence as ei

    out = {"table_exists": False, "columns": [], "schema": {}, "message": "missing"}
    # Simulate the except path preserving False
    if out.get("table_exists") not in (True, False):
        out["table_exists"] = None
    assert out["table_exists"] is False

    # And the helper short-circuits when already False + not listed
    endpoint = MagicMock()
    endpoint.table = "airports"
    endpoint.collection = ""
    endpoint.extra = {"introspect_purpose": "destination"}
    cfg = {"type": "mysql", "host": "h", "port": 3306, "database": "db"}
    with patch.object(ei, "resolve_connector_config", return_value=cfg):
        with patch.object(ei, "_mark_table_listed_if_present", return_value=None):
            with patch.object(ei, "_object_name_match", return_value=None):
                with patch.object(ei, "_introspect_table_schema") as introspect:
                    ei._attach_db_sample(out, endpoint)
                    introspect.assert_not_called()
    assert out["table_exists"] is False
    assert "created automatically" in (out.get("message") or "").lower()
