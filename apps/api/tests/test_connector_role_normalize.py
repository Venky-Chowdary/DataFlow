"""Dual-use connectors must never persist a one-sided catalog role."""

from services.connector_store import SavedConnector, normalize_connector_role


def test_normalize_forces_both_for_databases():
    assert normalize_connector_role("mysql", "source") == "both"
    assert normalize_connector_role("postgresql", "destination") == "both"
    assert normalize_connector_role("snowflake", "source") == "both"
    assert normalize_connector_role("mongodb", None) == "both"


def test_normalize_honors_source_only_file_types():
    assert normalize_connector_role("csv", "source") == "source"
    assert normalize_connector_role("email", "destination") == "destination"


def test_from_dict_upgrades_legacy_mysql_source_role():
    conn = SavedConnector.from_dict(
        {
            "id": "x",
            "name": "Local MySQL",
            "type": "mysql",
            "role": "source",
            "host": "127.0.0.1",
            "port": 3306,
            "database": "dataflow",
        }
    )
    assert conn.role == "both"
