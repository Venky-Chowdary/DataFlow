"""Dual-use connectors must never persist a one-sided catalog role."""

from services.connector_store import SavedConnector, listen_port_for_connector, normalize_connector_role


def test_normalize_forces_both_for_databases():
    assert normalize_connector_role("mysql", "source") == "both"
    assert normalize_connector_role("postgresql", "destination") == "both"
    assert normalize_connector_role("snowflake", "source") == "both"
    assert normalize_connector_role("mongodb", None) == "both"


def test_normalize_honors_source_only_file_types():
    assert normalize_connector_role("csv", "source") == "source"
    assert normalize_connector_role("email", "destination") == "destination"


def test_legacy_5432_fallback_becomes_the_driver_port():
    assert listen_port_for_connector("redis", 5432) == 6379
    assert listen_port_for_connector("redis", None) == 6379
    assert listen_port_for_connector("elasticsearch", 5432) == 9200
    assert listen_port_for_connector("neo4j", 5432) == 7474
    assert listen_port_for_connector("neo4j", 7687) == 7474
    assert listen_port_for_connector("kafka", 0) == 9092
    assert listen_port_for_connector("qdrant", "") == 6333
    assert listen_port_for_connector("weaviate", 5432) == 8080
    assert listen_port_for_connector("pgvector", 5432) == 5432
    assert listen_port_for_connector("postgresql", None) == 5432
    assert listen_port_for_connector("redis", 6380) == 6380
    healed = SavedConnector.from_dict(
        {
            "id": "redis-box",
            "name": "QA Redis Box",
            "type": "redis",
            "role": "both",
            "host": "redis.internal",
            "port": 5432,
        }
    )
    assert healed.port == 6379


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
