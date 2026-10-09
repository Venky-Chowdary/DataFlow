"""Pilot create_connector routing + credential extraction."""

from __future__ import annotations

from src.ai.copilot.connector_create import (
    build_connector_draft,
    draft_is_complete,
    extract_url_credentials,
    wants_create_connector,
)
from src.ai.copilot.tools import infer_tools_from_message


def test_wants_create_from_mysql_url_message():
    msg = (
        "create a connector for me with this mysql url "
        "mysql://root:secret@tokaido.proxy.rlwy.net:32253/railway"
    )
    assert wants_create_connector(msg)
    planned = infer_tools_from_message(msg)
    assert "create_connector" in [n for n, _ in planned]
    assert "search_knowledge" not in [n for n, _ in planned]


def test_extract_mysql_and_postgres_urls():
    mysql = extract_url_credentials(
        "mysql://root:x@tokaido.proxy.rlwy.net:32253/railway"
    )
    assert mysql and mysql["type"] == "mysql"
    assert mysql["port"] == 32253
    pg = extract_url_credentials(
        "postgresql://postgres:y@tokaido.proxy.rlwy.net:27396/railway"
    )
    assert pg and pg["type"] == "postgresql"
    assert pg["host"] == "tokaido.proxy.rlwy.net"


def test_build_draft_from_fields():
    msg = """
    create connector
    type: mysql
    host: tokaido.proxy.rlwy.net
    port: 32253
    database: railway
    username: root
    password: secret
    name: Railway MySQL
    """
    draft = build_connector_draft(msg)
    assert draft["type"] == "mysql"
    assert draft["host"] == "tokaido.proxy.rlwy.net"
    assert draft["port"] == 32253
    assert draft["username"] == "root"
    assert draft["name"] == "Railway MySQL"


def test_sqlite_draft_is_a_file_path_not_a_host(tmp_path, monkeypatch):
    root = tmp_path / "sqlite"
    root.mkdir()
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(root))
    inside = root / "payments.db"
    ok, err = draft_is_complete({"type": "sqlite", "database": str(inside)})
    assert ok is True, err
    assert err == ""

    ok, err = draft_is_complete({"type": "sqlite"})
    assert ok is False
    assert "path" in err.lower() or "database" in err.lower()

    ok, err = draft_is_complete({"type": "sqlite", "database": str(tmp_path / "outside.db")})
    assert ok is False
    assert "SQLITE_ROOT" in err

    ok, err = draft_is_complete({"type": "duckdb", "database": str(root / "warehouse.duckdb")})
    assert ok is True, err

    ok, err = draft_is_complete({"type": "postgresql", "database": "app"})
    assert ok is False
    assert "host" in err.lower()


def test_sqlite_probe_uses_the_same_path_allowlist(tmp_path, monkeypatch):
    from connectors.sqlite import test_sqlite

    root = tmp_path / "sqlite"
    root.mkdir()
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(root))
    refused = test_sqlite(
        host="",
        port=0,
        database=str(tmp_path / "outside.db"),
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
    )
    assert refused.ok is False
    assert "SQLITE_ROOT" in (refused.error or "")

    opened = test_sqlite(
        host="",
        port=0,
        database=str(root / "probe.db"),
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
    )
    assert opened.ok is True, opened.error
    assert (root / "probe.db").is_file()


def test_named_redis_box_uses_the_redis_port_not_postgres():
    draft = build_connector_draft(
        "",
        {"name": "QA Redis Box", "host": "redis.internal"},
    )
    assert draft["type"] == "redis"
    assert draft["port"] == 6379
    assert draft["host"] == "redis.internal"


def test_specialty_drivers_get_their_listen_ports():
    cases = {
        "elasticsearch": 9200,
        "neo4j": 7474,
        "kafka": 9092,
        "qdrant": 6333,
        "weaviate": 8080,
        "pgvector": 5432,
    }
    for driver, port in cases.items():
        draft = build_connector_draft(
            f"create a {driver} connector host graph.internal",
        )
        assert draft["type"] == driver, driver
        assert draft["port"] == port, driver


def test_explicit_port_is_kept():
    draft = build_connector_draft(
        "create a redis connector host cache.internal port 6380",
    )
    assert draft["type"] == "redis"
    assert draft["port"] == 6380


def test_redis_url_sets_type_and_port():
    parsed = extract_url_credentials("save redis://localhost:6379/0")
    assert parsed is not None
    assert parsed["type"] == "redis"
    assert parsed["port"] == 6379
    assert parsed["host"] == "localhost"


def test_build_draft_from_inline_prose():
    msg = (
        "create a postgres connector named Demo PG host localhost "
        "user demo password secret database appdb"
    )
    draft = build_connector_draft(msg)
    assert draft["type"] == "postgresql"
    assert draft["host"] == "localhost"
    assert draft["username"] == "demo"
    assert draft["password"] == "secret"
    assert draft["database"] == "appdb"
    assert draft["name"] == "Demo PG"
