"""Pilot create_connector routing + credential extraction."""

from __future__ import annotations

from src.ai.copilot.connector_create import (
    build_connector_draft,
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
