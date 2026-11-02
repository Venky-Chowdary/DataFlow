"""Last-@ URL authority — passwords may contain @."""

from __future__ import annotations

from connectors.sql_dsn import parse_sql_url, resolve_sql_endpoint, sync_credentials_into_connection_string
from connectors.sftp_common import parse_sftp_config
from connectors.url_authority import looks_like_userinfo_host, parse_url_authority, rebuild_url
from connectors.mongodb_common import normalize_mongodb_connection_string


def test_sql_password_keeps_at():
    parsed = parse_url_authority("postgresql://postgres:p@ss@tokaido.proxy.rlwy.net:27396/railway")
    assert parsed.host == "tokaido.proxy.rlwy.net"
    assert parsed.user == "postgres"
    assert parsed.password == "p@ss"
    assert parsed.port == 27396
    assert parsed.path == "/railway"


def test_scheme_less_password_with_at():
    assert looks_like_userinfo_host("postgres:p@ss@tokaido.proxy.rlwy.net:27396/railway")
    parsed = parse_sql_url(
        "postgres:p@ss@tokaido.proxy.rlwy.net:27396/railway",
        family="postgresql",
    )
    assert parsed["host"] == "tokaido.proxy.rlwy.net"
    assert parsed["password"] == "p@ss"
    assert parsed["username"] == "postgres"


def test_resolve_endpoint_password_with_at():
    ep = resolve_sql_endpoint(
        family="postgresql",
        host="localhost",
        port=5432,
        database="",
        username="",
        password="",
        connection_string="postgresql://postgres:venkatesh@170259@proxy.example:27396/app",
        default_port=5432,
    )
    assert ep["host"] == "proxy.example"
    assert ep["password"] == "venkatesh@170259"
    assert ep["username"] == "postgres"


def test_rebuild_encodes_at():
    parsed = parse_url_authority("mysql://root:p@ss@db.example:3306/app")
    rebuilt = rebuild_url(parsed)
    assert "p%40ss" in rebuilt
    assert rebuilt.startswith("mysql://root:p%40ss@db.example:3306/app")


def test_sftp_password_keeps_at():
    cfg = parse_sftp_config(connection_string="sftp://alice:secr@t@ftp.example.com:2222/data/file.csv")
    assert cfg.host == "ftp.example.com"
    assert cfg.username == "alice"
    assert cfg.password == "secr@t"
    assert cfg.port == 2222
    assert cfg.path == "/data/file.csv"


def test_mongo_password_keeps_at():
    uri = normalize_mongodb_connection_string(
        "mongodb://mongo:p@ss@mongodb.railway.internal:27017/trueresume",
        database="trueresume",
        auth_source="admin",
    )
    assert "p%40ss" in uri
    assert "@mongodb.railway.internal:27017" in uri
    assert "authSource=admin" in uri


def test_sync_credentials_rewrites_at_password():
    cfg = {
        "type": "oracle",
        "connection_string": "oracle+oracledb://svc:old@dbhost:1521/?service_name=ORCL",
        "username": "svc",
        "password": "n@w",
    }
    sync_credentials_into_connection_string(cfg)
    assert "n%40w" in cfg["connection_string"]
    assert "dbhost" in cfg["connection_string"]
