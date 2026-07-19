"""SQL DSN merge + Railway public/private host scenarios for MySQL & Postgres."""

from __future__ import annotations

from connectors.sql_dsn import (
    parse_sql_url,
    private_cloud_host_hint,
    resolve_sql_endpoint,
)
from src.transfer.connector_registry import humanize_connection_error


def test_parse_postgres_railway_public_url():
    url = "postgresql://postgres:secret@tokaido.proxy.rlwy.net:27396/railway"
    parsed = parse_sql_url(url, family="postgresql")
    assert parsed["host"] == "tokaido.proxy.rlwy.net"
    assert parsed["port"] == 27396
    assert parsed["username"] == "postgres"
    assert parsed["password"] == "secret"
    assert parsed["database"] == "railway"


def test_resolve_fields_only_postgres():
    ep = resolve_sql_endpoint(
        family="postgresql",
        host="tokaido.proxy.rlwy.net",
        port=27396,
        database="railway",
        username="postgres",
        password="secret",
        connection_string="",
        default_port=5432,
    )
    assert ep["host"] == "tokaido.proxy.rlwy.net"
    assert ep["port"] == 27396
    assert ep["database"] == "railway"


def test_resolve_connection_string_only():
    ep = resolve_sql_endpoint(
        family="postgresql",
        host="",
        port=0,
        database="",
        username="",
        password="",
        connection_string="postgresql://postgres:secret@tokaido.proxy.rlwy.net:27396/railway",
        default_port=5432,
    )
    assert ep["host"] == "tokaido.proxy.rlwy.net"
    assert ep["port"] == 27396
    assert ep["username"] == "postgres"
    assert ep["password"] == "secret"
    assert ep["database"] == "railway"


def test_resolve_url_pasted_into_host_field():
    ep = resolve_sql_endpoint(
        family="postgresql",
        host="postgresql://postgres:secret@tokaido.proxy.rlwy.net:27396/railway",
        port=0,
        database="",
        username="",
        password="",
        connection_string="",
        default_port=5432,
    )
    assert ep["host"] == "tokaido.proxy.rlwy.net"
    assert ep["port"] == 27396
    assert ep["database"] == "railway"


def test_explicit_fields_override_url():
    ep = resolve_sql_endpoint(
        family="mysql",
        host="override.example.com",
        port=3307,
        database="app",
        username="u2",
        password="p2",
        connection_string="mysql://u1:p1@original.example.com:3306/other",
        default_port=3306,
    )
    assert ep["host"] == "override.example.com"
    assert ep["port"] == 3307
    assert ep["username"] == "u2"
    assert ep["password"] == "p2"
    assert ep["database"] == "app"


def test_railway_internal_hint():
    hint = private_cloud_host_hint("postgres.railway.internal", "")
    assert "proxy.rlwy.net" in hint
    assert "private" in hint.lower()


def test_humanize_railway_internal():
    msg = humanize_connection_error(
        "postgresql",
        "could not translate host name \"postgres.railway.internal\" to address",
    )
    assert "proxy.rlwy.net" in msg
    assert "private" in msg.lower()


def test_mysql_url_still_parses():
    parsed = parse_sql_url(
        "mysql://root:x@tokaido.proxy.rlwy.net:32253/railway",
        family="mysql",
    )
    assert parsed["port"] == 32253
    assert parsed["database"] == "railway"
