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
    # When there is NO usable connection string host, form fields apply.
    ep = resolve_sql_endpoint(
        family="mysql",
        host="override.example.com",
        port=3307,
        database="app",
        username="u2",
        password="p2",
        connection_string="",
        default_port=3306,
    )
    assert ep["host"] == "override.example.com"
    assert ep["port"] == 3307
    assert ep["username"] == "u2"
    assert ep["password"] == "p2"
    assert ep["database"] == "app"


def test_connection_string_beats_localhost_defaults():
    """UI always sends host=localhost port=5432 defaults — must not override Railway URL."""
    ep = resolve_sql_endpoint(
        family="postgresql",
        host="localhost",
        port=5432,
        database="",
        username="",
        password="",
        connection_string=(
            "postgresql://postgres:secret@tokaido.proxy.rlwy.net:27396/railway"
        ),
        default_port=5432,
    )
    assert ep["host"] == "tokaido.proxy.rlwy.net"
    assert ep["port"] == 27396
    assert ep["username"] == "postgres"
    assert ep["password"] == "secret"
    assert ep["database"] == "railway"


def test_mysql_connection_string_beats_default_port():
    ep = resolve_sql_endpoint(
        family="mysql",
        host="localhost",
        port=3306,
        database="",
        username="",
        password="",
        connection_string="mysql://root:x@tokaido.proxy.rlwy.net:32253/railway",
        default_port=3306,
    )
    assert ep["host"] == "tokaido.proxy.rlwy.net"
    assert ep["port"] == 32253
    assert ep["username"] == "root"


def test_railway_internal_hint_outside_railway(monkeypatch):
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("RAILWAY_SERVICE_ID", raising=False)
    hint = private_cloud_host_hint("postgres.railway.internal", "")
    assert "proxy.rlwy.net" in hint
    assert "private" in hint.lower()


def test_railway_internal_hint_inside_railway(monkeypatch):
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    hint = private_cloud_host_hint("mysql.railway.internal", "")
    assert "same" in hint.lower() or "private port" in hint.lower()
    assert "proxy.rlwy.net" not in hint


def test_humanize_railway_internal_outside(monkeypatch):
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("RAILWAY_SERVICE_ID", raising=False)
    msg = humanize_connection_error(
        "postgresql",
        "could not translate host name \"postgres.railway.internal\" to address",
    )
    assert "proxy.rlwy.net" in msg
    assert "public" in msg.lower()


def test_humanize_railway_internal_inside(monkeypatch):
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    msg = humanize_connection_error(
        "mysql",
        "could not translate host name \"mysql.railway.internal\" to address",
    )
    assert "same Railway project" in msg or "private domain" in msg
    assert "proxy.rlwy.net" not in msg


def test_normalize_scheme_less_postgres_dsn():
    raw = "postgres:LoLGXbWuaJWSRAJlmygMCAGGcslAjvRT@tokaido.proxy.rlwy.net:27396/railway"
    from connectors.sql_dsn import normalize_sql_dsn, resolve_sql_endpoint

    assert normalize_sql_dsn(raw, family="postgresql").startswith("postgresql://")
    ep = resolve_sql_endpoint(
        family="postgresql",
        host="",
        port=0,
        database="",
        username="",
        password="",
        connection_string=raw,
        default_port=5432,
    )
    assert ep["host"] == "tokaido.proxy.rlwy.net"
    assert ep["port"] == 27396
    assert ep["username"] == "postgres"
    assert ep["password"] == "LoLGXbWuaJWSRAJlmygMCAGGcslAjvRT"
    assert ep["database"] == "railway"
    assert ep["host"] != "localhost"


def test_mysql_url_still_parses():
    parsed = parse_sql_url(
        "mysql://root:x@tokaido.proxy.rlwy.net:32253/railway",
        family="mysql",
    )
    assert parsed["port"] == 32253
    assert parsed["database"] == "railway"
