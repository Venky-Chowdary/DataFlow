"""Tests for SQLAlchemy drivername resolution used by Query + generic SQL."""

from connectors.generic_sql import (
    _build_url,
    _drivername,
    _normalize_sqlalchemy_url_string,
)


def test_mysql_drivername_uses_pymysql():
    assert _drivername("mysql") == "mysql+pymysql"
    assert _drivername("mariadb") == "mysql+pymysql"
    assert _drivername("amazon_rds_mysql") == "mysql+pymysql"


def test_postgres_drivername_uses_psycopg2():
    assert _drivername("postgresql") == "postgresql+psycopg2"
    assert _drivername("postgres") == "postgresql+psycopg2"
    assert _drivername("redshift") == "postgresql+psycopg2"


def test_normalize_mysql_connection_string_scheme():
    assert _normalize_sqlalchemy_url_string(
        "mysql://user:pass@localhost:3306/app",
        "mysql",
    ).startswith("mysql+pymysql://")
    assert _normalize_sqlalchemy_url_string(
        "mysql+pymysql://user:pass@localhost:3306/app",
        "mysql",
    ).startswith("mysql+pymysql://")


def test_build_url_mysql_host_port_uses_pymysql_driver():
    url = _build_url({
        "type": "mysql",
        "host": "127.0.0.1",
        "port": 3306,
        "database": "demo",
        "username": "root",
        "password": "secret",
    })
    assert str(url).startswith("mysql+pymysql://")
    assert "demo" in str(url)


def test_build_url_rewrites_saved_mysql_dsn():
    url = _build_url({
        "type": "mysql",
        "connection_string": "mysql://root:x@127.0.0.1:3306/demo",
    })
    assert str(url).startswith("mysql+pymysql://")


def test_mysql_compatible_catalog_types_share_pymysql():
    for t in ("tidb", "mariadb", "planetscale", "amazon_aurora"):
        assert _drivername(t) == "mysql+pymysql"


def test_normalize_postgres_and_redshift_schemes():
    assert _normalize_sqlalchemy_url_string(
        "postgres://u:p@h:5432/db",
    ).startswith("postgresql+psycopg2://")
    assert _normalize_sqlalchemy_url_string(
        "postgresql://u:p@h:5432/db",
    ).startswith("postgresql+psycopg2://")
    assert _normalize_sqlalchemy_url_string(
        "redshift://u:p@h:5439/db",
    ).startswith("postgresql+psycopg2://")
    # Already-correct scheme left alone.
    assert _normalize_sqlalchemy_url_string(
        "postgresql+psycopg2://u:p@h:5432/db",
    ).startswith("postgresql+psycopg2://")


def test_normalize_preserves_password_with_at_sign():
    raw = "mysql://root:p@ss@127.0.0.1:3306/demo"
    out = _normalize_sqlalchemy_url_string(raw, "mysql")
    assert out.startswith("mysql+pymysql://")
    assert "p@ss@" in out
