"""Tests for SQLAlchemy drivername resolution used by Query + generic SQL."""

from connectors.generic_sql import (
    _build_url,
    _drivername,
    _mssql_drivername,
    _mssql_odbc_driver,
    _normalize_sqlalchemy_url_string,
    adapt_mssql_sql,
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


def test_mssql_drivername_falls_back_to_pymssql_without_odbc_driver(monkeypatch):
    """pyodbc imported is not enough — missing libmsodbcsql must not win."""
    import connectors.generic_sql as gs

    monkeypatch.setattr(gs, "_mssql_odbc_driver", lambda: None)
    assert _mssql_drivername() == "mssql+pymssql"


def test_mssql_odbc_driver_prefers_installed_microsoft_driver():
    found = _mssql_odbc_driver()
    if found is None:
        assert _mssql_drivername() == "mssql+pymssql"
        return
    assert found.startswith("ODBC Driver")
    assert _mssql_drivername() == "mssql+pyodbc"
    url = str(
        _build_url(
            {
                "type": "sqlserver",
                "host": "127.0.0.1",
                "port": 1433,
                "database": "dataflow",
                "username": "sa",
                "password": "x",
            }
        )
    )
    assert "mssql+pyodbc" in url
    assert "ODBC+Driver" in url or "ODBC Driver" in url


def test_adapt_mssql_sql_rewrites_percent_s_for_pyodbc(monkeypatch):
    import connectors.generic_sql as gs

    monkeypatch.setattr(gs, "_mssql_drivername", lambda: "mssql+pyodbc")
    assert (
        adapt_mssql_sql("WHERE t.name = %s AND s.name = %s")
        == "WHERE t.name = ? AND s.name = ?"
    )


def test_adapt_mssql_sql_keeps_percent_s_for_pymssql(monkeypatch):
    import connectors.generic_sql as gs

    monkeypatch.setattr(gs, "_mssql_drivername", lambda: "mssql+pymssql")
    sql = "WHERE t.name = %s AND s.name = %s"
    assert adapt_mssql_sql(sql) == sql


def test_build_url_encodes_at_in_password():
    url = str(_build_url({
        "type": "mysql",
        "connection_string": "mysql://root:p@ss@127.0.0.1:3306/demo",
    }))
    assert url.startswith("mysql+pymysql://")
    assert "p%40ss" in url
    assert "127.0.0.1" in url
    assert "demo" in url


def test_build_url_sqlserver_trust_server_certificate():
    url = _build_url(
        {
            "type": "sqlserver",
            "host": "127.0.0.1",
            "port": 1433,
            "database": "dataflow",
            "username": "sa",
            "password": "x",
            "trust_server_certificate": True,
            "encrypt": "yes",
        }
    )
    query = dict(getattr(url, "query", {}) or {})
    assert query.get("TrustServerCertificate") == "Yes"
    assert str(query.get("Encrypt") or "").lower() == "yes"
