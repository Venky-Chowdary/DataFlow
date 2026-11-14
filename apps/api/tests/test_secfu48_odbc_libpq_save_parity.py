"""Follow-up review 5 (L4): the ODBC / libpq save check reads what SQLAlchemy and the dial read.

Commit c60454294 judged a URL as ODBC only when its scheme said ``odbc``. A
plain ``mssql://`` URL is ``mssql+pyodbc`` (SQLAlchemy's default driver), so
``?driver=<path>``, the host-as-DSN form and ``?odbc_connect=DSN%3D...`` were
saved and only refused at the dial. It also refused ``mssql+pyodbc://u:p@host
:1433/?database=sales&...`` as host-as-DSN, but SQLAlchemy reads ``database``
from the query and builds a normal ``Server=`` string. At the dial a bare
driver name that is not installed, or a libtool ``.la``, was not refused (the
driver manager may load either as a library). ``connect_args.service`` on a
generic SQL connector with a PostgreSQL URL passed save.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

_INSTALLED = "ODBC Driver 18 for SQL Server"


@pytest.fixture(autouse=True)
def drivers(monkeypatch):
    from connectors import host_config_selectors

    monkeypatch.setattr(host_config_selectors, "installed_odbc_drivers", lambda: frozenset({_INSTALLED}))


def _save(**data) -> str:
    from connectors.host_config_selectors import settings_refusal

    return settings_refusal(data)


def _dial(url: str) -> None:
    from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc
    from sqlalchemy.engine import make_url

    from connectors.host_config_selectors import check_guarded

    cargs, cparams = MSDialect_pyodbc().create_connect_args(make_url(url))
    check_guarded(SimpleNamespace(name="mssql", driver="pyodbc"), cargs, cparams)


@pytest.mark.parametrize("url", [
    "mssql://u:p@db.example.com/sales?driver=/tmp/x/lib.so",
    "mssql://u:p@zzdsn",
    "mssql:///?odbc_connect=DSN%3Dzzdsn%3BUID%3Du",
    "sybase://u:p@db.example.com/sales?driver=/tmp/x/lib.so",
])
@pytest.mark.parametrize("kind", ["mssql", "sqlserver", "generic_sql"])
def test_plain_mssql_spellings_are_refused_at_save(url, kind):
    refusal = _save(type=kind, connection_string=url)
    assert refusal and "zzdsn" not in refusal and "/tmp/x" not in refusal


def test_a_database_in_the_query_is_not_a_dsn():
    url = "mssql+pyodbc://u:p@db.example.com:1433/?database=sales&driver=ODBC+Driver+18+for+SQL+Server"
    assert _save(type="sqlserver", connection_string=url) == ""
    _dial(url)


def test_mssql_pymssql_is_not_odbc():
    assert _save(type="sqlserver", connection_string="mssql+pymssql://u:p@db.example.com:1433") == ""


@pytest.mark.parametrize("driver", ["libfoo", "x.la", "x.a", "lib.so.6", "x.bundle"])
def test_the_dial_refuses_a_driver_that_is_not_installed_or_a_library(driver):
    from connectors.host_config_selectors import HostConfigRefused

    with pytest.raises(HostConfigRefused, match="Driver"):
        _dial(f"mssql+pyodbc://u:p@db.example.com/sales?driver={driver}")
    assert _save(type="sqlserver", connection_string=f"mssql+pyodbc://u:p@db.example.com/sales?driver={driver}")


def test_the_dial_accepts_the_installed_driver():
    _dial("mssql+pyodbc://u:p@db.example.com/sales?driver=ODBC+Driver+18+for+SQL+Server")


@pytest.mark.parametrize("where", ["connect_args", "extra"])
def test_connect_args_service_on_a_generic_sql_postgres_connector(where):
    data = {"type": "generic_sql", "connection_string": "postgresql://u@db.example.com/app"}
    if where == "connect_args":
        data["connect_args"] = {"service": "prod"}
    else:
        data["extra"] = {"connect_args": {"service": "prod"}}
    assert "pg_service" in _save(**data)


def test_a_password_that_looks_like_service_is_fine():
    assert _save(type="postgresql", connection_string="postgresql://u:service%3Dx@db.example.com/app") == ""
