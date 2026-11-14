"""Follow-up review 6: an installed ODBC driver named in another letter case is accepted.

Commit eb9d4d326 compared a ``Driver`` / ``?driver=`` value with
``pyodbc.drivers()`` exactly, so ``?driver=odbc+driver+18+for+sql+server`` was
refused at save and at the dial. The driver manager looks a driver up in
``odbcinst.ini`` without regard to case: with unixODBC 2.3.12 (this image),
``DRIVER={odbc driver 18 for sql server}`` loads the installed Microsoft driver
(it reached the server: "Login timeout expired"), while a name with doubled or
surrounding spaces is not found and is tried as a library file ("Can't open
lib ... file not found"). So names are compared case-insensitively, and only
case-insensitively: odd spacing is not the installed driver and stays refused,
as do uninstalled names and paths.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

_INSTALLED = "ODBC Driver 18 for SQL Server"


@pytest.fixture(autouse=True)
def drivers(monkeypatch):
    from connectors import host_config_selectors

    monkeypatch.setattr(host_config_selectors, "installed_odbc_drivers", lambda: frozenset({_INSTALLED}))


def _save(driver: str) -> str:
    from connectors.host_config_selectors import settings_refusal

    return settings_refusal({"type": "sqlserver",
                             "connection_string": f"mssql+pyodbc://u:p@db.example.com/sales?driver={driver}"})


def _dial(driver: str) -> None:
    from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc
    from sqlalchemy.engine import make_url

    from connectors.host_config_selectors import check_guarded

    url = f"mssql+pyodbc://u:p@db.example.com/sales?driver={driver}"
    cargs, cparams = MSDialect_pyodbc().create_connect_args(make_url(url))
    check_guarded(SimpleNamespace(name="mssql", driver="pyodbc"), cargs, cparams)


@pytest.mark.parametrize("driver", ["odbc+driver+18+for+sql+server", "ODBC+DRIVER+18+FOR+SQL+SERVER",
                                    "Odbc+Driver+18+For+Sql+Server"])
def test_the_installed_driver_in_another_case_is_accepted(driver):
    assert _save(driver) == ""
    _dial(driver)
    from connectors.host_config_selectors import settings_refusal

    assert settings_refusal({"type": "sqlserver", "extra": {"driver": driver.replace("+", " ")}}) == ""


@pytest.mark.parametrize("driver", ["ODBC++Driver+18+for+SQL+Server", "+ODBC+Driver+18+for+SQL+Server+",
                                    "ODBC+Driver+17+for+SQL+Server", "/opt/microsoft/msodbcsql18/lib64/x.so"])
def test_odd_spacing_uninstalled_and_paths_stay_refused(driver):
    from connectors.host_config_selectors import HostConfigRefused

    assert _save(driver)
    with pytest.raises(HostConfigRefused):
        _dial(driver)
