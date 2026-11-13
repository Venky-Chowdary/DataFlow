"""PR 421 CI (follow-up review 6): a security refusal comes before "driver not installed".

The CI runner has no ODBC driver (``pyodbc.drivers() == []``). The installed-
driver check of eb9d4d326 / 8a4963917 ran inside the libpq / ODBC check, which
``egress_guard.connector_save_refusal`` calls before the TLS host-file check,
so a connector whose ODBC string named a client key on the host was refused
with "Driver ... is not an ODBC driver installed on this server. Use one of: ."
instead of the ``ClientKey`` refusal (``test_secfu33`` failed there and passed
on a machine with msodbcsql18). An uninstalled driver is an availability
error, not a security finding.

Now every security refusal (host files, DSN / FILEDSN / SAVEFILE, a driver path
or library, libpq ``service``, and the rest of the save chain) is reported
first; the installed-driver refusal comes last, at save and at the dial, and
says "No ODBC driver is installed on this server" when there is none. These
tests stub ``pyodbc.drivers()`` both ways, so they do not depend on the
machine.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

_INSTALLED = "ODBC Driver 18 for SQL Server"


@pytest.fixture(params=["none", "installed"])
def drivers(request, monkeypatch, tmp_path):
    pyodbc = pytest.importorskip("pyodbc")
    names = [] if request.param == "none" else [_INSTALLED]
    monkeypatch.setattr(pyodbc, "drivers", lambda: list(names))
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("SOURCE_PATH_ROOTS", "EXPORTS_DIR"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    return names


def _save(**fields) -> str:
    from services.egress_guard import connector_save_refusal

    return connector_save_refusal({"name": "s", "type": "sqlserver", "host": "db.example.com", **fields})


_ODBC = f"Driver={{{_INSTALLED}}};Server=db.example.com;Database=app"


@pytest.mark.parametrize("extra,expected", [
    ("ClientKey=/etc/ssl/private/platform.key", "ClientKey"),
    ("ClientCertificate=/etc/ssl/certs/platform.pem", "ClientCertificate"),
    ("SAVEFILE=/tmp/x.dsn", "SAVEFILE"),
    ("DSN=zz-dsn", "DSN"),
])
def test_a_security_refusal_comes_first_whatever_is_installed(drivers, extra, expected):
    refusal = _save(extra={"odbc_connect": f"{_ODBC};{extra}"})
    assert expected in refusal and "installed" not in refusal


def test_a_driver_library_is_refused_as_such(drivers):
    refusal = _save(connection_string="mssql+pyodbc://u:p@db.example.com/app?driver=/tmp/x/lib.so")
    assert "library file" in refusal


def test_the_installed_driver_is_an_availability_question(drivers):
    refusal = _save(connection_string="mssql+pyodbc://u:p@db.example.com/app?driver=ODBC+Driver+18+for+SQL+Server")
    if drivers:
        assert refusal == ""
    else:
        assert refusal.startswith("No ODBC driver is installed on this server")
        assert "Use one of: ." not in refusal


def test_an_uninstalled_driver_names_what_is_installed(drivers):
    refusal = _save(connection_string="mssql+pyodbc://u:p@db.example.com/app?driver=Some+Other+Driver")
    if drivers:
        assert "is not an ODBC driver installed" in refusal and _INSTALLED in refusal
    else:
        assert refusal.startswith("No ODBC driver is installed on this server")


def test_at_the_dial_security_comes_first(drivers):
    from connectors.host_config_selectors import HostConfigRefused, check_guarded

    dialect = SimpleNamespace(name="mssql", driver="pyodbc")
    with pytest.raises(HostConfigRefused, match="SAVEFILE"):
        check_guarded(dialect, ("DRIVER={Not Installed};Server=h;Database=d;SAVEFILE=/tmp/x.dsn",), {})
    with pytest.raises(HostConfigRefused, match="DSN"):
        check_guarded(dialect, ("DRIVER={Not Installed};dsn=zz",), {})
    with pytest.raises(HostConfigRefused, match="installed"):
        check_guarded(dialect, ("DRIVER={Not Installed};Server=h;Database=d",), {})
    if drivers:
        check_guarded(dialect, (f"DRIVER={{{_INSTALLED}}};Server=h;Database=d",), {})
    else:
        with pytest.raises(HostConfigRefused, match="No ODBC driver is installed"):
            check_guarded(dialect, (f"DRIVER={{{_INSTALLED}}};Server=h;Database=d",), {})
