"""Follow-up review 4 (L): a connection string cannot select the API host's own client configuration.

* libpq ``service=<name>`` reads that section of the host's
  ``~/.pg_service.conf`` / ``PGSERVICEFILE`` (host, port, user, password,
  TLS files) and dials it with the platform's settings.
* ODBC ``Driver=<path>`` (or ``?driver=`` in a ``+pyodbc`` URL, which
  SQLAlchemy writes as ``DRIVER={...}``) makes the driver manager load that
  shared library into the API process.
* ODBC ``DSN=`` reads a data source from the host's ``odbc.ini``; SQLAlchemy
  also writes ``dsn=<host>`` for a ``mssql+pyodbc://user:pw@host`` URL with no
  database. ``FILEDSN`` / ``SAVEFILE`` read or write a DSN file. The TLS-file
  rule judged ``FileDSN`` / ``SaveFile`` paths only by area, ``DSN`` and
  ``Driver`` not at all, and ``service`` was not read.

Now ``service`` is refused for a PostgreSQL-family connector (and wherever
libpq itself would read it); ``Driver`` may not be a path or a library file
and, at save, must be an installed ODBC driver's name; ``DSN`` is refused in
an ODBC string, in a ``+pyodbc`` URL's query and in the host-as-DSN URL form;
``FILEDSN`` / ``SAVEFILE`` are refused everywhere. Every reading of a string
runs (``tls_file_options._readings``: libpq, conninfo pairs, URL query, ODBC
pairs), at save and on the arguments each guarded engine's driver receives.
Messages name the option and the remedy, never a value.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

_INSTALLED = "ODBC Driver 18 for SQL Server"


@pytest.fixture(autouse=True)
def env(tmp_path, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("SOURCE_PATH_ROOTS", "EXPORTS_DIR", "EGRESS_ENFORCE_NEW"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(allowed))
    try:
        from connectors import host_config_selectors
    except ImportError:  # before the fix: the tests fail on their assertions, not here
        host_config_selectors = None
    if host_config_selectors is not None:
        monkeypatch.setattr(host_config_selectors, "installed_odbc_drivers", lambda: frozenset({_INSTALLED}))
    monkeypatch.setattr(
        "socket.socket.connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled"))
    )
    return {"allowed": allowed}


def _save(**fields):
    from services.egress_guard import connector_save_refusal

    return connector_save_refusal({"name": "c", "host": "db.example.com", **fields})


# --- libpq service ---------------------------------------------------------------------------------

_SERVICE = [
    {"connection_string": "postgresql://db.example.com/app?service=zzsentinel"},
    {"connection_string": "postgresql://db.example.com/app?sslmode=require&service=zzsentinel"},
    {"connection_string": "host=db.example.com service=zzsentinel dbname=app"},
    {"connection_string": "postgresql://db.example.com/app?application_name=a;b&service=zzsentinel"},
    {"extra": {"service": "zzsentinel"}},
    {"connect_args": {"service": "zzsentinel"}},
    {"extra": {"connect_args": {"service": "zzsentinel"}}},
    {"extra": {"connection_string": "postgresql://db.example.com/app?service=zzsentinel"}},
]


@pytest.mark.parametrize("fields", _SERVICE)
def test_libpq_service_is_refused_for_postgresql(fields):
    refusal = _save(type="postgresql", **fields)
    assert "service" in refusal and "pg_service" in refusal
    assert "zzsentinel" not in refusal


def test_service_in_a_libpq_string_is_refused_for_any_connector():
    assert "pg_service" in _save(type="redshift", connection_string="host=h service=zzsentinel")


def test_other_engines_and_service_name_are_unaffected():
    assert _save(type="snowflake", extra={"service": "x"}) == ""
    assert _save(type="oracle", extra={"service_name": "ORCLPDB1"}) == ""
    assert _save(type="postgresql", connection_string="postgresql://db.example.com/app?sslmode=require") == ""


def test_service_is_refused_at_the_dial():
    from connectors.tls_file_options import check_guarded_dial

    dialect = SimpleNamespace(name="postgresql", driver="psycopg2", _dataflow_tls_file_guard=True)
    with pytest.raises(ValueError, match="pg_service"):
        check_guarded_dial(dialect, (), {"host": "db.example.com", "service": "zzsentinel"})
    with pytest.raises(ValueError, match="pg_service"):
        check_guarded_dial(dialect, (), {"dsn": "host=db.example.com service=zzsentinel"})
    check_guarded_dial(dialect, (), {"host": "db.example.com", "dbname": "app"})


def test_a_real_engine_refuses_service_before_libpq(monkeypatch):
    import psycopg2

    from connectors.generic_sql import _build_engine

    called: list = []
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: called.append(k) or (_ for _ in ()).throw(
        AssertionError("psycopg2.connect reached")))
    engine = _build_engine({"type": "postgresql", "connection_string":
                            "postgresql://u:p@db.example.com:5432/app?service=zzsentinel"})
    try:
        with pytest.raises(ValueError, match="pg_service"):
            engine.connect()
    finally:
        engine.dispose()
    assert called == []


def test_the_replication_dial_refuses_service():
    from connectors.host_config_selectors import check_libpq_kwargs

    with pytest.raises(ValueError, match="pg_service"):
        check_libpq_kwargs({"host": "db.example.com", "service": "zzsentinel"})
    with pytest.raises(ValueError, match="pg_service"):
        check_libpq_kwargs({"dsn": "host=db.example.com service=zzsentinel"})
    check_libpq_kwargs({"host": "db.example.com", "dbname": "app"})


# --- ODBC Driver / DSN / FILEDSN / SAVEFILE ----------------------------------------------------------

_DRIVER_PATHS = [
    {"connection_string": "mssql+pyodbc://u:p@db.example.com/app?driver=/tmp/evil.so"},
    {"connection_string": "mssql+pyodbc://u:p@db.example.com/app?driver=%2Ftmp%2Fevil.so"},
    {"connection_string": "mssql+pyodbc://u:p@db.example.com/app?driver=libevil.so.1"},
    {"connection_string": "mssql+pyodbc://u:p@db.example.com/app?driver=C:%5Cx%5Cevil.dll"},
    {"connection_string": "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7B%2Ftmp%2Fevil.so%7D%3BSERVER%3Dh"},
    {"extra": {"odbc_connect": "Driver={/tmp/evil.so};Server=db.example.com"}},
    {"extra": {"driver": "/opt/lib/evil.so"}},
    {"extra": {"odbc_driver": "evil.dylib"}},
    {"connect_args": {"Driver": "/tmp/evil.so"}},
]


@pytest.mark.parametrize("fields", _DRIVER_PATHS)
def test_an_odbc_driver_path_is_refused(fields):
    refusal = _save(type="sqlserver", **fields)
    assert "Driver" in refusal and "evil" not in refusal


def test_an_odbc_driver_must_be_installed_at_save():
    refusal = _save(type="sqlserver", connection_string="mssql+pyodbc://u:p@db.example.com/app?driver=Some+Driver")
    assert "installed" in refusal and _INSTALLED in refusal
    url = "mssql+pyodbc://u:p@db.example.com/app?driver=ODBC+Driver+18+for+SQL+Server"
    assert _save(type="sqlserver", connection_string=url) == ""
    assert _save(type="sqlserver", extra={"driver": _INSTALLED}) == ""


_DSN = [
    {"connection_string": "mssql+pyodbc://u:p@db.example.com/app?dsn=zzsentinel"},
    {"connection_string": "mssql+pyodbc:///?odbc_connect=DSN%3Dzzsentinel"},
    {"connection_string": "mssql+pyodbc://u:p@zzsentinel"},
    {"connection_string": "mssql+pyodbc://u:p@db.example.com:1433"},
    {"extra": {"odbc_connect": "DSN=zzsentinel;UID=u"}},
    {"extra": {"odbc_connect": "Server=db.example.com;{DSN}=zzsentinel"}},
]


@pytest.mark.parametrize("fields", _DSN)
def test_an_odbc_dsn_is_refused(fields):
    refusal = _save(type="sqlserver", **fields)
    assert "DSN" in refusal and "odbc.ini" in refusal and "zzsentinel" not in refusal


@pytest.mark.parametrize("key", ["FILEDSN", "FileDsn", "SAVEFILE", "save_file"])
def test_filedsn_and_savefile_are_refused_even_inside_an_area(env, key):
    inside = str(env["allowed"] / "x.dsn")
    for fields in (
        {"extra": {"odbc_connect": f"{key}={inside};Server=db.example.com"}},
        {"extra": {key: inside}},
        {"connection_string": f"mssql+pyodbc://u:p@db.example.com/app?{key}={inside}"},
    ):
        refusal = _save(type="sqlserver", **fields)
        assert refusal and inside not in refusal, fields
    assert _save(type="postgresql", extra={key: inside})


@pytest.mark.parametrize(
    "odbc",
    ["DRIVER={/tmp/evil.so};Server=db.example.com;Database=app", "dsn=zzsentinel;UID=u;PWD=p",
     "dsn=db.example.com;UID=u;PWD=p;port=1433", "DRIVER={ODBC Driver 18 for SQL Server};SAVEFILE=/tmp/x.dsn"],
)
def test_the_odbc_string_the_driver_receives_is_checked(odbc):
    from connectors.tls_file_options import check_guarded_dial

    dialect = SimpleNamespace(name="mssql", driver="pyodbc", _dataflow_tls_file_guard=True)
    with pytest.raises(ValueError):
        check_guarded_dial(dialect, (odbc,), {})


def test_the_products_own_odbc_string_passes():
    from connectors.tls_file_options import check_guarded_dial

    dialect = SimpleNamespace(name="mssql", driver="pyodbc", _dataflow_tls_file_guard=True)
    for odbc in (f"DRIVER={{{_INSTALLED}}};Server=db.example.com,1433;Database=app;UID=u;PWD=p",
                 "DRIVER={ODBC Driver 17 for SQL Server};Server=db.example.com;Database=app"):
        check_guarded_dial(dialect, (odbc,), {})


def test_a_real_engine_refuses_a_driver_library_before_pyodbc(monkeypatch):
    pyodbc = pytest.importorskip("pyodbc")
    from connectors.generic_sql import _build_engine

    called: list = []
    monkeypatch.setattr(pyodbc, "connect", lambda *a, **k: called.append(a) or (_ for _ in ()).throw(
        AssertionError("pyodbc.connect reached")))
    engine = _build_engine({"type": "sqlserver", "connection_string":
                            "mssql+pyodbc://u:p@db.example.com:1433/app?driver=/tmp/evil.so"})
    try:
        with pytest.raises(ValueError, match="Driver"):
            engine.connect()
    finally:
        engine.dispose()
    assert called == []
