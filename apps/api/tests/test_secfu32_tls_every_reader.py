"""Follow-up review 3 (H1 + L1): a TLS file option is found however the value is spelled.

``tls_file_options.text_refusal`` used to pick ONE reader by a substring of
the text: any ``://`` meant "URL query only", else any ``;`` meant "ODBC pairs
only". The consumers do not read that way: libpq reads a conninfo with
``application_name=a;b`` or ``application_name=x://y`` as one value and still
takes ``sslcert`` / ``sslkey`` after it, and the ODBC driver manager reads
``APP=x://y`` as one value and still takes ``ServerCertificate`` /
``ClientCertificate`` / ``ClientKey``. So a ``;`` or ``://`` in any value hid
every file option (PostgreSQL replication DSN, generic_sql
``postgresql+psycopg2://...?dsn=<conninfo>``, ``mssql+pyodbc:///?odbc_connect=``).
A libpq URI with a ``?`` in the password and several hosts hid the query from
the "text after the first ?" reading, and SQLAlchemy cannot parse it (L1).

Now every reader runs (libpq's own ``parse_dsn``, the URL query as SQLAlchemy
and as "after each ?" read it, the brace-aware ODBC pairs, conninfo pairs) and
the text is refused when ANY of them finds a file option outside the areas.
"""

from __future__ import annotations

import socket
import urllib.parse

import pytest

_KEY = "/etc/ssl/private/c.key"
_CERT = "/etc/ssl/private/c.pem"
_CA = "/etc/ssl/certs/ca-certificates.crt"


class Dialled(Exception):
    """The driver's connect was reached (recorded, never a socket)."""


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("SOURCE_PATH_ROOTS", "EGRESS_ENFORCE_NEW", "EGRESS_ALLOWLIST"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )
    monkeypatch.setattr(
        socket, "getaddrinfo",
        lambda host, port, *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.215.14", port or 0))],
    )


@pytest.fixture
def drivers(monkeypatch):
    import psycopg2
    import pyodbc

    calls: list[tuple[str, tuple, dict]] = []

    def recorder(name):
        def connect(*args, **kwargs):
            calls.append((name, args, kwargs))
            raise Dialled(name)

        return connect

    monkeypatch.setattr(psycopg2, "connect", recorder("psycopg2"))
    monkeypatch.setattr(pyodbc, "connect", recorder("pyodbc"))
    monkeypatch.setattr("connectors.generic_sql._mssql_odbc_driver", lambda: "ODBC Driver 18 for SQL Server")
    return calls


def _named_no_path(message: str) -> None:
    assert message
    for value in ("/etc/", "c.key", "c.pem", "ca-certificates"):
        assert value not in message, message


_CONNINFO = [
    pytest.param(f"host=db.example.com dbname=d application_name=a;b sslcert={_CERT} sslkey={_KEY}",
                 id="conninfo-semicolon-in-value"),
    pytest.param(f"host=db.example.com dbname=d application_name=x://y sslcert={_CERT} sslkey={_KEY}",
                 id="conninfo-scheme-in-value"),
    pytest.param(f"host=db.example.com dbname=d application_name='a;b x://y' sslkey={_KEY}",
                 id="conninfo-quoted-value"),
    pytest.param(f"postgresql://u:p?x@db1.example.com:5432,db2.example.com:5432/d?sslcert={_CERT}",
                 id="uri-question-mark-in-password-multihost"),
    pytest.param(f"postgresql://u:p?x@db1.example.com:5432,db2.example.com:5432/d?sslkey={_KEY}"
                 "&application_name=a", id="uri-question-mark-in-password-multihost-key"),
    pytest.param(f"postgresql://u:p@db.example.com/d?application_name=a%3Bb&sslkey={_KEY}",
                 id="uri-encoded-semicolon"),
]


@pytest.mark.parametrize("dsn", _CONNINFO)
def test_a_libpq_dsn_is_refused_at_save(dsn):
    from psycopg2.extensions import parse_dsn

    from services.egress_guard import connector_save_refusal

    parsed = parse_dsn(dsn)
    assert parsed.get("sslcert") or parsed.get("sslkey")  # what libpq would read
    _named_no_path(connector_save_refusal({"type": "postgresql", "name": "x", "connection_string": dsn}))


@pytest.mark.parametrize("dsn", _CONNINFO)
def test_the_replication_transport_refuses_the_dsn(drivers, dsn):
    from connectors.postgresql_cdc_transport import StreamingReplicationTransport

    transport = StreamingReplicationTransport(dsn_kwargs={"dsn": dsn}, slot_name="s", publication_name="p")
    with pytest.raises(ValueError) as caught:
        transport._open()
    _named_no_path(str(caught.value))
    assert drivers == []


def _engine_dial(cfg: dict) -> None:
    from connectors.generic_sql import _build_engine

    engine = _build_engine(dict(cfg))
    try:
        engine.connect()
    finally:
        engine.dispose()


def test_generic_sql_refuses_a_conninfo_in_the_dsn_query(drivers):
    from services.egress_guard import connector_save_refusal

    conninfo = f"host=db.example.com dbname=d application_name=a;b sslcert={_CERT} sslkey={_KEY}"
    cfg = {"type": "postgresql", "name": "x",
           "connection_string": "postgresql+psycopg2://u:p@db.example.com:5432/d?dsn=" + urllib.parse.quote(conninfo)}
    _named_no_path(connector_save_refusal(dict(cfg)))
    with pytest.raises(ValueError) as caught:
        _engine_dial(cfg)
    _named_no_path(str(caught.value))
    assert drivers == []


@pytest.mark.parametrize("app", ["x://y", "{x://y;z}", "postgresql://h/d?a=b"])
def test_an_odbc_connect_string_with_a_scheme_in_a_value_is_refused(drivers, app):
    from services.egress_guard import connector_save_refusal

    odbc = ("Driver={ODBC Driver 18 for SQL Server};Server=tcp:db.example.com,1433;Database=d;UID=u;PWD=p;"
            f"APP={app};Encrypt=yes;ServerCertificate={_CA};"
            f"ClientCertificate=file:{_CERT};ClientKey=file:{_KEY}")
    cfg = {"type": "sqlserver", "name": "x",
           "connection_string": "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc)}
    _named_no_path(connector_save_refusal(dict(cfg)))
    with pytest.raises(ValueError) as caught:
        _engine_dial(cfg)
    _named_no_path(str(caught.value))
    assert drivers == []


def test_a_raw_odbc_string_with_a_scheme_in_a_value_is_refused_at_save():
    from services.egress_guard import connector_save_refusal

    odbc = f"Driver={{ODBC Driver 18 for SQL Server}};Server=db.example.com;APP=x://y;ClientKey=file:{_KEY}"
    _named_no_path(connector_save_refusal({"type": "sqlserver", "name": "x", "connection_string": odbc}))


@pytest.mark.parametrize("data", [
    pytest.param({"type": "postgresql", "connection_string":
                  "host=db.example.com dbname=d application_name=a;b sslmode=require"}, id="conninfo-no-file"),
    pytest.param({"type": "postgresql", "connection_string":
                  "postgresql://u:p?x@db1.example.com:5432,db2.example.com:5432/d?sslmode=verify-full"
                  "&sslrootcert=system"}, id="uri-system-ca"),
    pytest.param({"type": "sqlserver", "connection_string":
                  "Driver={ODBC Driver 18 for SQL Server};Server=db.example.com;APP=x://y;Encrypt=yes"},
                 id="odbc-no-file"),
    pytest.param({"type": "mongodb", "connection_string": "mongodb://u:p@db.example.com/d?tls=true"},
                 id="mongo-no-file"),
])
def test_settings_that_name_no_host_file_still_save(data):
    from services.egress_guard import connector_save_refusal

    assert connector_save_refusal(dict(data, name="x")) == ""
