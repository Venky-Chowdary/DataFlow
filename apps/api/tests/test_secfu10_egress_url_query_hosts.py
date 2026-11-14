"""Follow-up review 1, H1: the egress guard reads the host parameters in a URL's query.

libpq (and SQLAlchemy before it) takes ``host`` / ``hostaddr`` from a URL's
query and dials them instead of, or as well as, the authority host. The guard
judged only the authority, so ``postgresql://u:p@db.example.com/db?host=2852039166``
reached the cloud metadata address (169.254.169.254 written as one integer)
and ``?host=%2Frun%2Fpostgresql`` a Unix socket on the API host. libpq is C
code, so the Python socket guard never sees those dials: the pre-dial check
is the only one.

The guard now reads the query as residency does
(``residency_addressing.url_query_pairs``: SQLAlchemy keeps a ``#`` inside the
query), unquoted and comma-split, and judges every value. A ``/``-prefixed
value (or libpq's ``@`` abstract socket, or an empty ``host=``, which is the
default socket) is a local socket: refused like loopback for every engine that
is not a file engine. An authority that yields no host once decoded (encoded
twice) fails closed.
"""

from __future__ import annotations

import socket

import pytest

from services import egress_guard as eg

_ORDINARY = "postgresql://u:p@db.example.com:5432/db?sslmode=require"

# Every input in the review's finding, plus the ``#`` rule, a comma list, the
# other address keys and the socket spellings.
_METADATA = [
    "postgresql://u:p@db.example.com/db?host=2852039166",
    "postgresql://u:p@db.example.com/db?hostaddr=2852039166",
    "postgresql://u:p@db.example.com/db?host=169.254.169.254",
    "postgresql://u:p@db.example.com/db?x=1#&host=2852039166",
    "postgresql://u:p@db.example.com/db#?host=2852039166",
    "postgresql://u:p@db.example.com/db?host=db2.example.com,2852039166",
    "postgresql://u:p@db.example.com/db?HOST=%32%38%35%32%30%33%39%31%36%36",
    "postgresql://u:p@db.example.com/db?server=2852039166",
    "postgresql://u:p@%2531%2536%2539.254.169.254:5432/db",
]
_LOCAL = [
    "postgresql://u:p@db.example.com/db?host=localhost",
    "postgresql://u:p@db.example.com/db?host=0x7f000001",
    "postgresql://u:p@db.example.com/db?host=%2Frun%2Fpostgresql",
    "postgresql://u:p@db.example.com/db?host=/tmp",
    "postgresql://u:p@db.example.com/db?host=@abstract",
    "postgresql://u:p@db.example.com/db?host=",
    "postgresql://u:p@/db",
    "postgresql:///db?sslmode=require",
    "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7Bx%7D%3BSERVER%3Dlocalhost%3BDATABASE%3Dd",
]


def _fake_getaddrinfo(host, *args, **kwargs):
    """No DNS: numeric spellings resolve as the C library reads them, localhost is loopback."""
    if str(host).lower() == "localhost":
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0))]
    return _REAL_GETADDRINFO(host, None, flags=socket.AI_NUMERICHOST)


_REAL_GETADDRINFO = socket.getaddrinfo


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_EGRESS_ALLOWLIST", raising=False)
        monkeypatch.delenv(f"{prefix}_EGRESS_ENFORCE_NEW", raising=False)
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo)
    monkeypatch.setattr(eg, "_workspace_egress_entries", lambda _ws: [])

    def _no_dial(*_a, **_k):
        raise AssertionError("a test dialled")

    monkeypatch.setattr(socket.socket, "connect", _no_dial)


@pytest.fixture
def driver_calls(monkeypatch):
    import psycopg2

    calls: list[dict] = []

    def _connect(*_a, **kwargs):
        calls.append(kwargs)
        raise RuntimeError("intercepted: the driver was reached")

    monkeypatch.setattr(psycopg2, "connect", _connect)
    return calls


def _cfg(url: str, db_type: str = "postgresql") -> dict:
    return {"type": db_type, "host": "db.example.com", "connection_string": url, "workspace_id": "ws-secfu10"}


def test_premise_the_driver_layer_reads_these_query_keys():
    import psycopg2.extensions
    import sqlalchemy as sa

    query = dict(sa.engine.url.make_url("postgresql://u:p@db.example.com/db?x=1#&host=2852039166").query)
    assert query.get("host") == "2852039166"
    # libpq itself (a URI handed to psycopg2 as the DSN) replaces the authority host,
    # with an empty value meaning its default Unix socket.
    parse = psycopg2.extensions.parse_dsn
    assert parse("postgresql://u:p@db.example.com/db?host=%2Frun%2Fpostgresql")["host"] == "/run/postgresql"
    assert parse("postgresql://u:p@db.example.com/db?host=")["host"] == ""
    assert "host" not in parse("postgresql:///db")


@pytest.mark.parametrize("url", _METADATA + _LOCAL)
def test_build_engine_refuses_before_the_driver_is_called(url, driver_calls):
    from connectors.generic_sql import _build_engine

    db_type = "sqlserver" if url.startswith("mssql") else "postgresql"
    with pytest.raises(eg.EgressRefused):
        _build_engine(_cfg(url, db_type))
    assert driver_calls == []


@pytest.mark.parametrize("url", _METADATA)
def test_saving_a_metadata_target_in_the_query_is_refused_without_any_flag(url):
    refusal = eg.connector_save_refusal({"name": "c", **_cfg(url)})
    assert refusal, url
    assert "p@" not in refusal, "the refusal must not echo the URL's credentials"


@pytest.mark.parametrize("url", _LOCAL)
def test_saving_a_local_target_in_the_query_is_judged_like_the_same_authority_host(url, monkeypatch):
    db_type = "sqlserver" if url.startswith("mssql") else "postgresql"
    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    assert eg.connector_save_refusal({"name": "c", **_cfg(url, db_type)}), url
    assert eg.connector_save_refusal({"name": "c", **_cfg("postgresql://u:p@localhost/db")})


@pytest.mark.parametrize("db_type", ["postgresql", "redshift", "supabase"])
def test_a_socket_path_in_a_libpq_host_field_is_refused(db_type, driver_calls):
    from connectors.generic_sql import _build_engine

    with pytest.raises(eg.EgressRefused, match="socket"):
        _build_engine({"type": db_type, "host": "/run/postgresql", "database": "d", "workspace_id": "w"})
    assert driver_calls == []
    assert eg.metadata_endpoint_refusal("/data/lab.db", db_type="sqlite3") == ""


def test_a_double_encoded_authority_fails_closed():
    url = "postgresql://u:p@%2531%2536%2539.254.169.254:5432/db"
    refusal = eg.metadata_endpoint_refusal(url, db_type="postgresql")
    assert refusal and "%" not in refusal.split(":", 1)[0]


def test_an_ordinary_url_is_still_allowed(driver_calls, monkeypatch):
    from connectors.generic_sql import _build_engine

    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    assert eg.connector_save_refusal({"name": "c", **_cfg(_ORDINARY)}) == ""
    assert eg.metadata_endpoint_refusal("db.example.com", _ORDINARY, db_type="postgresql") == ""
    engine = _build_engine(_cfg(_ORDINARY))
    with pytest.raises(Exception, match="intercepted"):
        engine.connect()
    assert driver_calls and driver_calls[0].get("host") == "db.example.com"
    # A query host the operator chose deliberately is judged, not refused for being there.
    assert eg.metadata_endpoint_refusal(
        "postgresql://u:p@db.example.com/db?host=db1.example.com,db2.example.com", db_type="postgresql"
    ) == ""


def test_a_local_socket_follows_the_loopback_allowlist(monkeypatch):
    url = "postgresql://u:p@db.example.com/db?host=/run/postgresql"
    assert "socket" in eg.metadata_endpoint_refusal(url, db_type="postgresql")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    assert eg.metadata_endpoint_refusal(url, db_type="postgresql") == ""


@pytest.mark.parametrize(
    "db_type, value",
    [("sqlite", "sqlite:////tmp/x.db?mode=ro"), ("duckdb", "duckdb:////tmp/x.duckdb?access_mode=read_only")],
)
def test_file_engines_are_not_judged_as_sockets(db_type, value):
    assert eg.metadata_endpoint_refusal(value, db_type=db_type) == ""
