"""Follow-up review 1: the egress guard judges a MySQL / MariaDB ``unix_socket`` as loopback.

SQLAlchemy's PyMySQL dialect hands a URL's ``?unix_socket=`` to
``pymysql.connect``, which then opens that Unix socket on the API host instead
of dialling the URL's host. The egress guard judged only the host, and the
dial-time socket guard lets ``AF_UNIX`` paths through, so
``mysql+pymysql://u:p@db.example.com/db?unix_socket=/var/run/mysqld/mysqld.sock``
reached a database on the API host wherever loopback is refused.

A non-empty ``unix_socket`` (URL query key, the connector's ``unix_socket``
field or ``extra.unix_socket`` of a MySQL-family connector) is now judged like
the PostgreSQL socket forms (commit b7063cf76): as loopback, refused wherever
loopback is refused, allowed where the egress allowlist covers 127.0.0.1.
"""

from __future__ import annotations

import socket

import pytest

from services import egress_guard as eg

_SOCK = "/var/run/mysqld/mysqld.sock"
_URLS = [
    f"mysql+pymysql://u:p@db.example.com/db?unix_socket={_SOCK}",
    "mysql+pymysql://u:p@db.example.com/db?unix_socket=%2Fvar%2Frun%2Fmysqld%2Fmysqld.sock",
    "mysql+pymysql://u:p@/db?unix_socket=/tmp/mysql.sock",
    "mariadb+pymysql://u:p@db.example.com/db?charset=utf8mb4&unix_socket=/run/mysqld/mysqld.sock",
    # Not ``x=1``: a key PyMySQL does not take is refused at save (follow-up review 2, H3).
    "mysql+pymysql://u:p@db.example.com/db?charset=utf8mb4#&unix_socket=/tmp/mysql.sock",
    "mysql+pymysql://u:p@db.example.com/db?unix_socket=mysql.sock",
]
_ORDINARY = "mysql+pymysql://u:p@db.example.com:3306/db?charset=utf8mb4"


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
    import pymysql

    calls: list[dict] = []

    def _connect(*_a, **kwargs):
        calls.append(kwargs)
        raise RuntimeError("intercepted: the driver was reached")

    monkeypatch.setattr(pymysql, "connect", _connect)
    return calls


def _cfg(url: str, db_type: str = "mysql") -> dict:
    return {"type": db_type, "host": "db.example.com", "connection_string": url, "workspace_id": "ws-secfu19"}


def _forms() -> list[dict]:
    """Every way a connector names a MySQL socket, as a saved connector's settings."""
    base = {"name": "c", "type": "mysql", "host": "db.example.com", "workspace_id": "ws-secfu19"}
    return [
        *({**base, "connection_string": url} for url in _URLS),
        {**base, "host": _URLS[0]},  # a URL pasted into the host field
        {**base, "unix_socket": _SOCK},  # the connector field
        {**base, "extra": {"unix_socket": _SOCK}},
        {**base, "type": "mariadb", "extra": {"unix_socket": "mysqld.sock"}},
    ]


def test_premise_the_driver_dials_the_query_socket():
    import sqlalchemy as sa
    from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql

    for url in (_URLS[0], _URLS[2]):
        _args, kwargs = MySQLDialect_pymysql().create_connect_args(sa.engine.url.make_url(url))
        assert kwargs["unix_socket"].startswith("/"), url


@pytest.mark.parametrize("url", _URLS)
def test_build_engine_refuses_before_the_driver_is_called(url, driver_calls):
    from connectors.generic_sql import _build_engine

    db_type = "mariadb" if url.startswith("mariadb") else "mysql"
    with pytest.raises(eg.EgressRefused, match="socket"):
        _build_engine(_cfg(url, db_type))
    assert driver_calls == []
    assert "socket" in eg.metadata_endpoint_refusal(url, db_type=db_type)


@pytest.mark.parametrize("enforce_new", [False, True])
@pytest.mark.parametrize("allowlist", ["", "127.0.0.1/32", "127.0.0.0/8", "10.0.0.0/8"])
def test_every_form_is_saved_exactly_where_loopback_is(monkeypatch, enforce_new, allowlist):
    # Enforcement is the default (SEC-2); "0" is the explicit audit-only opt-out.
    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1" if enforce_new else "0")
    if allowlist:
        monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", allowlist)
    loopback = bool(eg.connector_save_refusal(
        {"name": "c", "type": "mysql", "host": "127.0.0.1", "workspace_id": "ws-secfu19"}
    ))
    assert loopback is (enforce_new and allowlist in ("", "10.0.0.0/8"))
    for form in _forms():
        refusal = eg.connector_save_refusal(dict(form))
        assert bool(refusal) is loopback, form
        if refusal:
            assert "socket" in refusal and "p@" not in refusal


def test_an_unchanged_socket_is_not_refused_on_a_rename(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    form = {"name": "c", "type": "mysql", "host": "db.example.com", "extra": {"unix_socket": _SOCK}}
    existing = SimpleNamespace(host="db.example.com", endpoint_url="", connection_string="",
                               type="mysql", extra={"unix_socket": _SOCK})
    assert eg.connector_save_refusal(dict(form), existing) == ""
    moved = SimpleNamespace(**{**vars(existing), "extra": {}})
    assert "socket" in eg.connector_save_refusal(dict(form), moved)


@pytest.mark.parametrize("allowlist", ["127.0.0.1/32", "127.0.0.0/8"])
def test_a_socket_is_allowed_where_the_allowlist_covers_127_0_0_1(monkeypatch, driver_calls, allowlist):
    from connectors.generic_sql import _build_engine

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", allowlist)
    for url in _URLS:
        assert eg.metadata_endpoint_refusal(url, db_type="mysql") == "", url
    engine = _build_engine(_cfg(_URLS[0]))
    with pytest.raises(Exception, match="intercepted"):
        engine.connect()
    assert driver_calls and driver_calls[0].get("unix_socket") == _SOCK


def test_what_is_not_a_socket_is_unchanged(driver_calls, monkeypatch):
    from connectors.generic_sql import _build_engine

    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    # An empty unix_socket is TCP to the host (PyMySQL ignores it).
    assert eg.metadata_endpoint_refusal(
        "mysql+pymysql://u:p@db.example.com/db?unix_socket=", db_type="mysql"
    ) == ""
    assert eg.connector_save_refusal({"name": "c", **_cfg(_ORDINARY)}) == ""
    engine = _build_engine(_cfg(_ORDINARY))
    with pytest.raises(Exception, match="intercepted"):
        engine.connect()
    assert driver_calls and driver_calls[0].get("host") == "db.example.com"
    assert "unix_socket" not in driver_calls[0]
    # Only MySQL-family drivers read a unix_socket field or extra.
    assert eg.connector_save_refusal(
        {"name": "c", "type": "postgresql", "host": "db.example.com", "extra": {"unix_socket": _SOCK}}
    ) == ""
