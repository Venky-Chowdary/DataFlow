"""Follow-up review 3 (M3 + M4): an Oracle connect string never makes python-oracledb read a host file.

M3: python-oracledb's extended Easy Connect takes driver parameters with a
``pyo.`` prefix: ``db:1521/svc?pyo.wallet_location=/etc/oracle/wallet`` sets
the wallet directory. The option name folded to ``pyowalletlocation``, not a
file option, so the host's wallet was presented to the tenant's server
(``copy_oracle_oracle._oracle_connect`` and generic_sql
``oracle+oracledb://u:p@/?dsn=...``).

M4: a DSN ``config-file:///path/x.json`` (and ``config-azure://``,
``config-ociobject://``, ... registered config providers, ``ldap://``) makes the
driver read the connection's user, password and host from a file on the API
host or a provider service. Any Oracle DSN that starts with ``config-`` or names
a protocol other than tcp / tcps is now refused at save and at dial; a DSN's
wallet / config directory is judged as python-oracledb itself parses it.
"""

from __future__ import annotations

import json
import socket
import urllib.parse

import pytest

_WALLET = "/etc/oracle/wallet"


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
def driver(monkeypatch):
    import oracledb

    calls: list[dict] = []

    def connect(*_args, **kwargs):
        calls.append(kwargs)
        raise Dialled("oracledb")

    monkeypatch.setattr(oracledb, "connect", connect)
    return calls


@pytest.fixture
def host_config(tmp_path):
    path = tmp_path / "hostcfg.json"
    path.write_text(json.dumps({"connect_descriptor": "evil.example.com:1521/x", "user": "hostuser"}))
    return str(path)


def _oracle(dsn: str) -> dict:
    return {"type": "oracle", "name": "x", "host": "db.example.com", "port": 2484, "username": "u",
            "password": "p", "dsn": dsn}


def _generic(dsn: str) -> dict:
    return {"type": "oracle", "name": "x", "connection_string": "oracle+oracledb://u:p@/?dsn=" + urllib.parse.quote(dsn)}


def _engine_dial(cfg: dict) -> None:
    from connectors.generic_sql import _build_engine

    engine = _build_engine(dict(cfg))
    try:
        engine.connect()
    finally:
        engine.dispose()


def _no_path(message: str) -> None:
    assert message
    assert "/etc/" not in message and "hostcfg" not in message, message


_PYO = [
    pytest.param(f"db.example.com:2484/svc?pyo.wallet_location={_WALLET}&transport_connect_timeout=5", id="easy"),
    pytest.param(f"tcps://db.example.com:2484/svc?pyo.wallet_location={_WALLET}", id="tcps"),
    pytest.param(f"db.example.com:2484/svc?PYO.Wallet_Location={_WALLET}", id="case"),
]


@pytest.mark.parametrize("dsn", _PYO)
def test_a_pyo_wallet_is_refused_at_save(dsn):
    import oracledb

    from services.egress_guard import connector_save_refusal

    params = oracledb.ConnectParams()
    params.parse_connect_string(dsn)
    assert params.wallet_location == _WALLET  # what the driver would open
    _no_path(connector_save_refusal(_oracle(dsn)))
    _no_path(connector_save_refusal(_generic(dsn)))


@pytest.mark.parametrize("dsn", [_PYO[0], _PYO[2]])  # the fast path replaces a "://" DSN by host:port/service
def test_a_pyo_wallet_is_refused_at_the_fast_path_dial(driver, dsn):
    from services.copy_oracle_oracle import _oracle_connect

    with pytest.raises(ValueError) as caught:
        _oracle_connect(_oracle(dsn))
    _no_path(str(caught.value))
    assert driver == []


@pytest.mark.parametrize("dsn", _PYO)
def test_a_pyo_wallet_is_refused_at_the_engine_dial(driver, dsn):
    with pytest.raises(ValueError) as caught:
        _engine_dial(_generic(dsn))
    _no_path(str(caught.value))
    assert driver == []


@pytest.mark.parametrize("scheme", ["config-file://", "CONFIG-FILE://", " config-file://"])
def test_a_config_file_dsn_is_refused_at_save_and_dial(driver, host_config, scheme):
    from services.egress_guard import connector_save_refusal

    dsn = scheme + host_config
    _no_path(connector_save_refusal(_oracle(dsn)))
    _no_path(connector_save_refusal(_generic(dsn)))
    with pytest.raises(ValueError) as caught:
        _engine_dial(_generic(dsn))
    _no_path(str(caught.value))
    assert driver == []


@pytest.mark.parametrize("dsn", [
    "config-azure://myappconfig?key=/x/",
    "config-ociobject://objectstorage.example.com/n/ns/b/bucket/o/cfg.json",
    "config-awss3://bucket/cfg.json",
    "ldap://ldap.example.com/db,cn=OracleContext",
])
def test_a_config_provider_or_ldap_dsn_is_refused(driver, dsn):
    from services.egress_guard import connector_save_refusal

    assert connector_save_refusal(_oracle(dsn))
    assert connector_save_refusal(_generic(dsn))
    with pytest.raises(ValueError):
        _engine_dial(_generic(dsn))
    assert driver == []


@pytest.mark.parametrize("dsn", [
    "db.example.com:1521/svc",
    "tcps://db.example.com:2484/svc?ssl_server_dn_match=true",
    "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=db.example.com)(PORT=2484))(CONNECT_DATA=(SERVICE_NAME=s)))",
])
def test_a_plain_oracle_dsn_saves_and_reaches_the_driver(driver, dsn):
    from services.egress_guard import connector_save_refusal

    assert connector_save_refusal(_oracle(dsn)) == ""
    assert connector_save_refusal(_generic(dsn)) == ""
    with pytest.raises(Dialled):
        _engine_dial(_generic(dsn))
    assert driver and driver[0]["dsn"] == dsn


def test_a_postgresql_uri_dsn_is_not_judged_as_an_oracle_one():
    from connectors.tls_file_options import check_dial
    from services.egress_guard import connector_save_refusal

    uri = "postgresql://u:p@db.example.com:5432/d?sslmode=require"
    check_dial({"dsn": uri})
    assert connector_save_refusal({"type": "postgresql", "name": "x", "connection_string": uri,
                                   "extra": {"dsn": uri}}) == ""
