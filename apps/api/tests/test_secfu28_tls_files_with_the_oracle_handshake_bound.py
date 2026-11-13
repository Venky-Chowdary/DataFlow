"""Follow-up review 2 (TLS files, commit 79c875813): the check also runs once the Oracle handshake bound is installed.

``services.statement_budget.install()`` (run by the first tool call or
preflight scope, then for the life of the process) registers a *class-level*
``do_connect`` listener on ``DefaultDialect`` that opens every Oracle
connection itself (D354, a handshake the connect timeout can end). SQLAlchemy
calls class-level listeners before an engine's own, so the TLS-file guard that
``generic_sql._build_engine`` registers on each engine never ran for Oracle in
a process that had made a tool call: ``wallet_location`` / ``config_dir`` on the
API host reached ``oracledb.connect``. Found when the commit's own test ran
after the live Oracle tests (it passed alone).

Now the bounded Oracle connect runs the same check for an engine the guard
marked, before it dials.
"""

from __future__ import annotations

import pytest

_HOST = "/etc/platform/wallet"


class Dialled(Exception):
    pass


@pytest.fixture
def oracle_connect(monkeypatch):
    import oracledb

    calls: list[dict] = []

    def connect(*args, **kwargs):
        calls.append(kwargs)
        raise Dialled("oracledb")

    monkeypatch.setattr(oracledb, "connect", connect)
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_SOURCE_PATH_ROOTS", raising=False)
    from services import statement_budget

    statement_budget.install()  # what the first tool call does in the API process
    return calls


def _dial(url: str) -> None:
    from connectors.generic_sql import _build_engine

    engine = _build_engine({"type": "oracle", "connection_string": url})
    try:
        engine.connect()
    finally:
        engine.dispose()


@pytest.mark.parametrize("option", ["wallet_location", "config_dir"])
def test_an_oracle_host_tls_file_is_refused_with_the_handshake_bound(oracle_connect, option):
    with pytest.raises(ValueError) as caught:
        _dial(f"oracle+oracledb://u:p@db.example.com:1521/?service_name=x&{option}={_HOST}")
    assert option in str(caught.value) and _HOST not in str(caught.value)
    assert oracle_connect == []


def test_an_oracle_dial_without_tls_files_still_uses_the_bounded_connect(oracle_connect):
    with pytest.raises(Dialled):
        _dial("oracle+oracledb://u:p@db.example.com:1521/?service_name=x")
    assert len(oracle_connect) == 1
