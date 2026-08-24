"""IDENTITY_INSERT must be released on every exit from the write.

The opt-in is per session and per table. A pooled connection handed back with
``IDENTITY_INSERT`` still ON makes the *next* table's load fail with an error
naming the wrong table — and only one table per session may hold it, so the
second table in a multi-table job cannot even open its own.
"""

from __future__ import annotations

from typing import Any

import pytest

from connectors.writer_common import IdentityInsertSession, begin_identity_insert


class _Conn:
    def __init__(self, *, fail_on_off: bool = False) -> None:
        self.statements: list[str] = []
        self._fail_on_off = fail_on_off

    def execute(self, statement: Any, _params: Any = None) -> Any:
        text = str(statement)
        if self._fail_on_off and "OFF" in text:
            raise RuntimeError("connection already closed")
        self.statements.append(text)
        return None


def test_open_and_close_bracket_the_write():
    conn = _Conn()
    session = IdentityInsertSession(conn, "[dbo].[orders]")
    session.open()
    session.close()
    assert conn.statements == [
        "SET IDENTITY_INSERT [dbo].[orders] ON",
        "SET IDENTITY_INSERT [dbo].[orders] OFF",
    ]


def test_close_is_idempotent_so_a_finally_after_a_close_is_safe():
    conn = _Conn()
    session = IdentityInsertSession(conn, "[dbo].[orders]")
    session.open()
    session.close()
    session.close()
    assert conn.statements.count("SET IDENTITY_INSERT [dbo].[orders] OFF") == 1


def test_close_without_open_sends_nothing():
    conn = _Conn()
    IdentityInsertSession(conn, "[dbo].[orders]").close()
    assert conn.statements == []


def test_a_dead_connection_does_not_mask_the_original_failure():
    """The write's own error must survive the release attempt."""
    conn = _Conn(fail_on_off=True)
    session = IdentityInsertSession(conn, "[dbo].[orders]")
    session.open()
    session.close()  # must not raise


@pytest.mark.parametrize(
    ("dialect", "identity_cols", "target_cols", "expect_session"),
    [
        # Not SQL Server: no session to open.
        ("postgresql", {"id"}, ["id", "name"], False),
        # No identity column on the destination table.
        ("mssql", set(), ["id", "name"], False),
        # Identity column exists but the load does not supply it — the
        # generator fills it and overriding would be wrong.
        ("mssql", {"id"}, ["name"], False),
        # The load carries the source's own keys: opt in.
        ("mssql", {"id"}, ["id", "name"], True),
        # Case-folded match still counts.
        ("mssql", {"ID"}, ["id", "name"], True),
    ],
)
def test_session_opens_only_when_the_load_supplies_the_key(
    monkeypatch, dialect, identity_cols, target_cols, expect_session
):
    monkeypatch.setattr(
        "connectors.writer_common.sqlserver_identity_columns",
        lambda *_a, **_k: identity_cols,
    )
    conn = _Conn()
    session = begin_identity_insert(
        conn,
        dialect_name=dialect,
        schema="dbo",
        table="orders",
        target_cols=target_cols,
    )
    assert (session is not None) is expect_session
    assert bool(conn.statements) is expect_session


def test_probe_failure_declines_the_session_rather_than_guessing(monkeypatch):
    """Loud failure on row 1 beats silently letting the destination renumber."""

    def _boom(*_a: Any, **_k: Any) -> set[str]:
        raise RuntimeError("catalog unreadable")

    monkeypatch.setattr("connectors.writer_common.sqlserver_identity_columns", _boom)
    conn = _Conn()
    assert (
        begin_identity_insert(
            conn,
            dialect_name="mssql",
            schema="dbo",
            table="orders",
            target_cols=["id"],
        )
        is None
    )
    assert conn.statements == []
