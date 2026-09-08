"""PostgreSQL CDC slot creation surfaces the real refusal, not an aborted txn."""

from __future__ import annotations

import pytest


class _AbortingCursor:
    """psycopg2-shaped cursor: after a failed statement every statement fails
    with ``current transaction is aborted`` until ROLLBACK TO SAVEPOINT."""

    def __init__(self, failures: dict[str, str]):
        self.failures = failures
        self.aborted = False
        self.statements: list[tuple[str, tuple]] = []
        self._row = None

    def execute(self, sql, params=()):
        self.statements.append((sql, tuple(params)))
        if sql.startswith("ROLLBACK TO SAVEPOINT"):
            self.aborted = False
            return
        if self.aborted:
            raise RuntimeError("current transaction is aborted, commands ignored")
        if "pg_create_logical_replication_slot" in sql:
            msg = self.failures.get(params[1])
            if msg:
                self.aborted = True
                raise RuntimeError(msg)
            self._row = ("0/16B3748",)

    def fetchone(self):
        return self._row


def test_pg_slot_quota_error_surfaces_instead_of_aborted_transaction():
    from connectors.postgresql_change_stream import _create_logical_slot

    cur = _AbortingCursor({"pgoutput": "all replication slots are in use", "test_decoding": "all replication slots are in use"})
    with pytest.raises(RuntimeError, match="all replication slots are in use"):
        _create_logical_slot(cur, "df_slot", "pgoutput")
    assert not any("test_decoding" in p for _, p in cur.statements)


def test_pg_slot_falls_back_to_test_decoding_only_when_plugin_missing():
    from connectors.postgresql_change_stream import _create_logical_slot

    cur = _AbortingCursor({"pgoutput": 'could not access file "pgoutput"'})
    assert _create_logical_slot(cur, "df_slot", "pgoutput") == ("0/16B3748", "test_decoding")
    cur = _AbortingCursor({})
    assert _create_logical_slot(cur, "df_slot", "pgoutput") == ("0/16B3748", "pgoutput")
