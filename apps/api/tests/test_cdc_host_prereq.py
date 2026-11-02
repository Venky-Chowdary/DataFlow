"""Fixed CDC server statements. No caller text is interpolated into SQL."""

from __future__ import annotations

import pytest

from services.cdc_host_prereq import (
    apply_mysql_replication_client,
    apply_postgres_logical_decoding,
)


class _Cursor:
    def __init__(self, answers: dict[str, tuple], *, fail: set[str] | None = None):
        self.answers = answers
        self.fail = fail or set()
        self.executed: list[str] = []
        self._last = ""

    def execute(self, sql: str, _params=None):
        self.executed.append(sql)
        self._last = sql
        for needle in self.fail:
            if needle in sql:
                raise RuntimeError(f"refused {needle}")

    def fetchone(self):
        for key, row in self.answers.items():
            if key in self._last:
                return row
        return None

    def close(self):
        return None


class _Conn:
    def __init__(self, cursor: _Cursor):
        self.autocommit = False
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _postgres_answers(**overrides: tuple) -> dict[str, tuple]:
    answers = {
        "is_superuser": ("on",),
        "wal_level": ("replica",),
        "max_replication_slots": ("10",),
        "max_wal_senders": ("10",),
        "data_directory": ("/var/lib/postgresql/data",),
    }
    answers.update(overrides)
    return answers


def test_postgres_superuser_stages_logical_without_stopping_the_postmaster():
    cur = _Cursor(_postgres_answers())
    out = apply_postgres_logical_decoding(_Conn(cur), restart=True)
    assert out["applied"] is True
    assert out["wal_level_staged"] == "logical"
    assert out["wal_level_before"] == "replica"
    assert out["restarted"] is False
    assert out["restart_required"] is True
    assert out["operator_requested_restart"] is True
    assert "host" in out["note"]
    assert any(sql.startswith("ALTER SYSTEM SET wal_level") for sql in cur.executed)
    joined = "\n".join(cur.executed)
    assert "COPY" not in joined
    assert "pg_ctl" not in joined
    assert "kill" not in joined


def test_postgres_already_logical_does_not_ask_for_a_restart():
    cur = _Cursor(
        _postgres_answers(
            wal_level=("logical",),
            max_replication_slots=("10",),
            max_wal_senders=("10",),
        )
    )
    out = apply_postgres_logical_decoding(_Conn(cur), restart=True)
    assert out["applied"] is True
    assert out["restart_required"] is False
    assert out["restarted"] is False
    assert "pg_ctl" not in "\n".join(cur.executed)


def test_postgres_reports_a_data_directory_without_executing_it():
    cur = _Cursor(_postgres_answers(data_directory=("/tmp/x; rm -rf /",)))
    out = apply_postgres_logical_decoding(_Conn(cur), restart=True)
    assert out["applied"] is True
    assert out["data_directory"] == "/tmp/x; rm -rf /"
    assert not any("COPY" in sql or "rm -rf" in sql for sql in cur.executed)


def test_postgres_without_superuser_does_not_alter():
    cur = _Cursor(
        {
            "is_superuser": ("off",),
            "wal_level": ("replica",),
            "data_directory": ("/var/lib/postgresql/data",),
        }
    )
    out = apply_postgres_logical_decoding(_Conn(cur), restart=True)
    assert out["applied"] is False
    assert not any(sql.startswith("ALTER") for sql in cur.executed)


def test_mysql_grant_uses_the_current_account_and_stops_when_refused():
    cur = _Cursor({"CURRENT_USER": ("qa_dataflow@%",)}, fail={"GRANT"})
    out = apply_mysql_replication_client(_Conn(cur), enable_gtid=True)
    assert out["applied"] is False
    assert out["account"] == "qa_dataflow@%"
    assert "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'qa_dataflow'@'%'" in out["note"]
    assert "gtid_mode was not changed" in out["note"]
    assert any("GRANT REPLICATION SLAVE, REPLICATION CLIENT" in sql for sql in cur.executed)
    assert not any("gtid_mode" in sql for sql in cur.executed)


def test_mysql_rejects_an_account_that_is_not_a_plain_name():
    cur = _Cursor({"CURRENT_USER": ("qa';drop@%",)})
    with pytest.raises(ValueError, match="plain"):
        apply_mysql_replication_client(_Conn(cur), enable_gtid=True)
    assert not any(sql.startswith("GRANT") for sql in cur.executed)
