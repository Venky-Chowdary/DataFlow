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


def test_postgres_superuser_stages_logical_and_restarts():
    cur = _Cursor(
        {
            "is_superuser": ("on",),
            "wal_level": ("replica",),
            "data_directory": ("/var/lib/postgresql/data",),
        }
    )
    out = apply_postgres_logical_decoding(_Conn(cur), restart=True)
    assert out["applied"] is True
    assert out["wal_level_staged"] == "logical"
    assert out["restarted"] is True
    assert any(sql.startswith("ALTER SYSTEM SET wal_level") for sql in cur.executed)
    assert any("pg_ctl restart -D /var/lib/postgresql/data" in sql for sql in cur.executed)
    assert not any("kill -TERM" in sql for sql in cur.executed)


def test_postgres_falls_back_to_pid1_when_pg_ctl_cannot_restart():
    cur = _Cursor(
        {
            "is_superuser": ("on",),
            "wal_level": ("replica",),
            "data_directory": ("/var/lib/postgresql/data",),
        },
        fail={"pg_ctl"},
    )
    out = apply_postgres_logical_decoding(_Conn(cur), restart=True)
    assert out["restarted"] is True
    assert any("kill -TERM 1" in sql for sql in cur.executed)


def test_postgres_refuses_a_data_directory_that_could_change_the_command():
    cur = _Cursor(
        {
            "is_superuser": ("on",),
            "wal_level": ("replica",),
            "data_directory": ("/tmp/x; rm -rf /",),
        }
    )
    with pytest.raises(ValueError, match="plain path"):
        apply_postgres_logical_decoding(_Conn(cur), restart=True)
    assert not any("COPY" in sql for sql in cur.executed)


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
    assert any("GRANT REPLICATION SLAVE, REPLICATION CLIENT" in sql for sql in cur.executed)
    assert not any("gtid_mode" in sql for sql in cur.executed)


def test_mysql_rejects_an_account_that_is_not_a_plain_name():
    cur = _Cursor({"CURRENT_USER": ("qa';drop@%",)})
    with pytest.raises(ValueError, match="plain"):
        apply_mysql_replication_client(_Conn(cur), enable_gtid=True)
    assert not any(sql.startswith("GRANT") for sql in cur.executed)
