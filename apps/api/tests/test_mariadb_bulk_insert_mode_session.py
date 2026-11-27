"""Session checks for bulk loads, per engine and version (D302 follow-up).

MariaDB 10.6+ puts an INSERT / LOAD DATA into an empty InnoDB table in a bulk
insert mode when unique_checks=0 AND foreign_key_checks=0 (MDEV-515,
MDEV-24621). With a concurrent reader that mode hung the server for good
(live proof: test_mariadb_bulk_load_concurrent_reader_live.py). MySQL has no
such mode and keeps its checks-off bulk session.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_load_data import (  # noqa: E402
    apply_mysql_bulk_load_session,
    execute_load_data_local,
    guard_mariadb_bulk_insert_session,
    mariadb_empty_table_bulk_insert_possible,
    mysql_bulk_load_session_settings,
    parse_mysql_family_version,
)

IDENTITY_SQL = "SELECT @@version, @@version_comment"
MARIADB_10_11 = "5.5.5-10.11.14-MariaDB-0ubuntu0.24.04.1"
MARIADB_10_6_0 = "5.5.5-10.6.0-MariaDB"
MARIADB_10_5 = "5.5.5-10.5.27-MariaDB-log"
MARIADB_11 = "11.4.5-MariaDB-ubu2404"
MYSQL_8 = "8.0.40"
MYSQL_84 = "8.4.3"
PERCONA = "8.0.39-30"
AURORA = "8.0.mysql_aurora.3.08.0"


class _Conn:
    def __init__(self, server_version: Any = None) -> None:
        if server_version is not None:
            self.server_version = server_version


class _Cursor:
    """Records SQL; answers the few reads the helpers make."""

    def __init__(self, server_version: Any = None, *, session=(1, 1), version_row=None,
                 fail_on: str = "", comment: str = "") -> None:
        self.connection = _Conn(server_version)
        # What the server answers to SELECT @@version, @@version_comment: by
        # default the handshake value (no proxy in between).
        if version_row is None and server_version is not None:
            version_row = (server_version.decode() if isinstance(server_version, bytes) else server_version, comment)
        self.session = list(session)
        self.version_row = version_row
        self.fail_on = fail_on
        self.sql: list[str] = []
        self._row: Any = None

    def execute(self, sql: str, *_args: Any) -> None:
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("refused")
        self.sql.append(sql)
        if sql == IDENTITY_SQL:
            self._row = self.version_row
        elif sql.startswith("SELECT @@SESSION.unique_checks"):
            self._row = tuple(self.session)
        elif sql.startswith("SELECT @@SESSION.sql_mode"):
            self._row = ("STRICT_TRANS_TABLES,STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO,"
                         "NO_ZERO_DATE,NO_ZERO_IN_DATE,NO_ENGINE_SUBSTITUTION",)
        else:
            m = re.fullmatch(r"SET SESSION unique_checks=(\d)", sql)
            if m:
                self.session[0] = int(m.group(1))
            m = re.fullmatch(r"SET SESSION foreign_key_checks=(\d)", sql)
            if m:
                self.session[1] = int(m.group(1))
            self._row = None

    def fetchone(self) -> Any:
        return self._row

    def close(self) -> None:
        pass

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_a: Any) -> bool:
        return False


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (MARIADB_10_11, (True, (10, 11, 14))),
        (MARIADB_10_6_0, (True, (10, 6, 0))),
        (MARIADB_10_5, (True, (10, 5, 27))),
        (MARIADB_11, (True, (11, 4, 5))),
        (b"5.5.5-10.11.14-MariaDB", (True, (10, 11, 14))),
        ("10.6-MariaDB", (True, (10, 6, 0))),
        ("MariaDB", (True, None)),
        (MYSQL_8, (False, (8, 0, 40))),
        (PERCONA, (False, (8, 0, 39))),
        ("5.7.44-log", (False, (5, 7, 44))),
        ("", (False, None)),
        (None, (False, None)),
    ],
)
def test_version_parse_strips_the_mariadb_handshake_prefix(raw, expected):
    assert parse_mysql_family_version(raw) == expected


@pytest.mark.parametrize(
    ("version", "possible"),
    [
        (MARIADB_10_11, True),
        (MARIADB_10_6_0, True),  # MDEV-515 shipped in 10.6.0
        (MARIADB_11, True),
        ("MariaDB", True),  # MariaDB of unknown version: fail safe
        (MARIADB_10_5, False),  # no empty-table bulk insert before 10.6
        (MYSQL_8, False),
        (MYSQL_84, False),
        (PERCONA, False),
        (AURORA, False),
        ("", False),
    ],
)
def test_bulk_insert_mode_is_possible_only_on_mariadb_10_6_and_later(version, possible):
    assert mariadb_empty_table_bulk_insert_possible(version) is possible


@pytest.mark.parametrize(
    ("version", "checks"),
    [
        (MYSQL_8, 0),
        (MYSQL_84, 0),
        (PERCONA, 0),
        (AURORA, 0),
        (MARIADB_10_5, 0),
        (MARIADB_10_6_0, 1),
        (MARIADB_10_11, 1),
        (MARIADB_11, 1),
        ("MariaDB", 1),
        ("", 1),  # version unreadable: never risk the hang
    ],
)
def test_bulk_load_session_settings_per_engine(version, checks):
    assert mysql_bulk_load_session_settings(version) == {
        "unique_checks": checks,
        "foreign_key_checks": checks,
    }


def test_mysql_bulk_session_is_unchanged_checks_off():
    cur = _Cursor(MYSQL_8)
    assert apply_mysql_bulk_load_session(cur) == {"unique_checks": 0, "foreign_key_checks": 0}
    # The server is asked once (the handshake is not trusted), then the
    # exact statements the route ran before.
    assert cur.sql == [IDENTITY_SQL, "SET SESSION unique_checks=0", "SET SESSION foreign_key_checks=0"]


def test_mariadb_bulk_session_keeps_both_checks_on_explicitly():
    cur = _Cursor(MARIADB_10_11, session=(0, 0))  # e.g. server globals off
    assert apply_mysql_bulk_load_session(cur) == {"unique_checks": 1, "foreign_key_checks": 1}
    assert cur.sql == [IDENTITY_SQL, "SET SESSION unique_checks=1", "SET SESSION foreign_key_checks=1"]
    assert cur.session == [1, 1]


def test_bulk_session_reads_version_when_the_driver_does_not_expose_it():
    cur = _Cursor(None, version_row=("10.11.14-MariaDB",))
    assert apply_mysql_bulk_load_session(cur)["unique_checks"] == 1
    assert cur.sql[0] == IDENTITY_SQL
    cur = _Cursor(None, version_row=("8.0.40",))
    assert apply_mysql_bulk_load_session(cur)["unique_checks"] == 0


def test_bulk_session_unreadable_version_fails_safe_to_checks_on():
    cur = _Cursor(None, fail_on="@@version_comment")
    assert apply_mysql_bulk_load_session(cur) == {"unique_checks": 1, "foreign_key_checks": 1}


def test_explicit_conn_caches_the_answer_it_was_given():
    conn = _Conn(MYSQL_8)
    cur = _Cursor(MYSQL_8, version_row=(MARIADB_10_11, ""))
    assert apply_mysql_bulk_load_session(cur, conn)["unique_checks"] == 1
    again = _Cursor(MYSQL_8, version_row=("8.0.40", ""))
    assert apply_mysql_bulk_load_session(again, conn)["unique_checks"] == 1  # cached per connection
    assert IDENTITY_SQL not in again.sql


@pytest.mark.parametrize(
    ("session", "changed"),
    [((0, 0), True), ((0, 1), False), ((1, 0), False), ((1, 1), False), (("OFF", "OFF"), True)],
)
def test_guard_turns_unique_checks_on_only_when_both_are_off_on_mariadb(session, changed):
    cur = _Cursor(MARIADB_10_11, session=session)
    assert guard_mariadb_bulk_insert_session(cur) is changed
    assert ("SET SESSION unique_checks=1" in cur.sql) is changed
    # foreign_key_checks is never touched: a caller's FK choice stands.
    assert not any("foreign_key_checks=" in s for s in cur.sql)


@pytest.mark.parametrize("version", [MYSQL_8, PERCONA, MARIADB_10_5])
def test_guard_changes_nothing_off_mariadb_10_6(version):
    cur = _Cursor(version, session=(0, 0))
    assert guard_mariadb_bulk_insert_session(cur) is False
    assert cur.sql == [IDENTITY_SQL]


def test_mariadb_behind_a_proxy_announcing_mysql_is_guarded():
    """Review: ProxySQL / MaxScale can announce 8.0.x in the handshake for a
    MariaDB backend; the handshake alone made the guard skip it."""
    cur = _Cursor(MYSQL_8, session=(0, 0), version_row=(MARIADB_10_11, "mariadb.org binary distribution"))
    assert guard_mariadb_bulk_insert_session(cur) is True
    assert "SET SESSION unique_checks=1" in cur.sql


@pytest.mark.parametrize("row", [("8.0.40", "(ProxySQL)"), ("5.5.30", "MaxScale"), None])
def test_unknown_or_proxy_answered_identity_fails_safe(row):
    cur = _Cursor(MYSQL_8, session=(0, 0), version_row=row, fail_on="" if row else "@@version_comment")
    assert guard_mariadb_bulk_insert_session(cur) is True
    assert apply_mysql_bulk_load_session(_Cursor(MYSQL_8, version_row=row,
                                                 fail_on="" if row else "@@version_comment")) == {
        "unique_checks": 1, "foreign_key_checks": 1}


def test_every_load_data_on_mariadb_runs_with_a_check_on(tmp_path):
    staged = tmp_path / "df_mysql_ld_stage.tsv"
    staged.write_text("1\n")
    cur = _Cursor(MARIADB_10_11, session=(0, 0))
    execute_load_data_local(cur, path=str(staged), table_q="`t`", columns=["a"])
    load = next(i for i, s in enumerate(cur.sql) if s.startswith("LOAD DATA"))
    assert "SET SESSION unique_checks=1" in cur.sql[:load]


def test_load_data_on_mysql_adds_no_statement(tmp_path):
    staged = tmp_path / "df_mysql_ld_stage.tsv"
    staged.write_text("1\n")
    cur = _Cursor(MYSQL_8, session=(0, 0))
    execute_load_data_local(cur, path=str(staged), table_q="`t`", columns=["a"])
    assert cur.sql[0] == IDENTITY_SQL
    assert len(cur.sql) == 2 and cur.sql[1].startswith("LOAD DATA LOCAL INFILE")


class _GuardConn(_Conn):
    def __init__(self, server_version: Any, session=(0, 0)) -> None:
        super().__init__(server_version)
        self.cur = _Cursor(server_version, session=session)
        self.cur.connection = self

    def cursor(self) -> _Cursor:
        return self.cur


def test_write_session_guards_turn_a_check_on_for_mariadb_with_globals_off():
    from connectors.write_resilience import apply_mysql_session_guards

    conn = _GuardConn(MARIADB_10_11, session=(0, 0))
    apply_mysql_session_guards(conn)
    assert "SET SESSION unique_checks=1" in conn.cur.sql
    conn = _GuardConn(MYSQL_8, session=(0, 0))
    apply_mysql_session_guards(conn)
    assert not any("unique_checks" in s for s in conn.cur.sql)


def test_pooled_sqlalchemy_mariadb_session_is_guarded_and_never_breaks_the_pool():
    from connectors.mysql_load_data import (
        guard_mariadb_bulk_insert_pool_session as _guard_mariadb_bulk_insert_pool_session,
    )

    conn = _GuardConn(MARIADB_11, session=(0, 0))
    _guard_mariadb_bulk_insert_pool_session(conn)
    assert conn.cur.session == [1, 0]
    broken = _GuardConn(MARIADB_11, session=(0, 0))
    broken.cur.fail_on = "@@SESSION.unique_checks"
    _guard_mariadb_bulk_insert_pool_session(broken)  # swallowed, logged


def test_only_the_bulk_load_helper_turns_unique_checks_off():
    """Any new 'unique_checks=0' must go through the engine-aware helper."""
    offenders = []
    for top in ("connectors", "services", "src"):
        for path in (_API_ROOT / top).rglob("*.py"):
            if path.name == "mysql_load_data.py":
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            if re.search(r"unique_checks\s*=\s*(0|off|false)", text, re.I):
                offenders.append(str(path.relative_to(_API_ROOT)))
    assert offenders == []
