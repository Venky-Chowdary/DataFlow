"""MySQL session guards append STRICT modes without wiping existing sql_mode."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.write_resilience import (  # noqa: E402
    _MYSQL_STRICT_MODES,
    _ensure_mysql_strict_sql_mode,
    apply_mysql_session_guards,
)


def test_ensure_mysql_strict_appends_missing_modes():
    cur = MagicMock()
    cur.fetchone.return_value = ("ONLY_FULL_GROUP_BY",)

    _ensure_mysql_strict_sql_mode(cur)

    set_calls = [
        c for c in cur.execute.call_args_list if c.args and "sql_mode =" in str(c.args[0])
    ]
    assert len(set_calls) == 1
    mode_arg = set_calls[0].args[1][0]
    for required in _MYSQL_STRICT_MODES:
        assert required in mode_arg
    assert "ONLY_FULL_GROUP_BY" in mode_arg


def test_ensure_mysql_strict_noop_when_already_strict():
    cur = MagicMock()
    cur.fetchone.return_value = (",".join(_MYSQL_STRICT_MODES),)

    _ensure_mysql_strict_sql_mode(cur)

    # Only the SELECT @@SESSION.sql_mode — no SET.
    assert cur.execute.call_count == 1
    assert "SELECT" in str(cur.execute.call_args.args[0]).upper()

def test_apply_mysql_session_guards_sets_timeouts_and_strict():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.return_value = ("",)

    apply_mysql_session_guards(conn)

    executed = " ".join(str(c.args[0]) for c in cur.execute.call_args_list if c.args)
    assert "wait_timeout" in executed
    assert "innodb_lock_wait_timeout" in executed
    assert "time_zone" in executed
    assert any(
        "sql_mode =" in str(c.args[0]) for c in cur.execute.call_args_list if c.args
    )


def test_apply_mysql_session_guards_raises_when_strict_mode_unavailable():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.return_value = ("",)

    def _execute(sql, *args):
        if "sql_mode" in str(sql).lower() and "SELECT" not in str(sql).upper():
            raise RuntimeError("Access denied for SET sql_mode")

    cur.execute.side_effect = _execute
    try:
        apply_mysql_session_guards(conn, require_strict_sql_mode=True)
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "STRICT" in str(exc) or "sql_mode" in str(exc).lower()


def test_apply_mssql_session_guards_raises_when_ansi_unavailable():
    from connectors.write_resilience import apply_mssql_session_guards

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.execute.side_effect = RuntimeError("SET ANSI_WARNINGS denied")
    try:
        apply_mssql_session_guards(conn, require_ansi_warnings=True)
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "ANSI" in str(exc) or "truncate" in str(exc).lower()
