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
    assert any(
        "sql_mode =" in str(c.args[0]) for c in cur.execute.call_args_list if c.args
    )
