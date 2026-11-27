"""Review 2 item 3: the writer's forced-PRIMARY-KEY probe never reads a failure as "not forced".

The probe swallowed every error per variable (``except Exception: continue``),
so a server it could not read was treated as one that does not force a key.
Only "unknown system variable" (1193: this server has no such variable) is
"not a requirement"; anything else raises.
"""

from __future__ import annotations

import pytest

from services.dest_require_primary_key import (
    PrimaryKeyRequirementUnreadable,
    server_forces_primary_key_on,
)


class _Err(Exception):
    def __init__(self, code: int, msg: str) -> None:
        super().__init__(code, msg)


class _Cursor:
    def __init__(self, answers: dict[str, object]) -> None:
        self.answers = answers
        self.row = None

    def execute(self, sql: str) -> None:
        key = sql.replace("SELECT ", "").strip()
        value = self.answers.get(key)
        if isinstance(value, Exception):
            raise value
        self.row = (value,)

    def fetchone(self):
        return self.row


def test_an_unknown_variable_is_not_a_requirement():
    cur = _Cursor({"VERSION()": "10.11.14-MariaDB",
                   "@@sql_require_primary_key": _Err(1193, "Unknown system variable"),
                   "@@innodb_force_primary_key": 1})
    assert server_forces_primary_key_on(cur) == (True, "10.11.14-MariaDB")
    cur = _Cursor({"VERSION()": "8.0.36",
                   "@@sql_require_primary_key": 0,
                   "@@innodb_force_primary_key": _Err(1193, "Unknown system variable")})
    assert server_forces_primary_key_on(cur) == (False, "8.0.36")


@pytest.mark.parametrize("error", [_Err(2013, "Lost connection"), _Err(1227, "Access denied"),
                                   RuntimeError("cursor closed")])
def test_any_other_failure_is_unreadable_never_not_forced(error):
    cur = _Cursor({"VERSION()": "8.0.36", "@@sql_require_primary_key": error,
                   "@@innodb_force_primary_key": 0})
    with pytest.raises(PrimaryKeyRequirementUnreadable):
        server_forces_primary_key_on(cur)
