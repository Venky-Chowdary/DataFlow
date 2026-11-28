"""SQL Server DATETIME stores 1/300 s ticks: the guard refuses only what it cannot store.

Classic DATETIME keeps a millisecond whose last digit is 0, 3 or 7 (.120, .217,
.993). The guard used to accept only .000, .003 and .007 of each second, so a
valid value such as 10:01:00.120 was quarantined.
"""

from __future__ import annotations

from datetime import datetime

import pytest
import sqlalchemy as sa

from connectors.generic_sql import _refuse_sqlserver_datetime_rounding


def _check(ms_us: int) -> None:
    _refuse_sqlserver_datetime_rounding(
        datetime(2026, 11, 24, 10, 1, 0, ms_us),
        logical="DATETIME",
        sa_type=sa.DateTime(),
        db_type="mssql",
    )


@pytest.mark.parametrize("us", [0, 3000, 7000, 10000, 120000, 217000, 993000, 997000])
def test_every_value_datetime_can_store_is_accepted(us: int) -> None:
    _check(us)


@pytest.mark.parametrize("us", [1000, 5000, 216000, 999000, 120500, 3001])
def test_a_value_datetime_would_round_is_refused(us: int) -> None:
    with pytest.raises(ValueError, match="1/300 s"):
        _check(us)
