"""D47 AC-2: scd2 / mirror into Snowflake without the dialect are refused at plan.

QA: preflight passed every gate, then the scd2 and mirror jobs failed 0 rows
"SQLAlchemy dialect/driver for snowflake is not available", with irrelevant
remedies. The image now pins ``snowflake-sqlalchemy`` (requirements.txt, D30),
and AC-1 (both modes run, two runs each) is ``test_d47_snowflake_scd2_mirror``.
This pins AC-2 on QA's route: when the dialect is missing, ``plan_transfer``
and ``start_transfer`` refuse before Confirm and name the package, and no job
is created (R8).
"""

from __future__ import annotations

import pytest

from tests.test_d30_snowflake_dialect_missing_is_reported import dialect_missing  # noqa: F401
from tests.test_d182_snowflake_own_table_second_run import (  # noqa: F401 - fixtures
    _call,
    route,
    snowflake_semantics,
)


@pytest.mark.parametrize("mode", ["scd2", "mirror"])
def test_history_modes_name_the_missing_snowflake_dialect_before_confirm(
    route, dialect_missing, mode
):
    dest = f"D47_NODIALECT_{mode.upper()}"
    for tool in ("plan_transfer", "start_transfer"):
        out = _call(tool, route, dest, mode)
        text = f"{out.error} {out.output}"
        assert not out.success or "ack_id" not in (out.output or {}), (tool, text[:800])
        assert "snowflake-sqlalchemy" in text, (tool, text[:800])
