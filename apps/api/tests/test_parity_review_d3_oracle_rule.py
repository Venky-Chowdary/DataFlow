"""Review D3: the classifier's own Oracle set duplicated (and drifted from)
``oracle_empty_string.empty_is_null_destination``.

``_ORACLE = {"oracle", "oracle_db"}`` missed the engine names the rest of the
product treats as Oracle (``oracledb``, ``oracle_autonomous``), so an Oracle
Autonomous destination storing '' as NULL read as an unexplained difference.
The classifier now asks the shared rule.
"""

from __future__ import annotations

import pytest

from services.oracle_empty_string import empty_is_null_destination
from services.parity_classifier import CellContext, null_vs_empty


@pytest.mark.parametrize("engine", ["oracle", "oracledb", "oracle_autonomous", "ORACLE", "postgresql", "mysql", ""])
def test_null_vs_empty_follows_the_shared_oracle_rule(engine):
    found = null_vs_empty("", None, CellContext(dest_engine=engine))
    assert found is not None and found["code"] == "null_vs_empty"
    assert found["explained"] is empty_is_null_destination(engine)
