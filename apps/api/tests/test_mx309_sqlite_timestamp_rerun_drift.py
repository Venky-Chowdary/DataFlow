"""QA MX3-09 — run 2 of PG→SQLite incremental blocked by our own DDL.

Run 1 creates ``updated_at TIMESTAMP`` on SQLite. Run 2 introspects it and
drift/G3 graded ``TIMESTAMP_NTZ → TIMESTAMP`` as ``narrow_type``
(rc-fidelity-collapse): ``destination_temporal_fractional_digits`` had no
SQLite default, so bare ``TIMESTAMP`` fell to the fail-closed FSP 0, and the
source's unparameterised ``TIMESTAMP_NTZ`` was read at Snowflake's 9 digits.
SQLite has no temporal storage class; the writer binds ``isoformat`` text,
which keeps every microsecond a Python datetime carries. Earlier fix 88854cfb
only covered timezone *polarity*, not fractional precision.
"""

from __future__ import annotations

import pytest

from services.schema_drift import detect_schema_drift
from services.type_system import (
    destination_temporal_fractional_digits,
    is_precision_collapse_coercion,
)


@pytest.mark.parametrize(
    "source_type",
    ["TIMESTAMP_NTZ", "TIMESTAMP", "timestamp without time zone", "TIMESTAMP(6)", "TIME"],
)
def test_sqlite_timestamp_carrier_is_not_a_narrowing(source_type):
    target = "TIME" if source_type == "TIME" else "TIMESTAMP"
    assert not is_precision_collapse_coercion(source_type, target, dest_db="sqlite")


def test_sqlite_bare_temporal_keeps_microseconds():
    for spelling in ("TIMESTAMP", "TIMESTAMP_NTZ", "TIME"):
        assert destination_temporal_fractional_digits(spelling, dest_db="sqlite") == 6


def test_real_narrowing_still_blocks():
    # MySQL bare TIMESTAMP is FSP 0; an explicit SQLite TIMESTAMP(0) is a
    # declared whole-second column; SQL Server DATETIME2 holds 7 digits.
    assert is_precision_collapse_coercion("TIMESTAMP(6)", "TIMESTAMP", dest_db="mysql")
    assert is_precision_collapse_coercion("TIMESTAMP(6)", "TIMESTAMP(0)", dest_db="sqlite")
    assert is_precision_collapse_coercion("DATETIME2", "TIMESTAMP", dest_db="sqlite")
    # Unnamed engine stays fail-closed.
    assert is_precision_collapse_coercion("TIMESTAMP(6)", "TIMESTAMP", dest_db="")


def test_second_run_drift_on_our_own_sqlite_ddl_is_clean():
    cols = ["id", "amount", "updated_at"]
    drift = detect_schema_drift(
        source_columns=cols,
        source_schema={"id": "INTEGER", "amount": "DECIMAL(10,2)", "updated_at": "TIMESTAMP_NTZ"},
        target_columns=cols,
        target_schema={"id": "INTEGER", "amount": "DECIMAL(10,2)", "updated_at": "TIMESTAMP"},
        mappings=[{"source": c, "target": c} for c in cols],
        destination_db_type="sqlite",
        table_exists=True,
        cursor_fields=["updated_at"],
    )
    breaking = (drift.get("schema_evolution") or drift.get("classification") or {}).get("breaking") or []
    assert not [b for b in breaking if b.get("column") == "updated_at"], drift
    assert not [
        m for m in (drift.get("type_mismatches") or []) if m.get("source") == "updated_at"
    ], drift
