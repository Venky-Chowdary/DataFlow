"""Unit pins for the round-10 fidelity fixes (end to end: test_r10_fidelity_run2_e2e)."""

from __future__ import annotations

import sqlite3

import pytest

from services.decimal_observe import ieee_float_create_new_risk, observe_numeric_samples
from services.text_temporal_readback import time_of_day_over_date

_RESIDUE = ["0.30000000000000004", "0.1", "0.7", "0.2", "1.0000000000000002"]


@pytest.mark.parametrize(
    ("inferred", "values", "want"),
    [
        ("DATE", ["2026-10-01T00:00:00", "2026-10-02T00:00:00"], "TIMESTAMP"),
        ("DATE", ["2026-10-01 00:00:00.000000"], "TIMESTAMP"),
        ("DATE", ["2026-10-01", "2026-10-02"], "DATE"),
        ("DATE", [None, b"\x00", "2026-10-01"], "DATE"),
        ("TIMESTAMP", ["2026-10-01T00:00:00"], "TIMESTAMP"),
        ("VARCHAR", ["2026-10-01T00:00:00"], "VARCHAR"),
    ],
)
def test_time_of_day_over_date(inferred, values, want):
    assert time_of_day_over_date(inferred, values) == want


def test_sqlite_read_back_of_dataflows_temporal_text_is_a_timestamp(tmp_path):
    """D225: the TEXT column DataFlow creates for an Oracle DATE, all cells midnight."""
    from services.schema_introspect import introspect_schema

    db = tmp_path / "d.sqlite"
    conn = sqlite3.connect(db)
    conn.execute('CREATE TABLE t ("ID" INTEGER, "D" TEXT, "DAY" TEXT)')
    conn.executemany("INSERT INTO t VALUES (?, ?, ?)",
                     [(i, f"2026-10-0{i}T00:00:00", f"2026-10-0{i}") for i in range(1, 6)])
    conn.commit()
    conn.close()
    info = introspect_schema("sqlite", database=str(db), table="t")
    types = {c["name"]: c["inferred_type"] for c in info["columns"]}
    assert types["D"].upper().startswith("TIMESTAMP"), types
    assert types["DAY"] == "DATE", types


@pytest.mark.parametrize(
    ("dest_db", "target", "kind"),
    [
        ("sqlite", "TEXT", "ieee_float_residue_kept_exact"),
        ("mongodb", "decimal", "ieee_float_residue_kept_exact"),
        ("dynamodb", "N", "ieee_float_residue_kept_exact"),
        ("postgresql", "NUMERIC(18,17)", "ieee_float_residue_kept_exact"),
        # Not an exact carrier: the chip still holds the column.
        ("sqlite", "REAL", "ieee_float_artifact"),
        ("mongodb", "double", "ieee_float_artifact"),
        ("postgresql", "TEXT", "ieee_float_artifact"),
        ("postgresql", "VARCHAR(8)", "ieee_float_artifact"),
        ("sqlserver", "DECIMAL(10,2)", "ieee_float_artifact"),
    ],
)
def test_residue_chip_knows_each_destinations_exact_decimal_carrier(dest_db, target, kind):
    """D161 remainder: SQLite's exact decimal is TEXT, Mongo's is Decimal128."""
    risk = ieee_float_create_new_risk(
        observe_numeric_samples(_RESIDUE), target_type=target, dest_db=dest_db
    )
    assert risk and risk["kind"] == kind, risk


def test_native_decimal_carrier_respects_its_digit_capacity():
    wide = [f"1234567890123456789.{'1' * 16}0000000000000004", "0.1"]
    risk = ieee_float_create_new_risk(
        observe_numeric_samples(wide), target_type="decimal", dest_db="mongodb"
    )
    assert risk is None or risk["kind"] != "ieee_float_residue_kept_exact", risk
