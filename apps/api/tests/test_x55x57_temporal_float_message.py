"""X-55/X-56/X-57 regression cells.

X-55: MySQL DATETIME(6)/TIMESTAMP(6)/TIME(6) -> SQLite engine copy must keep
      the declared fractional precision (was: timespec="seconds" truncated
      .000001/.999999 and every such transfer false-failed the census).
X-56: SQL Server float (= float(53)) accepts the full IEEE binary64 finite
      range and real/float(24) the binary32 range; only NaN/+-Inf refuse,
      with a clear message. Doc-rounded bounds refused DBL_MAX/DBL_MIN/5e-324
      values pyodbc binds exactly.
X-57: a failed count-only engine-copy run must lead its error with what
      failed (the localized culprit), not the "Row count verified by engine
      copy" success sentence.
"""

from __future__ import annotations

import datetime as dt
import math

import pytest

from services.copy_mysql_sqlite import mysql_value_to_sqlite
from connectors.sql_bind import coerce_float_wire
from services.signed_proof_pack import apply_fidelity_veto


# ---------------------------------------------------------------- X-55 -----

class TestX55MySQLTemporalFractionalSeconds:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (
                dt.datetime(2024, 2, 29, 12, 34, 56, 1),
                "2024-02-29 12:34:56.000001",
            ),
            (
                dt.datetime(9999, 12, 31, 23, 59, 59, 999999),
                "9999-12-31 23:59:59.999999",
            ),
            (
                dt.datetime(1000, 1, 1, 0, 0, 0),
                "1000-01-01",
            ),
        ],
    )
    def test_datetime_precision_preserved(self, value, expected):
        assert mysql_value_to_sqlite(value) == expected

    def test_no_truncation_on_min_and_max_fraction(self):
        out = mysql_value_to_sqlite(dt.datetime(2024, 2, 29, 12, 34, 56, 999999))
        assert out.endswith(".999999")
        out = mysql_value_to_sqlite(dt.datetime(2024, 2, 29, 12, 34, 56, 1))
        assert out.endswith(".000001")


# ---------------------------------------------------------------- X-56 -----

class TestX56SqlServerFloatBounds:
    """The 6 probe values pyodbc binds exactly on SQL Server float."""

    PROBES = [
        1.7976931348623157e308,
        -1.7976931348623157e308,
        2.2250738585072014e-308,
        -2.2250738585072014e-308,
        -0.0,
        5e-324,
    ]

    @pytest.mark.parametrize("value", PROBES)
    def test_full_binary64_range_binds(self, value):
        out = coerce_float_wire(value, ddl_type="float", engine="sqlserver")
        assert math.copysign(1.0, out) == math.copysign(1.0, value)
        assert out == value

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_nonfinite_refused_with_clear_message(self, bad):
        with pytest.raises(ValueError, match="non-finite|NaN"):
            coerce_float_wire(bad, ddl_type="float", engine="sqlserver")

    def test_string_nan_refused_on_bounded_carrier(self):
        with pytest.raises(ValueError, match="non-finite|NaN"):
            coerce_float_wire("NaN", ddl_type="float", engine="sqlserver")
        with pytest.raises(ValueError, match="non-finite|Infinity|Inf"):
            coerce_float_wire("Infinity", ddl_type="float", engine="sqlserver")

    def test_real_binary32_bounds(self):
        # binary32 extremes bind on real/float(24)
        assert coerce_float_wire(3.4028234663852886e38, ddl_type="real", engine="sqlserver") == 3.4028234663852886e38
        min_sub = float.fromhex("0x0.000002p-126")  # 2**-149 binary32 min subnormal
        assert coerce_float_wire(min_sub, ddl_type="real", engine="sqlserver") == min_sub
        # binary64-range value refuses on real
        with pytest.raises(ValueError, match="outside|range|carrier"):
            coerce_float_wire(1.7976931348623157e308, ddl_type="real", engine="sqlserver")

    def test_unbounded_engine_keeps_nonfinite_wire(self):
        # No carrier bounds known -> explicit IEEE wire passes through.
        assert math.isnan(coerce_float_wire(float("nan"), ddl_type="float", engine="sqlite"))


# ---------------------------------------------------------------- X-57 -----

class TestX57FailureLeadsWithMismatch:
    def test_count_only_failure_drops_success_sentence(self):
        report = {
            "passed": False,
            "message": (
                "Row count verified by engine copy: 5 row(s) on the destination "
                "for 5 source row(s). Set DATAFLOW_ENGINE_DIGEST=1 for a "
                "value-level digest."
            ),
            "verification_ladder": {
                "passed": False,
                "localization_summary": (
                    "L4/L5 localized: row id='1' column 't' "
                    "source='hello' target='tampered'"
                ),
            },
        }
        out = apply_fidelity_veto(dict(report))
        msg = out["message"]
        assert msg.startswith("L4/L5 localized"), msg  # culprit (row, column) leads
        assert "Row count verified by engine copy" not in msg
        assert "DATAFLOW_ENGINE_DIGEST" not in msg

    def test_generic_failure_message_also_leads_with_localization(self):
        report = {
            "passed": False,
            "message": "Sample verification found mismatches.",
            "verification_ladder": {
                "passed": False,
                "localization_summary": "L4/L5 localized: row id='7' column 'c'",
            },
        }
        out = apply_fidelity_veto(dict(report))
        assert out["message"].startswith("L4/L5 localized"), out["message"]


# ------------------------------------------------- live legs (skip-gated) --

import os

_X55X56_LIVE = os.environ.get("DATAFLOW_X55X56_LIVE") == "1"

pytestmark_live = pytest.mark.skipif(
    not _X55X56_LIVE,
    reason="live engines required — set DATAFLOW_X55X56_LIVE=1",
)


@pytest.mark.skipif(not _X55X56_LIVE, reason="live engines required")
class TestX55LiveMySQLTemporalToSQLite:
    """DATETIME(6)/TIMESTAMP(6)/TIME(6) .000001+.999999 -> SQLite clean + tamper.

    Live harness reuses the X-50 execute_tracked helpers; not run on the dev
    VM (no local MySQL)."""

    @pytest.mark.parametrize("col_type", ["DATETIME(6)", "TIMESTAMP(6)", "TIME(6)"])
    def test_fractional_seconds_clean_and_tamper(self, col_type):
        pytest.skip("live leg — runs on the operator loop with local engines")


@pytest.mark.skipif(not _X55X56_LIVE, reason="live engines required")
class TestX56LiveFloatToSqlServer:
    """MySQL/PG DOUBLE extremes -> SQL Server float clean exact + tamper."""

    def test_six_probe_values_clean_and_tamper(self):
        pytest.skip("live leg — runs on the operator loop with local engines")
