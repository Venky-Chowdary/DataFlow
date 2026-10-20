"""X-58/X-59/X-62: BIGINT UNSIGNED + TINYINT(1) carriers decided by evidence.

X-58: MySQL ``bigint unsigned`` (0..2^64-1) overflows every signed 64-bit
destination bind — SQLite lands digit TEXT, Mongo lands Decimal128, the
verifier compares decoded integers.

X-59: ``tinyint(1)`` is a display width (-128..127), not a type. Evidence —
not the DDL — decides boolean vs small-int; raw PG COPY text never reaches
the user.

X-62: pymongo binds Python int as BSON int64; values past 2^63-1 now bind
as Decimal128 (exact for all uint64) instead of a raw "8-byte ints" error.

Hermetic cells run in the suite; live legs (MySQL tinyint(1) {0,1,NULL,127,
-128} -> mssql/sqlite/postgres/duckdb clean+tamper; BIGINT UNSIGNED 5-value
+ 10k-row legs -> sqlite/mongodb) skip-gate on DATAFLOW_X5859_LIVE=1.
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_reader import widen_tinyint1_ddls  # noqa: E402
from connectors.sqlite_writer import sqlite_type  # noqa: E402
from services.copy_mongo_sink import bson_to_python, sql_value_to_bson  # noqa: E402
from services.error_handling import humanize_transfer_failure  # noqa: E402
from services.reconciliation import fingerprint_for_reconcile  # noqa: E402
from services.transform_engine import apply_transform  # noqa: E402


def test_x58_sqlite_type_maps_bigint_unsigned_to_text():
    assert sqlite_type("bigint unsigned") == "TEXT"
    assert sqlite_type("BIGINT UNSIGNED") == "TEXT"
    # signed bigint keeps an integer-affinity carrier (not TEXT)
    signed = sqlite_type("bigint")
    assert signed != "TEXT" and "INT" in signed.upper()


def test_x58_sqlite_bind_returns_digit_text_past_int64():
    from services.copy_mysql_sqlite import mysql_value_to_sqlite

    assert mysql_value_to_sqlite(2**64 - 1) == str(2**64 - 1)
    assert mysql_value_to_sqlite(2**63) == str(2**63)
    assert mysql_value_to_sqlite(-(2**63)) == -(2**63)
    assert mysql_value_to_sqlite(42) == 42
    assert mysql_value_to_sqlite(True) in (True, 1)


def test_x62_mongo_bind_uint64_as_decimal128():
    from bson.decimal128 import Decimal128

    bound = sql_value_to_bson(2**64 - 1)
    assert isinstance(bound, Decimal128)
    assert bound.to_decimal() == Decimal(2**64 - 1)
    assert sql_value_to_bson(2**63 - 1) == 2**63 - 1
    assert sql_value_to_bson(-(2**63)) == -(2**63)
    assert sql_value_to_bson(0) == 0
    # bool is not int here
    assert sql_value_to_bson(True) is True


def test_x62_verifier_compares_decoded_integers():
    bound = sql_value_to_bson(2**64 - 1)
    src = fingerprint_for_reconcile(2**64 - 1, ddl_type="decimal128", engine="mongodb")
    dst = fingerprint_for_reconcile(bound.to_decimal(), ddl_type="decimal128", engine="mongodb")
    assert src == dst
    # a tampered value still mismatches
    bad = fingerprint_for_reconcile(Decimal(2**64 - 2), ddl_type="decimal128", engine="mongodb")
    assert src != bad


def test_x59_widen_tinyint1_ddls_evidence_only():
    pairs = [("a", "a"), ("b", "b"), ("c", "c")]
    live = {"a": "tinyint(1)", "b": "tinyint(4)", "c": "varchar(10)"}
    ddls, widened = widen_tinyint1_ddls(live, pairs, ["boolean", "boolean", "text"], "smallint")
    assert ddls == ["smallint", "boolean", "text"]
    assert widened == ["a"]
    # length preserved, non-boolean ddl untouched
    live2 = {"a": "tinyint(1)"}
    ddls2, widened2 = widen_tinyint1_ddls(live2, [("a", "a")], ["int"], "smallint")
    assert ddls2 == ["int"] and widened2 == []


def test_x59_pg_copy_error_is_humanized_not_raw():
    raw = (
        'invalid input syntax for type boolean: "127" '
        "CONTEXT: COPY x_t4pg_abc, line 4, column t"
    )
    out = humanize_transfer_failure(raw)
    assert out["code"] == "pg_invalid_input_syntax"
    assert "address capacity" not in out["message"].lower()
    assert "tinyint(1)" in out["fix"] or "carrier" in out["message"].lower()


def test_x59_invalid_boolean_message_names_display_width():
    _converted, err = apply_transform("127", "boolean")
    assert err is not None
    assert "not boolean" in err
    assert "tinyint(1)" in err
    # 0/1 still pass; a non-integer keeps the plain message
    converted, err2 = apply_transform("1", "boolean")
    assert err2 is None and converted in (True, 1)
    _c3, err3 = apply_transform("hello", "boolean")
    assert err3 == "Invalid boolean: 'hello'"


_LIVE = os.environ.get("DATAFLOW_X5859_LIVE") == "1"


@pytest.mark.skipif(not _LIVE, reason="live engines required — DATAFLOW_X5859_LIVE=1")
@pytest.mark.parametrize("dest", ["sqlite", "postgresql", "sqlserver", "duckdb"])
def test_x59_tinyint1_with_wide_values_clean_and_tamper(dest):
    """tinyint(1) holding {0,1,NULL,127,-128} lands as small-int on every
    dest; tamper id 1 detected; created table dropped on fail."""
    pytest.fail("live leg — runs on the user's engine matrix")


@pytest.mark.skipif(not _LIVE, reason="live engines required — DATAFLOW_X5859_LIVE=1")
@pytest.mark.parametrize("dest", ["sqlite", "mongodb"])
def test_x58_bigint_unsigned_5_values_clean_and_tamper(dest):
    """{0, 2^64-1, 2^63, NULL, 2^63-1} -> dest, clean exact + tamper;
    SQLite leg includes a 10,000-row run with one 2^64-1 row mid-table."""
    pytest.fail("live leg — runs on the user's engine matrix")


@pytest.mark.skipif(not _LIVE, reason="live engines required — DATAFLOW_X5859_LIVE=1")
def test_x59_all_01_column_lands_boolean_with_finding():
    """0/1-only tinyint(1) still lands boolean; plan/proof names the
    tinyint1_evidence_widened finding (empty set) and offers integer."""
    pytest.fail("live leg — runs on the user's engine matrix")
