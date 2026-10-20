"""X-81/X-82: BIT(8) declared MySQL bind + fixed-width bit-string compare.

X-81: MySQL BIT(n) binds the integer the bits represent — the driver packs
it into n bits; the '00000000' text is n CHARACTERS and died "Data too
long for column". PG-family and engines taking a bit literal keep text.
X-82: the verifier rendered bit cells as integers, stripping leading zeros
('00000000' -> '0') and false-failing clean runs. Cells now fingerprint as
fixed-width bit strings so a literal source, an int read-back, and raw
bytes fold to one canonical value while real tampering still differs.
"""

from __future__ import annotations

import os

import pytest

from connectors.sql_bind import coerce_bitstring_wire
from services.reconciliation import _bit_cell_fingerprint, _fingerprint_with_ddl


def test_mysql_bit_binds_integer():
    assert coerce_bitstring_wire("00000000", width=8, engine="mysql") == 0
    assert coerce_bitstring_wire("00000001", width=8, engine="mysql") == 1
    assert coerce_bitstring_wire("10101010", width=8, engine="mysql") == 0b10101010
    assert coerce_bitstring_wire(1, width=8, engine="mysql") == 1
    for eng in ("mariadb", "aurora_mysql"):
        assert coerce_bitstring_wire("00000001", width=8, engine=eng) == 1


def test_literal_engines_keep_bit_text():
    for eng in ("postgresql", "duckdb", ""):
        assert coerce_bitstring_wire("00000000", width=8, engine=eng) == "00000000"


def test_width_still_enforced():
    with pytest.raises(ValueError, match="does not match BIT\\(8\\)"):
        coerce_bitstring_wire("000000001", width=8, engine="mysql")
    with pytest.raises(ValueError, match="exceeds BIT VARYING"):
        coerce_bitstring_wire("111111111", width=8, varying=True, engine="mysql")


def test_bit_fingerprint_fixed_width():
    # Source literal, MySQL int read-back, raw bytes, and stripped text all
    # land on the same cell under BIT(8).
    expected = _bit_cell_fingerprint("00000000", 8)
    assert _bit_cell_fingerprint(0, 8) == expected
    assert _bit_cell_fingerprint(b"\x00", 8) == expected
    assert _bit_cell_fingerprint("0", 8) == expected
    # Leading zeros are significant: '00000001' != '00000000' under width 8.
    assert _bit_cell_fingerprint("00000001", 8) != expected
    # Through the ddl-aware path.
    a = _fingerprint_with_ddl("00000000", "BIT(8)", "duckdb")
    b = _fingerprint_with_ddl(0, "BIT(8)", "duckdb")
    assert a == b
    assert _fingerprint_with_ddl("00000001", "BIT(8)", "duckdb") != a


@pytest.mark.skipif(not os.environ.get("DATAFLOW_X81_LIVE"), reason="live engines — user-run")
def test_x81_live_pg_bit8_to_mysql():
    pytest.fail("harness: PG BIT(8) -> MySQL binds, verifies, tamper caught")
