"""Iceberg Map≡CREATE decimal fidelity — no bare invent, no silent clamp."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

pa = pytest.importorskip("pyarrow")

from connectors.iceberg_writer import (  # noqa: E402
    _apply_iceberg_write_quarantine,
    _ensure_iceberg_decimal_carrier,
    _logical_to_arrow_type,
    _logical_to_iceberg_type,
)
from services.type_system import ddl_type, parse_numeric_precision_scale  # noqa: E402


def test_ensure_bare_decimal_stamps_ssot_for_quarantine():
    """Bare DECIMAL must become decimal(38,10) so fit quarantine can parse (p,s)."""
    ens = _ensure_iceberg_decimal_carrier("DECIMAL")
    assert ens.lower().replace(" ", "") == "decimal(38,10)"
    assert parse_numeric_precision_scale(ens) == (38, 10)
    assert ens == ddl_type("iceberg", "DECIMAL")


def test_ensure_oversize_decimal_fail_closed_to_string():
    assert _ensure_iceberg_decimal_carrier("DECIMAL(40,10)") == "string"
    assert _ensure_iceberg_decimal_carrier("BIGNUMERIC(76,38)") == "string"
    assert ddl_type("iceberg", "DECIMAL(40,10)") == "string"


def test_ensure_preserves_in_cap_stamp():
    assert _ensure_iceberg_decimal_carrier("DECIMAL(28,12)").lower().replace(" ", "") == (
        "decimal(28,12)"
    )
    assert _ensure_iceberg_decimal_carrier("NUMBER(12,4)").lower().replace(" ", "") == (
        "decimal(12,4)"
    )


def test_arrow_bare_decimal_matches_ssot_not_silent_invent_mismatch():
    t = _logical_to_arrow_type("DECIMAL", pa)
    assert pa.types.is_decimal(t)
    assert t.precision == 38
    assert t.scale == 10


def test_arrow_preserves_in_cap_stamp():
    t = _logical_to_arrow_type("DECIMAL(28,12)", pa)
    assert pa.types.is_decimal(t)
    assert t.precision == 28
    assert t.scale == 12


def test_arrow_oversize_fail_closed_to_string_never_clamp():
    """DECIMAL(40,10) must not silently become decimal128(38,10)."""
    t = _logical_to_arrow_type("DECIMAL(40,10)", pa)
    assert pa.types.is_string(t) or pa.types.is_large_string(t)
    t2 = _logical_to_arrow_type("BIGNUMERIC(76,38)", pa)
    assert pa.types.is_string(t2) or pa.types.is_large_string(t2)


def test_iceberg_ddl_bare_decimal_matches_ddl_type():
    assert _logical_to_iceberg_type("DECIMAL") == ddl_type("iceberg", "DECIMAL")
    assert _logical_to_iceberg_type("DECIMAL(40,10)") == "string"


def test_bare_decimal_overflow_quarantines_after_ensure():
    """Overflow must hold out once bare stamp is SSOT-parameterized."""
    details: list[dict] = []
    carrier = _ensure_iceberg_decimal_carrier("DECIMAL")
    # 29 integer digits + 2 scale > DECIMAL(38,10) integer capacity (28)
    overflow = "9" * 29 + ".99"
    ok = "1.50"
    out = _apply_iceberg_write_quarantine(
        [(overflow,), (ok,)],
        ["amount"],
        [carrier],
        details,
        policy="quarantine",
    )
    assert out == [(ok,)]
    assert details and "Iceberg" in details[0]["reason"]
