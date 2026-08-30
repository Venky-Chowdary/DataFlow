"""Gate-8 number checksum uses the write-path parser — no Decimal(text) invent.

Auto ``1,234`` / ``1.234`` / ``1.000`` stay opaque. Locale money the write
path binds folds to the dest DECIMAL so a clean transfer does not false-fail.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.reconciliation import (  # noqa: E402
    _canonicalize_number,
    checksum_rows,
    normalize_cell,
)
from services.transform_engine import decimal_wire_value  # noqa: E402


def test_canonicalize_refuses_auto_ambiguous_three_digit_groups():
    """Decimal(text) used to invent 1.000 → 1 and 1.234 → 1.234."""
    assert decimal_wire_value("1,234") is None
    assert decimal_wire_value("1.234") is None
    assert decimal_wire_value("1.000") is None
    assert decimal_wire_value("1.005") is None
    assert _canonicalize_number("1,234") is None
    assert _canonicalize_number("1.234") is None
    assert _canonicalize_number("1.000") is None
    assert _canonicalize_number("1.005") is None


def test_normalize_keeps_auto_ambiguous_opaque():
    """Refused cells must not collapse onto a different dest number."""
    assert normalize_cell("1,234") == "1,234"
    assert normalize_cell("1.234") == "1.234"
    assert normalize_cell("1.000") == "1.000"
    assert normalize_cell("1.005") == "1.005"
    # The invent Gate-8 used to make: 1.000 checksummed as integer 1.
    assert normalize_cell("1.000") != normalize_cell(1)
    assert normalize_cell("1.000") != normalize_cell(Decimal("1"))
    assert normalize_cell("1.230") != normalize_cell(Decimal("1.23"))


def test_normalize_folds_bindable_scale_and_locale_money():
    """Write-path binds still equate source wire to dest DECIMAL read-back."""
    assert normalize_cell("1.2345") == "1.2345"
    assert normalize_cell("1.00") == "1"
    assert normalize_cell("$1,234.56") == normalize_cell(Decimal("1234.56"))
    assert normalize_cell("€1.234,56") == normalize_cell(Decimal("1234.56"))
    assert normalize_cell("$1,234") == normalize_cell(Decimal("1234"))
    assert normalize_cell("1,234.56") == "1234.56"
    assert normalize_cell("1.234,56") == "1234.56"
    assert normalize_cell("USD 1,234.56") == "1234.56"


def test_canonicalize_still_folds_ieee_residue_and_exact_decimals():
    assert _canonicalize_number("106.60000000000001") == _canonicalize_number(
        Decimal("106.6")
    )
    assert _canonicalize_number("1.23E-10") == _canonicalize_number(Decimal("1.23E-10"))
    assert _canonicalize_number("9.5") == _canonicalize_number(Decimal("9.5000000000"))


def test_checksum_rows_locale_money_matches_dest_decimal():
    source = [{"id": "1", "amt": "$1,234.56"}]
    dest = [{"id": "1", "amt": Decimal("1234.56")}]
    assert checksum_rows(source, ["id", "amt"]) == checksum_rows(dest, ["id", "amt"])


def test_checksum_rows_auto_ambiguous_does_not_match_invented_number():
    """EU 1.000 (thousands) must not checksum as dest integer 1."""
    source = [{"id": "1", "amt": "1.000"}]
    dest_one = [{"id": "1", "amt": 1}]
    dest_thousand = [{"id": "1", "amt": 1000}]
    src_chk = checksum_rows(source, ["id", "amt"])
    assert src_chk != checksum_rows(dest_one, ["id", "amt"])
    assert src_chk != checksum_rows(dest_thousand, ["id", "amt"])
