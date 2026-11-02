"""ARRAY integer elements use the write-path number parser.

Decimal(text) invented Auto 1.234 as numeric and missed $1,234 / €1.234
that scalar INTEGER bind stores — the array gate then quarantined a row
the writer would accept.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.array_wire import _is_numeric_wire  # noqa: E402
from connectors.writer_common import array_element_unfit_reason, fits_integer  # noqa: E402
from services.transform_engine import decimal_wire_value, integer_wire_value  # noqa: E402


def test_locale_money_integer_array_matches_scalar_bind():
    assert decimal_wire_value("$1,234") == 1234
    assert integer_wire_value("$1,234") == 1234
    assert decimal_wire_value("€1.234") == 1234
    assert integer_wire_value("€1.234") == 1234
    assert _is_numeric_wire("$1,234") is True
    assert _is_numeric_wire("€1.234") is True
    assert fits_integer("$1,234", "INTEGER") is True
    assert array_element_unfit_reason("$1,234", "INTEGER") is None
    assert array_element_unfit_reason("€1.234", "INTEGER") is None
    assert array_element_unfit_reason("$1,234", "BIGINT") is None


def test_auto_ambiguous_grouping_is_not_numeric_wire():
    for token in ("1.234", "1,234", "1.000", "1.005"):
        assert decimal_wire_value(token) is None
        assert _is_numeric_wire(token) is False
        assert array_element_unfit_reason(token, "INTEGER")
        assert "not numeric" in array_element_unfit_reason(token, "INTEGER")


def test_fractional_money_is_unfit_integer_not_non_numeric():
    """$1,234.56 binds as decimal; INTEGER names fractional, not 'not numeric'."""
    assert decimal_wire_value("$1,234.56") is not None
    assert integer_wire_value("$1,234.56") is None
    assert _is_numeric_wire("$1,234.56") is True
    reason = array_element_unfit_reason("$1,234.56", "INTEGER")
    assert reason
    assert "not numeric" not in reason
    assert array_element_unfit_reason("$1,234.56", "DECIMAL(10,2)") is None


def test_wordy_true_stays_non_numeric_on_integer_array():
    """coerce_integer_wire still refuses wordy true — do not invent 1 here."""
    assert decimal_wire_value("true") is None
    assert _is_numeric_wire("true") is False
    assert array_element_unfit_reason("true", "INTEGER")
    assert array_element_unfit_reason("true", "BOOLEAN") is None
