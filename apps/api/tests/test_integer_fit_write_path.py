"""INTEGER fit reasons use the write path — Auto 1.000 is not overflow.

Decimal(text) invented Auto 1.000 as integral 1 and said it exceeded the
integer wire budget. The writer refuses that token. Locale money with
cents is fractional. Dest-canonical 1.234 stays the fractional message.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.writer_common import fits_integer, integer_fit_failure  # noqa: E402


def test_auto_1_000_is_not_an_integer_not_overflow():
    for token in ("1.000", "1,234"):
        assert fits_integer(token, "INT", dest_db="mysql") is False
        reason = integer_fit_failure(token, "INT", dest_db="mysql")
        assert reason
        assert "not an integer" in reason
        assert "exceeds" not in reason
        assert "budget" not in reason


def test_auto_1_234_and_locale_money_cents_are_fractional():
    reason = integer_fit_failure("1.234", "INT", dest_db="mysql")
    assert reason and "fractional" in reason
    money = integer_fit_failure("$1,234.56", "INT", dest_db="mysql")
    assert money and "fractional" in money
    assert integer_fit_failure("$1,234", "INT", dest_db="mysql") is None
    assert integer_fit_failure("€1.234", "INT", dest_db="mysql") is None


def test_true_overflow_still_names_range():
    reason = integer_fit_failure("2147483648", "INTEGER", dest_db="mysql")
    assert reason
    assert "out of range" in reason or "does not fit" in reason or "exceeds" in reason
