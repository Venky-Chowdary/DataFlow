"""Keyset page-max uses write-path numbers, not Decimal(text) on the whole page.

Decimal(v) invented Auto 1.234 and, when one cell was $1,234, failed the
whole column into text so '99' beat '200' and the next seek re-read the
head. Locale money must sort as the write path stores it. Auto 1,234 must
not invent thousands.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.keyset_pagination import (  # noqa: E402
    compare_keyset_bookmark,
    max_keyset_bookmark,
)


def test_integer_page_still_picks_200_not_99():
    rows = [["99", "a"], ["200", "b"], ["150", "c"]]
    assert max_keyset_bookmark(rows, ["id", "p"], ["id"]) == "200"
    assert compare_keyset_bookmark("99", "200") == -1


def test_locale_money_orders_as_write_path_number():
    rows = [["99", "a"], ["200", "b"], ["$1,234", "c"]]
    assert max_keyset_bookmark(rows, ["id", "p"], ["id"]) == "$1,234"
    assert compare_keyset_bookmark("$1,234", "200") == 1
    assert compare_keyset_bookmark("€1.234", "200") == 1
    eu = [["€1.234", "c"], ["200", "b"], ["99", "a"]]
    assert max_keyset_bookmark(eu, ["id", "p"], ["id"]) == "€1.234"


def test_dest_canonical_decimal_stays_numeric_identity():
    rows = [["1.234", "a"], ["2.5", "b"], ["0.5", "c"]]
    assert max_keyset_bookmark(rows, ["id", "p"], ["id"]) == "2.5"
    assert compare_keyset_bookmark("1.234", "2.5") == -1


def test_auto_ambiguous_comma_does_not_invent_thousands():
    """1,234 cannot bind Auto. Mixed with 200 keeps text — max is not 1234."""
    rows = [["1,234", "a"], ["200", "b"]]
    bm = max_keyset_bookmark(rows, ["id", "p"], ["id"])
    assert bm != "1,234"
    assert bm == "200"
    assert compare_keyset_bookmark("1,234", "200") == -1
