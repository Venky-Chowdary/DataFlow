"""Row filters compare write-path Decimals, not IEEE float(parsed).

float() after a successful bind collapsed money scale into binary float.
Auto 1,234 still cannot bind, so compare stays text — do not invent 1234.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.row_filter import apply_row_filter  # noqa: E402


def test_locale_money_compares_as_write_path_decimal():
    rows = [{"id": "1", "amount": "$1,234.56"}, {"id": "2", "amount": "99"}]
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "amount", "operator": "gt", "value": "1000"}
    )} == {"1"}
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "amount", "operator": "eq", "value": "€1.234,56"}
    )} == {"1"}


def test_auto_ambiguous_grouping_stays_text_not_invented_thousands():
    rows = [{"id": "1", "amount": "1,234"}, {"id": "2", "amount": "200"}]
    # Wire refuses both as a pair with 1000; 1,234 vs 200 is text — max-like gt
    # must not treat 1,234 as 1234.
    got = apply_row_filter(rows, {"column": "amount", "operator": "gt", "value": "200"})
    assert {r["id"] for r in got} != {"1"}
