"""VECTOR components use the write-path float binder, not float(text).

float('1.234') invented a component. JSON [9007199254740993] collapsed.
Locale money binds. Native IEEE JSON numbers pass through.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.transform_engine import apply_transform  # noqa: E402


def test_plain_json_and_csv_still_bind():
    ok, err = apply_transform("[0.1, 0.2, 0.3]", "vector")
    assert err is None
    assert ok == [0.1, 0.2, 0.3]
    csv, csv_err = apply_transform("1.5, 2.0, 3.0", "vector")
    assert csv_err is None
    assert csv == [1.5, 2.0, 3.0]


def test_locale_money_component_binds():
    ok, err = apply_transform('["$1.50", "2"]', "vector")
    assert err is None
    assert ok == [1.5, 2.0]


def test_auto_grouping_component_refuses():
    bad, err = apply_transform('["1.234", "2"]', "vector")
    assert bad is None and err
    # CSV comma is the dimension separator — 1,234 is two integer components.
    split, split_err = apply_transform("1,234", "vector")
    assert split_err is None
    assert split == [1.0, 234.0]


def test_ieee_lossy_mantissa_and_bool_refuse():
    bad, err = apply_transform("[9007199254740993, 1]", "vector")
    assert bad is None and err
    boolish, berr = apply_transform("[true, 1]", "vector")
    assert boolish is None and berr
    nan, nerr = apply_transform("[0.1, NaN]", "vector")
    assert nan is None and nerr
