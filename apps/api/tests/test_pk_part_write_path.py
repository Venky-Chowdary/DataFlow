"""CDC leftover PK parts use coerce_integer_wire, not isdigit().

$1,234 the write path stores must bind. Auto 1,234 / 1.000 stay text.
Informal true stays text — never invent 1.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.row_conservation import coerce_pk_part  # noqa: E402


def test_plain_int_string_still_binds():
    assert coerce_pk_part("42") == 42
    assert coerce_pk_part("-7") == -7
    assert coerce_pk_part(99) == 99


def test_locale_money_binds():
    assert coerce_pk_part("$1,234") == 1234
    assert coerce_pk_part("€1.234") == 1234


def test_auto_grouping_stays_text():
    assert coerce_pk_part("1,234") == "1,234"
    assert coerce_pk_part("1.000") == "1.000"
    assert coerce_pk_part("1.234") == "1.234"


def test_informal_true_stays_text():
    assert coerce_pk_part("true") == "true"
    assert coerce_pk_part(True) is True
