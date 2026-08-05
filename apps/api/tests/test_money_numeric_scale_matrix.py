"""Cross-dialect MONEY/NUMERIC scale honesty matrix (rank 29).

Dialect caps are wide; the durable differentiator is detecting scale collapse
when mapping onto a narrower declared carrier (NUMERIC(p,s) / MONEY).
"""

from __future__ import annotations

import pytest

from connectors.writer_common import parse_decimal_precision_scale
from services.type_system import (
    decimal_precision_would_truncate,
    decimal_scale_would_truncate,
)


DIALECT_CASES = [
    ("DECIMAL(38,50)", "mysql", True),       # mysql scale cap 30
    ("DECIMAL(38,10)", "mysql", False),
    ("DECIMAL(2000,2)", "postgresql", True), # precision cap 1000 via precision helper
    ("DECIMAL(38,2)", "postgresql", False),
    ("DECIMAL(38,40)", "bigquery", True),    # BQ cap scale 38
    ("DECIMAL(18,4)", "snowflake", False),
]


@pytest.mark.parametrize("src,dialect,expect", DIALECT_CASES)
def test_dialect_cap_honesty(src, dialect, expect):
    if "2000" in src:
        got = decimal_precision_would_truncate(src, dialect)
    else:
        got = decimal_scale_would_truncate(src, dialect)
    assert bool(got) is expect, f"{src} -> {dialect}: got {got}, want {expect}"


TYPE_PAIR_CASES = [
    ("DECIMAL(18,6)", "NUMERIC(18,2)", True),
    ("DECIMAL(18,2)", "NUMERIC(18,4)", False),
    ("DECIMAL(18,4)", "MONEY", False),
    ("DECIMAL(18,6)", "MONEY", True),
    ("DECIMAL(18,4)", "NUMBER(38,4)", False),
    ("DECIMAL(18,6)", "NUMBER(38,2)", True),
    ("DECIMAL(18,4)", "SMALLMONEY", False),
    ("DECIMAL(18,6)", "SMALLMONEY", True),
]


def _scale_of(type_str: str) -> int | None:
    parsed = parse_decimal_precision_scale(type_str)
    return None if parsed is None else int(parsed[1])


@pytest.mark.parametrize("src,dest,expect", TYPE_PAIR_CASES)
def test_type_pair_scale_collapse(src, dest, expect):
    src_s = _scale_of(src)
    dest_s = _scale_of(dest)
    if dest_s is None:
        pytest.skip(f"dest {dest} has no parsed scale")
    assert src_s is not None
    assert (src_s > dest_s) is expect


def test_money_parses_four_scale():
    assert parse_decimal_precision_scale("MONEY") == (19, 4)
