"""L2 column aggregates: one decimal parse per cell, the same aggregates.

``compute_column_aggregates`` used to call ``_wire_less`` (two parses) for the
min and for the max and parse again for the sum. It now parses each cell once
and keeps the extremes' parse. This compares it with the old algorithm over
mixed numeric / text / locale-shaped cells under every number-locale pin.
"""

from __future__ import annotations

import hashlib
import random
from decimal import Decimal

import pytest

from services import verification_ladder as vl
from services.transform_engine import reset_active_number_locale, set_active_number_locale


def _reference(rows, columns):
    from services.reconciliation import normalize_cell

    out = {}
    for col in columns:
        nulls = non_null = 0
        mn = mx = None
        total: Decimal | None = Decimal(0)
        ok = True
        distinct, fps = set(), []
        for rec in rows:
            text = vl._cell_text(rec.get(col))
            if text is None:
                nulls += 1
                continue
            non_null += 1
            distinct.add(text)
            fps.append(normalize_cell(text))
            if mn is None or vl._wire_less(text, mn):
                mn = text
            if mx is None or vl._wire_less(mx, text):
                mx = text
            if ok:
                d = vl._l2_sum_decimal(text)
                if d is None:
                    ok, total = False, None
                else:
                    total = (total or Decimal(0)) + d
        fps.sort()
        digest = hashlib.sha256()
        for fp in fps:
            digest.update(fp.encode("utf-8"))
            digest.update(b"\n")
        out[col] = (nulls, non_null, len(distinct), mn, mx,
                    None if not ok or total is None else format(total, "f"), digest.hexdigest())
    return out


@pytest.mark.parametrize("locale", ["", "US", "EU", "WIRE"])
@pytest.mark.parametrize("seed", range(6))
def test_single_parse_aggregates_match_the_old_algorithm(seed, locale):
    rng = random.Random(seed)
    pool = ["9", "10", "-3", "1,234", "1.234", "1.000", "0.5", "1e3", "abc", "", " 7 ",
            "true", "false", "$1,234.50", "€1.234,50", "2024-01-01", None, "NaN", "12.50"]
    rows = []
    for _ in range(400):
        rows.append({
            "a": rng.choice(pool),
            "b": str(rng.randrange(-10**6, 10**6)) if rng.random() > 0.1 else None,
            "c": f"{rng.randrange(0, 10**4)}.{rng.randrange(0, 10**4):04d}",
            "d": rng.choice([True, False, None, 3, Decimal("2.50"), 1.25]),
        })
    token = set_active_number_locale(locale)
    try:
        got = vl.compute_column_aggregates(rows, ["a", "b", "c", "d"])
        want = _reference(rows, ["a", "b", "c", "d"])
    finally:
        reset_active_number_locale(token)
    for col, agg in got.items():
        assert (agg.null_count, agg.non_null_count, agg.distinct_count, agg.min_value,
                agg.max_value, agg.sum_value, agg.value_fingerprint) == want[col], col
