"""Schematic index — million-scale column variant lookup."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_schematic_index_scale():
    from services.schematic_index import lookup_schematic, schematic_count

    count = schematic_count()
    assert count >= 1_000_000, f"Expected 1M+ schematics, got {count}"

    # Enterprise prefix variants
    assert lookup_schematic("dim_customer_id") == "customer_id"
    assert lookup_schematic("stg_cust_id") == "customer_id"
    assert lookup_schematic("stg_amt") == "amount"


def test_schematic_match_boost():
    from services.schematic_index import schematic_match_boost

    score = schematic_match_boost("billing_cust_id", "customer_id")
    assert score is not None and score >= 0.95
