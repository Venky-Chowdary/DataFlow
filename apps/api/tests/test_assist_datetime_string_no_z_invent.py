"""Assist datetime→string must not name a forced Z the write path does not stamp.

Naive wall-clock stays naive. UTC-aware uses Z. Date→datetime stays
lossless widening. The matrix remains non-authoritative.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.knowledge.type_conversions import (  # noqa: E402
    AUTHORITATIVE,
    suggest_type_conversion,
)
from services.transform_engine import apply_transform  # noqa: E402


def test_assist_datetime_string_does_not_force_z():
    assert AUTHORITATIVE is False
    hint = suggest_type_conversion("datetime", "string")
    assert hint is not None
    assert hint.get("authoritative") is False
    assert hint.get("format") == "%Y-%m-%dT%H:%M:%S"
    assert not str(hint.get("format") or "").endswith("Z")
    assert "no Z invent" in (hint.get("note") or "")


def test_write_path_naive_stays_naive_utc_keeps_z():
    naive, err = apply_transform("2024-06-01T12:00:00", "datetime")
    assert err is None
    assert naive == "2024-06-01T12:00:00"
    assert not str(naive).endswith("Z")
    aware, err2 = apply_transform("2024-06-01T12:00:00Z", "datetime")
    assert err2 is None
    assert str(aware).endswith("Z")


def test_date_to_datetime_stays_lossless_widening():
    hint = suggest_type_conversion("date", "datetime")
    assert hint is not None
    assert hint.get("lossy") is False
    midnight, err = apply_transform("2024-06-01", "datetime")
    assert err is None
    assert midnight == "2024-06-01T00:00:00"
    assert not str(midnight).endswith("Z")
