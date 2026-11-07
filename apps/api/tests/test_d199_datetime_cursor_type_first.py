"""D199: a datetime cursor is typed without parsing every value as a number.

``compare_cursor_values`` infers the watermark type per comparison, so a keyset
walk over 800k rows ran the integer and decimal parsers on every timestamp
before the datetime rule (which wins first) decided anyway.
"""
from __future__ import annotations

import services.transform_engine as te
from services.cdc_engine import WatermarkType, infer_watermark_type
from services.sync_cursor import compare_cursor_values, encode_keyset_bookmark


def _boom(*_a, **_k):
    raise AssertionError("datetime cursor parsed as a number")


def test_datetime_samples_skip_number_parsers(monkeypatch):
    monkeypatch.setattr(te, "decimal_wire_value", _boom)
    samples = ["2026-01-01 00:00:00.123456", "2026-01-02 08:30:00"]
    assert infer_watermark_type(samples) == WatermarkType.DATETIME


def test_composite_datetime_compare_skips_decimal_parser_for_the_cursor_part(monkeypatch):
    calls: list[str] = []
    real = te.decimal_wire_value

    def spy(value):
        calls.append(str(value))
        return real(value)

    monkeypatch.setattr(te, "decimal_wire_value", spy)
    a = encode_keyset_bookmark(["2026-01-01 00:00:00.5", "7"])
    b = encode_keyset_bookmark(["2026-01-01 00:00:00.25", "9"])
    assert compare_cursor_values(a, b) == 1
    assert not any(":" in c for c in calls), calls


def test_mixed_and_numeric_samples_keep_their_types():
    assert infer_watermark_type(["1", "2", "3"]) == WatermarkType.INTEGER
    assert infer_watermark_type(["1.5", "2.25"]) == WatermarkType.FLOAT
    assert infer_watermark_type(["abc", "2026-01-01"]) == WatermarkType.STRING
    assert infer_watermark_type(["1700000000", "1700000001"]) == WatermarkType.DATETIME
