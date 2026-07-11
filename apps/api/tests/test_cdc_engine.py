"""Tests for CDC / incremental sync engine."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.cdc_engine import (  # noqa: E402
    WatermarkType,
    advance_watermark,
    compare_watermarks,
    deduplicate_batch,
    diff_by_primary_key,
    infer_watermark_type,
    max_watermark,
    row_fingerprint,
    validate_sync_contract,
)


def test_infer_datetime_watermark():
    samples = ["2025-01-01T10:00:00Z", "2025-01-02T11:00:00Z", "2025-01-03T12:00:00Z"]
    assert infer_watermark_type(samples) == WatermarkType.DATETIME


def test_infer_integer_watermark():
    samples = ["1", "2", "100", "999"]
    assert infer_watermark_type(samples) == WatermarkType.INTEGER


def test_max_watermark_datetime():
    values = ["2025-01-01", "2025-03-01", "2025-02-01"]
    assert max_watermark(values, WatermarkType.DATETIME) == "2025-03-01"


def test_advance_watermark_monotonic():
    new, advanced = advance_watermark("2025-01-01", ["2025-01-02", "2025-01-03"], WatermarkType.DATETIME)
    assert advanced is True
    assert new == "2025-01-03"


def test_advance_watermark_rejects_regression():
    new, advanced = advance_watermark("2025-03-01", ["2025-01-01"], WatermarkType.DATETIME)
    assert advanced is False
    assert new == "2025-03-01"


def test_compare_watermarks_integer():
    assert compare_watermarks("100", "50", WatermarkType.INTEGER) == 1
    assert compare_watermarks("50", "100", WatermarkType.INTEGER) == -1


def test_row_fingerprint_stable():
    row = {"id": "1", "amount": "100.00"}
    assert row_fingerprint(row) == row_fingerprint(row)
    assert row_fingerprint(row) != row_fingerprint({"id": "2", "amount": "100.00"})


def test_diff_detects_insert_update_delete():
    previous = {"1": {"id": "1", "name": "Alice"}, "2": {"id": "2", "name": "Bob"}}
    current = {"1": {"id": "1", "name": "Alice Updated"}, "3": {"id": "3", "name": "Carol"}}
    batch = diff_by_primary_key(previous, current)
    assert len(batch.inserts) == 1
    assert len(batch.updates) == 1
    assert len(batch.deletes) == 1
    assert batch.deletes[0] == "2"


def test_deduplicate_keeps_last():
    rows = [{"id": "1", "v": "a"}, {"id": "1", "v": "b"}, {"id": "2", "v": "c"}]
    deduped, dupes = deduplicate_batch(rows, "id", keep="last")
    assert dupes == 1
    assert deduped[0]["v"] == "b"


def test_validate_sync_contract_requires_cursor():
    issues = validate_sync_contract({"sync_mode": "incremental_append"})
    assert any("cursor" in i.lower() for i in issues)


def test_validate_sync_contract_requires_pk_for_cdc():
    issues = validate_sync_contract({"sync_mode": "cdc", "cursor_field": "updated_at"})
    assert any("primary_key" in i.lower() for i in issues)
