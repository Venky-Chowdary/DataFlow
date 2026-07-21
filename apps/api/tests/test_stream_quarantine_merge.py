"""Streaming transfer must merge quarantine details across batches (no silent loss)."""

from __future__ import annotations


def _merge_batch_details(
    prev_details: list[dict],
    incoming_details: list[dict],
    *,
    batch_start: int,
    cap: int = 200,
) -> list[dict]:
    """Mirror stream._apply_result absolute-row merge for unit testing."""
    new_details: list[dict] = []
    for raw in incoming_details:
        detail = dict(raw)
        local_row = int(detail.get("row") or 0)
        if local_row > 0:
            detail["batch_row"] = local_row
            detail["batch_offset"] = batch_start
            detail["row"] = batch_start + local_row
        new_details.append(detail)
    return (prev_details + new_details)[:cap]


def test_quarantine_details_merge_keeps_early_batch_findings():
    batch1 = [{"row": 1, "column": "a", "reason": "bad"}, {"row": 18, "column": "b", "reason": "bad"}]
    batch2 = [{"row": 1, "column": "c", "reason": "bad"}]  # would overwrite without merge
    merged = _merge_batch_details([], batch1, batch_start=0)
    merged = _merge_batch_details(merged, batch2, batch_start=1000)
    assert len(merged) == 3
    assert merged[0]["row"] == 1
    assert merged[1]["row"] == 18
    assert merged[2]["row"] == 1001
    assert merged[2]["batch_row"] == 1
    assert merged[2]["batch_offset"] == 1000


def test_quarantine_details_merge_caps_at_200():
    huge = [{"row": i + 1, "column": "x", "reason": "bad"} for i in range(150)]
    more = [{"row": i + 1, "column": "y", "reason": "bad"} for i in range(100)]
    merged = _merge_batch_details(huge, more, batch_start=10_000)
    assert len(merged) == 200
    assert merged[-1]["row"] == 10_000 + 50  # last kept from second batch truncated
