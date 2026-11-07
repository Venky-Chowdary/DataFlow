"""D258: an all-rows-quarantined failure was titled a missing destination column.

The quarantine reasons quoted after the sentence name columns ("Column 'name'
…", "column 'ts' …"), and the ``column '`` needle matched first.
"""

from __future__ import annotations

from services.error_handling import humanize_transfer_failure


def test_all_rows_quarantined_is_not_a_missing_column() -> None:
    raw = (
        "All 30 row(s) were quarantined, so nothing was written to the destination — "
        "column 'ts': timezone-aware value into NTZ destination TIMESTAMP"
    )
    out = humanize_transfer_failure(ValueError(raw))

    assert out["code"] == "all_rows_quarantined", out
    assert out["category"] == "data_quality"
    assert "not on the destination" not in out["title"]
    assert "capacity" not in out["message"]
    assert "timezone-aware value into NTZ" in out["message"]
