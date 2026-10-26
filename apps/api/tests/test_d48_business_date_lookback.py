"""D48 — business_date cursor: supported via a declared re-read lookback.

Repro (pf_75c52906cdef): `business_date` was advertised as a declarable
cursor semantic but refused in every incremental mode — there was no way to
run a date cursor at all. Now `cursor_lookback_days` on the stream contract
turns the refusal into a supported mode: the bounded read seeds at
watermark - N days and re-reads collapse on the dedupe key.
"""

from __future__ import annotations

from services.sync_cursor import IncrementalReadScope


def _verdict(mode: str, lookback: int = 0):
    from services.cursor_semantics import evaluate_cursor_semantics

    return evaluate_cursor_semantics(
        sync_mode=mode,
        cursor_field="order_date",
        declared="business_date",
        validation_mode="strict",
        lookback_days=lookback,
    )


def test_business_date_without_lookback_still_refused():
    for mode in ("incremental_upsert", "incremental_append", "incremental_deduped"):
        v = _verdict(mode)
        assert v.status == "block", mode
        assert "never read" in v.reason
        # The blocked refusal now names the supported path.
        assert any("lookback" in alt for alt in v.alternatives)


def test_business_date_with_lookback_supported_all_modes():
    for mode in ("incremental_upsert", "incremental_append", "incremental_deduped"):
        v = _verdict(mode, lookback=3)
        assert v.status == "ok", (mode, v.reason)
        assert "3" in v.reason
        # Honest residual disclosure — never claim full update capture.
        assert "outside the window" in v.reason
        assert v.captures_updates is False


def test_contract_carries_lookback():
    from services.sync_cursor import SyncContract

    c = SyncContract.from_dict(
        {
            "name": "s",
            "cursor_field": "order_date",
            "cursor_semantics": "business_date",
            "cursor_lookback_days": "7",
        }
    )
    assert c.cursor_lookback_days == 7
    # Alias accepted; garbage/zero degrades to no-lookback (still refused).
    assert (
        SyncContract.from_dict({"lookback_days": 2}).cursor_lookback_days == 2
    )
    assert (
        SyncContract.from_dict({"cursor_lookback_days": "abc"}).cursor_lookback_days
        == 0
    )


def test_read_watermark_shifts_for_date_and_datetime():
    s = IncrementalReadScope(
        cursor_column="order_date",
        watermark="2024-01-15",
        lookback_days=3,
    )
    assert s.read_watermark == "2024-01-12"
    s2 = IncrementalReadScope(
        cursor_column="order_date",
        watermark="2024-01-15T10:30:00",
        lookback_days=2,
    )
    assert s2.read_watermark == "2024-01-13T10:30:00"
    # No lookback declared → verbatim (all existing cursors unchanged).
    s3 = IncrementalReadScope(cursor_column="id", watermark="100")
    assert s3.read_watermark == "100"


def test_read_watermark_refuses_unparseable():
    import pytest

    s = IncrementalReadScope(
        cursor_column="order_date", watermark="not-a-date", lookback_days=3
    )
    with pytest.raises(ValueError, match="lookback"):
        _ = s.read_watermark


def test_records_after_watermark_uses_lookback_seed():
    from services.sync_cursor import records_after_watermark

    scope = IncrementalReadScope(
        cursor_column="order_date",
        primary_key="id",
        watermark="2024-01-15",
        lookback_days=3,
    )
    delta, unbounded = records_after_watermark(
        [
            {"id": 1, "order_date": "2024-01-10"},  # outside window — landed
            {"id": 2, "order_date": "2024-01-13"},  # inside window — re-read
            {"id": 3, "order_date": "2024-01-16"},  # past watermark — new
        ],
        scope.cursor_column,
        scope.read_watermark,
        primary_key=scope.primary_key,
    )
    assert unbounded == 0
    assert [r["id"] for r in delta] == [2, 3]
