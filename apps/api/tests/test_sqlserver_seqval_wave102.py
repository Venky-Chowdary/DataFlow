"""Wave 102: SQL Server CDC mid-LSN seqval pagination (livelock fix)."""

from __future__ import annotations

from connectors.sqlserver_cdc_native import SqlServerNativeCdc


def _row(lsn: bytes, seq: bytes, op: int = 2, pk: str = "1") -> tuple:
    # Columns mirror fn_cdc_get_all_changes: start_lsn, seqval, operation, id
    return (lsn, seq, op, pk)


COLS = ["__$start_lsn", "__$seqval", "__$operation", "id"]


def test_after_cursor_skips_already_acked_prefix():
    lsn = bytes.fromhex("00000000000000000001")
    rows = [
        _row(lsn, bytes.fromhex("01"), pk="1"),
        _row(lsn, bytes.fromhex("02"), pk="2"),
        _row(lsn, bytes.fromhex("03"), pk="3"),
        _row(bytes.fromhex("00000000000000000002"), bytes.fromhex("01"), pk="4"),
    ]
    kept = SqlServerNativeCdc._after_cursor(
        rows,
        COLS,
        from_lsn_hex=lsn.hex(),
        from_seqval_hex=bytes.fromhex("02").hex(),
    )
    assert [r[3] for r in kept] == ["3", "4"]


def test_truncate_allows_mid_lsn_split_and_advances_seqval():
    lsn = bytes.fromhex("00000000000000000001")
    rows = [
        _row(lsn, bytes.fromhex(f"{i:02x}"), pk=str(i)) for i in range(1, 8)
    ]
    keep, next_lsn, next_seq = SqlServerNativeCdc._truncate_at_lsn_boundary(
        rows, COLS, batch_size=3
    )
    assert len(keep) == 3
    assert next_lsn == lsn.hex()
    assert next_seq == bytes.fromhex("03").hex()


def test_mid_lsn_resume_returns_remainder_not_prefix():
    """Poll #2 after a truncated LSN must return the unread tail, not poll #1."""
    lsn = bytes.fromhex("00000000000000000001")
    full = [
        _row(lsn, bytes.fromhex(f"{i:02x}"), pk=str(i)) for i in range(1, 8)
    ]
    # Poll 1: take first 3.
    keep1, next_lsn, next_seq = SqlServerNativeCdc._truncate_at_lsn_boundary(
        full, COLS, batch_size=3
    )
    assert [r[3] for r in keep1] == ["1", "2", "3"]
    # Poll 2: TVF returns the inclusive window again; filter + truncate.
    after = SqlServerNativeCdc._after_cursor(
        full, COLS, from_lsn_hex=next_lsn, from_seqval_hex=next_seq
    )
    keep2, _, _ = SqlServerNativeCdc._truncate_at_lsn_boundary(
        after, COLS, batch_size=3
    )
    assert [r[3] for r in keep2] == ["4", "5", "6"]
    # And the prior "must not split" path would have re-emitted 1..3 forever.
    assert [r[3] for r in keep2] != [r[3] for r in keep1]


def test_after_cursor_without_seqval_skips_entire_consumed_lsn():
    lsn = bytes.fromhex("00000000000000000001")
    rows = [
        _row(lsn, bytes.fromhex("01"), pk="1"),
        _row(bytes.fromhex("00000000000000000002"), bytes.fromhex("01"), pk="2"),
    ]
    kept = SqlServerNativeCdc._after_cursor(
        rows, COLS, from_lsn_hex=lsn.hex(), from_seqval_hex=""
    )
    assert [r[3] for r in kept] == ["2"]
