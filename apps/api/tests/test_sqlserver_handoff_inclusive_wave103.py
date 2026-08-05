"""SQL Server CDC: the snapshot handoff LSN must not be skipped.

``fn_cdc_get_all_changes`` takes an inclusive ``@from_lsn``, so the reader
filters out the already-acked prefix at the resume LSN. An empty seqval used to
mean "this whole LSN is consumed", which is true after a clean poll boundary but
false right after a snapshot handoff — there, ``start_lsn`` has never been
streamed. Conflating the two dropped every change committed at the handoff LSN
while the snapshot was running: silent data loss with a clean-looking resume.
"""

from __future__ import annotations

from connectors.sqlserver_cdc_native import SqlServerNativeCdc

_COLS = ["__$start_lsn", "__$seqval", "__$operation", "id"]


def _rows() -> list[tuple]:
    lsn_a = bytes.fromhex("0a")
    lsn_b = bytes.fromhex("0b")
    return [
        (lsn_a, bytes.fromhex("01"), 2, 1),
        (lsn_a, bytes.fromhex("02"), 2, 2),
        (lsn_b, bytes.fromhex("01"), 2, 3),
    ]


def test_handoff_keeps_every_row_at_the_unread_start_lsn() -> None:
    kept = SqlServerNativeCdc._after_cursor(
        _rows(), _COLS, from_lsn_hex="0a", from_seqval_hex="", inclusive=True
    )
    assert [r[3] for r in kept] == [1, 2, 3], "handoff LSN rows must survive"


def test_consumed_lsn_without_seqval_is_still_skipped() -> None:
    """A clean poll boundary must not re-emit the LSN it already finished."""
    kept = SqlServerNativeCdc._after_cursor(
        _rows(), _COLS, from_lsn_hex="0a", from_seqval_hex="", inclusive=False
    )
    assert [r[3] for r in kept] == [3]


def test_mid_lsn_seqval_cursor_ignores_the_inclusive_flag() -> None:
    """An explicit seqval is authoritative: only the acked prefix is dropped."""
    for inclusive in (True, False):
        kept = SqlServerNativeCdc._after_cursor(
            _rows(), _COLS, from_lsn_hex="0a", from_seqval_hex="01", inclusive=inclusive
        )
        assert [r[3] for r in kept] == [2, 3], inclusive


def test_resume_token_with_seqval_is_not_inclusive() -> None:
    from connectors.sqlserver_cdc_native import encode_mssql_cdc_token

    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        schema="dbo",
        resume_token=encode_mssql_cdc_token(
            "0a", table="orders", phase="streaming", capture_instance="", seqval="01"
        ),
    )
    assert cdc.start_seqval == "01"
    assert cdc._resume_inclusive is False


def test_legacy_token_without_seqval_resolves_to_at_least_once() -> None:
    """Ambiguous legacy tokens re-read rather than skip: duplicates over loss."""
    from connectors.sqlserver_cdc_native import encode_mssql_cdc_token

    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        schema="dbo",
        resume_token=encode_mssql_cdc_token(
            "0a", table="orders", phase="streaming", capture_instance=""
        ),
    )
    assert cdc.start_seqval == ""
    assert cdc._resume_inclusive is True


def test_ack_with_seqval_clears_inclusive_resume() -> None:
    from connectors.sqlserver_cdc_native import encode_mssql_cdc_token

    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        schema="dbo",
    )
    cdc._resume_inclusive = True
    cdc.ack(
        encode_mssql_cdc_token(
            "0b", table="orders", phase="streaming", capture_instance="", seqval="02"
        )
    )
    assert cdc.start_lsn == "0b"
    assert cdc.start_seqval == "02"
    assert cdc._resume_inclusive is False
