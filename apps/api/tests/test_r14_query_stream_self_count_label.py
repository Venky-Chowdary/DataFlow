"""A callable/query stream's ledger must say its read side is self-counted.

When a multi-stream job runs a stream's own CALL/SELECT, stream_multi passes
``count_source=False``: the read population is the reader's own count of the
result set, not an independent source COUNT(*). The ledger used to label that
``gate8_source_count`` — the same word as a real independent count — so a
balanced ledger read as proof when the read side had checked itself.
"""

from __future__ import annotations

from services.row_conservation import (
    READ_SELF_COUNTED,
    account_job_streams,
    record_stream_health,
)


class _Endpoint:
    """Never counted: count_source=False must not touch the source."""


def _record(health, name, *, count_source, source_rows):
    summary = {
        "source_row_count": source_rows,
        "source_row_count_source": "reader",
        "reconciliation": {"target_rows": source_rows, "phase": "post_write_row_count",
                           "coverage": "row_count"},
    }
    return record_stream_health(
        health,
        name=name,
        status="completed",
        records_processed=source_rows,
        summary=summary,
        sync_mode="full_refresh_overwrite",
        count_source=count_source,
        source=None if count_source else _Endpoint(),
    )


def test_callable_stream_ledger_is_labelled_self_counted():
    health: list = []
    entry = _record(health, "proc_stream", count_source=False, source_rows=3)
    ledger = entry["row_accounting"]
    assert ledger["rows_read"] == 3
    assert ledger["rows_read_source"] == READ_SELF_COUNTED
    assert "own count" in ledger["note"]


def test_table_stream_ledger_keeps_independent_label():
    health: list = []
    entry = record_stream_health(
        health,
        name="tbl",
        status="completed",
        records_processed=2,
        summary={"reconciliation": {"source_rows": 2, "target_rows": 2,
                                    "phase": "post_write_row_count",
                                    "coverage": "row_count"}},
        sync_mode="full_refresh_overwrite",
    )
    assert entry["row_accounting"]["rows_read_source"] != READ_SELF_COUNTED


def test_job_rollup_names_self_counted_streams():
    health: list = []
    _record(health, "proc_stream", count_source=False, source_rows=3)
    record_stream_health(
        health,
        name="tbl",
        status="completed",
        records_processed=2,
        summary={"reconciliation": {"source_rows": 2, "target_rows": 2,
                                    "phase": "post_write_row_count",
                                    "coverage": "row_count"}},
        sync_mode="full_refresh_overwrite",
    )
    rolled = account_job_streams(health).to_dict()
    per = {item["stream"]: item for item in rolled["per_stream"]}
    assert per["proc_stream"]["rows_read_source"] == READ_SELF_COUNTED
    assert per["tbl"]["rows_read_source"] != READ_SELF_COUNTED
