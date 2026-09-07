"""CDC must measure its own source population, and its poll must terminate.

Two defects proven here, both observed on a live PostgreSQL→MySQL CDC run:

1. Run failed with "Source row count unmeasured — Gate-8 refuses conservation
   invented from writer acknowledgements alone" *after* correctly writing every
   row: Validate cleared, the destination held the right data, and the run was
   still marked failed because nothing carried a reader-side population into
   reconcile.
2. A cursor poll re-read the same page forever (the seek answers
   ``cursor > bookmark`` and the bookmark never moved), so acknowledgement and
   quarantine counters climbed far past the source population.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.transfer.cdc_transfer import CdcEngine, CdcState, _merge_cdc_dest_summary
from src.transfer.stream_row_accounting import stamp_source_row_count


def _batch(headers: list[str], rows: list[list[str]]) -> SimpleNamespace:
    return SimpleNamespace(headers=headers, rows=rows)


def _engine(**kw):
    base = dict(
        src_cfg={"database": "test"},
        src_type="generic_sql",
        table_name="src",
        cursor_field="updated_at",
        primary_key="id",
        watermark="2024-01-01 00:00:00",
        columns=["id", "updated_at"],
        batch_size=2,
    )
    base.update(kw)
    return CdcEngine(**base)


def test_cdc_poll_advances_bookmark_and_terminates():
    headers = ["id", "updated_at"]
    page1 = [["1", "2024-01-02 00:00:00"], ["2", "2024-01-03 00:00:00"]]
    page2 = [["3", "2024-01-04 00:00:00"]]
    with patch("src.transfer.cdc_transfer._read_batch") as mock_read:
        mock_read.side_effect = [
            (_batch(headers, page1), None),
            (_batch(headers, page2), None),
        ]
        batches = list(_engine().poll())

    assert [r["id"] for b in batches for r in b.inserts] == ["1", "2", "3"]
    # The second read seeks past the first page's maximum, not the stale
    # watermark — re-reading page 1 is what never terminated.
    second_call = mock_read.call_args_list[1].kwargs
    assert second_call["cursor_after"].startswith("2024-01-03 00:00:00")


def test_cdc_poll_refuses_a_cursor_that_cannot_advance():
    """A whole page sharing one cursor value with no tie-break is a spin."""
    headers = ["id", "updated_at"]
    page = [["1", "2024-01-02 00:00:00"], ["2", "2024-01-02 00:00:00"]]
    with patch("src.transfer.cdc_transfer._read_batch") as mock_read:
        mock_read.return_value = (_batch(headers, page), None)
        engine = _engine(primary_key="updated_at")  # cursor is its own key
        with pytest.raises(RuntimeError, match="did not advance"):
            list(engine.poll())
    # Failed closed on the second read at the latest — never an unbounded loop.
    assert len(mock_read.call_args_list) <= 2


def test_cdc_poll_uses_tiebreak_when_cursor_repeats():
    headers = ["id", "updated_at"]
    page1 = [["1", "2024-01-02 00:00:00"], ["2", "2024-01-02 00:00:00"]]
    page2 = [["3", "2024-01-02 00:00:00"]]
    with patch("src.transfer.cdc_transfer._read_batch") as mock_read:
        mock_read.side_effect = [
            (_batch(headers, page1), None),
            (_batch(headers, page2), None),
        ]
        batches = list(_engine().poll())

    assert [r["id"] for b in batches for r in b.inserts] == ["1", "2", "3"]
    second = mock_read.call_args_list[1].kwargs
    # Composite bookmark (cursor + pk) so rows sharing a timestamp are neither
    # skipped nor re-read.
    assert second["cursor_primary_key"] == "id"
    assert "\x1f" in second["cursor_after"]


def test_merge_dest_summary_drops_writer_per_batch_source_row_count():
    """The writer's per-batch count is not the run's population."""
    state = CdcState()
    _merge_cdc_dest_summary(
        state, {"rows_written": 500, "source_row_count": 500}, job_id="j1"
    )
    merged = _merge_cdc_dest_summary(
        state, {"rows_written": 7, "source_row_count": 7}, job_id="j1"
    )
    assert "source_row_count" not in merged
    assert "source_row_count_source" not in merged


def test_cdc_reader_population_is_stamped_for_gate8():
    state = CdcState()
    state.source_changes_read = 7
    state.rows_written = 7
    summary: dict[str, object] = {}
    stamp_source_row_count(
        summary,
        reader_count=state.source_changes_read,
        rows_written=state.rows_written,
        source="cdc_reader_changes",
    )
    assert summary["source_row_count"] == 7
    assert summary["source_row_count_source"] == "cdc_reader_changes"


def test_quiet_cdc_poll_is_a_measured_zero_not_unmeasured():
    summary: dict[str, object] = {}
    stamp_source_row_count(
        summary, reader_count=0, rows_written=0, source="cdc_reader_changes"
    )
    assert summary["source_row_count"] == 0
    assert summary["source_row_count_source"] == "cdc_reader_changes_empty"


def test_reader_zero_with_rows_written_stays_unmeasured():
    """Never let the writer's ack close conservation on its own."""
    summary: dict[str, object] = {}
    stamp_source_row_count(
        summary, reader_count=0, rows_written=9, source="cdc_reader_changes"
    )
    assert "source_row_count" not in summary
    assert summary["source_row_count_source"] == "unmeasured"
