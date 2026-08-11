"""Cursor meaning decides whether an incremental read can lose rows.

Live proof for these rules is in ``cursor_semantics_live_results.json``
(PostgreSQL + MySQL, 18/18): a `created_at` cursor under `incremental_deduped`
left the destination holding a stale row while the run reported success, and an
`order_date` cursor skipped a backdated insert permanently.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.cursor_semantics import (  # noqa: E402
    BUSINESS_DATE,
    CDC_POSITION,
    INSERT_ONLY,
    MODIFICATION_TIMESTAMP,
    MONOTONIC_SEQUENCE,
    evaluate_cursor_semantics,
)
from services.destination_key_collision_probe import (  # noqa: E402
    rows_a_cursor_read_will_deliver,
)
from services.keyset_pagination import KEYSET_SEP  # noqa: E402
from services.sync_cursor import SyncContract, max_cursor_value  # noqa: E402


def _verdict(**kwargs):
    base = {
        "sync_mode": "incremental_append",
        "cursor_field": "updated_at",
        "declared": "",
    }
    base.update(kwargs)
    return evaluate_cursor_semantics(**base)


class TestUndeclaredCursor:
    def test_update_bearing_mode_refuses_an_undeclared_cursor(self):
        """`incremental_deduped` promises changed rows land; nothing proves it can."""
        v = _verdict(sync_mode="incremental_deduped", cursor_field="created_at")
        assert v.blocks
        assert "created_at" in v.reason
        assert v.primary_action
        assert v.captures_updates is False

    def test_append_refuses_an_undeclared_cursor_in_strict(self):
        """A backdated insert behind the watermark is never read again."""
        v = _verdict(cursor_field="order_date")
        assert v.blocks
        assert "backdated" in v.reason

    def test_balanced_accepts_undeclared_append_without_claiming_completeness(self):
        v = _verdict(cursor_field="order_date", validation_mode="balanced")
        assert not v.blocks
        assert v.captures_updates is False
        assert v.monotonic_on_insert is False

    def test_full_refresh_has_no_cursor_opinion(self):
        v = _verdict(sync_mode="full_refresh_overwrite")
        assert v.status == "not_applicable"


class TestDeclaredCursor:
    def test_modification_timestamp_captures_updates(self):
        v = _verdict(sync_mode="incremental_deduped", declared=MODIFICATION_TIMESTAMP)
        assert not v.blocks
        assert v.captures_updates and v.monotonic_on_insert

    def test_insert_only_is_sound_under_a_key_resolved_mode(self):
        """A source that never updates has no updates to miss."""
        v = _verdict(sync_mode="incremental_deduped", declared=INSERT_ONLY)
        assert not v.blocks
        assert v.captures_updates is False
        assert "does not make them" in v.reason

    def test_monotonic_sequence_captures_inserts(self):
        v = _verdict(cursor_field="id", declared=MONOTONIC_SEQUENCE)
        assert not v.blocks
        assert v.monotonic_on_insert

    def test_cdc_position_captures_updates(self):
        v = _verdict(sync_mode="cdc", cursor_field="lsn", declared=CDC_POSITION)
        assert not v.blocks and v.captures_updates

    def test_business_date_is_refused_even_when_declared(self):
        """The declaration is not a waiver — the loss is still silent."""
        v = _verdict(cursor_field="order_date", declared=BUSINESS_DATE)
        assert v.blocks
        assert "calendar" in v.reason

    def test_undefined_declaration_is_refused(self):
        v = _verdict(declared="probably_fine")
        assert v.blocks
        assert "probably_fine" in v.reason


def test_contract_parses_the_declaration():
    c = SyncContract.from_dict(
        {
            "name": "orders",
            "sync_mode": "incremental_deduped",
            "cursor_field": "updated_at",
            "primary_key": "id",
            "cursor_semantics": "Modification_Timestamp",
        }
    )
    assert c.cursor_semantics == MODIFICATION_TIMESTAMP


def test_contract_without_a_declaration_is_empty_not_guessed():
    c = SyncContract.from_dict({"name": "orders", "cursor_field": "created_at"})
    assert c.cursor_semantics == ""


class TestPreWriteChecksSeeTheDelta:
    """A pre-write check must judge the batch, not the table it is drawn from."""

    ROWS = [
        {"id": 1, "updated_at": "2024-01-01T00:00:00"},
        {"id": 2, "updated_at": "2024-01-02T00:00:00"},
        {"id": 3, "updated_at": "2024-03-01T00:00:00"},
    ]

    def test_rows_at_or_before_the_watermark_are_not_in_the_batch(self):
        delta = rows_a_cursor_read_will_deliver(
            self.ROWS, cursor_column="updated_at", watermark="2024-01-02T00:00:00"
        )
        assert [r["id"] for r in delta] == [3]

    def test_composite_watermark_uses_the_tiebreak_the_reader_uses(self):
        """Peers sharing a cursor value are split on the primary key, as the seek is."""
        rows = [
            {"id": 1, "updated_at": "2024-01-02T00:00:00"},
            {"id": 2, "updated_at": "2024-01-02T00:00:00"},
            {"id": 3, "updated_at": "2024-01-02T00:00:00"},
        ]
        delta = rows_a_cursor_read_will_deliver(
            rows,
            cursor_column="updated_at",
            watermark=f"2024-01-02T00:00:00{KEYSET_SEP}2",
            tiebreak_column="id",
        )
        assert [r["id"] for r in delta] == [3]

    def test_no_watermark_leaves_the_batch_whole(self):
        delta = rows_a_cursor_read_will_deliver(
            self.ROWS, cursor_column="updated_at", watermark=None
        )
        assert len(delta) == 3

    def test_an_unreadable_cursor_value_does_not_shrink_the_batch(self):
        """Unknown stays unknown: a check must not narrow on a guess."""
        rows = [{"id": 1}, {"id": 2}]
        delta = rows_a_cursor_read_will_deliver(
            rows, cursor_column="updated_at", watermark="2024-01-02T00:00:00"
        )
        assert len(delta) == 2

    def test_the_watermark_written_is_the_watermark_the_delta_is_measured_against(self):
        """One encoding on both sides, or the second run reads the wrong window."""
        rows = [["2024-01-02T00:00:00", 1], ["2024-01-02T00:00:00", 2]]
        wm = max_cursor_value(rows, ["updated_at", "id"], "updated_at", "id")
        delta = rows_a_cursor_read_will_deliver(
            [
                {"updated_at": "2024-01-02T00:00:00", "id": 2},
                {"updated_at": "2024-01-02T00:00:00", "id": 3},
            ],
            cursor_column="updated_at",
            watermark=wm,
            tiebreak_column="id",
        )
        assert [r["id"] for r in delta] == [3]
