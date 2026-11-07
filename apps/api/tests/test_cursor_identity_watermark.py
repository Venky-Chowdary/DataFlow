"""A watermark belongs to the column it was measured on.

Repointing a route's cursor (``id`` → ``updated_at``) used to inherit the old
column's value: Postgres aborted the read with ``invalid input syntax for type
timestamp: "250"``, and on comparable types it would have silently skipped every
row between the two orderings. Both Validate and the read side must refuse.
"""

from __future__ import annotations

import services.sync_cursor as sync_cursor
from services.preflight_cursor_gate import (
    build_sync_contract_gate,
    cursor_destination_reset_issue,
    cursor_identity_issue,
)
from services.sync_cursor import IncrementalReadScope, resolve_incremental_read_scope


def _contracts(cursor: str) -> list[dict[str, object]]:
    return [{
        "name": "sp_src",
        "selected": True,
        "sync_mode": "incremental_append",
        "cursor_field": cursor,
        "primary_key": "id",
        "cursor_semantics": "monotonic_sequence",
    }]


def _scope(cursor: str) -> IncrementalReadScope:
    return resolve_incremental_read_scope(
        sync_mode="incremental_append",
        stream_contracts=_contracts(cursor),
        source_type="postgresql",
        source_database="dataflow",
        source_object="sp_src",
        dest_type="mysql",
        dest_database="dataflow",
        dest_object="sp_dst",
    )


def test_watermark_records_the_column_it_was_measured_on(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)

    scope = _scope("id")
    sync_cursor.set_watermark(scope.cursor_key, "250", metadata={"cursor_column": "id"})

    value, meta = sync_cursor.get_watermark_record(scope.cursor_key)
    assert value == "250"
    assert meta["cursor_column"] == "id"

    same = _scope("id")
    assert same.watermark == "250"
    assert same.cursor_column_changed is False


def test_repointed_cursor_does_not_inherit_the_previous_column_value(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)

    id_scope = _scope("id")
    sync_cursor.set_watermark(id_scope.cursor_key, "250", metadata={"cursor_column": "id"})

    moved = _scope("updated_at")
    assert moved.watermark_cursor_column == "id"
    assert moved.cursor_column_changed is True

    issue = cursor_identity_issue(moved)
    assert "updated_at" in issue and "id" in issue
    assert moved.cursor_key in issue  # the reset control names the exact key


def test_validate_blocks_a_repointed_cursor_with_a_reset_action(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)

    id_scope = _scope("id")
    sync_cursor.set_watermark(id_scope.cursor_key, "250", metadata={"cursor_column": "id"})
    moved = _scope("updated_at")

    gate = build_sync_contract_gate(
        _contracts("updated_at"),
        sync="incremental_append",
        validation="strict",
        dest="mysql",
        src="postgresql",
        kind="database",
        source_columns=["id", "updated_at"],
        pass_status="pass",
        block_status="block",
        read_scope=moved,
    )
    assert gate["status"] == "block"
    issues = gate["details"]["issues"]
    assert any("cdc-cursors/clear" in i for i in issues)


def test_unchanged_cursor_still_passes(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)

    scope = _scope("id")
    sync_cursor.set_watermark(scope.cursor_key, "250", metadata={"cursor_column": "id"})

    gate = build_sync_contract_gate(
        _contracts("id"),
        sync="incremental_append",
        validation="strict",
        dest="mysql",
        src="postgresql",
        kind="database",
        source_columns=["id", "updated_at"],
        pass_status="pass",
        block_status="block",
        read_scope=_scope("id"),
    )
    assert gate["status"] == "pass"


def test_emptied_destination_refuses_to_resume_from_the_old_watermark(
    tmp_path, monkeypatch
) -> None:
    """A dropped/truncated destination voids the watermark's claim."""
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)

    scope = _scope("id")
    sync_cursor.set_watermark(scope.cursor_key, "250", metadata={"cursor_column": "id"})
    resumed = _scope("id")

    issue = cursor_destination_reset_issue(resumed, 0)
    assert "cdc-cursors/clear" in issue and resumed.cursor_key in issue
    # A destination that still holds the history resumes normally.
    assert cursor_destination_reset_issue(resumed, 250) == ""
    # Unknown is not evidence of a reset.
    assert cursor_destination_reset_issue(resumed, None) == ""


def test_first_run_without_a_watermark_is_never_reset_blocked(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)

    assert cursor_destination_reset_issue(_scope("id"), 0) == ""


def test_legacy_watermark_without_provenance_is_not_treated_as_a_conflict(
    tmp_path, monkeypatch
) -> None:
    """Watermarks written before provenance existed must keep bounding their route."""
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)

    scope = _scope("id")
    sync_cursor.set_watermark(scope.cursor_key, "250")

    reread = _scope("id")
    assert reread.watermark == "250"
    assert reread.cursor_column_changed is False
    assert cursor_identity_issue(reread) == ""
