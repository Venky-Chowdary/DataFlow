"""CDC initial load uses the log position, not a table cursor column."""

from __future__ import annotations

from services.preflight_cursor_gate import build_sync_contract_gate


def _gate(contracts, **overrides):
    kwargs = dict(
        sync="cdc",
        validation="strict",
        dest="postgresql",
        src="mysql",
        kind="database",
        source_columns=["id", "amount"],
        pass_status="pass",
        block_status="block",
    )
    kwargs.update(overrides)
    return build_sync_contract_gate(contracts, **kwargs)


def test_cdc_with_primary_key_and_no_table_cursor_is_complete():
    gate = _gate([{"name": "orders", "primary_key": ["id"], "sync_mode": "cdc"}])
    assert gate["status"] == "pass", gate.get("message")


def test_cdc_without_primary_key_still_blocks():
    gate = _gate([{"name": "orders", "sync_mode": "cdc"}])
    assert gate["status"] == "block"
    assert "primary key" in gate["message"].lower()


def test_incremental_append_without_cursor_still_blocks():
    gate = _gate(
        [{"name": "orders", "primary_key": ["id"], "sync_mode": "incremental_append"}],
        sync="incremental_append",
    )
    assert gate["status"] == "block"
    assert "cursor" in gate["message"].lower()


def test_object_store_cdc_stays_refused():
    gate = _gate(
        [{"name": "orders", "primary_key": ["id"], "sync_mode": "cdc"}],
        dest="s3",
    )
    assert gate["status"] == "block"
    assert "object store" in gate["message"].lower() or "cdc" in gate["message"].lower()
