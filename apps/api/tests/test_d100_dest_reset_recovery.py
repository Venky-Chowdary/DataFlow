"""D100 — destination-reset refusal: honest "missing" wording, MCP recovery
actions, and first-fire schedule parking.

Repro chain (prod c6c16128):
1. DROP the destination table of a scheduled incremental → job fails closed
   saying "the destination is empty" — it is *missing*, not empty, and the
   only named recovery is a raw REST call.
2. The same refusal is not in the deterministic-failure table, so a */5
   schedule re-fails for beats before parking instead of flagging on the
   first fire.
3. No MCP/Pilot tool exposes "clear cursor / rebaseline" — operators must
   hand-craft REST calls out of the error text.
"""

from __future__ import annotations

import sqlite3

import pytest

from services.sync_cursor import IncrementalReadScope

#: The refusal text a destination-reset produces (kept verbatim so tests below
#: classify the real message, not a paraphrase).
RESET_MESSAGE = (
    "Destination reset since the last incremental run: the stored watermark "
    "'2024-01-01T00:00:00' on 'updated_at' claims those rows are already at "
    "rest, but the destination table is missing — resuming would skip the "
    "whole history and report success. Clear the cursor (MCP tool "
    "clear_cursor_resnapshot with cursor_key 't:public.orders:updated_at', "
    "or POST /api/v1/ops/cdc-cursors/clear) to re-snapshot, or rebaseline the "
    "route (MCP tool rebaseline_transfer, or POST "
    "/api/v1/transfer/continuity/rebaseline), or point the run at the "
    "destination that holds the history."
)


def _scope() -> IncrementalReadScope:
    return IncrementalReadScope(
        cursor_column="updated_at",
        watermark="2024-01-01T00:00:00",
        cursor_key="t:public.orders:updated_at",
    )


def test_dest_missing_message_names_missing_not_empty_and_recovery_actions():
    from services.preflight_cursor_gate import cursor_destination_reset_issue

    msg = cursor_destination_reset_issue(_scope(), 0, dest_missing=True)
    assert "destination table is missing" in msg
    assert "destination is empty" not in msg
    # Both recovery actions named (MCP tools + REST endpoints).
    assert "cdc-cursors/clear" in msg
    assert "continuity/rebaseline" in msg
    assert "clear_cursor_resnapshot" in msg
    assert "rebaseline_transfer" in msg
    assert "t:public.orders:updated_at" in msg
    assert msg == RESET_MESSAGE


def test_dest_present_but_empty_keeps_empty_wording():
    from services.preflight_cursor_gate import cursor_destination_reset_issue

    msg = cursor_destination_reset_issue(_scope(), 0, dest_missing=False)
    assert "destination is empty" in msg
    assert "missing" not in msg
    # Unknown (None) also degrades honestly to the old wording.
    msg_unknown = cursor_destination_reset_issue(_scope(), 0, dest_missing=None)
    assert "destination is empty" in msg_unknown


def test_dest_object_missing_probe_true_false_never_guess(tmp_path):
    from services.preflight_cursor_gate import dest_object_missing

    db = tmp_path / "dest.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()
    cfg = {"type": "sqlite", "database": str(db)}
    assert dest_object_missing("sqlite", cfg, "orders") is False
    assert dest_object_missing("sqlite", cfg, "ghost") is True
    # Unresolvable destination → None, never a false "missing" verdict.
    assert dest_object_missing("postgresql", {}, "orders") is None


def test_refuse_unusable_cursor_state_reports_missing_table(monkeypatch, tmp_path):
    import services.preflight_cursor_gate as gate

    db = tmp_path / "dest.sqlite"
    sqlite3.connect(db).close()
    cfg = {"type": "sqlite", "database": str(db)}
    monkeypatch.setattr(gate, "countable_dest_rows", lambda *a, **k: 0)
    with pytest.raises(ValueError, match="destination table is missing"):
        gate.refuse_unusable_cursor_state(_scope(), "sqlite", cfg, "ghost")


def test_dest_reset_classifies_deterministic_first_fire():
    """A dropped-table refusal can never self-heal — park on the first beat,
    not after _CONSECUTIVE_FAILURE_PARK (3) identical failures."""
    from services.failure_retry_policy import DETERMINISTIC, classify_failure

    verdict = classify_failure(error=RESET_MESSAGE, phase="read", rows_written=0)
    assert verdict.kind == DETERMINISTIC
    assert "cdc-cursors/clear" in verdict.corrective_action
    assert "clear_cursor_resnapshot" in verdict.corrective_action
    assert "rebaseline_transfer" in verdict.corrective_action


def test_schedule_parks_on_first_dest_reset_failure():
    """_park_reason must fire on the very first deterministic refusal."""
    from services.schedule_runner import _park_reason
    from services.failure_retry_policy import DETERMINISTIC, classify_failure

    class _Sched:
        run_history = []

    verdict = classify_failure(error=RESET_MESSAGE, phase="read", rows_written=0)
    assert verdict.kind == DETERMINISTIC
    decision = {"failure_class": {"kind": verdict.kind}, "retry": True}
    assert _park_reason(_Sched(), decision, {"error": RESET_MESSAGE}) == (
        "deterministic_refusal"
    )


def test_mcp_recovery_tools_staged():
    from src.ai.copilot.lifecycle_tools import (
        LIFECYCLE_TOOL_DEFINITIONS,
        stage_clear_cursor_resnapshot,
        stage_rebaseline_transfer,
    )
    from src.ai.copilot.tool_permissions import (
        ACK_KIND_PERMISSIONS,
        TOOL_PERMISSIONS,
    )

    names = {t["name"] for t in LIFECYCLE_TOOL_DEFINITIONS}
    assert "clear_cursor_resnapshot" in names
    assert "rebaseline_transfer" in names
    for tool in ("clear_cursor_resnapshot", "rebaseline_transfer"):
        assert tool in TOOL_PERMISSIONS
        assert tool in ACK_KIND_PERMISSIONS

    out = stage_clear_cursor_resnapshot("t:public.orders:updated_at", "dropped dest")
    assert out.success is True
    assert out.output["requires_confirm"] is True
    assert out.output["preview"]["cursor_key"] == "t:public.orders:updated_at"
    # Nothing executes until Confirm — the ledger only staged it.
    assert "ack_id" in out.output

    # No key → clarify, never guess a cursor.
    assert stage_clear_cursor_resnapshot("", "").success is False
    # Rebaseline requires a reason and a destination connector.
    assert stage_rebaseline_transfer(name="dst", reason="").success is False
    assert stage_rebaseline_transfer(reason="accepted").success is False
    staged = stage_rebaseline_transfer(
        connector_id="conn-1", table="orders", reason="dest rebuilt"
    )
    assert staged.success is True
    assert staged.output["requires_confirm"] is True
    assert staged.output["reason"] == "dest rebuilt"
