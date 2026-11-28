"""A Pilot/MCP plan's preflight blocker carries the options the gate offers.

The '' into a NOT NULL Oracle text column gate (g3o) blocks with one signed
option ("Create note as NULL-able", approved through ``risk_acceptance``).
The chat projection of the blocker kept only the message and the fix text,
so the option never reached the operator and the approve step had nothing to
name (oracle-live: test_oracle_story_live_retests
test_oracle_empty_strings_into_a_new_not_null_column_block_then_approve,
..._into_an_existing_not_null_column_stay_blocked: KeyError 'suggested_actions').
"""

from __future__ import annotations

from typing import Any

import services.preflight_service as preflight_service
from src.ai.copilot import transfer_tools

_OPTION = {"id": "create_nullable_empty_as_null", "label": "Create note as NULL-able"}


def _plan(monkeypatch, blocker: dict[str, Any]) -> dict[str, Any]:
    def fake_preflight(**_kwargs: Any) -> dict[str, Any]:
        return {"run_id": "pf_test", "passed": False, "gates": [], "blockers": [blocker], "warnings": []}

    monkeypatch.setattr(preflight_service, "run_file_preflight", fake_preflight)
    return transfer_tools._run_preflight(
        src_conn={"id": "s", "type": "postgresql"}, dst_conn={"id": "d", "type": "oracle"},
        src_table="t", dst_table="T2", src_rows=[{"name": "note", "inferred_type": "TEXT"}],
        sample_rows=[{"note": ""}], mappings=[{"source": "note", "target": "note"}],
        mode="full_refresh_overwrite", schema_policy="manual_review", validation_mode="strict",
        src_db_type="postgresql", source_config={}, dest_db_type="oracle", dest_exists=False,
        known_row_count=3,
    )


def test_the_gates_options_reach_the_plan(monkeypatch) -> None:
    out = _plan(monkeypatch, {"id": "g3o_oracle_empty_not_null", "message": "note: 2 empty string(s)",
                              "details": {"suggested_actions": [_OPTION]}, "suggested_actions": [_OPTION]})
    (blocker,) = out["blockers"]
    assert blocker["suggested_actions"] == [_OPTION]


def test_options_only_in_the_details_reach_the_plan_too(monkeypatch) -> None:
    out = _plan(monkeypatch, {"id": "g3o_oracle_empty_not_null", "message": "m",
                              "details": {"suggested_actions": [_OPTION]}})
    assert out["blockers"][0]["suggested_actions"] == [_OPTION]


def test_a_blocker_with_no_option_says_so(monkeypatch) -> None:
    out = _plan(monkeypatch, {"id": "g3o_oracle_empty_not_null", "message": "existing NOT NULL column",
                              "details": {}, "suggested_actions": []})
    assert out["blockers"][0]["suggested_actions"] == []
