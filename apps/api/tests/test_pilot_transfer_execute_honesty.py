"""Pilot Confirm must not outrun Studio Execute (review-grade / local / invent create-new)."""

from __future__ import annotations


def test_execute_cleared_requires_approve_decision():
    from src.ai.copilot.transfer_tools import _is_execute_cleared, _transfer_decision

    assert _transfer_decision({"passed": True}) == ""
    assert _is_execute_cleared({"passed": True}) is False
    assert _is_execute_cleared({
        "passed": True,
        "proof_bundle": {"transfer_decision": {"decision": "review"}},
    }) is False
    assert _is_execute_cleared({
        "passed": True,
        "run_id": "pf_local_abc",
        "proof_bundle": {"transfer_decision": {"decision": "approve"}},
    }) is False
    assert _is_execute_cleared({
        "passed": True,
        "run_id": "pf_api_1",
        "proof_bundle": {"transfer_decision": {"decision": "approve"}},
    }) is True


def test_dest_table_exists_tri_state_never_invents_create_new_on_fail():
    from src.ai.copilot.transfer_tools import _dest_table_exists_tri_state

    assert _dest_table_exists_tri_state({"ok": True, "columns": [{"name": "id"}]}) is True
    assert _dest_table_exists_tri_state({"ok": False, "error": "connection refused", "columns": []}) is None
    assert _dest_table_exists_tri_state({"ok": True, "columns": []}) is None
    assert _dest_table_exists_tri_state({
        "ok": False,
        "error": "relation \"foo\" does not exist",
        "columns": [],
    }) is False


def test_risky_conversions_include_cast_and_mutate():
    from src.ai.copilot.transfer_tools import _risky_conversions, _type_conversions

    mappings = [
        {"source": "a", "target": "a", "source_type": "TIMESTAMP", "target_type": "TIMESTAMP", "fidelity": "cast"},
        {"source": "b", "target": "b", "source_type": "INT", "target_type": "INT", "fidelity": "mutate", "transform": "trim"},
        {"source": "c", "target": "c", "source_type": "DECIMAL", "target_type": "INTEGER", "fidelity": "lossy_cast"},
        {"source": "d", "target": "d", "source_type": "INT", "target_type": "BIGINT", "fidelity": "preserve"},
        {"source": "e", "target": "e", "source_type": "UUID", "target_type": "CHAR(36)", "create_new_risks": [{"kind": "uuid_domain"}]},
    ]
    conversions = _type_conversions(mappings)
    risky = _risky_conversions(conversions)
    kinds = {(c["source_column"], c["fidelity"]) for c in risky}
    assert ("a", "cast") in kinds
    assert ("b", "mutate") in kinds
    assert ("c", "lossy_cast") in kinds
    assert ("e", "cast") in kinds
    assert not any(c["source_column"] == "d" for c in risky)
