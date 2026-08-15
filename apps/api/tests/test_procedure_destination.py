"""Destination stored-procedure plan + row-apply — named fixture, not marketing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.procedure_destination import (
    MODE_HOOKS,
    MODE_ROW_APPLY,
    ProcedureDestinationError,
    REASON_CALL_FAILED,
    REASON_DEST_CDC,
    REASON_UNBOUND,
    apply_rows_via_procedure,
    assert_dest_procedure_sync_allowed,
    binds_for_row,
    dest_write_mode_of,
    plan_dest_procedure,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "procedure_destination_matrix.json"


def _load() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_named_dest_procedure_parse_matrix() -> None:
    data = _load()
    ok = 0
    for case in data["plan_accept"]:
        dest = {
            "type": case["dialect"],
            "extra": case["extra"],
        }
        plan = plan_dest_procedure(dest)
        assert plan is not None, case["id"]
        assert plan.mode == case["expect_mode"]
        if case["expect_mode"] == MODE_ROW_APPLY:
            assert plan.row_spec is not None
            assert plan.row_spec.identifier
        ok += 1
    assert ok == len(data["plan_accept"])


def test_named_dest_procedure_reject_matrix() -> None:
    data = _load()
    refused = 0
    for case in data["plan_reject"]:
        dest = {"type": case["dialect"], "extra": case["extra"]}
        with pytest.raises(ProcedureDestinationError):
            plan_dest_procedure(dest)
        refused += 1
    assert refused == len(data["plan_reject"])


def test_row_apply_quarantines_unbound_and_failed_calls() -> None:
    dest = {
        "type": "postgresql",
        "extra": {
            "dest_write_mode": "procedure",
            "dest_procedure_call": "CALL upsert_order(:id, :amt)",
            "dest_procedure_param_map": {"id": "order_id", "amt": "order_amt"},
        },
    }
    plan = plan_dest_procedure(dest)
    assert dest_write_mode_of(dest) == MODE_ROW_APPLY
    calls: list[tuple[str, dict]] = []

    def execute(sql: str, binds: dict) -> None:
        if binds.get("id") == "bad":
            raise RuntimeError("procedure raised")
        calls.append((sql, dict(binds)))

    written, ddl, summary = apply_rows_via_procedure(
        dest,
        [
            {"order_id": "1", "order_amt": 10},
            {"order_id": "2"},  # missing amt
            {"order_id": "bad", "order_amt": 3},
        ],
        execute_call=execute,
        plan=plan,
    )
    assert written == 1
    assert summary["sql_error"] is True
    assert summary["sql_error_count"] == 2
    assert summary["quarantine_count"] == 2
    reasons = {q["reason"] for q in summary["quarantine"]}
    assert REASON_UNBOUND in reasons
    assert REASON_CALL_FAILED in reasons
    assert calls[0][1]["id"] == "1"
    assert "Dest procedure row-apply" in ddl[0]
    assert summary["exactly_once_claimed_platform"] is False


def test_binds_for_row_case_insensitive_never_invents() -> None:
    dest = {
        "type": "mysql",
        "extra": {
            "dest_write_mode": "procedure",
            "dest_procedure_call": "CALL put_row(:id)",
            "dest_procedure_param_map": {"id": "OrderId"},
        },
    }
    plan = plan_dest_procedure(dest)
    assert plan is not None and plan.row_spec is not None
    binds, missing = binds_for_row(
        {"orderid": 9}, param_map=plan.param_map, spec=plan.row_spec
    )
    assert binds["id"] == 9
    assert missing == []
    _binds2, missing2 = binds_for_row(
        {"other": 1}, param_map=plan.param_map, spec=plan.row_spec
    )
    assert "id" in missing2


def test_hooks_plan_without_row_apply() -> None:
    dest = {
        "type": "sqlserver",
        "extra": {
            "dest_procedure_before": "EXEC dbo.DisableIndexes",
            "dest_procedure_after": "EXEC dbo.RebuildIndexes",
        },
    }
    plan = plan_dest_procedure(dest)
    assert plan is not None
    assert plan.mode == MODE_HOOKS
    assert plan.before_spec is not None
    assert plan.after_spec is not None
    assert dest_write_mode_of({"type": "postgresql", "extra": {}}) == "table"


def test_row_apply_refuses_cdc() -> None:
    dest = {
        "type": "postgresql",
        "extra": {
            "dest_write_mode": "procedure",
            "dest_procedure_call": "CALL upsert_order(:id)",
            "dest_procedure_param_map": {"id": "id"},
        },
    }
    with pytest.raises(ProcedureDestinationError) as exc:
        assert_dest_procedure_sync_allowed("cdc", dest)
    assert exc.value.reason == REASON_DEST_CDC
    assert_dest_procedure_sync_allowed("cdc", {"type": "postgresql", "extra": {}})


def test_named_matrix_floor() -> None:
    data = _load()
    assert data["measured_floor"] == 1.0
    assert data["platform_exactly_once_claimed"] is False
    assert data["pass"] == len(data["plan_accept"]) + len(data["plan_reject"])
