"""Agentic repair propose → approve → apply mappings."""

from __future__ import annotations

import csv
import io
import sqlite3
from copy import deepcopy

import pytest

from services.agentic_repair import (
    apply_actions_to_mappings,
    apply_actions_with_report,
    decide_proposal,
    propose_from_preflight,
    propose_from_quarantine,
)
from services.validation_assistant import _suggested_actions


def test_quarantine_propose_and_approve_applies_mappings() -> None:
    details = [
        {"column": "amount", "reason": "Invalid decimal: 'abc'", "value": "abc"},
        {"column": "when", "reason": "Invalid date", "value": "n/a"},
    ]
    p = propose_from_quarantine(details, job_id="job1")
    assert p.status == "proposed"
    assert p.actions
    assert p.auto_applicable is False

    mappings = [{"source": "amount", "destination": "amount"}, {"source": "when", "destination": "when"}]
    decided = decide_proposal(
        p.id,
        approve=True,
        actor="tester",
        apply_fn=lambda actions: apply_actions_with_report(mappings, actions),
    )
    assert decided.status == "applied"
    updated = decided.apply_result["mappings"]
    amount = next(m for m in updated if m["source"] == "amount")
    assert amount.get("destination_type") == "VARCHAR" or amount.get("target_type") == "VARCHAR"


def test_reject_proposal() -> None:
    p = propose_from_quarantine([{"column": "x", "reason": "bad"}], job_id="j2")
    decided = decide_proposal(p.id, approve=False, actor="tester")
    assert decided.status == "rejected"


def test_live_ddl_change_target_type_does_not_mutate_mappings() -> None:
    out = apply_actions_to_mappings(
        [{"source": "DEP_TIME", "destination": "DEP_TIME", "target_type": "NUMBER(9,6)"}],
        [
            {
                "kind": "change_target_type",
                "column": "DEP_TIME",
                "to_type": "NUMBER(12,9)",
                "requires_ddl": True,
            }
        ],
    )
    assert out[0].get("target_type") == "NUMBER(9,6)"
    assert out[0].get("destType") is None


def test_unproven_change_target_type_does_not_mutate_mappings() -> None:
    out = apply_actions_to_mappings(
        [{"source": "DEP_TIME", "destination": "DEP_TIME", "target_type": "NUMBER(9,6)"}],
        [
            {
                "kind": "change_target_type",
                "column": "DEP_TIME",
                "to_type": "NUMBER(11,8)",
                "apply_proven": False,
            }
        ],
    )
    assert out[0].get("target_type") == "NUMBER(9,6)"


def test_apply_actions_to_mappings_transform() -> None:
    out = apply_actions_to_mappings(
        [{"source": "ts", "destination": "ts"}],
        [{"kind": "add_transform", "column": "ts", "transform": "to_timestamp"}],
    )
    assert out[0]["transform"] == "to_timestamp"


def test_apply_transform_is_idempotent() -> None:
    actions = [
        {"kind": "add_transform", "column": "ts", "transform": "to_timestamp"},
        {"kind": "add_transform", "column": "ts", "transform": "to_timestamp"},
    ]
    out = apply_actions_to_mappings([{"source": "ts", "destination": "ts"}], actions)
    assert out[0]["transform"] == "to_timestamp"
    assert len(out[0]["transforms"]) == 1


def test_cannot_redecide_applied_proposal() -> None:
    p = propose_from_quarantine([{"column": "x", "reason": "bad"}], job_id="j3")
    mappings = [{"source": "x", "destination": "x"}]
    decide_proposal(
        p.id,
        approve=True,
        actor="tester",
        apply_fn=lambda actions: apply_actions_with_report(mappings, actions),
    )
    try:
        decide_proposal(p.id, approve=False, actor="tester")
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "status" in str(exc)


def test_approved_can_later_apply() -> None:
    p = propose_from_quarantine(
        [{"column": "amount", "reason": "Invalid decimal: 'x'", "value": "x"}],
        job_id="j4",
    )
    audit = decide_proposal(p.id, approve=True, actor="tester")
    assert audit.status == "approved"
    mappings = [{"source": "amount", "destination": "amount"}]
    applied = decide_proposal(
        p.id,
        approve=True,
        actor="tester",
        apply_fn=lambda actions: apply_actions_with_report(mappings, actions),
    )
    assert applied.status == "applied"


def test_duplicate_key_actions_are_guidance_not_mutative() -> None:
    actions = _suggested_actions(
        blockers=[
            {
                "id": "g9_data_integrity",
                "message": "Data integrity failed: id: duplicate key values (a×2)",
            },
            {
                "id": "g6_target_ddl",
                "message": "Primary key candidate 'id' has 12 duplicate value(s) in source sample",
            },
            {
                "id": "g8_reconciliation",
                "message": "Dry-run reconciliation failed — 12 duplicate target key(s) on id",
            },
        ],
        column_fixes=[],
    )
    kinds = {a.get("kind") for a in actions}
    assert "fix_source_keys" in kinds
    assert "normalize_control_chars" not in kinds
    assert "quarantine_and_rerun" not in kinds
    assert not any(a.get("kind") == "change_target_type" for a in actions)


def test_non_mutative_approve_does_not_fake_applied() -> None:
    pf = {
        "passed": False,
        "blockers": [
            {
                "id": "g9_data_integrity",
                "message": "id: duplicate key values (abc×2)",
            },
            {
                "id": "g6_target_ddl",
                "message": "Primary key candidate 'id' has 2 duplicate value(s) in source sample",
            },
            {
                "id": "g8_reconciliation",
                "message": "Dry-run reconciliation failed — 2 duplicate target key(s) on id",
            },
        ],
        "gates": [
            {"id": "g9_data_integrity", "status": "block", "message": "id: duplicate key values"},
            {"id": "g6_target_ddl", "status": "block", "message": "Primary key candidate 'id' has duplicates"},
            {"id": "g8_reconciliation", "status": "block", "message": "duplicate target key(s) on id"},
        ],
    }
    p = propose_from_preflight(pf, job_id="dup-job")
    assert p.diagnosis.get("mapping_applyable") is False
    assert p.diagnosis.get("root_cause") == "duplicate_identity_keys"
    mappings = [{"source": "id", "destination": "id"}, {"source": "name", "destination": "name"}]
    decided = decide_proposal(
        p.id,
        approve=True,
        actor="tester",
        apply_fn=lambda actions: apply_actions_with_report(mappings, actions),
    )
    assert decided.status == "approved"
    assert decided.apply_result.get("applied") is False
    assert decided.apply_result.get("reason") == "no_mutative_actions"


def test_decide_route_records_authenticated_actor_not_body_actor() -> None:
    from fastapi import FastAPI, Request
    from fastapi.testclient import TestClient

    from src.routers.repair_router import router

    app = FastAPI()

    @app.middleware("http")
    async def _auth(request: Request, call_next):
        request.state.user_email = "mallory@corp.com"
        return await call_next(request)

    app.include_router(router, prefix="/api/v1")
    client = TestClient(app)

    p = propose_from_quarantine(
        [{"column": "amount", "reason": "Invalid decimal: 'abc'", "value": "abc"}],
        job_id="job-actor",
    )
    res = client.post(
        f"/api/v1/repair/proposals/{p.id}/decide",
        json={"approve": False, "actor": "victim@corp.com"},
    )
    assert res.status_code == 200, res.text
    assert res.json()["decided_by"] == "mallory@corp.com"


@pytest.mark.parametrize("source_format", ["sqlite", "csv"])
def test_approved_strip_controls_revalidates_and_executes_local_text(
    tmp_path, monkeypatch, source_format,
) -> None:
    from services import agentic_repair, mongodb_service
    from services.preflight_service import run_file_preflight
    from src.transfer import engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_copy_csv_local import _isolate_cursor_store
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    jobs = _FakeMongo()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: jobs)
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: jobs)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(agentic_repair, "_proposals_coll", lambda: None)
    monkeypatch.setattr(agentic_repair, "_audit_coll", lambda: None)
    _isolate_cursor_store(monkeypatch, tmp_path)

    parent_id = "repair-parent"
    jobs.update_job_status(
        parent_id, "failed", error="historical failure", records_transferred=0,
        rejected_details=[{"column": "title", "reason": "historical finding"}],
    )
    parent_before = deepcopy(jobs.get_job(parent_id))
    columns = ["id", "title", "untouched"]
    column_types = {"id": "INTEGER", "title": "TEXT", "untouched": "TEXT"}
    rows = [
        (1, "hello\u200bworld", "keep\u200bthis"),
        (2, "clean", "unchanged"),
        (3, "a\u200bb", "also\u200bkeep"),
    ]
    destination_path = tmp_path / "destination.sqlite"
    with sqlite3.connect(destination_path) as connection:
        connection.execute("CREATE TABLE repaired (id INTEGER, clean_title TEXT, untouched TEXT)")
        schema_before = connection.execute("PRAGMA table_info(repaired)").fetchall()

    source_content = b""
    source_path = tmp_path / "source.sqlite"
    if source_format == "sqlite":
        with sqlite3.connect(source_path) as connection:
            connection.execute("CREATE TABLE source_rows (id INTEGER, title TEXT, untouched TEXT)")
            connection.executemany("INSERT INTO source_rows VALUES (?, ?, ?)", rows)
        source = EndpointConfig(
            kind="database", format="sqlite", database=str(source_path), table="source_rows",
        )
        with sqlite3.connect(source_path) as connection:
            sampled = connection.execute("SELECT id, title, untouched FROM source_rows ORDER BY id").fetchall()
    else:
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)
        source_content = buffer.getvalue().encode("utf-8")
        sampled = [
            (int(row["id"]), row["title"], row["untouched"])
            for row in csv.DictReader(io.StringIO(source_content.decode("utf-8")))
        ]
        source = EndpointConfig(kind="file", format="csv")
    assert sampled == rows
    population = [dict(zip(columns, row)) for row in sampled]
    mappings = [
        {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 1.0},
        {
            "source": "title", "target": "clean_title", "target_type": "TEXT",
            "confidence": 1.0, "transform_override": True,
        },
        {"source": "untouched", "target": "untouched", "target_type": "TEXT", "confidence": 1.0},
    ]
    original_mappings = deepcopy(mappings)

    def revalidate(current_mappings):
        return run_file_preflight(
            columns=columns, column_types=column_types, row_count=len(rows),
            mappings=current_mappings, sample_rows=population,
            population_rows=population, rows_are_population=True,
            source_kind=source.kind, source_format=source.format,
            source_connected=True, destination_connected=True,
            destination_db_type="sqlite", destination_table="repaired",
            destination_table_exists=True, destination_can_create=True,
            destination_can_write=True,
            destination_column_types={"id": "INTEGER", "clean_title": "TEXT", "untouched": "TEXT"},
            sync_mode="full_refresh_append", validation_mode="strict",
        )

    baseline = revalidate(mappings)
    assert baseline["passed"] is True, baseline
    assert mappings == original_mappings

    action = {"kind": "add_transform", "column": "title", "transform": "strip_controls"}

    def proposal(proposal_id):
        candidate = agentic_repair.RepairProposal(
            id=proposal_id, job_id=parent_id, source="contract",
            summary="Explicitly remove format controls from title only",
            actions=[dict(action)], auto_applicable=False,
        )
        agentic_repair._upsert_proposal(candidate)
        return candidate

    apply_calls = []

    def apply(actions):
        apply_calls.append(deepcopy(actions))
        return apply_actions_with_report(mappings, actions)

    rejected = proposal("repair-rejected")
    assert agentic_repair.get_proposal(rejected.id).status == "proposed"
    assert mappings == original_mappings
    decision = decide_proposal(rejected.id, approve=False, actor="tester", apply_fn=apply)
    assert decision.status == "rejected"
    assert apply_calls == []
    assert mappings == original_mappings
    with sqlite3.connect(destination_path) as connection:
        assert connection.execute("SELECT * FROM repaired").fetchall() == []

    approved = proposal("repair-approved")
    assert mappings == original_mappings
    decision = decide_proposal(approved.id, approve=True, actor="tester", apply_fn=apply)
    assert decision.status == "applied", decision.apply_result
    assert decision.decided_by == "tester"
    assert apply_calls == [[action]]
    assert decision.apply_result["applied"] is True
    assert decision.apply_result["mutated_count"] == 1
    assert decision.apply_result["skipped_actions"] == []
    applied_mappings = decision.apply_result["mappings"]
    expected_mappings = deepcopy(original_mappings)
    expected_mappings[1].update(
        transform="strip_controls", transforms=[{"type": "strip_controls"}],
    )
    assert applied_mappings == expected_mappings
    assert mappings == original_mappings
    persisted = agentic_repair.get_proposal(approved.id)
    assert persisted.status == "applied"
    assert persisted.apply_result["mappings"] == applied_mappings
    validated = revalidate(applied_mappings)
    assert validated["passed"] is True, validated
    with sqlite3.connect(destination_path) as connection:
        assert connection.execute("SELECT * FROM repaired").fetchall() == []
        assert connection.execute("PRAGMA table_info(repaired)").fetchall() == schema_before

    request = TransferRequest(
        source=source,
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(destination_path), table="repaired",
        ),
        mappings=applied_mappings, column_types=column_types,
        source_filename="source.csv" if source_format == "csv" else "",
        source_content=source_content, sync_mode="full_refresh_append",
        validation_mode="strict", triggered_by="tester",
    )
    assert request.skip_preflight is False
    child_id = "repair-child"
    jobs.update_job_status(child_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, child_id)
    assert result.success is True, result.error
    assert result.records_transferred == 3
    assert result.validation_plan["passed"] is True, result.validation_plan
    assert result.reconciliation["passed"] is True, result.reconciliation
    assert int(result.reconciliation.get("rejected_rows") or 0) == 0
    with sqlite3.connect(destination_path) as connection:
        assert connection.execute(
            "SELECT id, clean_title, untouched FROM repaired ORDER BY id",
        ).fetchall() == [
            (1, "helloworld", "keep\u200bthis"),
            (2, "clean", "unchanged"),
            (3, "ab", "also\u200bkeep"),
        ]
        assert connection.execute("SELECT COUNT(*) FROM repaired").fetchone() == (3,)
        assert connection.execute("PRAGMA table_info(repaired)").fetchall() == schema_before
    if source_format == "sqlite":
        with sqlite3.connect(source_path) as connection:
            assert connection.execute(
                "SELECT id, title, untouched FROM source_rows ORDER BY id",
            ).fetchall() == rows
    else:
        assert request.source_content == source_content
    assert jobs.get_job(parent_id) == parent_before
