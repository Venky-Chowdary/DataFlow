"""Enterprise write honesty — coerce_null gate, CAST semantics, Dynamo identity, catalog."""

from __future__ import annotations

from connectors.writer_common import (
    allow_job_coerce_null_writes,
    build_mapped_rows_with_details,
    transform_error_policy,
)
from services.migration_risk_contract import (
    create_migration_risk_contract,
    execution_policy_semantics,
    resolve_write_action_for_mapping,
)
from services.primary_key import pick_dynamodb_identity_column, resolve_identity_key
from src.transfer.connector_capabilities import (
    TRANSFER_READY_CATALOG_IDS,
    get_capabilities,
    resolve_driver_type,
)


def test_job_coerce_null_demoted_without_allow() -> None:
    assert transform_error_policy("coerce_null") == "quarantine"
    assert transform_error_policy("coerce_null", allow_coerce_null=True) == "coerce_null"
    with allow_job_coerce_null_writes(True):
        assert transform_error_policy("coerce_null") == "coerce_null"


def test_build_mapped_rows_refuses_job_coerce_null_without_contract() -> None:
    """Primary write: bad cell is quarantined, not silently NULLed."""
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["id", "age"],
        data_rows=[["1", "10"], ["2", "nope"], ["3", "30"]],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "age", "target": "age", "confidence": 0.99, "transform": "to_integer"},
        ],
        target_cols=["id", "age"],
        column_types={"id": "string", "age": "string"},
        dest_types={"id": "string", "age": "integer"},
        error_policy="coerce_null",
    )
    assert len(mapped) == 2  # bad row held out
    assert any(d.get("policy") == "quarantine" for d in details)


def test_build_mapped_rows_allows_staging_coerce_null() -> None:
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["id", "age"],
        data_rows=[["1", "10"], ["2", "nope"], ["3", "30"]],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "age", "target": "age", "confidence": 0.99, "transform": "to_integer"},
        ],
        target_cols=["id", "age"],
        column_types={"id": "string", "age": "string"},
        dest_types={"id": "string", "age": "integer"},
        error_policy="coerce_null",
        allow_job_coerce_null=True,
    )
    assert len(mapped) == 3
    assert any(d.get("policy") == "coerce_null" for d in details)
    from services.value_serializer import is_missing_sentinel

    # Bad age cell is DF_MISSING (omit-from-SET), not SQL NULL wipe.
    assert any(is_missing_sentinel(row[1]) for row in mapped)


def test_cast_and_continue_default_is_quarantine_not_write() -> None:
    sem = execution_policy_semantics()["CAST_AND_CONTINUE"]
    assert sem["write_action"] == "quarantine"
    assert sem["row_written"] is False
    assert sem.get("writes_cast_value") is False
    assert "not written" in (sem.get("notes") or "").lower() or "hold" in (
        sem.get("notes") or ""
    ).lower()

    c = create_migration_risk_contract(
        column="age",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Pilot accepts cast-fail holdout",
        execution_policy="CAST_FAIL_QUARANTINE",
    )
    assert c.execution_policy == "CAST_AND_CONTINUE"
    action, pol, _rid = resolve_write_action_for_mapping(
        {"source": "age", "target": "age", "risk_contract": c.to_dict()},
        "quarantine",
    )
    assert action == "quarantine"
    assert pol == "CAST_AND_CONTINUE"


def test_cast_contract_coerce_null_only_when_policy_asks() -> None:
    c = create_migration_risk_contract(
        column="age",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Explicit NULL on cast failure",
        execution_policy="CAST_AND_CONTINUE",
        quarantine_policy="COERCE_NULL",
    )
    action, pol, _rid = resolve_write_action_for_mapping(
        {"source": "age", "target": "age", "risk_contract": c.to_dict()},
        "quarantine",
    )
    assert action == "coerce_null"
    assert pol == "CAST_AND_CONTINUE"


def test_dynamo_identity_matches_writer_order_id() -> None:
    assert pick_dynamodb_identity_column(["capital", "order_id", "name"]) == "order_id"
    assert pick_dynamodb_identity_column(["id", "order_id"]) == "id"
    src, tgt = resolve_identity_key(
        mappings=[
            {"source": "order_id", "target": "order_id", "confidence": 0.99},
            {"source": "name", "target": "name", "confidence": 0.9},
        ],
        source_columns=["order_id", "name"],
        dest_kind="dynamodb",
        purpose="uniqueness",
    )
    assert src == "order_id"
    assert tgt == "order_id"


def test_mongo_identity_still_id_only() -> None:
    src, tgt = resolve_identity_key(
        mappings=[
            {"source": "user_id", "target": "user_id", "confidence": 0.99},
            {"source": "_id", "target": "_id", "confidence": 0.99},
        ],
        dest_kind="mongodb",
        purpose="uniqueness",
    )
    assert src == "_id"
    assert tgt == "_id"


def test_empty_currency_transform_quarantines_not_silent_null() -> None:
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["amt"],
        data_rows=[[""], ["$12.00"]],
        mappings=[
            {"source": "amt", "target": "amt", "transform": "currency", "confidence": 0.9}
        ],
        target_cols=["amt"],
        column_types={"amt": "string"},
        dest_types={"amt": "decimal"},
        error_policy="quarantine",
    )
    assert len(mapped) == 1
    assert any("empty" in str(d.get("reason") or "").lower() for d in details)


def test_empty_text_to_integer_quarantines_not_silent_null() -> None:
    """MySQL '' → Postgres INTEGER must not write NULL without contract/job path."""
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["id", "age"],
        data_rows=[["1", ""], ["2", "30"]],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {
                "source": "age",
                "target": "age",
                "confidence": 0.99,
                "transform": "integer",
            },
        ],
        target_cols=["id", "age"],
        column_types={"id": "string", "age": "string"},
        dest_types={"id": "string", "age": "integer"},
        error_policy="quarantine",
        dest_kind="postgresql",
    )
    assert len(mapped) == 1
    assert details
    assert any("empty" in str(d.get("reason") or "").lower() for d in details)
    assert any(d.get("policy") == "quarantine" for d in details)


def test_empty_text_to_integer_coerce_null_only_with_contract() -> None:
    c = create_migration_risk_contract(
        column="age",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Explicit empty→NULL",
        execution_policy="CAST_AND_CONTINUE",
        quarantine_policy="COERCE_NULL",
    )
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["age"],
        data_rows=[[""], ["3"]],
        mappings=[
            {
                "source": "age",
                "target": "age",
                "transform": "integer",
                "risk_contract": c.to_dict(),
            }
        ],
        target_cols=["age"],
        column_types={"age": "string"},
        dest_types={"age": "integer"},
        error_policy="quarantine",
        allow_job_coerce_null=False,
    )
    assert len(mapped) == 2  # empty coerced to NULL under contract
    assert any(d.get("policy") == "coerce_null" for d in details)


def test_composite_pk_stamped_on_quarantine_details() -> None:
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["org_id", "code", "age"],
        data_rows=[["o1", "a", "nope"]],
        mappings=[
            {"source": "org_id", "target": "org_id", "confidence": 0.99},
            {"source": "code", "target": "code", "confidence": 0.99},
            {"source": "age", "target": "age", "transform": "integer", "confidence": 0.9},
        ],
        target_cols=["org_id", "code", "age"],
        column_types={"org_id": "string", "code": "string", "age": "string"},
        dest_types={"org_id": "string", "code": "string", "age": "integer"},
        error_policy="quarantine",
        dest_kind="postgresql",
        destination_pk_columns=["org_id", "code"],
    )
    assert len(mapped) == 0
    assert details
    assert details[0].get("primary_key") == ["org_id", "code"]
    assert details[0].get("pk_value", {}).get("org_id") == "o1"
    assert details[0].get("pk_value", {}).get("code") == "a"


def test_unverified_risk_contract_fails_closed_not_job_policy() -> None:
    """Present but bad signature must abort — never demote to job quarantine."""
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["age"],
        data_rows=[["nope"], ["3"]],
        mappings=[
            {
                "source": "age",
                "target": "age",
                "transform": "integer",
                "risk_contract": {
                    "risk_id": "mrc-tampered",
                    "column": "age",
                    "source_type": "TEXT",
                    "destination_type": "INTEGER",
                    "execution_policy": "SKIP_ROW",
                    "approved_by": "ops@dataflow.app",
                    "reason": "tampered",
                    "signature": "not-a-valid-hmac",
                },
            }
        ],
        target_cols=["age"],
        column_types={"age": "string"},
        dest_types={"age": "integer"},
        error_policy="quarantine",
    )
    assert len(mapped) == 1
    assert details
    assert details[0].get("policy") == "fail"
    assert details[0].get("execution_policy") == "FAIL_JOB"


def test_skip_row_excluded_from_replay_dlq_persist(monkeypatch) -> None:
    from services.quarantine_dlq import persist_rejected_rows, persist_job_quarantine_outcome

    events: list[dict] = []

    def _capture(**kwargs):
        events.append(kwargs)
        return {"ok": True, "ts": "t"}

    monkeypatch.setattr("services.quarantine_dlq.append_dlq_event", _capture)
    out = persist_rejected_rows(
        job_id="job-skip",
        rejected_details=[
            {
                "row": 1,
                "reason": "bad",
                "execution_policy": "SKIP_ROW",
                "disposition": "skipped",
                "quarantine_required": False,
            }
        ],
    )
    assert out is not None
    assert out.get("rows") == 0
    assert out.get("skipped_contract") == 1
    # Durable skip_audit — never replay quarantine action.
    assert len(events) == 1
    assert events[0].get("action") == "skip_audit"
    assert events[0].get("rows") == 1
    assert all(e.get("action") != "quarantine" for e in events)
    outcome = persist_job_quarantine_outcome(
        {
            "rejected_details": [
                {
                    "row": 1,
                    "execution_policy": "SKIP_ROW",
                    "disposition": "skipped",
                    "quarantine_required": False,
                }
            ]
        }
    )
    assert outcome["ok"] is True
    assert outcome["rejected_count"] == 0


def test_transfer_ready_excludes_preflight_false_sftp_email() -> None:
    assert "sftp" not in TRANSFER_READY_CATALOG_IDS
    assert "email" not in TRANSFER_READY_CATALOG_IDS
    for cid in ("sftp", "email"):
        driver = resolve_driver_type(cid)
        caps = get_capabilities(driver, cid)
        assert caps.get("preflight") is False


def test_transfer_ready_ids_require_preflight_when_not_file() -> None:
    """No preflight:False driver may sit in TRANSFER_READY (except file sources)."""
    bad: list[str] = []
    for cid in sorted(TRANSFER_READY_CATALOG_IDS):
        driver = resolve_driver_type(cid)
        caps = get_capabilities(driver, cid)
        if caps.get("file_source"):
            continue
        if caps.get("preflight") is False:
            bad.append(cid)
    assert not bad, f"TRANSFER_READY with preflight:False: {bad}"


def test_prepare_records_for_vector_write_applies_transforms() -> None:
    """Vector writers must not zip raw headers — Risk Contracts + transforms apply."""
    from connectors.writer_common import prepare_records_for_vector_write

    records, rejected, abort = prepare_records_for_vector_write(
        headers=["id", "age", "title"],
        data_rows=[["1", "10", "hello"], ["2", "nope", "skip"]],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {
                "source": "age",
                "target": "age_num",
                "confidence": 0.99,
                "transform": "to_integer",
                "target_type": "integer",
            },
        ],
        column_types={"id": "string", "age": "string", "title": "string"},
        error_policy="quarantine",
        dest_kind="pgvector",
        label="pgvector",
    )
    assert abort is None
    assert len(records) == 1
    # Source-key content_column still resolves after rename mapping.
    assert records[0]["age"] == 10 or records[0]["age"] == "10"
    assert records[0]["age_num"] == 10 or records[0]["age_num"] == "10"
    # Unmapped metadata column must pass through.
    assert records[0]["title"] == "hello"
    assert rejected


def test_prepare_records_for_vector_write_fail_job_aborts() -> None:
    from connectors.writer_common import prepare_records_for_vector_write

    c = create_migration_risk_contract(
        column="age",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Vector FAIL_JOB must abort before embed",
        execution_policy="FAIL_JOB",
    )
    records, rejected, abort = prepare_records_for_vector_write(
        headers=["id", "age"],
        data_rows=[["1", "nope"]],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {
                "source": "age",
                "target": "age",
                "confidence": 0.99,
                "transform": "to_integer",
                "target_type": "integer",
                "risk_contract": c.to_dict(),
            },
        ],
        column_types={"id": "string", "age": "string"},
        error_policy="quarantine",
        dest_kind="milvus",
        label="milvus",
    )
    assert records == []
    assert rejected
    assert abort is not None
    assert "FAIL_JOB" in abort or "risk contract" in abort.lower() or "rejected" in abort.lower()


def test_preflight_request_accepts_source_config() -> None:
    from src.routers.preflight_router import PreflightRequest

    body = PreflightRequest(
        columns=["id"],
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        source_config={"type": "mysql", "host": "db.example", "database": "crown"},
    )
    assert body.source_config is not None
    assert body.source_config["type"] == "mysql"
