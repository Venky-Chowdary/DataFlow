"""Module 9 — Quarantine rows must carry the first-class recovery contract."""

from __future__ import annotations

import pytest

from connectors.writer_common import append_write_quarantine_detail
from services.quarantine_dlq import persist_rejected_rows
from services.quarantine_row_contract import (
    REQUIRED_QUARANTINE_FIELDS,
    QuarantineRowContractError,
    assert_quarantine_rows_contract,
    normalize_quarantine_row,
    normalize_quarantine_rows,
    quarantine_row_missing_fields,
)


def test_required_fields_match_charter():
    for f in (
        "original_value",
        "expected_type",
        "actual_type",
        "failure_reason",
        "transform_attempted",
        "recovery_suggestion",
        "source_pk",
        "destination_pk",
        "job_id",
        "connector",
        "retry_status",
    ):
        assert f in REQUIRED_QUARANTINE_FIELDS


def test_normalize_fills_contract_without_inventing_pk():
    raw = {
        "row": 2,
        "column": "amount",
        "target": "amount",
        "value": "oops",
        "reason": "Invalid decimal: 'oops'",
        "source_type": "VARCHAR",
        "target_type": "DECIMAL(18,2)",
        "transform": "decimal",
        "values": {"id": "42", "amount": "oops"},
        "source_values": {"id": "42", "amount": "oops"},
    }
    row = normalize_quarantine_row(raw, job_id="job-9", connector="postgresql")
    assert row["original_value"] == "oops"
    assert row["expected_type"] == "DECIMAL(18,2)"
    assert row["actual_type"] == "VARCHAR"
    assert row["failure_reason"].startswith("Invalid decimal")
    assert row["transform_attempted"] == "decimal"
    assert row["recovery_suggestion"]
    assert row["source_pk"] == "42"
    assert row["source_pk_proven"] is True
    assert row["job_id"] == "job-9"
    assert row["connector"] == "postgresql"
    assert row["retry_status"] == "open"
    assert quarantine_row_missing_fields(row) == []


def test_normalize_resolves_primary_key_column_names_to_values():
    """Writer stamps primary_key=["id"]. That is not the row identity."""
    from services.quarantine_dlq import replay_row_identity

    a = normalize_quarantine_row(
        {
            "row": 2,
            "column": "age",
            "value": "not-a-number",
            "primary_key": ["id"],
            "pk_value": {"id": "2"},
            "values": {"id": "2", "age": "not-a-number"},
            "source_values": {"id": "2", "age": "not-a-number"},
        }
    )
    b = normalize_quarantine_row(
        {
            "row": 4,
            "column": "age",
            "value": "also-bad",
            "primary_key": ["id"],
            "pk_value": {"id": "4"},
            "values": {"id": "4", "age": "also-bad"},
            "source_values": {"id": "4", "age": "also-bad"},
        }
    )
    assert a["source_pk"] == "2"
    assert b["source_pk"] == "4"
    assert replay_row_identity(a) != replay_row_identity(b)
    assert replay_row_identity(a) == "pk:2"
    assert replay_row_identity(b) == "pk:4"


def test_normalize_does_not_invent_pk_when_absent():
    row = normalize_quarantine_row(
        {"reason": "bad", "column": "x", "value": "1"},
        job_id="j",
        connector="mysql",
    )
    assert row["source_pk"] is None
    assert row["source_pk_proven"] is False
    assert row["destination_pk"] is None
    assert row["destination_pk_proven"] is False


def test_append_write_quarantine_stamps_contract_fields():
    details: list[dict] = []
    append_write_quarantine_detail(
        details,
        {
            "row": 1,
            "column": "flag",
            "target": "flag",
            "value": "Y",
            "reason": "Invalid boolean: 'Y'",
            "policy": "quarantine",
            "source_type": "TEXT",
            "target_type": "BOOLEAN",
            "transform": "boolean",
        },
        mapped_row=["Y"],
        target_cols=["flag"],
        mappings=[{"source": "flag", "target": "flag"}],
    )
    assert len(details) == 1
    d = details[0]
    for f in REQUIRED_QUARANTINE_FIELDS:
        assert f in d, f
    assert d["failure_reason"]
    assert d["recovery_suggestion"]
    assert d["retry_status"] == "open"


def test_persist_normalizes_and_requires_job_id(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    monkeypatch.setattr(dlq, "_dlq_coll", lambda: None)
    with pytest.raises(QuarantineRowContractError):
        # Empty job_id must fail closed for durable persist.
        persist_rejected_rows(
            job_id="",
            rejected_details=[{"reason": "bad", "value": "x"}],
        )
    ev = persist_rejected_rows(
        job_id="job-ok",
        rejected_details=[{"reason": "bad", "value": "x", "column": "c"}],
        workspace_id="ws",
    )
    assert ev is not None
    stamped = ev["details"]["rejected_details"][0]
    assert stamped["job_id"] == "job-ok"
    assert stamped["failure_reason"] == "bad"
    assert_quarantine_rows_contract([stamped])


def test_normalize_batch():
    rows = normalize_quarantine_rows(
        [{"reason": "a", "value": 1}, {"reason": "b", "value": 2}],
        job_id="batch",
        connector="snowflake",
    )
    assert len(rows) == 2
    assert all(r["job_id"] == "batch" for r in rows)
