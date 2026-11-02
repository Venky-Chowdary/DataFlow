"""R2-C1 / R2-C2 — replay must not write masks or the null sentinel.

Operator previews mask PII. The durable ledger keeps the source cell.
Replay reads the ledger. A ledger that only has masks is refused.
"""

from __future__ import annotations

import pytest

from services.value_serializer import SQL_NULL_SENTINEL


def test_null_sentinel_is_sql_null_for_email_phone_postal():
    from services.transform_engine import apply_transform

    for transform in ("email", "phone", "postal", "iban", "none", "trim"):
        value, err = apply_transform(SQL_NULL_SENTINEL, transform)
        assert err is None, transform
        assert value is None, transform


def test_email_transform_writes_null_not_the_sentinel():
    from connectors.writer_common import build_mapped_rows_with_details

    mapped, errors, rejected = build_mapped_rows_with_details(
        headers=["id", "email", "phone", "postal"],
        data_rows=[["1", SQL_NULL_SENTINEL, SQL_NULL_SENTINEL, SQL_NULL_SENTINEL]],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {"source": "email", "target": "email", "transform": "email"},
            {"source": "phone", "target": "phone", "transform": "phone"},
            {"source": "postal", "target": "postal", "transform": "postal"},
        ],
        target_cols=["id", "email", "phone", "postal"],
        column_types={
            "id": "VARCHAR",
            "email": "VARCHAR",
            "phone": "VARCHAR",
            "postal": "VARCHAR",
        },
    )
    assert errors == [], errors
    assert rejected == []
    assert mapped == [("1", None, None, None)]
    assert SQL_NULL_SENTINEL not in repr(mapped)


def test_persist_stores_canonical_cells_and_keeps_the_masked_preview(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq
    from src.transfer.job_quarantine import _persist_job_quarantine

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    monkeypatch.setattr(dlq, "_dlq_coll", lambda: None)
    summary = {
        "rejected_rows": 1,
        "rejected_details": [
            {
                "row": 1,
                "column": "email",
                "value": "j***@***.com",
                "source_values": {"id": "1", "email": "j***@***.com"},
            }
        ],
    }
    canonical = [
        {
            "row": 1,
            "column": "email",
            "value": "jane@example.com",
            "source_values": {"id": "1", "email": "jane@example.com", "note": SQL_NULL_SENTINEL},
        }
    ]
    _persist_job_quarantine("job-mask", summary, canonical_details=canonical)
    assert summary["rejected_details"][0]["value"] == "j***@***.com"
    events = dlq.list_dlq_events(job_id="job-mask")
    assert events
    stored = events[0]["details"]["rejected_details"][0]
    assert stored["source_values"]["email"] == "jane@example.com"
    assert stored["source_values"]["note"] == SQL_NULL_SENTINEL


def test_replay_prefers_dlq_canonical_over_masked_job(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    monkeypatch.setattr(dlq, "_dlq_coll", lambda: None)
    dlq.persist_rejected_rows(
        job_id="job-replay",
        rejected_details=[
            {
                "row": 1,
                "column": "email",
                "value": "jane@example.com",
                "source_values": {"id": "1", "email": "jane@example.com"},
                "reason": "cast",
            }
        ],
    )
    job = {
        "_id": "job-replay",
        "rejected_details": [
            {
                "row": 1,
                "column": "email",
                "value": "j***@***.com",
                "source_values": {"id": "1", "email": "j***@***.com"},
                "reason": "cast",
            }
        ],
    }
    rows = dlq.canonical_replay_details(job)
    assert rows[0]["source_values"]["email"] == "jane@example.com"


def test_replay_refuses_when_only_masks_are_stored():
    import services.quarantine_dlq as dlq

    job = {
        "_id": "job-masked-only",
        "rejected_details": [
            {
                "row": 1,
                "column": "email",
                "value": "j***@***.com",
                "source_values": {
                    "email": "j***@***.com",
                    "phone": "(555) 1\u20264567",
                    "note": "__DF_S\u2026LL__",
                },
                "reason": "cast",
            }
        ],
    }
    with pytest.raises(dlq.MaskedQuarantineReplayError, match="masked previews"):
        dlq.canonical_replay_details(job)
