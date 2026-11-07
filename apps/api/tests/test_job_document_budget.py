"""MongoDB transfer_jobs BSON budget — DocumentTooLarge prevention."""

from __future__ import annotations

from services.job_document_budget import (
    UPDATE_SET_BUDGET_BYTES,
    apply_job_update_with_budget,
    emergency_strip_job_update,
    estimate_bson_size,
    is_document_too_large_error,
    slim_checkpoint_for_job_store,
    slim_rejected_details,
    trim_job_update_payload,
)


def test_slim_rejected_details_caps_preview_and_cells():
    details = [
        {
            "row": i,
            "reason": "cast failed",
            "values": {f"col_{j}": ("x" * 2000) for j in range(40)},
        }
        for i in range(200)
    ]
    preview, total, truncated = slim_rejected_details(details)
    assert total == 200
    assert truncated is True
    assert len(preview) <= 40
    cell = next(iter(preview[0]["values"].values()))
    assert isinstance(cell, str)
    assert len(cell) <= 240


def test_slim_checkpoint_strips_sample_dumps():
    cp = slim_checkpoint_for_job_store(
        {
            "rows_processed": 100,
            "rejected_rows": 50,
            "rejected_details": [{"row": 1, "values": {"a": "b" * 5000}}] * 100,
            "sample_rows": [{"huge": "y" * 10000}] * 50,
            "cursor_value": 99,
        }
    )
    assert cp["cursor_value"] == 99
    assert "sample_rows" not in cp
    assert len(cp["rejected_details"]) <= 25
    assert cp["rejected_details_truncated"] is True


def test_trim_job_update_keeps_under_budget():
    fat_details = [
        {"row": i, "values": {f"c{j}": "z" * 4000 for j in range(30)}}
        for i in range(500)
    ]
    updates = {
        "status": "running",
        "message": "Writing…",
        "checkpoint": {
            "rows_processed": 2099,
            "rejected_details": fat_details,
            "sample_rows": fat_details,
        },
        "destination_summary": {
            "rejected_details": fat_details,
            "rejected_rows": 500,
        },
        "rejected_details": fat_details,
    }
    trimmed = trim_job_update_payload(updates)
    assert estimate_bson_size(trimmed) <= UPDATE_SET_BUDGET_BYTES
    assert trimmed.get("rejected_details_truncated") is True
    assert len(trimmed.get("rejected_details") or []) <= 40


def test_apply_job_update_retries_on_document_too_large():
    class _FakeResult:
        modified_count = 1
        matched_count = 1

    calls: list[dict] = []

    class _Coll:
        def update_one(self, filt, doc):
            payload = doc.get("$set") or {}
            calls.append(payload)
            if len(calls) == 1:
                raise Exception("'update' command document too large")
            return _FakeResult()

    result = apply_job_update_with_budget(
        _Coll(),
        {"_id": "x"},
        {
            "status": "failed",
            "error": "boom",
            "rejected_details": [{"values": {"a": "b" * 100}}] * 10,
        },
    )
    assert result.modified_count == 1
    assert len(calls) == 2
    assert calls[1].get("_job_document_budget", {}).get("emergency_strip") is True
    assert calls[1].get("rejected_details") == []


def test_is_document_too_large_error_detects_pymongo_message():
    assert is_document_too_large_error(Exception("'update' command document too large"))
    assert not is_document_too_large_error(Exception("connection refused"))


def test_emergency_strip_preserves_reject_count():
    stripped = emergency_strip_job_update(
        {
            "status": "failed",
            "error": "x" * 5000,
            "rejected_rows": 1200,
            "rejected_details": [{"row": 1}] * 100,
            "checkpoint": {"rejected_details": [{"row": 1}] * 100},
        }
    )
    assert stripped["rejected_rows"] == 1200
    assert stripped["rejected_details"] == []
    assert stripped["destination_summary"]["rejected_rows"] == 1200
    assert stripped["destination_summary"]["job_document_budget_emergency"] is True


def test_emergency_strip_keeps_resume_tokens_and_nested_reject_count():
    stripped = emergency_strip_job_update(
        {
            "status": "running",
            "records_processed": 2000,
            "chunk_current": 3,
            "destination_summary": {"rejected_rows": 900, "rejected_details": [{"x": 1}] * 50},
            "checkpoint": {
                "rows_processed": 2000,
                "cursor_value": 1999,
                "file_offset": 4096,
                "rejected_rows": 900,
                "rejected_details": [{"row": i, "values": {"a": "b" * 1000}} for i in range(100)],
            },
        }
    )
    assert stripped["checkpoint"]["cursor_value"] == 1999
    assert stripped["checkpoint"]["file_offset"] == 4096
    assert stripped["checkpoint"]["rejected_details"] == []
    assert stripped["rejected_rows"] == 900
    assert stripped["destination_summary"]["rejected_rows"] == 900
