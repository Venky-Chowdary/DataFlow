"""Mid-write SQL salvage must dual-stamp full-row quarantine values (replayable)."""

from __future__ import annotations

from connectors.writer_common import (
    append_write_quarantine_detail,
    mapped_row_quarantine_values,
)


def test_mapped_row_quarantine_values_handles_dict_rows():
    vals = mapped_row_quarantine_values(
        {"id": 1, "amount": "bad", "note": "keep"},
        ["id", "amount", "note"],
    )
    assert vals["id"] == "1"
    assert vals["amount"] == "bad"
    assert vals["note"] == "keep"


def test_salvage_style_append_stamps_full_values_and_source():
    details: list[dict] = []
    row = (7, "not-a-date", "alice@example.com")
    mappings = [
        {"source": "src_id", "target": "id"},
        {"source": "src_ts", "target": "ts"},
        {"source": "src_email", "target": "email"},
    ]
    append_write_quarantine_detail(
        details,
        {
            "row": 42,
            "column": "ts",
            "value": "not-a-date",
            "reason": "invalid input syntax for type timestamp",
            "policy": "quarantine",
        },
        mapped_row=row,
        target_cols=["id", "ts", "email"],
        mappings=mappings,
    )
    assert len(details) == 1
    d = details[0]
    assert isinstance(d.get("values"), dict)
    assert d["values"]["id"] == "7"
    assert d["values"]["email"] == "alice@example.com"
    assert "ts" in d["values"]
    assert isinstance(d.get("source_values"), dict)
    assert d["source_values"].get("src_id") == "7"
    assert d["source_values"].get("src_email") == "alice@example.com"


def test_dict_salvage_append_does_not_use_key_order_invent():
    details: list[dict] = []
    row = {"note": "ok", "id": "9", "amount": "x"}
    append_write_quarantine_detail(
        details,
        {
            "row": 1,
            "column": "amount",
            "value": "x",
            "reason": "numeric value out of range",
            "policy": "quarantine",
        },
        mapped_row=row,
        target_cols=["id", "amount", "note"],
    )
    d = details[0]
    # Must be column-aligned values, not list(dict.keys()) order invent.
    assert d["values"]["id"] == "9"
    assert d["values"]["note"] == "ok"
    assert d["values"]["amount"] == "x"


def test_merge_hydrates_truncated_job_from_dlq(monkeypatch, tmp_path):
    from services import quarantine_dlq
    from services.quarantine_from_preflight import merge_job_quarantine

    monkeypatch.setattr(quarantine_dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    job_id = "job-hydrate-trunc"
    big = [
        {
            "row": i,
            "column": "age",
            "value": "bad",
            "reason": f"cast fail {i}",
            "policy": "quarantine",
            "values": {"id": str(i), "age": "bad"},
        }
        for i in range(1, 251)
    ]
    quarantine_dlq.persist_rejected_rows(job_id=job_id, rejected_details=big)
    job = {
        "id": job_id,
        "rejected_details": big[:10],
        "rejected_details_total": 250,
        "rejected_details_truncated": True,
        "rejected_rows": 250,
    }
    merged = merge_job_quarantine(job)
    assert len(merged) >= 250
    assert {d["row"] for d in merged if isinstance(d, dict)} >= set(range(1, 251))
