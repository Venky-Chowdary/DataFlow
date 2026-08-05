"""GA Module E — quarantine DLQ persists all rejected rows via chunks."""

from __future__ import annotations

from unittest.mock import patch

from services.quarantine_dlq import persist_rejected_rows


def test_persist_rejected_rows_chunks_beyond_500():
    details = [
        {"row": i, "column": "c", "reason": "cast", "value": str(i)}
        for i in range(650)
    ]
    events: list[dict] = []

    def _capture(**kwargs):
        events.append(kwargs)
        return {"ok": True, "action": kwargs.get("action"), "rows": kwargs.get("rows")}

    with patch("services.quarantine_dlq.append_dlq_event", side_effect=_capture):
        out = persist_rejected_rows(job_id="job-chunk", rejected_details=details)

    assert out is not None
    assert out["total_rejected"] == 650
    assert out["chunks"] >= 3
    assert sum(e["rows"] for e in events) == 650
    # Every chunk carries rejected_details bodies (not an empty overflow marker).
    assert all(e["details"].get("rejected_details") for e in events)
