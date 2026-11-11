"""Re-review 3 (LOW): a re-read failure's reason is fixed text, never an exception's own text.

``_reread_reason`` returned ``str(exc)`` for any LookupError — a KeyError deep
in a driver carries the missing key, which can be a row value.
"""

from __future__ import annotations

from services import parity_cache
from services import parity_report as pr


def _job() -> dict:
    return {"_id": "lr", "status": "completed",
            "reconciliation": {"passed": False, "source_rows": 10, "target_rows": 10,
                               "culprit_keys": {"changed": ["7"]}},
            "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"}}


def test_a_driver_key_error_does_not_leak_its_key(monkeypatch):
    parity_cache.clear()

    def boom(job, ev, keys):
        raise KeyError("alice@example.com")

    monkeypatch.setattr(pr, "_bounded_reread", boom)
    reason = pr.build_parity_report(_job(), reread=True)["evidence"]["reread"]["t"]["reason"]
    assert "alice" not in reason
    parity_cache.clear()


def test_our_own_reason_is_kept():
    parity_cache.clear()
    reason = pr.build_parity_report(_job(), reread=True)["evidence"]["reread"]["t"]["reason"]
    assert reason == "the job has no saved route to re-read"
    parity_cache.clear()
