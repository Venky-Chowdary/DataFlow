"""Train 2: the Jobs grid's parity stamp changes when verification after the write decides.

Verification (Parity at Scale) finishes without touching ``updated_at`` or the
parity state, so a stamp built from those alone kept an open grid's cached
"Verification running" (studio-ui-pass2's verdict cache x p3's verification).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _job(block=None):
    from services.parity_scale.lease import LEASE_FIELD

    job = {
        "_id": "j1", "status": "completed", "updated_at": "2026-11-27T10:00:00+00:00",
        "parity": {"version": 3, "summary": {"at": "2026-11-27T10:00:05+00:00", "verdict": "verifying"}},
    }
    if block is not None:
        job["parity_scale"] = block
        expires = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        job[LEASE_FIELD] = {"token": "t", "owner": "w", "expires_at": expires}
    return job


def test_the_stamp_changes_when_verification_decides_and_nothing_else_moves():
    from services.job_list_view import parity_stamp, slim_job_for_list

    running = _job({"state": "running", "started_at": "2026-11-27T10:00:06+00:00", "tables": []})
    passed = _job({"state": "passed", "started_at": "2026-11-27T10:00:06+00:00",
                   "finished_at": "2026-11-27T10:09:00+00:00", "tables": []})
    assert parity_stamp(running) != parity_stamp(passed)
    assert slim_job_for_list(running)["parity_stamp"] != slim_job_for_list(passed)["parity_stamp"]
    # Same updated_at and parity state on both: only the verification moved.
    assert running["updated_at"] == passed["updated_at"] and running["parity"] == passed["parity"]


def test_a_job_never_verified_after_the_write_keeps_its_stamp():
    from services.job_list_view import parity_stamp

    assert parity_stamp(_job()) == "3:2026-11-27T10:00:05+00:00"
    assert parity_stamp({"status": "completed"}) == ""
