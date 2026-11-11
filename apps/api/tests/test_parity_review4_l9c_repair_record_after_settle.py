"""Re-review 4, 9c (Low): a failed repair-workflow record never loses a widen's outcome.

``approve_fix`` recorded the decision in the repair workflow before settling the fix. When that
record raised (the proposal store unavailable), the ALTER had already run but the fix stayed
``applying`` and settled an hour later as "never finished (API restarted)". The outcome is now
settled first; the record failing afterwards is still reported.
"""

from __future__ import annotations

import pytest

from tests.test_parity_review3_low_preview_bound_to_approval import _widen, widen_job  # noqa: F401


def test_the_widen_outcome_is_settled_even_if_the_repair_record_fails(widen_job, monkeypatch):  # noqa: F811
    import services.agentic_repair as agentic_repair
    from services.parity_fixes import approve_fix

    svc, job_id, plan = widen_job
    fix = _widen(svc, job_id)

    def unavailable(**_kw):
        raise RuntimeError("proposal store unavailable")

    monkeypatch.setattr(agentic_repair, "propose_from_parity", unavailable)
    with pytest.raises(RuntimeError, match="proposal store unavailable"):
        approve_fix(job_id, fix["id"], actor="dba@x", note="go", preview_sha256=fix["preview_sha256"])
    assert plan["ran"], "the ALTER ran"
    record = svc.get_job(job_id)["parity"]["fixes"][fix["id"]]
    assert record["status"] == "applied", record
    assert record["result"].startswith("Ran: ")
