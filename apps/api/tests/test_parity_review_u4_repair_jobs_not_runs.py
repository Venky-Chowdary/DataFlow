"""Review U4: a repair job is not a run of its own on the dashboard.

A Parity Studio re-sync (or a quarantine replay) is a transfer job with a
``parent_job_id``. The summary counted it as a run, so one migration with a fix
read as two runs (and its verdict as a second, unrelated one). Decision:
repair jobs are excluded from ``runs`` and the heatmap; their outcome is the
parent run's re-check round, already on the parent's report.
"""

from __future__ import annotations


def _job(**over) -> dict:
    job = {"status": "completed", "name": "pay nightly",
           "reconciliation": {"passed": True, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True},
           "destination_summary": {"table": "pay", "primary_key_columns": ["id"], "sync_mode": "upsert"}}
    job.update(over)
    return job


def test_repair_children_are_not_runs(monkeypatch):
    from services import parity_summary

    jobs = {
        "parent": _job(),
        "resync": _job(parent_job_id="parent", name="re-sync"),
        "replay": _job(transfer_request={"parent_job_id": "parent"}, name="replay"),
        "other": _job(name="pay backfill"),
    }

    class _Store:
        def list_jobs(self, limit, workspace_filter):
            return [{"_id": k} for k in reversed(list(jobs))][:limit]

        def get_job(self, job_id):
            return {**jobs[job_id], "_id": job_id}

    monkeypatch.setattr(parity_summary, "job_store", lambda: _Store())
    body = parity_summary.build_summary(None, limit=2)
    assert [r["job_id"] for r in body["runs"]] == ["parent", "other"]
    assert body["kpis"]["runs"] == 2
    assert body["heatmap"]["job_ids"] == ["parent", "other"]
