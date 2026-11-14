"""REL-4: a connection drop while verifying says the written rows remain.

Before the fix the job failed with raw driver text ("server closed the
connection unexpectedly") and never said the 1.5M committed rows were still in
the destination.
"""

from __future__ import annotations


class _Mongo:
    def __init__(self):
        self.job = {"phase": "reconcile", "records_processed": 1_500_000}
        self.updates: list[dict] = []

    def get_job(self, job_id):
        return dict(self.job)

    def update_job_status(self, job_id, status, **kwargs):
        self.updates.append({"status": status, **kwargs})

    def __getattr__(self, name):
        return lambda *a, **k: None


def test_verification_drop_names_the_rows_that_stay():
    import psycopg2

    from src.transfer.job_failure import _fail_runtime_job

    mongo = _Mongo()
    exc = psycopg2.OperationalError("server closed the connection unexpectedly")
    message, _extras = _fail_runtime_job(mongo, "job", exc)
    text = message + " " + " ".join(str(u.get("error") or "") for u in mongo.updates)
    assert "1,500,000 row(s) it wrote are still in the destination" in text, text
    assert "server closed the connection unexpectedly" not in (mongo.updates[-1].get("error") or "")
