"""Train 2: a cancelled overwrite that restores held backups still reports a failed backup drop.

D302 (restore on cancel) returned before train 1's restore-outcome carry, so a
backup drop that failed while the cancel settled the other streams' backups
never reached the job. No database: the drivers are fakes.
"""

from __future__ import annotations

import contextvars

from services.error_handling import TransferCancelled


class _Request:
    sync_mode = "full_refresh_overwrite"
    stream_contracts = None


class _Store:
    def get_job(self, job_id):
        return {"status": "running"}

    def update_job_fields(self, job_id, fields):
        return None


def test_a_cancelled_multi_table_overwrite_job_carries_the_failed_backup_drop(monkeypatch):
    import connectors.table_manager as tm
    import src.transfer.cancel_restore as cr
    from services import cancel_check
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine_finish import PARKED_BACKUPS_KEY, _settle_backup_after_error
    from src.transfer.job_failure import _fail_runtime_job
    from src.transfer.models import EndpointConfig

    calls: list[tuple] = []

    def discard(cfg, backup):
        calls.append(("discard", backup))
        raise RuntimeError("DROP command denied to user 'u'")

    monkeypatch.setattr(tm, "restore_mysql_overwrite", lambda cfg, t, b: calls.append(("restore", t, b)))
    monkeypatch.setattr(tm, "discard_mysql_overwrite", discard)
    monkeypatch.setattr(cr, "_count", lambda destination, table: 2)

    # Stream "items" landed (its backup parked); the cancel stopped "orders" mid-overwrite.
    dest = EndpointConfig(
        kind="database", format="mysql", host="127.0.0.1", port=3306, database="d",
        username="u", password="p", table="orders",
        extra={
            "overwrite_backup": "orders__df_bak", "overwrite_backup_engine": "mysql",
            PARKED_BACKUPS_KEY: {"items": {"overwrite_backup": "items__df_bak",
                                           "overwrite_backup_engine": "mysql"}},
        },
    )
    exc = TransferCancelled("Transfer cancelled by user")

    def settle():
        cancel_check.set_progress_meter(cancel_check.ProgressMeter(_Store(), "job1"))
        return _settle_backup_after_error(dest, exc, request=_Request())

    assert contextvars.copy_context().run(settle) is True
    assert ("restore", "orders", "orders__df_bak") in calls and ("discard", "items__df_bak") in calls
    assert (exc.overwrite_restore or {}).get("status") == "discard_failed"

    mongo = MemoryMongoDBService()
    mongo.create_transfer_job({"_id": "job1", "status": "running", "records_processed": 0})
    mongo.request_job_cancel("job1")
    _fail_runtime_job(mongo, "job1", exc, request=None)
    job = mongo.get_job("job1")
    assert job["status"] == "cancelled", job
    for text in (job.get("error"), (job.get("cancel_outcome") or {}).get("message")):
        assert "items__df_bak" in str(text) and "could not be dropped" in str(text), job
    assert "rows it had before this run" in str(job.get("error")), job  # the restore is still said
