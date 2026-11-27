"""Cancel of an overwrite: which backups are put back, and what the job says.

No database: the restore/discard drivers and the destination count are fakes.
"""

from __future__ import annotations

import contextvars

import pytest

from services.error_handling import TransferCancelled


def _dest(**extra):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database", format="mysql", host="127.0.0.1", port=3306,
        database="d", username="u", password="p", table="orders", extra=dict(extra),
    )


class _Request:
    def __init__(self, sync_mode: str) -> None:
        self.sync_mode = sync_mode
        self.stream_contracts = None


class _Store:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {"job1": {"status": "running"}}
        self.writes: list[dict] = []

    def get_job(self, job_id):
        return dict(self.jobs.get(job_id) or {})

    def update_job_fields(self, job_id, fields):
        self.writes.append(dict(fields))
        self.jobs.setdefault(job_id, {}).update(fields)


@pytest.fixture
def drivers(monkeypatch):
    import connectors.table_manager as tm
    import src.transfer.cancel_restore as cr

    calls: list[tuple] = []
    monkeypatch.setattr(tm, "restore_mysql_overwrite", lambda cfg, t, b: calls.append(("restore", t, b)))
    monkeypatch.setattr(tm, "discard_mysql_overwrite", lambda cfg, b: calls.append(("discard", b)))
    monkeypatch.setattr(tm, "restore_route_backups", lambda cfg, bk: calls.append(("restore_routes", sorted(bk))) or list(bk))
    monkeypatch.setattr(tm, "discard_route_backups", lambda cfg, bk: calls.append(("discard_routes", sorted(bk))))
    monkeypatch.setattr(cr, "_count", lambda destination, table: 2)
    return calls


def _settle(destination, exc, request, store):
    from services import cancel_check
    from src.transfer.engine_finish import _settle_backup_after_error

    def run():
        cancel_check.set_progress_meter(cancel_check.ProgressMeter(store, "job1"))
        return _settle_backup_after_error(destination, exc, request=request)

    return contextvars.copy_context().run(run)


def test_a_cancelled_overwrite_restores_its_backup_and_says_so(drivers):
    from src.transfer.cancel_restore import RESTORING_MESSAGE, stamp_cancel_outcome

    store = _Store()
    exc = TransferCancelled("Transfer cancelled by user")
    dest = _dest(overwrite_backup="orders__df_bak", overwrite_backup_engine="mysql")
    assert _settle(dest, exc, _Request("full_refresh_overwrite"), store) is True
    assert drivers == [("restore", "orders", "orders__df_bak")]
    # The job said what it was doing before the (possibly long) restore.
    assert store.writes[0] == {"message": RESTORING_MESSAGE}
    assert (exc.destination_rows_now, exc.destination_rows_by_run, exc.write_rolled_back) == (2, 0, True)
    outcome = stamp_cancel_outcome(exc, {"message": "Transfer cancelled by user"})
    assert outcome["message"] == (
        "Cancelled. The destination holds the rows it had before this run (2 rows)."
    )
    assert outcome["pre_run_rows_restored"] is True


def test_a_failed_restore_keeps_the_backup_and_says_how_to_put_it_back(monkeypatch, drivers):
    import connectors.table_manager as tm
    from src.transfer.cancel_restore import stamp_cancel_outcome

    def broken(cfg, table, backup):
        drivers.append(("restore-failed", table, backup))
        raise tm.TableDropError(table, RuntimeError("Lock wait timeout exceeded"))

    monkeypatch.setattr(tm, "restore_mysql_overwrite", broken)
    exc = TransferCancelled("Transfer cancelled by user")
    dest = _dest(overwrite_backup="orders__df_bak", overwrite_backup_engine="mysql")
    assert _settle(dest, exc, _Request("full_refresh_overwrite"), _Store()) is False
    # Never discarded: the pre-run rows stay in the backup.
    assert drivers == [("restore-failed", "orders", "orders__df_bak")]
    outcome = stamp_cancel_outcome(exc, {"message": "Transfer cancelled by user"})
    msg = outcome["message"]
    assert msg.startswith("Cancelled. The destination could not be put back"), msg
    assert "orders__df_bak" in msg and "INSERT INTO orders SELECT * FROM orders__df_bak" in msg
    assert outcome["pre_run_rows_restored"] is False
    assert outcome["pre_run_backups"] == [{"table": "orders", "backup": "orders__df_bak"}]


def test_a_cancelled_keyed_incremental_still_discards_its_backup(drivers):
    """D134 is kept for resumable writes: their committed prefix is behind the
    checkpoint, and restoring the strict-run backup would remove it."""
    exc = TransferCancelled("Transfer cancelled by user")
    dest = _dest(route_backups={"orders": {"table": "orders", "backup": "orders__dfbak_j1"}})
    _settle(dest, exc, _Request("incremental_deduped"), _Store())
    assert drivers == [("discard_routes", ["orders"])]
    assert getattr(exc, "cancel_restore", None) is None


def test_an_overwrite_with_route_backups_puts_every_table_back(drivers):
    from src.transfer.cancel_restore import stamp_cancel_outcome

    exc = TransferCancelled("Transfer cancelled by user")
    dest = _dest(route_backups={
        "orders": {"table": "orders", "backup": "orders__dfbak_j1"},
        "items": {"table": "items", "backup": "items__dfbak_j1"},
    })
    assert _settle(dest, exc, _Request("full_refresh_overwrite"), _Store()) is True
    assert drivers == [("restore_routes", ["items", "orders"])]
    msg = stamp_cancel_outcome(exc, {})["message"]
    assert msg.startswith("Cancelled. Every destination table holds the rows it had before this run"), msg


def test_an_unknown_mode_restores_only_what_an_overwrite_clear_made(drivers):
    """Callers without the request (multi-table set-aside settle): the
    overwrite clear's own backup is put back, a strict-run route backup is not."""
    exc = TransferCancelled("Transfer cancelled by user")
    dest = _dest(
        overwrite_backup="orders__df_bak", overwrite_backup_engine="mysql",
        route_backups={"orders": {"table": "orders", "backup": "orders__dfbak_j1"}},
    )
    _settle(dest, exc, None, _Store())
    assert ("restore", "orders", "orders__df_bak") in drivers
    assert ("discard_routes", ["orders"]) in drivers
    assert not any(c[0] == "restore_routes" for c in drivers)


def test_a_failed_run_still_restores_as_before(drivers):
    from src.transfer.engine_finish import OVERWRITE_RESTORED_NOTE

    exc = RuntimeError("Lost connection to MySQL server during query")
    dest = _dest(overwrite_backup="orders__df_bak", overwrite_backup_engine="mysql")
    assert _settle(dest, exc, _Request("full_refresh_overwrite"), _Store()) is True
    assert drivers == [("restore", "orders", "orders__df_bak")]
    assert exc.failed_batch_undo_note == OVERWRITE_RESTORED_NOTE
    assert getattr(exc, "cancel_restore", None) is None
