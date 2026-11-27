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
    import connectors.overwrite_restore_steps as steps

    monkeypatch.setattr(
        steps, "manual_restore_steps",
        lambda **kw: f"STEPS({kw['engine']},{kw['table']},{kw['backup']})",
    )
    exc = TransferCancelled("Transfer cancelled by user")
    dest = _dest(overwrite_backup="orders__df_bak", overwrite_backup_engine="mysql")
    assert _settle(dest, exc, _Request("full_refresh_overwrite"), _Store()) is False
    # Never discarded: the pre-run rows stay in the backup.
    assert drivers == [("restore-failed", "orders", "orders__df_bak")]
    outcome = stamp_cancel_outcome(exc, {"message": "Transfer cancelled by user"})
    msg = outcome["message"]
    assert msg.startswith("Cancelled. The destination could not be put back"), msg
    assert "the pre-run rows are kept in orders__df_bak" in msg, msg
    assert "To put them back: STEPS(mysql,orders,orders__df_bak)." in msg, msg
    assert outcome["pre_run_rows_restored"] is False
    assert outcome["pre_run_backups"] == [{"table": "orders", "backup": "orders__df_bak"}]


def test_a_cancelled_keyed_incremental_still_discards_its_backup(drivers):
    """D134 is kept for resumable writes: their committed prefix is behind the
    checkpoint, and restoring the strict-run backup would remove it."""
    exc = TransferCancelled("Transfer cancelled by user")
    dest = _dest(route_backups={"orders": {
        "table": "orders", "backup": "orders__dfbak_j1", "sync_mode": "incremental_deduped",
    }})
    _settle(dest, exc, _Request("incremental_deduped"), _Store())
    assert drivers == [("discard_routes", ["orders"])]
    assert getattr(exc, "cancel_restore", None) is None


def test_an_overwrite_with_route_backups_puts_every_table_back(drivers):
    from src.transfer.cancel_restore import stamp_cancel_outcome

    exc = TransferCancelled("Transfer cancelled by user")
    ow = "full_refresh_overwrite"
    dest = _dest(route_backups={
        "orders": {"table": "orders", "backup": "orders__dfbak_j1", "sync_mode": ow},
        "items": {"table": "items", "backup": "items__dfbak_j1", "sync_mode": ow},
    })
    assert _settle(dest, exc, _Request("full_refresh_overwrite"), _Store()) is True
    assert drivers == [("restore_routes", ["items", "orders"])]
    msg = stamp_cancel_outcome(exc, {})["message"]
    assert msg.startswith("Cancelled. Put back to the rows they had before this run: "), msg
    assert "orders (2 rows)" in msg and "items (2 rows)" in msg, msg


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


class _Contracts(_Request):
    """A multi-table request: the first contract is an overwrite."""

    def __init__(self, *modes: str) -> None:
        super().__init__(modes[0])
        self.stream_contracts = [
            {"name": f"s{i}", "sync_mode": m, "selected": True} for i, m in enumerate(modes)
        ]


def test_a_mixed_run_restores_only_the_overwrite_streams_backup(drivers):
    """Review 1 (D134): contracts [A overwrite, B incremental_deduped strict
    keyed], cancel during B. B's route backup must be discarded (its committed
    prefix is behind its watermark), whatever the first contract says."""
    from services.multi_stream_cancel import ROWS_BEFORE_KEY

    exc = TransferCancelled("Transfer cancelled by user")
    dest = _dest(
        route_backups={"orders": {
            "table": "orders", "backup": "orders__dfbak_j1", "sync_mode": "incremental_deduped",
        }},
        **{ROWS_BEFORE_KEY: {
            "a_table": {"rows_before": 2, "sync_mode": "full_refresh_overwrite"},
            "orders": {"rows_before": 5, "sync_mode": "incremental_deduped"},
        }},
    )
    _settle(dest, exc, _Contracts("full_refresh_overwrite", "incremental_deduped"), _Store())
    assert ("discard_routes", ["orders"]) in drivers
    assert not any(c[0] == "restore_routes" for c in drivers)


def test_an_unstamped_route_backup_is_never_restored_on_a_cancel(drivers):
    exc = TransferCancelled("Transfer cancelled by user")
    dest = _dest(route_backups={"orders": {"table": "orders", "backup": "orders__dfbak_j1"}})
    _settle(dest, exc, _Request("full_refresh_overwrite"), _Store())
    assert drivers == [("discard_routes", ["orders"])]


def test_a_mixed_run_record_names_restored_and_kept_tables():
    """Review 2: [overwrite A, append B] — A is put back, B keeps the rows
    this run committed; the record must not say the whole destination holds
    its pre-run rows."""
    from src.transfer.cancel_restore import stamp_cancel_outcome

    exc = TransferCancelled("Transfer cancelled by user")
    exc.cancel_restore = {"restored": True, "tables": [
        {"table": "a_table", "backup": "a_table__df_bak", "engine": "mysql", "kind": "own",
         "restored": True, "rows_now": 2},
    ]}
    outcome = stamp_cancel_outcome(exc, {"message": "x", "streams": [
        {"table": "b_table", "rows_before": 10, "rows_now": 50, "rows_by_run": 40},
        {"table": "a_table", "rows_before": 2, "rows_now": 2, "rows_by_run": 0},
    ]})
    msg = outcome["message"]
    assert "holds the rows it had before this run" not in msg, msg
    assert "a_table (2 rows)" in msg and "b_table (40 rows)" in msg, msg
    assert outcome["restored_tables"] == [{"table": "a_table", "rows_now": 2}]


def test_a_cancel_after_the_tables_write_completed_keeps_its_rows(drivers):
    """Review 3: once every source row of the table is committed, a cancel
    (e.g. at the final progress write) is after the write: no restore."""
    from src.transfer.cancel_restore import mark_write_complete

    exc = TransferCancelled("Transfer cancelled by user")
    dest = _dest(overwrite_backup="orders__df_bak", overwrite_backup_engine="mysql")
    mark_write_complete(dest, "orders")
    _settle(dest, exc, _Request("full_refresh_overwrite"), _Store())
    assert drivers == [("discard", "orders__df_bak")]
    assert getattr(exc, "cancel_restore", None) is None


def test_cancel_and_close_of_one_connection_never_overlap():
    """Review 5: the producer's cancel and the D302 watch's close ran on two
    threads; psycopg2's close frees the cancel handle the other was using."""
    import threading
    import time

    from services.source_read_cancel import abort_connection, cancel_connection

    events: list[str] = []

    class _Conn:
        closed = 0

        def cancel(self):
            assert not self.closed, "cancel after close"
            events.append("cancel-start")
            time.sleep(0.05)
            events.append("cancel-end")

        def close(self):
            events.append("close")
            self.closed = 1

    for _ in range(20):
        conn = _Conn()
        events.clear()
        t = threading.Thread(target=cancel_connection, args=(conn,))
        t.start()
        abort_connection(conn)
        t.join(5)
        cancel_connection(conn)  # after the close: skipped, not raised
        i = events.index("close")
        assert "cancel-start" not in events[i:], events
        assert events.count("cancel-start") == events.count("cancel-end"), events


def test_a_mongo_restore_replaces_in_one_step_and_never_drops_without_a_backup(monkeypatch):
    import connectors.table_manager as tm

    calls: list[tuple] = []

    class _Coll:
        def __init__(self, name):
            self.name = name

        def rename(self, target, **kw):
            calls.append(("rename", self.name, target, kw))

    class _Db:
        def __init__(self, names):
            self.names = names

        def list_collection_names(self):
            return list(self.names)

        def drop_collection(self, name):
            calls.append(("drop", name))

        def __getitem__(self, name):
            return _Coll(name)

    monkeypatch.setattr(tm, "_mongo_db", lambda cfg: _Db(["orders", "orders__df_bak"]))
    tm.restore_mongodb_overwrite({}, "orders", "orders__df_bak")
    assert calls == [("rename", "orders__df_bak", "orders", {"dropTarget": True})]
    calls.clear()
    monkeypatch.setattr(tm, "_mongo_db", lambda cfg: _Db(["orders"]))
    tm.restore_mongodb_overwrite({}, "orders", "orders__df_bak")
    assert calls == [], "the live collection was dropped with no backup to put back"
