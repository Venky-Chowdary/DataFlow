"""Long-running jobs must stay one writer and must not empty an occupied table."""

from __future__ import annotations

import base64
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from services.lease_heartbeat import start_lease_heartbeat
from services.schema_inference import infer_type
from services.type_system import is_lossy_coercion
from services.worker_fleet import reclaim_stale_claims
from services.worker_leases import WorkerLeaseStore
from src.ai.copilot.schedule_cadence import describe_stored_cadence
from src.transfer.stream_row_accounting import reader_population


def test_resume_does_not_add_the_checkpoint_to_a_full_reread():
    # 400k source, resume offset 180k, pass re-read all 400k → 580k used to
    # be the Gate-8 source count (DEF-C-020).
    assert reader_population(
        committed_offset=580_000,
        resume_offset=180_000,
        total_rows=400_000,
    ) == 400_000


def test_tail_resume_keeps_offset_plus_tail():
    assert reader_population(
        committed_offset=400_000,
        resume_offset=180_000,
        total_rows=400_000,
    ) == 400_000


def test_base64_alphabet_text_stays_text_and_is_not_a_preserve():
    raw = base64.b64encode(b"a long note made of ordinary words, not a blob").decode()
    assert len(raw) >= 32
    assert infer_type([raw]) in {"VARCHAR", "TEXT"}
    assert infer_type([raw], field_name="payload") == "BINARY"
    assert is_lossy_coercion("VARCHAR", "BINARY") is True
    assert is_lossy_coercion("BINARY", "BINARY") is False


def test_cron_label_follows_the_anchor_not_the_interval_preset():
    assert describe_stored_cadence("hourly", "7 * * * *", "UTC") == "Hourly at :07 UTC"
    assert describe_stored_cadence("daily", "40 21 * * 1-5", "UTC") == "Weekdays at 21:40 UTC"
    assert describe_stored_cadence("hourly", "", "UTC") == "Every hour"


def test_heartbeat_holds_the_lease_past_the_ttl(monkeypatch):
    monkeypatch.setattr(WorkerLeaseStore, "_mongo_collection", lambda self: None)
    store = WorkerLeaseStore("writer-a")
    assert store.acquire("job-long", ttl_seconds=1)
    stop = start_lease_heartbeat(
        store,
        "job-long",
        ttl_seconds=1,
        interval_seconds=0.05,
    )
    try:
        time.sleep(0.35)
        assert store.is_held("job-long") is True
    finally:
        stop()
        store.release("job-long")


def test_reclaim_does_not_start_a_cancelled_job(monkeypatch):
    import services.worker_fleet as fleet

    class _Cursor(list):
        def limit(self, _n):
            return self

    class _Coll:
        def __init__(self):
            self.docs = {
                "job-c": {
                    "_id": "job-c",
                    "job_id": "job-c",
                    "status": "claimed",
                    "claimed_at": datetime.now(timezone.utc) - timedelta(seconds=600),
                    "worker": "dead",
                }
            }

        def find(self, _query):
            return _Cursor(self.docs.values())

        def update_one(self, filt, update):
            doc = self.docs.get(filt["_id"])
            if doc and doc.get("status") == filt.get("status"):
                doc.update(update["$set"])

    coll = _Coll()

    class _NotHeld:
        def is_held(self, _job_id: str) -> bool:
            return False

    monkeypatch.setattr(fleet, "_queue_coll", lambda: coll)
    monkeypatch.setattr(fleet, "_transfer_job_cancelled", lambda _job_id: True)
    assert reclaim_stale_claims(older_than_seconds=120, lease_store=_NotHeld()) == 0  # type: ignore[arg-type]
    assert coll.docs["job-c"]["status"] == "cancelled"


def test_overwrite_pins_the_pre_drop_count(monkeypatch):
    from src.transfer.engine import _pin_overwrite_rows_before
    from src.transfer.models import EndpointConfig

    monkeypatch.setattr(
        "services.dest_precount.precount_destination",
        lambda *_a, **_k: 200_000,
    )
    dest = EndpointConfig(kind="database", format="mysql", table="orders")
    _pin_overwrite_rows_before(dest)
    assert dest.extra["overwrite_rows_before"] == 200_000


def test_failed_mysql_overwrite_restores_the_backup(monkeypatch):
    import src.transfer.engine as engine

    seen: dict[str, bool] = {}

    def _restore(dest, *, restore: bool) -> bool:
        seen["restore"] = restore
        dest.extra.pop("overwrite_backup", None)
        return True

    monkeypatch.setattr(engine, "_settle_overwrite_backup", _restore)
    dest = SimpleNamespace(
        extra={"overwrite_backup": "orders__df_bak", "overwrite_backup_engine": "mysql"}
    )
    summary = {"target_rows_before": 0, "table": "orders"}
    message = engine._note_failed_batch_undo(
        SimpleNamespace(destination=dest, sync_mode="full_refresh_overwrite"),
        summary,
        "Row count mismatch: source 580000 expected target 580000 vs target 0",
    )
    assert seen["restore"] is True
    assert summary["partial_batch_undo"] == "restored"
    assert "empty again" not in message
    assert "restored" in message


def test_manual_run_increments_run_count_when_the_job_ends(tmp_path, monkeypatch):
    import services.schedule_runner as runner
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    sched = store.create_schedule({
        "name": "manual",
        "source_connector_id": "src",
        "source_table": "t",
        "dest_connector_id": "dst",
        "dest_table": "u",
        "interval": "hourly",
        "cron": "7 * * * *",
        "mappings": [{"source": "id", "target": "id"}],
    })
    assert sched.to_dict()["cadence_label"] == "Hourly at :07 UTC"
    assert store.mark_schedule_running(sched.id, "inst") is not None
    assert store.set_running_job(sched.id, "job-manual") is not None
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda _job_id: {"status": "completed", "records_transferred": 4},
    )
    runner.record_schedule_for_finished_job("job-manual")
    done = store.get_schedule(sched.id)
    assert done.run_count == 1
    assert done.running is False
    assert done.last_job_id == "job-manual"


def test_second_overwrite_drops_the_replacement_not_the_backup(monkeypatch):
    import connectors.table_manager as tm

    sql: list[str] = []

    class _Cur:
        def execute(self, statement, _args=None):
            sql.append(statement)

        def fetchall(self):
            last = sql[-1]
            if "COLUMN_NAME" in last:
                return [("id", "int")]
            return [("orders",), ("orders__df_bak",)]

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            return None

    monkeypatch.setattr(
        "connectors.mysql_conn.get_connection",
        lambda **_k: _Conn(),
    )
    monkeypatch.setattr("connectors.mysql_conn.enable_autocommit", lambda _c: None)
    backup, kept = tm.retire_mysql_overwrite(
        {"database": "qa"}, "orders", [{"target": "id"}]
    )
    assert backup == "orders__df_bak"
    assert kept == []
    joined = "\n".join(sql)
    assert "DROP TABLE `orders__df_bak`" not in joined
    assert "DROP TABLE `orders`" in joined
    assert "RENAME TABLE `orders__df_bak` TO `orders`" in joined
    assert "RENAME TABLE `orders` TO `orders__df_bak`" in joined


def test_progress_write_refused_after_cancel_stops_the_writer():
    from services.error_handling import TransferCancelled
    from src.transfer.engine import _progress_write_or_cancel

    class _Mongo:
        def update_job_status(self, *_a, **_k):
            return False

        def get_job(self, _job_id):
            return {"status": "running", "cancel_requested": True}

    try:
        _progress_write_or_cancel(_Mongo(), "job-1", phase="writing")
    except TransferCancelled:
        return
    raise AssertionError("cancel did not stop the writer")
