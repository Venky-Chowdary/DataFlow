"""File-drop and resume defects from the v6 QA wave.

Checkpoint resume must survive a rejected job document. An incremental
bookmark belongs to one destination. A schedule's next run is the next
cadence boundary. Excel dates and leading-zero codes stay the types the
destination column already is. A mapped column the live table does not
have is additive drift, not a passing preflight.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from services.checkpoint_service import Checkpoint, CheckpointService
from services.error_handling import humanize_transfer_failure
from services.excel_parser import excel_cell_value
from services.file_load_ledger import (
    file_already_loaded,
    file_bytes_digest,
    file_route_key,
    record_file_loaded,
)
from services.schema_drift import detect_schema_drift
from services.schema_inference import _classify_value, infer_column
from services.sync_cursor import resolve_incremental_read_scope, set_watermark


class _Cell:
    def __init__(self, value, fmt="General"):
        self.value = value
        self.number_format = fmt


def test_sidecar_resume_survives_a_rejected_job_document():
    class _RejectJob:
        def __init__(self):
            self.checkpoints: dict = {}
            self.jobs: dict = {}

        def get_job(self, job_id):
            return self.jobs.get(job_id)

        def update_job_status(self, job_id, status, **kwargs):
            return False

    service = CheckpointService(_RejectJob())
    token = Checkpoint(
        job_id="job-120k",
        chunk_index=6,
        rows_processed=120000,
        cursor_value="120000",
        coerced_null_rows=3,
        target_rows_before=10,
    )
    assert service.save(token) is True
    assert service.has_failed_saves is False
    loaded = service.load("job-120k")
    assert loaded is not None
    assert loaded.chunk_index == 6
    assert loaded.rows_processed == 120000
    assert loaded.coerced_null_rows == 3
    assert loaded.target_rows_before == 10


def _contracts() -> list[dict]:
    return [{
        "name": "orders",
        "selected": True,
        "sync_mode": "incremental_append",
        "cursor_field": "updated_at",
        "primary_key": "id",
    }]


def _scope(destination):
    return resolve_incremental_read_scope(
        sync_mode="incremental_append",
        stream_contracts=_contracts(),
        source_type="postgresql",
        source_database="qa",
        source_object="orders",
        dest_type="mysql",
        dest_database="qa",
        dest_object="orders",
        destination=destination,
    )


def test_two_destinations_do_not_share_one_incremental_bookmark(tmp_path, monkeypatch):
    import services.sync_cursor as sync_cursor

    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)
    legacy = resolve_incremental_read_scope(
        sync_mode="incremental_append",
        stream_contracts=_contracts(),
        source_type="postgresql",
        source_database="qa",
        source_object="orders",
        dest_type="mysql",
        dest_database="qa",
        dest_object="orders",
    )
    set_watermark(legacy.cursor_key, "2026-10-06T08:00:00\x1f30", metadata={"cursor_column": "updated_at"})

    mysql = _scope({"connector_id": "mysql-live", "host": "mysql.internal", "port": 3306})
    maria = _scope({"connector_id": "maria-live", "host": "maria.internal", "port": 3306})
    assert mysql.cursor_key != maria.cursor_key
    assert mysql.watermark == "2026-10-06T08:00:00\x1f30"
    assert maria.watermark is None


def test_route_without_destination_identity_keeps_the_legacy_key(tmp_path, monkeypatch):
    import services.sync_cursor as sync_cursor

    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)
    scope = resolve_incremental_read_scope(
        sync_mode="incremental_append",
        stream_contracts=_contracts(),
        source_type="postgresql",
        source_database="qa",
        source_object="orders",
        dest_type="mysql",
        dest_database="qa",
        dest_object="orders",
    )
    set_watermark(scope.cursor_key, "40", metadata={"cursor_column": "updated_at"})
    again = resolve_incremental_read_scope(
        sync_mode="incremental_append",
        stream_contracts=_contracts(),
        source_type="postgresql",
        source_database="qa",
        source_object="orders",
        dest_type="mysql",
        dest_database="qa",
        dest_object="orders",
    )
    assert again.cursor_key == scope.cursor_key
    assert again.watermark == "40"


def test_incremental_upsert_is_a_schedule_sync_mode(tmp_path, monkeypatch):
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    sched = store.create_schedule({
        "name": "Upsert orders",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "sync_mode": "incremental_upsert",
        "primary_key": "id",
    })
    assert sched.sync_mode == "incremental_deduped"


def test_completion_pinned_schedule_advances_to_the_next_boundary(tmp_path, monkeypatch):
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    sched = store.create_schedule({
        "name": "Every five",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "cron": "*/5 * * * *",
        "sync_mode": "incremental",
        "mappings": [{"source": "id", "target": "id"}],
    })
    stuck = datetime(2026, 10, 8, 1, 0, 12, tzinfo=timezone.utc).isoformat()
    store._save_all([
        store.PipelineSchedule.from_dict({
            **store.get_schedule(sched.id).to_dict(),
            "last_run_at": stuck,
            "next_run_at": stuck,
            "enabled": True,
        })
    ])
    assert store.unstick_completion_pinned_schedules() == 1
    done = store.get_schedule(sched.id)
    nxt = store._parse_ts(done.next_run_at)
    assert nxt is not None and nxt > datetime.now(timezone.utc)
    assert nxt.second == 0
    assert done.id not in {item.id for item in store.due_schedules()}


def test_excel_midnight_is_a_date_and_a_real_time_stays_a_timestamp():
    assert infer_column(["2026-10-06T00:00:00"], field_name="order_date")["logical_type"] == "DATE"
    assert infer_column(["2026-10-06T08:00:00"], field_name="order_date")["logical_type"] == "TIMESTAMP"
    assert _classify_value("2026-10-06T00:00:00Z") == "TIMESTAMPTZ"
    midnight = excel_cell_value(_Cell(datetime(2026, 10, 6, 0, 0, 0), "yyyy-mm-dd"))
    assert midnight == date(2026, 10, 6)
    timed = excel_cell_value(_Cell(datetime(2026, 10, 6, 8, 0, 0), "yyyy-mm-dd h:mm:ss"))
    assert timed == datetime(2026, 10, 6, 8, 0, 0)


def test_leading_zero_codes_stay_text_and_plain_ids_stay_integers():
    assert infer_column(["02115", "02116"], field_name="zip_code")["logical_type"] == "VARCHAR"
    assert infer_column(["2115", "10001"], field_name="zip_code")["logical_type"] == "VARCHAR"
    ids = [str(i) for i in range(50)]
    assert infer_column(ids, field_name="id")["logical_type"] == "INTEGER"
    assert excel_cell_value(_Cell(2115, "00000")) == "02115"


def test_mapped_column_missing_on_the_live_table_is_additive_drift():
    common = dict(
        source_columns=["id", "discount"],
        source_schema={"id": "INTEGER", "discount": "DECIMAL"},
        target_columns=["id"],
        target_schema={"id": "INTEGER"},
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "discount", "target": "discount"},
        ],
        table_exists=True,
        destination_db_type="postgresql",
    )
    review = detect_schema_drift(**common, schema_policy="manual_review")
    assert review["schema_evolution"]["action"] == "review"
    assert any(
        item.get("column") == "discount" and item.get("reason") == "mapped_target_absent"
        for item in review["schema_evolution"]["additive"]
    )
    propagate = detect_schema_drift(**common, schema_policy="propagate_columns")
    assert propagate["schema_evolution"]["action"] == "propagate"


def test_unchanged_file_digest_is_remembered_per_route(tmp_path, monkeypatch):
    import services.file_load_ledger as ledger

    monkeypatch.setattr(ledger, "STORE_PATH", tmp_path / "ledger.json")
    monkeypatch.setattr(ledger, "_mongo_ledger", lambda: None)
    payload = b"id,zip\n1,02115\n"
    digest = file_bytes_digest(payload)
    route = file_route_key(
        filename="codes.csv",
        dest_type="postgresql",
        dest_database="qa",
        dest_object="codes",
        destination={"connector_id": "pg-1"},
    )
    other = file_route_key(
        filename="codes.csv",
        dest_type="postgresql",
        dest_database="qa",
        dest_object="codes",
        destination={"connector_id": "pg-2"},
    )
    assert file_already_loaded(route, digest) is False
    record_file_loaded(route, digest)
    assert file_already_loaded(route, digest) is True
    assert file_already_loaded(other, digest) is False


def test_missing_object_and_raw_primary_key_are_operator_text():
    from services.object_streaming import _raise_missing_object

    class _Missing(Exception):
        response = {"Error": {"Code": "NoSuchKey"}}

    with pytest.raises(ValueError, match="no object 'drop/orders.csv'"):
        _raise_missing_object(_Missing("NoSuchKey"), bucket="qa-drop", key="drop/orders.csv")

    missing = humanize_transfer_failure("NoSuchKey: The specified key does not exist")
    assert missing["code"] == "object_missing"
    assert "NoSuchKey" not in missing["title"]

    identity = humanize_transfer_failure(KeyError("primary_key"))
    assert identity["code"] == "missing_primary_key"
    assert "identity" in identity["title"].lower()
