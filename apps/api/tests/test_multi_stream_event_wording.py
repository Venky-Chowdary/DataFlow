"""Multi-table progress lines name the table. The job total is not a checksum."""

from __future__ import annotations

from services.batch_progress import (
    batch_write_message,
    opening_analysis_message,
    opening_batch_message,
)
from services.reconcile_coverage import annotate_last_stream_checksum_note
from src.transfer.reconcile_heartbeat import reconcile_heartbeat_scope


def test_opening_lines_do_not_call_the_endpoint_count_the_job():
    contracts = [
        {"name": "customers", "selected": True},
        {"name": "orders", "selected": True},
    ]
    assert opening_analysis_message(contracts) == "Analyzing the restored endpoint…"
    opening = opening_batch_message(2, contracts)
    assert "2 rows on the restored endpoint" in opening
    assert "each of 2 tables" in opening
    assert "in batches" not in opening
    assert opening_batch_message(2, [{"name": "orders"}]) == "Streaming 2 rows in batches…"
    assert opening_analysis_message([{"name": "orders"}]) == "Analyzing source table…"


def test_batch_lines_name_each_table_so_equal_counts_stay_distinct():
    contracts = [{"name": "customers"}, {"name": "orders"}]
    customers = batch_write_message(
        1, 1, 2, checkpoint={"cdc_stream": "customers"}, stream_contracts=contracts,
    )
    orders = batch_write_message(
        1, 1, 2, checkpoint={"cdc_stream": "orders"}, stream_contracts=contracts,
    )
    assert customers == "Writing batch 1/1 (2 rows) on customers…"
    assert orders == "Writing batch 1/1 (2 rows) on orders…"
    assert customers != orders
    single = batch_write_message(1, 1, 2, checkpoint={"cdc_stream": "orders"})
    assert single == "Writing batch 1/1 (2 rows)…"
    cdc = batch_write_message(
        1, 1, 3, is_cdc=True, checkpoint={"cdc_stream": "orders"}, stream_contracts=contracts,
    )
    assert cdc == "CDC applied 3 change(s) on orders…"


def test_heartbeat_scope_marks_last_stream_checksum():
    scope = reconcile_heartbeat_scope({
        "checksum_mode": "source_reread",
        "multi_stream": True,
        "table": "orders",
        "streams": [{"name": "customers"}, {"name": "orders"}],
    })
    assert scope["proof_kind"] == "source_reread"
    assert scope["checksum_scope"] == "last_stream"
    assert scope["checksum_table"] == "orders"
    assert scope["stream_count"] == 2
    assert reconcile_heartbeat_scope({"checksum_mode": "inline_write_pass"}) == {
        "proof_kind": "inline_write_pass",
    }
    assert reconcile_heartbeat_scope(None) == {"proof_kind": "full"}


def test_stream_written_note_names_the_table(monkeypatch):
    calls: list[tuple] = []

    class _Mongo:
        def update_job_status(self, job_id, status, **kwargs):
            calls.append((job_id, status, kwargs))
            return True

    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service",
        lambda: _Mongo(),
    )
    from src.transfer.stream_multi import _publish_stream_written

    _publish_stream_written("job-1", "customers", 2)
    _publish_stream_written("", "orders", 2)
    assert len(calls) == 1
    assert calls[0][0] == "job-1"
    assert calls[0][1] == "running"
    assert calls[0][2]["message"] == "Wrote 2 rows on customers…"
    assert calls[0][2]["phase"] == "writing"
    assert "records_processed" not in calls[0][2]


def test_last_stream_checksum_note_refuses_job_migration_proven():
    summary = {
        "multi_stream": True,
        "streams": [{"name": "customers"}, {"name": "orders"}],
        "checksum_note": (
            "Independent source re-read after the write pass (scan pagination) — "
            "dest read-back can earn full_checksum / migration_proven."
        ),
    }
    annotate_last_stream_checksum_note(summary)
    note = summary["checksum_note"]
    assert "migration_proven." in note
    assert note.endswith("does not earn migration_proven for the job.")
    annotate_last_stream_checksum_note(summary)
    assert summary["checksum_note"] == note

    quiet = {"multi_stream": True, "streams": [{}, {}], "checksum_note": "writer basis"}
    annotate_last_stream_checksum_note(quiet)
    assert quiet["checksum_note"] == "writer basis"
