"""C16 review: a cancel claims a rollback only on the route's evidence.

Independent review of 345121382, reproduced live on PostgreSQL:

* a committed same-size replacement (500 rows -> 500 other rows) was reported
  as "No row of this run was kept: the write was rolled back ... as it did
  before the run" — the row count alone was taken as proof of a rollback;
* 500 -> 0 was reported as "holds 0 row(s), as it did before the run";
* a cancelled ``incremental_deduped`` COPY that only updated keys was reported
  as "No row of this run was kept" with ``records_processed`` 0 — the keyed
  predicate only knew "upsert"/"merge".

Each case goes through the job-failure path a cancelled COPY route takes
(``_fail_runtime_job``) against a live PostgreSQL table.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h


@pytest.fixture
def table():
    h.pg_conn().close()
    name = f"r14c16e_{uuid.uuid4().hex[:8]}"
    h.execute("postgresql", f'CREATE TABLE "{name}" (id INT PRIMARY KEY, v TEXT)')
    h.execute("postgresql", f'INSERT INTO "{name}" SELECT g, \'pre\' FROM generate_series(1, 500) g')
    try:
        yield name
    finally:
        h.drop_table("postgresql", name)


def _cancel(table: str, sync_mode: str, *, rolled_back: bool = False, tally: int = 2048) -> dict:
    from services.error_handling import TransferCancelled
    from services.job_status import mark_write_rolled_back
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.job_failure import _fail_runtime_job
    from src.transfer.models import EndpointConfig, TransferRequest

    pg = h.dest_config("postgresql")
    ep = {k: pg[k] for k in ("host", "port", "database", "username", "password")}
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="src_unused", **ep),
        destination=EndpointConfig(
            kind="database", format="postgresql", schema="public", table=table,
            extra={"dest_table_existed_before": True, "cancel_rows_before": 500}, **ep,
        ),
        sync_mode=sync_mode,
        skip_preflight=True,
    )
    mongo = MemoryMongoDBService()
    job_id = "r14c16e" + uuid.uuid4().hex[:10]
    mongo.create_transfer_job({"_id": job_id, "status": "running", "records_processed": tally})
    mongo.request_job_cancel(job_id)
    mongo.update_job_status(job_id, "cancelled", phase="cancelled", message="Transfer cancelled by user")
    exc = TransferCancelled("Transfer cancelled by user", rows_written=tally)
    if rolled_back:
        mark_write_rolled_back(exc)
    _fail_runtime_job(mongo, job_id, exc, request=request)
    return mongo.get_job(job_id) or {}


def _replace(table: str, rows: int) -> None:
    h.execute("postgresql", f'TRUNCATE "{table}"')
    if rows:
        h.execute(
            "postgresql",
            f'INSERT INTO "{table}" SELECT g + 10000, \'new\' FROM generate_series(1, {rows}) g',
        )


def test_committed_same_size_replacement_is_not_called_a_rollback(table):
    _replace(table, 500)  # the overwrite committed 500 other rows
    job = _cancel(table, "full_refresh_overwrite")
    out = job["cancel_outcome"]
    assert out["rolled_back"] is False, out
    assert out["rows_committed_source"] == "unknown", out
    assert "rolled back" not in job["message"] and "as it did before" not in job["message"]
    assert "now holds 500 row(s)" in job["message"], job["message"]
    # Unknown is not zero: a retry must not see "no writes".
    assert job.get("records_processed") is None
    from services.execution_engine_contract import committed_rows_of

    assert committed_rows_of(job)[1] is False


def test_zero_rows_left_is_never_reported_as_the_pre_run_state(table):
    _replace(table, 0)
    for rolled in (False, True):
        job = _cancel(table, "full_refresh_overwrite", rolled_back=rolled)
        assert "as it did before" not in job["message"], job["message"]
        assert job["cancel_outcome"]["destination_rows_now"] == 0


def test_rolled_back_overwrite_with_evidence_says_so(table):
    job = _cancel(table, "full_refresh_overwrite", rolled_back=True)
    out = job["cancel_outcome"]
    assert out["rows_committed"] == 0 and out["rolled_back"] is True, out
    assert "the write was rolled back" in job["message"]
    assert "holds 500 row(s), as it did before the run" in job["message"]


@pytest.mark.parametrize("sync_mode", ["incremental_deduped", "upsert", "cdc"])
def test_a_keyed_copy_that_only_updated_keys_is_not_no_rows_kept(table, sync_mode):
    h.execute("postgresql", f'UPDATE "{table}" SET v = \'updated\' WHERE id <= 300')
    job = _cancel(table, sync_mode)
    assert "No row of this run was kept" not in job["message"], job["message"]
    assert job["cancel_outcome"]["rows_committed_source"] == "unknown", job["cancel_outcome"]
    assert job.get("records_processed") is None


def test_a_real_cancelled_keyed_copy_over_existing_rows_is_unknown(monkeypatch):
    """The same repro through a real PostgreSQL -> PostgreSQL upsert COPY,
    cancelled mid-copy from the job's own probe (second review: the earlier
    legs injected the exception)."""
    import threading

    import services.cancel_check as cc
    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    pg = h.dest_config("postgresql")
    ep = {k: pg[k] for k in ("host", "port", "database", "username", "password")}
    token = uuid.uuid4().hex[:8]
    src, dst = f"r14c16k_s_{token}", f"r14c16k_d_{token}"
    for t in (src, dst):
        h.execute("postgresql", f'CREATE TABLE "{t}" (id INT PRIMARY KEY, v TEXT)')
        h.execute("postgresql", f'INSERT INTO "{t}" SELECT g, \'v\' || g FROM generate_series(1, 60000) g')
    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    job_id = "r14c16k" + uuid.uuid4().hex[:10]
    real = cc.cancel_checkpoint
    polls = {"n": 0}

    def probe(rows_written=None):
        polls["n"] += 1
        if polls["n"] == 5:
            store.request_job_cancel(job_id)
            store.update_job_status(job_id, "cancelled", operator_command=True)
        return real(rows_written)

    monkeypatch.setattr(cc, "cancel_checkpoint", probe)
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public", table=src, **ep),
        destination=EndpointConfig(kind="database", format="postgresql", schema="public", table=dst, **ep),
        sync_mode="upsert",
        stream_contracts=[{"name": src, "selected": True, "primary_key": ["id"], "sync_mode": "upsert"}],
        skip_preflight=True, validation_mode="balanced",
    )
    store.update_job_status(job_id, "pending", transfer_request={})
    try:
        worker = threading.Thread(target=lambda: UniversalTransferEngine().execute_tracked(request, job_id))
        worker.start()
        worker.join(300)
        job = store.get_job(job_id) or {}
        assert job.get("status") == "cancelled", (job.get("status"), job.get("error"))
        assert polls["n"] >= 5, "the copy never reached the cancel"
        assert "No row of this run was kept" not in str(job.get("message")), job.get("message")
        out = job.get("cancel_outcome") or {}
        assert out.get("rows_committed_source") in {"unknown", "committed_batches"}, out
        if out.get("rows_committed_source") == "unknown":
            assert job.get("records_processed") is None
    finally:
        for t in (src, dst):
            h.drop_table("postgresql", t)
