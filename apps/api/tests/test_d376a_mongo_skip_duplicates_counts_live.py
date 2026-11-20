"""D376 (a) live: skip_duplicates into MongoDB counts every kept row.

QA (schedule QAV3-sched-skip, PostgreSQL -> MongoDB, skip_duplicates): run 2
found all 30 keys stored, kept them, and the job read records_processed 0 and
'0 event(s) merged' with the 30 skipped rows uncounted. AC-1: get_job and the
trust report read rows_skipped 30.

Drives the engine against the local PostgreSQL and the MongoDB replica set,
with the in-memory job store the API uses in memory mode (it applies the same
promote_rows_skipped as the Mongo-backed store).
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.r13_mapping_harness import _port_up, drop, endpoint, run_sql  # noqa: E402

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")
ROWS = 30


@pytest.fixture
def store(tmp_path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from services.mongodb_service import MemoryMongoDBService

    fake = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.mark.parametrize("changed", [False, True], ids=["unchanged", "source_changed"])
@pytest.mark.parametrize("sync_mode", ["full_refresh_append", "incremental_deduped"])
def test_second_run_reports_every_kept_row_as_skipped(store, sync_mode, changed):
    import pymongo

    from services.parity_report import build_parity_report
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    if not _port_up(27017):
        pytest.skip("no MongoDB on :27017")
    endpoint("postgresql", "probe")
    tag = uuid.uuid4().hex[:8]
    src, db = f"d376a_{tag}", f"d376a_{tag}"
    run_sql("postgresql", f'CREATE TABLE "{src}" (id INT PRIMARY KEY, name TEXT, amount INT)')
    run_sql(
        "postgresql",
        f'INSERT INTO "{src}" VALUES (%s, %s, %s)',  # nosec B608
        params=[(i, f"n{i}", i * 10) for i in range(1, ROWS + 1)],
    )
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        dest = EndpointConfig(
            kind="database", format="mongodb", connection_string=MONGO_URI,
            database=db, collection="c", extra={"duplicate_policy": "skip_duplicates"},
        )
        contract = {"name": src, "sync_mode": sync_mode, "primary_key": "id", "selected": True}
        jobs = []
        for run in (1, 2):
            if run == 2 and changed:
                # New images of stored keys: skip_duplicates keeps the stored ones.
                run_sql("postgresql", f'UPDATE "{src}" SET name = name || \'-v2\'')  # nosec B608
            job_id = f"d376a{run}" + uuid.uuid4().hex[:12]
            store.update_job_status(job_id, "pending", transfer_request={})
            result = UniversalTransferEngine().execute_tracked(
                TransferRequest(
                    source=endpoint("postgresql", src),
                    destination=dest,
                    mappings=[
                        {"source": c, "target": c, "confidence": 1.0}
                        for c in ("id", "name", "amount")
                    ],
                    sync_mode=sync_mode,
                    duplicate_policy="skip_duplicates",
                    stream_contracts=[contract],
                    validation_mode="balanced",
                    skip_preflight=True,
                ),
                job_id,
            )
            assert result.success, result.error
            jobs.append(store.get_job(job_id))
        assert client[db].c.count_documents({}) == ROWS
        names = {d["id"]: d["name"] for d in client[db].c.find({})}
        assert names == {i: f"n{i}" for i in range(1, ROWS + 1)}, names

        first, second = jobs
        assert int(first.get("rows_skipped") or 0) == 0, first.get("rows_skipped")
        # AC-1: the job the operator reads names the 30 kept rows.
        assert second.get("rows_skipped") == ROWS, {
            k: second.get(k) for k in ("rows_skipped", "records_processed", "message")
        }
        summary = second.get("destination_summary") or {}
        assert summary.get("rows_skipped") == ROWS, summary
        assert int(summary.get("rows_written") or 0) == 0, summary
        recon = second.get("reconciliation") or {}
        assert recon.get("passed") is True, recon
        # The 30 rows were read and kept: not "no new source rows".
        assert recon.get("source_rows") == ROWS, recon
        assert "no new source rows" not in str(recon.get("message") or "").lower(), recon

        # AC-1: the trust report explains the 30 rows by the duplicate policy,
        # and never counts the 30 stored rows as more than 30.
        report = build_parity_report(second, reread=False, job_id=str(second.get("job_id") or ""))
        diffs = report.get("differences") or []
        policy = [
            d for d in diffs
            if "duplicate policy (skip_duplicates)" in str((d.get("cause") or {}).get("detail"))
        ]
        assert [(d["kind"], d["rows"]) for d in policy] == [
            ("missing_in_destination", ROWS)
        ], diffs
        assert all((d.get("cause") or {}).get("explained") for d in diffs), diffs
        assert all(int(d.get("rows") or 0) <= ROWS for d in diffs), diffs
    finally:
        client.drop_database(db)
        client.close()
        drop("postgresql", src)
