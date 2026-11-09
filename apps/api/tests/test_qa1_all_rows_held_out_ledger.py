"""QA1: a run that quarantined every row showed "held out 0" on the Run page.

The all-held-out failure was rebuilt into an unmeasured ledger ("Source row
count was not measured", held out 0) while the DLQ held every row. The run
read and judged each row: the ledger must say read N = held out N, written 0,
destination COUNT(*) not taken.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.row_conservation import account_job  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import TransferRequest  # noqa: E402
from tests.test_r12_d258_nothing_written_names_quarantine_reasons import (  # noqa: E402,F401
    _run_all_rows_held_out,
    fake_mongo,
)
from tests.typed_fidelity_helpers import (  # noqa: E402
    drop_mysql_table,
    drop_pg_table,
    mysql_endpoint,
    pg_connect,
    pg_endpoint,
    require_ports,
    uniq,
)


def _assert_counted_and_held_out(ledger: dict, n: int) -> None:
    assert ledger.get("conservation_kind") == "write_refused", ledger
    assert ledger.get("rows_read") == n, ledger
    assert ledger.get("rows_quarantined") == n, ledger
    assert ledger.get("rows_written") == 0, ledger
    assert ledger.get("dest_count") is None, ledger
    assert ledger.get("rows_read_source") == "measured_read", ledger
    assert "not measured" not in str(ledger.get("note") or ""), ledger
    assert "Preflight" not in str(ledger.get("note") or ""), ledger


def _assert_job_keeps_ledger(fake_mongo, job_id: str, n: int) -> None:
    job = fake_mongo.jobs[job_id]
    assert job.get("status") == "failed"
    _assert_counted_and_held_out(dict(job.get("row_accounting") or {}), n)
    assert job.get("rejected_rows") == n
    _assert_counted_and_held_out(account_job(job).to_dict(), n)


def test_all_rows_held_out_ledger_counts_the_read(tmp_path: Path, fake_mongo) -> None:
    result = _run_all_rows_held_out(tmp_path, fake_mongo)

    assert result.success is False
    assert "were quarantined, so nothing was written" in str(result.error or "")
    _assert_counted_and_held_out(dict(result.row_accounting or {}), 5)
    _assert_job_keeps_ledger(fake_mongo, result.job_id, 5)


@pytest.mark.parametrize("sync_mode", ["dedupe_skip", "full_refresh_overwrite"])
def test_live_pg_to_mysql_all_rows_held_out_ledger(fake_mongo, sync_mode: str) -> None:
    """QA1's route: PostgreSQL → MySQL, every row refused at write."""
    require_ports(5432, 3306)
    src, dst = uniq("qa1_held_src"), uniq("qa1_held_dst")
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{src}" (id INTEGER PRIMARY KEY, name TEXT, qty TEXT)')
            cur.executemany(
                f'INSERT INTO "{src}" VALUES (%s, %s, %s)',
                [(i, f"person {i}", f"oops{i}") for i in range(1, 13)],
            )
        conn.commit()
    finally:
        conn.close()
    try:
        dest = mysql_endpoint(dst)
        dest.extra = {**(dest.extra or {}), "primary_key": "id"}
        request = TransferRequest(
            source=pg_endpoint(src),
            destination=dest,
            mappings=[
                {"source": "id", "target": "id", "target_type": "INTEGER"},
                {"source": "name", "target": "name", "target_type": "TEXT"},
                {"source": "qty", "target": "qty", "target_type": "INTEGER", "transform": "integer"},
            ],
            sync_mode=sync_mode,
            stream_contracts=[{"name": src, "primary_key": "id"}],
            skip_preflight=True,
            validation_mode="balanced",
        )
        job_id = "qa1h" + uuid.uuid4().hex[:16]
        fake_mongo.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)

        assert result.success is False, result
        assert "were quarantined, so nothing was written" in str(result.error or ""), result.error
        _assert_counted_and_held_out(dict(result.row_accounting or {}), 12)
        _assert_job_keeps_ledger(fake_mongo, job_id, 12)
    finally:
        drop_pg_table(src)
        drop_mysql_table(dst)
