"""PostgreSQL -> existing PostgreSQL append: binary COPY only onto the same types.

Found while reproducing D294 on a PostgreSQL destination. The binary COPY fast
path checked the live column types only for overwrite (D195). An append onto
an existing table whose column is a lossless widening (INT4 -> INT8) or a
cast target (TEXT -> INT4, the D294 quarantine pair) streamed the source's
wire format and the whole run failed with the driver's 'insufficient data
left in message'. The row path widens, casts or quarantines per cell.
"""

from __future__ import annotations

import os
import uuid


os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.r13_mapping_harness import (  # noqa: E402
    drop,
    endpoint,
    isolated,  # noqa: F401 - fixture
    run_sql,
)


def _run(fake, src_def, dst_def, rows, *, skip_preflight=False):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    tag = uuid.uuid4().hex[:8]
    src, dst = f"pgca_s_{tag}", f"pgca_d_{tag}"
    run_sql("postgresql", f'CREATE TABLE "{src}" (id INT PRIMARY KEY, {src_def})')
    run_sql("postgresql", f'INSERT INTO "{src}" VALUES (%s, %s)', params=rows)  # nosec B608
    run_sql("postgresql", f'CREATE TABLE "{dst}" (id INT PRIMARY KEY, {dst_def})')
    try:
        job_id = "pgca" + uuid.uuid4().hex[:12]
        fake.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=endpoint("postgresql", src),
                destination=endpoint("postgresql", dst),
                mappings=[
                    {"source": c, "target": c, "confidence": 1.0} for c in ("id", "qty")
                ],
                sync_mode="full_refresh_append",
                validation_mode="balanced",
                skip_preflight=skip_preflight,
            ),
            job_id,
        )
        landed = run_sql("postgresql", f'SELECT id, qty FROM "{dst}" ORDER BY id')  # nosec B608
        return result, fake.get_job(job_id) or {}, landed
    finally:
        drop("postgresql", src)
        drop("postgresql", dst)


def test_widening_append_onto_existing_table_lands_every_row(isolated):  # noqa: F811
    endpoint("postgresql", "probe")
    result, _job, landed = _run(isolated, "qty INT", "qty BIGINT", [(1, 10), (2, 20)])
    assert result.success, result.error
    assert landed == [(1, 10), (2, 20)]


def test_cast_append_quarantines_the_bad_cell_and_lands_the_rest(isolated):  # noqa: F811
    endpoint("postgresql", "probe")
    result, job, landed = _run(
        isolated, "qty TEXT", "qty INT", [(1, "10"), (2, "NOTANUMBER")], skip_preflight=True
    )
    assert result.success, result.error
    assert job.get("status") == "completed_with_quarantine", job.get("status")
    assert landed == [(1, 10)]


def test_same_types_still_take_the_binary_copy(isolated):  # noqa: F811
    endpoint("postgresql", "probe")
    result, job, landed = _run(isolated, "qty INT", "qty INT", [(1, 10), (2, 20)])
    assert result.success, result.error
    assert landed == [(1, 10), (2, 20)]
    summary = job.get("destination_summary") or {}
    assert str(summary.get("load_method") or "").startswith("copy_binary"), summary.get(
        "load_method"
    )
