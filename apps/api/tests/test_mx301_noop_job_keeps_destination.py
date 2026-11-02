"""QA MX3-01 — a completed no-delta incremental job keeps its destination table.

Run 2 of an incremental route with no new source rows wrote no batch, so no
writer summary named the table; the completion record stored an empty
``destination_collection`` and job listings fell back to the database / engine
name (``destination="QA_E2E_sqlite.db"``, ``dest_table=null``). Live PG→PG and
PG→SQLite, append and upsert.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_rt02_incremental_append_l1_dest_count import (  # noqa: F401
    _endpoint,
    _pg,
    isolated,
    pytestmark,
)
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


@pytest.mark.parametrize("dest_kind", ["postgresql", "sqlite"])
@pytest.mark.parametrize("mode", ["incremental_append", "incremental_upsert"])
def test_no_delta_run_records_destination_table(isolated, tmp_path, dest_kind, mode):  # noqa: F811
    suffix = uuid.uuid4().hex[:8]
    src, dst = f"QA_Mx301_S_{suffix}", f"QA_Mx301_D_{suffix}"
    dest = (
        _endpoint(dst)
        if dest_kind == "postgresql"
        else EndpointConfig(
            kind="database", format="sqlite",
            connection_string=f"sqlite:///{tmp_path / 'dst.sqlite'}", table=dst,
        )
    )
    conn = _pg()

    def run():
        job_id = "mx301" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=_endpoint(src),
                destination=dest,
                sync_mode=mode,
                stream_contracts=[{
                    "name": src, "cursor_field": "updated_at", "primary_key": ["id"],
                    "sync_mode": mode, "selected": True,
                }],
                skip_preflight=True,
                validation_mode="strict",
            ),
            job_id,
        )
        return result, isolated.get_job(job_id) or {}

    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{src}" (id int PRIMARY KEY, updated_at timestamp NOT NULL)'
            )
            cur.execute(
                f'INSERT INTO "{src}" SELECT i, timestamp \'2026-01-01\' + i * interval '
                "'1 minute' FROM generate_series(1, 10) i"
            )
        first, first_doc = run()
        assert first.success, first.error
        assert first_doc.get("destination_collection") == dst
        second, second_doc = run()  # no new rows
        assert second.success, second.error
        assert second_doc.get("destination_collection") == dst, second_doc.get(
            "destination_collection"
        )
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}", "{dst}"')
        conn.close()
