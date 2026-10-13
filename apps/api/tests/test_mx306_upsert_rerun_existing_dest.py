"""QA MX3-06 — run 2 of PG→PG incremental_upsert into the existing table.

QA saw run 2 issue ``CREATE TABLE`` on the destination run 1 created
(suspected: a quoted mixed-case name ``QA_E2E_...`` resolved case-folded).
Pinned live with mixed-case quoted names, single and composite keys: run 2
merges into the existing table (row count and updated value), run 3 is a
quiet no-op.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_rt02_incremental_append_l1_dest_count import (  # noqa: F401
    _count,
    _endpoint,
    _pg,
    isolated,
    pytestmark,
)
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import TransferRequest


def _upsert(fake, src: str, dst: str, pk: list[str]):
    job_id = "mx306" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=_endpoint(src),
            destination=_endpoint(dst),
            sync_mode="incremental_upsert",
            stream_contracts=[{
                "name": src, "cursor_field": "updated_at", "primary_key": pk,
                "sync_mode": "incremental_upsert", "selected": True,
            }],
            skip_preflight=True,
            validation_mode="strict",
        ),
        job_id,
    )


@pytest.mark.parametrize("composite", [False, True], ids=["single_key", "composite_key"])
def test_pg_pg_mixed_case_upsert_run2_merges_into_existing_table(isolated, composite):  # noqa: F811
    suffix = uuid.uuid4().hex[:8]
    src, dst = f"QA_Mx306_S_{suffix}", f"QA_Mx306_D_{suffix}"
    conn = _pg()
    pk = ["region", "id"] if composite else ["id"]
    try:
        with conn.cursor() as cur:
            region = "region text NOT NULL, " if composite else ""
            cur.execute(
                f'CREATE TABLE "{src}" ({region}id int NOT NULL, amount numeric(10,2), '
                f"updated_at timestamp NOT NULL, PRIMARY KEY ({', '.join(pk)}))"
            )
            sel = "'eu', " if composite else ""
            cur.execute(
                f'INSERT INTO "{src}" SELECT {sel}i, i * 1.5, '
                "timestamp '2026-01-01' + i * interval '1 minute' FROM generate_series(1, 20) i"
            )
        first = _upsert(isolated, src, dst, pk)
        assert first.success, first.error
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE \"{src}\" SET amount = 999, updated_at = timestamp '2026-03-01' "
                "WHERE id = 1"
            )
            cur.execute(
                f'INSERT INTO "{src}" SELECT {sel}i, i * 1.5, timestamp \'2026-02-01\' '
                "FROM generate_series(21, 23) i"
            )
        second = _upsert(isolated, src, dst, pk)
        assert second.success, second.error
        ddl = " | ".join(str(x) for x in (second.ddl_executed or []))
        assert "already exists" not in ddl.lower(), ddl
        assert _count(conn, dst) == 23
        with conn.cursor() as cur:
            cur.execute(f'SELECT amount FROM "{dst}" WHERE id = 1')
            assert float(cur.fetchone()[0]) == 999.0
        third = _upsert(isolated, src, dst, pk)
        assert third.success, third.error
        assert _count(conn, dst) == 23
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}", "{dst}"')
        conn.close()
