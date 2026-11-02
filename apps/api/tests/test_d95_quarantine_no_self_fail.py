"""D67/D95: a balanced run that quarantines rows must not fail Gate-8 on its
own quarantine and roll the landed rows back.

Pre-fix the L3 verification ladder digested the whole source re-read —
including the rows the writer held out to quarantine — while the destination
digest covered only the landed rows. The digests could never agree, the
fidelity veto flipped ``passed`` to False, and the job undid the 39 good
rows it had just landed ("Landed rows match. 1 quarantined row(s) are in the
source digest..."). The fix excludes the quarantined keys (resolved from the
writer's rejected_details) from the compared source population and lets L3
recompute both digests on the same scope — the same re-scoping rule the
batch-keys path already used.
"""
from __future__ import annotations

import uuid

import psycopg2
import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

PG_CONN = dict(
    host="127.0.0.1", port=5432, database="dataflow",
    user="dataflow", password="dataflow",
)
PG_CFG = dict(
    host="127.0.0.1", port=5432, database="dataflow",
    username="dataflow", password="dataflow",
)


def _conn():
    c = psycopg2.connect(**PG_CONN)
    c.autocommit = True
    return c


def _run_balanced_check_job(sync_mode: str):
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"d95_s_{sfx}", f"d95_d_{sfx}"
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{src}" (id int PRIMARY KEY, name text, amt numeric(12,2))'
            )
            cur.executemany(
                f'INSERT INTO "{src}" VALUES (%s,%s,%s)',
                [(i, f"n{i}", 10.5) for i in range(1, 40)] + [(40, "bad", -5.25)],
            )
            cur.execute(
                f'CREATE TABLE "{dst}" (id int PRIMARY KEY, name text, '
                f"amt numeric(12,2) CHECK (amt >= 0))"
            )
    finally:
        conn.close()
    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=src, **PG_CFG,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=dst, **PG_CFG,
        ),
        sync_mode=sync_mode,
        validation_mode="balanced",
        stream_contracts=[{
            "name": src, "sync_mode": sync_mode,
            "primary_key": "id", "selected": True,
        }],
        skip_preflight=True,
    )
    res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "{dst}"')
            dest_rows = int(cur.fetchone()[0])
    finally:
        conn.close()
    d = res.__dict__ if hasattr(res, "__dict__") else {}
    recon = d.get("reconciliation") or {}
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE "{src}"')
            cur.execute(f'DROP TABLE "{dst}"')
    finally:
        conn.close()
    return res, recon, dest_rows


@pytest.mark.parametrize("sync_mode", ["incremental", "upsert"])
def test_d95_balanced_quarantine_does_not_fail_job(sync_mode):
    """1 CHECK violation quarantined; 39 rows must stay landed, job must pass.

    The upsert leg also proves the COPY fast path declines when the
    destination carries a CHECK constraint — one violating row previously
    failed the whole INSERT…ON CONFLICT batch with no quarantine."""
    res, recon, dest_rows = _run_balanced_check_job(sync_mode)
    assert dest_rows == 39, (
        f"good rows were rolled back (dest={dest_rows}); "
        f"error={str(getattr(res, 'error', ''))[:200]}"
    )
    assert recon.get("passed") is True, (
        f"job failed on its own quarantine: {str(recon.get('message'))[:300]}"
    )
    assert int(recon.get("rejected_rows") or 0) == 1
