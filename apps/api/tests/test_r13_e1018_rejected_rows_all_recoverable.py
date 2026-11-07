"""E1-018 — every rejected row is recoverable; repeated reasons never collapse records.

QA: PostgreSQL fixture -> existing MySQL / MariaDB table, invalid rows to
quarantine. The job reported 7 rejected but quarantine held 5.

Measured on 3e258e90f: PG -> MariaDB (local_infile ON) took the COPY + LOAD
DATA fast path. ``copy_pg_mysql._consume`` ran ``SHOW WARNINGS`` and then the
bulk-session restore (``SET SESSION ...``) on the same cursor before
``fetchall()``, so every truncation / clamp warning was read as "none": the
over-long codes and out-of-range quantities loaded silently changed, the key
census failed the job with 0 rows, and no row was quarantined. MySQL takes the
same path wherever ``local_infile`` is ON (CI, production).

Also covered: the durable DLQ read deduplicated on (row, column, reason), so two
distinct records refused for the same reason in the same column with a repeated
row number collapsed into one finding.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402

_ROWS = [
    (1, "TOOLONGCODE", 1), (2, "TOOLONGCODE", 2), (3, "OK3", 300000), (4, "", 4),
    (5, "TOOLONGCODE", 5), (6, "東京", 300000), (7, "  ", 7), (8, None, 8),
    (12, "TOOLONGCODE", 12), (13, "OK13", 300000),
]
_REJECTED = {1, 2, 3, 5, 6, 12, 13}


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


def _pg():
    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow", user="dataflow",
        password="dataflow", connect_timeout=3,
    )
    conn.autocommit = True
    return conn


def _my(port: int):
    import pymysql

    return pymysql.connect(
        host="localhost", port=port, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True, connect_timeout=3,
    )


@pytest.mark.parametrize("dest_format,port", [("mysql", 3306), ("mariadb", 3307)])
def test_every_rejected_row_is_quarantined(dest_format, port, monkeypatch):
    try:
        pg, my = _pg(), _my(port)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"engine unavailable: {exc}")
    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    tag = uuid.uuid4().hex[:8]
    src, dst = f"r13_e1018_s_{tag}", f"r13_e1018_d_{tag}"
    try:
        with pg.cursor() as cur:
            cur.execute(f'CREATE TABLE public."{src}" (id int PRIMARY KEY, code text, qty int)')
            cur.executemany(f'INSERT INTO public."{src}" VALUES (%s,%s,%s)', _ROWS)
        with my.cursor() as cur:
            cur.execute(f"CREATE TABLE `{dst}` (id INT PRIMARY KEY, code VARCHAR(5), qty SMALLINT)")
        job_id = uuid.uuid4().hex[:24]
        req = TransferRequest(
            source=EndpointConfig(
                kind="database", format="postgresql", host="localhost", port=5432,
                database="dataflow", username="dataflow", password="dataflow",
                schema="public", table=src,
            ),
            destination=EndpointConfig(
                kind="database", format=dest_format, host="localhost", port=port,
                database="dataflow", username="dataflow", password="dataflow",
                schema="dataflow", table=dst,
            ),
            mappings=[{"source": c, "target": c} for c in ("id", "code", "qty")],
            sync_mode="full_refresh_append",
            skip_preflight=True,
            validation_mode="balanced",
        )
        result = UniversalTransferEngine().execute_tracked(req, job_id)
        job = fake.jobs.get(job_id, {})
        assert result.success is True, result.error
        assert job.get("status") == "completed_with_quarantine"
        details = job.get("rejected_details") or []
        rejected_ids = {int((d.get("values") or {}).get("id")) for d in details}
        assert rejected_ids == _REJECTED
        from services.quarantine_dlq import quarantine_details_from_dlq

        durable = quarantine_details_from_dlq(job_id)
        assert {int((d.get("values") or {}).get("id")) for d in durable} == _REJECTED
        with my.cursor() as cur:
            cur.execute(f"SELECT id, code, qty FROM `{dst}` ORDER BY id")
            landed = cur.fetchall()
        assert [r[0] for r in landed] == [4, 7, 8]
        assert landed[0][1] == "" and landed[1][1] == "  " and landed[2][1] is None
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dst}_df_quarantine`")
        pg.close()
        my.close()


def test_dlq_read_keeps_distinct_records_with_the_same_row_and_reason(monkeypatch):
    import services.quarantine_dlq as q

    reason = "value length 11 exceeds MySQL VARCHAR(5)"
    unit_a = [{"row": 1, "column": "code", "reason": reason, "value": "TOOLONGCODE",
               "values": {"id": "1", "code": "TOOLONGCODE"}}]
    unit_b = [{"row": 1, "column": "code", "reason": reason, "value": "TOOLONGCODE",
               "values": {"id": "2", "code": "TOOLONGCODE"}}]
    events = [
        {"action": "quarantine_chunk", "ts": "1", "details": {"chunk_index": 0, "rejected_details": unit_a}},
        {"action": "quarantine_chunk", "ts": "2", "details": {"chunk_index": 1, "rejected_details": unit_b}},
        # The same finding persisted twice still reads once.
        {"action": "quarantine_chunk", "ts": "3", "details": {"chunk_index": 1, "rejected_details": unit_b}},
    ]
    monkeypatch.setattr(q, "list_dlq_events", lambda **_kw: list(events))
    out = q.quarantine_details_from_dlq("job-e1018")
    assert sorted(d["values"]["id"] for d in out) == ["1", "2"]
