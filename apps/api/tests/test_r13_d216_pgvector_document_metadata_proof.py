"""D216 — the pgvector document layout passes its own metadata check when it is right.

QA: MySQL (31 tricky rows) -> new pgvector table, document layout, strict. Every
job FAILED "pgvector document metadata does not hold 30 of 31 source row(s)
value for value" while the stored metadata was correct; the JSON payload was
stored as an escaped string inside metadata, not a nested object.

Measured on 3e258e90f: the writer stores a MySQL TIMESTAMP as an RFC 3339
instant (``2026-01-02T03:04:05Z``) but the document proof passed no instant
fields to ``compare_documents``, so every row with a timestamp compared the
naive source text to the zoned string and "failed"; a JSON column landed as its
escaped text, and inside JSONB a nested array of numbers became the text
"[1,2]". Now the proof reads instant fields off the stored documents, the
writer nests declared-JSON values, and JSONB keeps nested arrays.
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


def _rows():
    out = []
    for i in range(1, 32):
        out.append((
            i,
            ["Zoë ✓", "", "  ", None, "日本語"][i % 5],
            None if i % 7 == 0 else f"{i}.50",
            None if i % 6 == 0 else f"2026-01-{(i % 28) + 1:02d} 03:04:05",
            None if i % 5 == 0 else "2026-10-30 23:59:59",
            i % 2,
            None if i % 4 == 0 else json.dumps({"k": i, "s": "v", "arr": [1, 2], "n": None}),
            "note" if i % 3 else "",
        ))
    return out


def test_mysql_into_pgvector_document_layout_passes_and_nests_json(monkeypatch):
    try:
        import psycopg2
        import pymysql

        pg = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow",
                              password="dataflow", connect_timeout=3)
        pg.autocommit = True
        with pg.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
            if not cur.fetchone():
                pytest.skip("pgvector extension not installed")
        my = pymysql.connect(host="localhost", port=3306, user="dataflow", password="dataflow",
                             database="dataflow", autocommit=True, connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"engine unavailable: {exc}")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    tag = uuid.uuid4().hex[:8]
    src, dst = f"r13_d216_s_{tag}", f"r13_d216_d_{tag}"
    try:
        with my.cursor() as cur:
            cur.execute(
                f"CREATE TABLE `{src}` (id INT PRIMARY KEY, name VARCHAR(50), amount DECIMAL(12,2), "
                "seen TIMESTAMP NULL, dt DATETIME NULL, flag TINYINT(1), payload JSON, note TEXT)"
            )
            cur.executemany(f"INSERT INTO `{src}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", _rows())
        dest = EndpointConfig(
            kind="database", format="pgvector", host="localhost", port=5432, database="dataflow",
            username="dataflow", password="dataflow", schema="public", table=dst,
            extra={"vector_layout": "document", "content_column": "note", "skip_chunking": True},
        )
        source = EndpointConfig(kind="database", format="mysql", host="localhost", port=3306,
                                database="dataflow", username="dataflow", password="dataflow",
                                schema="dataflow", table=src)
        for _run in (1, 2):  # second run into the table DataFlow created
            req = TransferRequest(source=source, destination=dest, sync_mode="full_refresh_overwrite",
                                  skip_preflight=True, validation_mode="strict")
            result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
            assert result.success is True, result.error
            assert result.records_transferred == 31
        with pg.cursor() as cur:
            cur.execute(f'SELECT DISTINCT ON (source_id) metadata FROM public."{dst}" ORDER BY source_id, chunk_index')
            docs = {int(m["id"]): m for (m,) in cur.fetchall()}
        assert docs[1]["payload"] == {"k": 1, "s": "v", "arr": [1, 2], "n": None}
        assert docs[4]["payload"] is None
        assert docs[1]["seen"] == "2026-01-02T03:04:05Z" and docs[6]["seen"] is None
        assert docs[1]["name"] == "" and docs[2]["name"] == "  " and docs[3]["name"] is None and docs[4]["name"] == "日本語"
        assert docs[7]["amount"] is None
    finally:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dst}"')
        my.close()
        pg.close()


def test_the_proof_still_names_a_real_instant_or_payload_change():
    from datetime import datetime

    from services.pgvector_document_verify import compare_documents, stored_instant_keys

    fields = [("id", "id"), ("seen", "seen"), ("payload", "payload")]
    source = [
        {"id": 1, "seen": datetime(2026, 1, 2, 3, 4, 5), "payload": '{"k": 1, "arr": [1, 2]}'},
        {"id": 2, "seen": None, "payload": None},
    ]
    good = [
        {"id": 1, "seen": "2026-01-02T03:04:05Z", "payload": {"k": 1, "arr": [1, 2]}},
        {"id": 2, "seen": None, "payload": None},
    ]
    assert stored_instant_keys(good, fields) == {"seen"}
    assert compare_documents(source, good, fields, {"id"}, set(), stored_instant_keys(good, fields))["passed"]
    for tampered in (
        [{**good[0], "seen": "2026-01-02T03:04:06Z"}, good[1]],
        [{**good[0], "payload": {"k": 1, "arr": "[1,2]"}}, good[1]],
    ):
        proof = compare_documents(source, tampered, fields, {"id"}, set(), stored_instant_keys(tampered, fields))
        assert proof["passed"] is False and proof["unmatched_source_rows"] == 1
