"""D216 - pgvector document layout: correct metadata must pass its own proof.

QA: MySQL/MariaDB (31 tricky rows) -> new pgvector table, document layout,
strict. The job FAILED 'pgvector document metadata does not hold N of 31
source row(s) value for value (fields: id, name, ...)' although the stored
metadata was right. Round 13 fixed the instant and nested-JSON parts; on
this code a row with DECIMAL(18,6) 99999999999.999999 still failed.

Root cause: ``read_document_metadata`` let psycopg2 decode the ``jsonb``
column with ``json.loads``. JSONB holds the number exactly (``->>`` reads
99999999999.999999), but the proof compared the source decimal with the
float 100000000000.0. The message also listed every carried field instead
of the one that differed.

Fix: read ``metadata::text`` and parse it with ``json_loads_exact``; JSON
values canonicalise with exact numbers on both sides; the proof names the
differing field(s).
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.pgvector_document_verify import compare_documents  # noqa: E402

BIG = "99999999999.999999"


def test_an_exact_decimal_equals_its_exact_jsonb_read_back():
    fields = [("id", "id"), ("amount", "amount"), ("payload", "payload")]
    source = [{"id": 1, "amount": Decimal(BIG), "payload": '{"x": 99999999999.999999, "s": "日本 🎉"}'}]
    stored = [{"id": 1, "amount": Decimal(BIG), "payload": {"x": Decimal(BIG), "s": "日本 🎉"}}]
    assert compare_documents(source, stored, fields, {"id", "amount"})["passed"]
    rounded = [{"id": 1, "amount": 100000000000.0, "payload": {"x": Decimal(BIG), "s": "日本 🎉"}}]
    proof = compare_documents(source, rounded, fields, {"id", "amount"})
    assert not proof["passed"] and proof["differing_fields"] == ["amount"]


def test_a_real_mismatch_names_its_field():
    fields = [("id", "id"), ("name", "name"), ("amount", "amount")]
    source = [{"id": i, "name": f"n{i}", "amount": f"{i}.50"} for i in (1, 2, 3)]
    stored = [{"id": i, "name": f"n{i}", "amount": float(f"{i}.5")} for i in (1, 2, 3)]
    assert compare_documents(source, stored, fields, {"id", "amount"})["passed"]
    stored[1] = {**stored[1], "name": "tampered"}
    proof = compare_documents(source, stored, fields, {"id", "amount"})
    assert proof["unmatched_source_rows"] == 1 and proof["differing_fields"] == ["name"]


# ------------------------------------------------------------ live MySQL -> pgvector
def _engines():
    from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

    if not (pg_up() and mysql_up()):
        pytest.skip("PostgreSQL and MySQL must be reachable")
    psycopg2 = pytest.importorskip("psycopg2")
    pymysql = pytest.importorskip("pymysql")
    pg, my = pg_creds(), mysql_creds()
    pgc = psycopg2.connect(host=pg["host"], port=pg["port"], dbname=pg["database"],
                           user=pg["username"], password=pg["password"])
    pgc.autocommit = True
    with pgc.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
        if not cur.fetchone():
            pgc.close()
            pytest.skip("pgvector extension not installed")
    myc = pymysql.connect(host=my["host"], port=my["port"], user=my["username"],
                          password=my["password"], database=my["database"], autocommit=True)
    return pg, my, pgc, myc


def _rows():
    long = "Lorem ipsum 日本語 🎉 " * 60  # several chunks
    out = []
    for i in range(1, 32):
        out.append((
            i,
            ["Zoë ✓", "", "  ", None, "日本語 🎉"][i % 5],
            None if i % 7 == 0 else (BIG if i == 2 else ("-0.000001" if i == 3 else f"{i}.500000")),
            None if i % 6 == 0 else f"2026-01-{(i % 28) + 1:02d} 03:04:05",
            i % 2,
            None if i % 4 == 0 else f'{{"k": {i}, "x": {BIG if i == 2 else "1.5"}, "arr": [1, 2], "n": null}}',
            long if i in (3, 9) else ("note" if i % 3 else ""),
            2**40 + i,
        ))
    return out


@pytest.mark.timeout(300)
def test_mysql_decimal_edge_into_pgvector_document_layout_passes_its_proof(monkeypatch):
    pg, my, pgc, myc = _engines()
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Mongo(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields or {})
            return True

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    tag = uuid.uuid4().hex[:8]
    src, dst = f"d216_s_{tag}", f"d216_d_{tag}"
    try:
        with myc.cursor() as cur:
            cur.execute(
                f"CREATE TABLE `{src}` (id INT PRIMARY KEY, name VARCHAR(50), amount DECIMAL(18,6), "
                "seen TIMESTAMP NULL, flag TINYINT(1), payload JSON, note TEXT, big BIGINT)"
            )
            cur.executemany(f"INSERT INTO `{src}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", _rows())
        dest = EndpointConfig(
            kind="database", format="pgvector", host=pg["host"], port=pg["port"], database=pg["database"],
            username=pg["username"], password=pg["password"], schema="public", table=dst,
            extra={"vector_layout": "document", "embedding_model": "hash/32"},
        )
        source = EndpointConfig(kind="database", format="mysql", host=my["host"], port=my["port"],
                                database=my["database"], username=my["username"], password=my["password"],
                                schema=my["database"], table=src)
        req = TransferRequest(source=source, destination=dest, sync_mode="full_refresh_overwrite",
                              validation_mode="strict", source_timezone="UTC")
        result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
        assert result.success is True, result.error
        with pgc.cursor() as cur:
            # (MySQL itself stores a JSON number as a double, so payload.x is
            # whatever MySQL holds; the DECIMAL column is exact end to end.)
            cur.execute(f"""SELECT metadata->>'amount', jsonb_typeof(metadata->'payload') FROM public."{dst}" """
                        "WHERE source_id = '2' ORDER BY chunk_index LIMIT 1")
            assert cur.fetchone() == (BIG, "object")
            cur.execute(f'SELECT COUNT(DISTINCT source_id), COUNT(*) FROM public."{dst}"')
            docs, chunks = cur.fetchone()
        assert docs == 31 and chunks > 31
    finally:
        with myc.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        with pgc.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dst}"')
        myc.close()
        pgc.close()
