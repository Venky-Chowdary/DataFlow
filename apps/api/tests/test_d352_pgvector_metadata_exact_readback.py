"""D352 — pgvector metadata 99999999999.999999 read back as 100000000000.0.

QA (round 38): a pgvector destination's metadata JSON showed
99999999999.999999 as 100000000000.0 while the job said it kept every source
value. Measured on 26ae371c1 with a live PostgreSQL 16 + pgvector 0.6: the
writer stores every digit (``metadata->>'amount'`` is ``99999999999.999999``,
``jsonb_typeof`` is ``number``) and the document proof reads ``metadata::text``
exactly, so the job's claim about the stored value is true. The rounding is
psycopg2's default jsonb decoder (``json.loads`` -> binary64) in the readers
DataFlow uses to show and re-read that metadata: the query playground, the
pgvector target sample and the pgvector checksum read-back. Those now decode
the stored text exactly, and the proof still fails a document whose stored
number really differs from the source.
"""

from __future__ import annotations

import os
import socket
import uuid
from decimal import Decimal
from types import SimpleNamespace

import pytest

psycopg2 = pytest.importorskip("psycopg2")

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

PG = {
    "host": os.environ.get("PGHOST") or "127.0.0.1",
    "port": int(os.environ.get("PGPORT") or 5432),
    "user": os.environ.get("PGUSER") or "dataflow",
    "password": os.environ.get("PGPASSWORD") or "dataflow",
    "dbname": os.environ.get("PGDATABASE") or "dataflow",
}
DEST = {
    "host": PG["host"],
    "port": PG["port"],
    "database": PG["dbname"],
    "username": PG["user"],
    "password": PG["password"],
}

# NUMERIC(20,6) edges, plus the wider literal the brief names (JSONB only).
EDGES = {
    1: "99999999999.999999",
    2: "0.1",
    3: "0.000001",
    4: "12345678901234.123456",
    5: "-99999999999.999999",
    6: "123456789012345.123456",
}


def _pg(*, vector: bool = False):
    try:
        socket.create_connection((PG["host"], PG["port"]), timeout=1).close()
    except OSError:
        pytest.skip(f"PostgreSQL not reachable on {PG['host']}:{PG['port']}")
    try:
        conn = psycopg2.connect(connect_timeout=3, **PG)
    except Exception as exc:  # noqa: BLE001 - environmental: wrong credentials
        pytest.skip(f"PostgreSQL login failed: {exc}")
    conn.autocommit = True
    if vector:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_available_extensions WHERE name = 'vector'")
            if not cur.fetchone():
                conn.close()
                pytest.skip("pgvector extension is not installed on this PostgreSQL")
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    return conn


def _doc_table(conn, amounts: dict[int, str]) -> str:
    """A pgvector-shaped document table (metadata JSONB) holding exact numbers."""
    table = "d352_doc_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {table} (id TEXT PRIMARY KEY, content TEXT, "
            "source_id TEXT, chunk_index INTEGER, metadata JSONB)"
        )
        for key, amount in amounts.items():
            cur.execute(
                f"INSERT INTO {table} VALUES (%s, %s, %s, 0, "
                "('{\"id\": ' || %s || ', \"amount\": ' || %s || '}')::jsonb)",
                (str(key), f"n{key}", str(key), str(key), amount),
            )
    return table


def _drop(conn, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")


def _exact(value) -> Decimal:
    assert not isinstance(value, bool)
    return Decimal(str(value))


def test_target_sample_reads_every_metadata_digit():
    from services.target_sample_vector import read_pgvector_target_sample

    conn = _pg()
    table = _doc_table(conn, EDGES)
    try:
        rows = read_pgvector_target_sample(
            DEST, schema="public", table_name=table, cols=None, keys=None,
            sort_key=None, limit=50,
        )
        got = {int(r["id"]): r["amount"] for r in rows}
        assert {k: _exact(v) for k, v in got.items()} == {
            k: Decimal(v) for k, v in EDGES.items()
        }, got
        # Keyed read (Gate-8 / delete checks) takes the same path.
        keyed = read_pgvector_target_sample(
            DEST, schema="public", table_name=table, cols=["id", "amount"],
            keys=["1"], sort_key="id", limit=5,
        )
        assert _exact(keyed[0]["amount"]) == Decimal("99999999999.999999"), keyed
    finally:
        _drop(conn, table)
        conn.close()


def test_pgvector_checksum_readback_tells_rounded_metadata_from_exact():
    from services.reconciliation import verify_pgvector_table

    conn = _pg()
    exact = _doc_table(conn, {1: "99999999999.999999"})
    rounded = _doc_table(conn, {1: "100000000000.0"})
    try:
        sums = [
            verify_pgvector_table(
                host=PG["host"], port=PG["port"], database=PG["dbname"],
                username=PG["user"], password=PG["password"], schema="public",
                connection_string="", ssl=False, table_name=t,
            )
            for t in (exact, rounded)
        ]
        assert sums[0][0] == sums[1][0] == 1
        assert sums[0][1] and sums[1][1]
        # Before D352 both read back as 100000000000.0: one checksum for two values.
        assert sums[0][1] != sums[1][1]
    finally:
        _drop(conn, exact)
        _drop(conn, rounded)
        conn.close()


@pytest.mark.parametrize("connector_type", ["pgvector", "postgresql"])
def test_query_playground_shows_metadata_digits(connector_type):
    from src.routers.query_router import _run_sql_query

    conn = _pg()
    table = _doc_table(conn, EDGES)
    try:
        connector = SimpleNamespace(
            type=connector_type, host=PG["host"], port=PG["port"], database=PG["dbname"],
            username=PG["user"], password=PG["password"], connection_string="",
            schema="public", warehouse="", ssl=False, extra={}, auth_mode="",
        )
        body = SimpleNamespace(
            query=f"SELECT id, metadata FROM {table} ORDER BY id", database="",
            params={}, limit=50,
        )
        rows, _columns, _schema, _truncated = _run_sql_query(connector, body)
        got = {int(r["id"]): r["metadata"] for r in rows}
        assert all(isinstance(m, dict) for m in got.values()), got  # still a parsed document
        assert {k: _exact(m["amount"]) for k, m in got.items()} == {
            k: Decimal(v) for k, v in EDGES.items()
        }, got
    finally:
        _drop(conn, table)
        conn.close()


# ------------------------------------------------------------- end to end


class _Mongo:
    def __init__(self):
        from tests.test_property2_golden_path_never_blocked import _FakeMongo

        class _M(_FakeMongo):
            def update_job_fields(self, job_id, fields):
                self.jobs.setdefault(job_id, {}).update(fields or {})
                return True

        self.svc = _M()


def _transfer(monkeypatch, src: str, dst: str):
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setenv("DATAFLOW_EMBEDDING_MODEL", "hash/32")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    mongo = _Mongo().svc
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: mongo)
    endpoint = dict(DEST, schema="public")
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table=src, **endpoint),
        destination=EndpointConfig(
            kind="database", format="pgvector", table=dst,
            extra={"vector_layout": "document", "content_column": "note", "skip_chunking": True},
            **endpoint,
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = uuid.uuid4().hex[:24]
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    return result, (mongo.jobs.get(job_id) or {}).get("reconciliation") or {}


@pytest.fixture
def numeric_source():
    conn = _pg(vector=True)
    tag = uuid.uuid4().hex[:8]
    src, dst = f"d352_src_{tag}", f"d352_dst_{tag}"
    values = {k: v for k, v in EDGES.items() if k != 6}  # 6 overflows NUMERIC(20,6)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, amount NUMERIC(20,6), note TEXT)")
        for key, amount in values.items():
            cur.execute(f"INSERT INTO {src} VALUES (%s, %s, %s)", (key, amount, f"n{key}"))
    try:
        yield conn, src, dst, values
    finally:
        _drop(conn, src)
        _drop(conn, dst)
        conn.close()


def test_pg_numeric_into_pgvector_metadata_is_exact_and_read_back_exactly(monkeypatch, numeric_source):
    from services.target_sample_vector import read_pgvector_target_sample

    conn, src, dst, values = numeric_source
    result, recon = _transfer(monkeypatch, src, dst)
    assert result.success is True, result.error
    proof = recon.get("document_value_proof") or {}
    assert recon.get("passed") is True and proof.get("passed") is True, recon
    with conn.cursor() as cur:
        cur.execute(f"SELECT source_id, metadata->>'amount' FROM {dst} ORDER BY source_id")
        stored = {int(k): Decimal(v) for k, v in cur.fetchall()}
    assert stored == {k: Decimal(v) for k, v in values.items()}
    # DataFlow's own read-back of that metadata says the same digits.
    rows = read_pgvector_target_sample(
        DEST, schema="public", table_name=dst, cols=None, keys=None, sort_key=None, limit=50,
    )
    assert {int(r["source_id"]): _exact(r["amount"]) for r in rows} == stored


def test_job_does_not_claim_every_value_when_metadata_was_rounded(monkeypatch, numeric_source):
    """A writer that floats the decimal is caught: no 'holds every source value'."""
    import connectors.writer_common as wc

    real = wc.vector_prepare_metadata

    def float64_round_trip(meta, *, keep_null=False):
        out = real(meta, keep_null=keep_null)
        return {k: float(v) if isinstance(v, Decimal) else v for k, v in out.items()}

    monkeypatch.setattr(wc, "vector_prepare_metadata", float64_round_trip)
    conn, src, dst, _values = numeric_source
    result, recon = _transfer(monkeypatch, src, dst)
    with conn.cursor() as cur:
        cur.execute(f"SELECT metadata->>'amount' FROM {dst} WHERE source_id = '1'")
        assert cur.fetchone()[0] == "100000000000.0"  # the loss really landed
    proof = recon.get("document_value_proof") or {}
    assert recon.get("passed") is False and proof.get("passed") is False, recon
    assert "amount" in (proof.get("differing_fields") or []), proof
    assert "holds every source value" not in str(recon.get("message") or "")
    assert result.success is False
