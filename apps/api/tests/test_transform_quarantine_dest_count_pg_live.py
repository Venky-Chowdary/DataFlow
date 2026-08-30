"""Transform hold-out dest COUNT — live Postgres + honest warehouse skips.

The shared algorithm lives in TransformRunner (every dialect). This file
proves dest COUNT on Postgres when :5432 is open. BigQuery/Redshift emulators
are skipped with reason — not greened by absence.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest

from services.transform_models import DataTest, TransformModel
from services.transform_runner import TransformRunner

PG = dict(
    host="localhost",
    port=5432,
    database="dataflow",
    username="dataflow",
    password="dataflow",
)


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def _pg_connect():
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"],
        port=PG["port"],
        database=PG["database"],
        user=PG["username"],
        password=PG["password"],
    )
    conn.autocommit = True
    return conn


def _pg_count(table: str) -> int:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            )
            if int(cur.fetchone()[0]) == 0:
                return 0
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _pg_nulls(table: str, column: str) -> int:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}" WHERE "{column}" IS NULL')
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _pg_drop(*tables: str) -> None:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f'DROP TABLE IF EXISTS public."{table}" CASCADE')
    finally:
        conn.close()


@pytest.mark.skipif(not _reachable("127.0.0.1", 5432), reason="Postgres not reachable")
def test_live_pg_not_null_holdout_dest_count(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    suffix = uuid.uuid4().hex[:8]
    src = f"xf_src_{suffix}"
    model_name = f"stg_xf_{suffix}"
    q_table = f"{model_name}_df_quarantine"
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}" CASCADE')
            cur.execute(f'CREATE TABLE public."{src}" (id INTEGER, email TEXT)')
            cur.execute(
                f'INSERT INTO public."{src}" (id, email) VALUES '
                "(1, 'a@example.com'), (2, NULL), (3, 'c@example.com')"
            )
    finally:
        conn.close()

    runner = TransformRunner(
        {"type": "postgresql", **PG, "schema": "public"},
        dialect="postgresql",
        schema="public",
        project_id=f"pg-holdout-{suffix}",
        workspace_id="ws-pg-holdout",
    )
    try:
        result = runner.run(
            [
                TransformModel(
                    name=model_name,
                    sql=f"SELECT id, email FROM {{{{ source('{src}') }}}}",
                    materialization="table",
                    tests=[DataTest(test_type="not_null", column="email")],
                )
            ]
        )
        ledger = result.row_accounting()
        mart = _pg_count(model_name)
        nulls = _pg_nulls(model_name, "email")
        q_count = _pg_count(q_table)
        assert result.status == "partial", result.error
        assert mart == 2
        assert nulls == 0
        assert q_count == 1
        assert ledger["rows_written"] == 2
        assert ledger["rows_quarantined"] == 1
        assert mart + ledger["rows_quarantined"] == 3
        events = dlq.list_dlq_events(job_id=f"xform-pg-holdout-{suffix}")
        assert events
        finding = events[0]["details"]["rejected_details"][0]
        assert finding.get("values")
        assert str(finding["values"].get("id")) in {"2", "2.0"}
    finally:
        _pg_drop(src, model_name, q_table)


@pytest.mark.parametrize(
    "label,host,port",
    (
        ("bigquery_emulator", "127.0.0.1", 9050),
        ("redshift_emulator", "127.0.0.1", 5439),
    ),
)
def test_transform_holdout_warehouse_emulator_not_live(label: str, host: str, port: int):
    if not _reachable(host, port):
        pytest.skip(
            f"{label} not reachable on {host}:{port} — transform hold-out is the "
            "shared TransformRunner path, proven on sqlite + live Postgres"
        )
    pytest.skip(
        f"{label} port is open but this VM has no certified transform warehouse "
        "fixture (no dest COUNT artifact)"
    )
