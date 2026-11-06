"""D101 — an approved upsert into an existing keyless table runs the DDL it disclosed.

QA: upsert into an existing PostgreSQL table with no unique key — green
preflight, raw "could not create unique index" at run time, and DataFlow
silently added a UNIQUE INDEX to the user's table. Devin's fix makes Validate
disclose the CREATE UNIQUE INDEX and refuse it until approved; the MCP
``ddl_acknowledgement`` carries the approval (``ddl_changes_acknowledged``).

Measured on 3e258e90f: on a PostgreSQL -> PostgreSQL route the approved run
still failed "there is no unique or exclusion constraint matching the ON
CONFLICT specification". The incremental engine copy merged its staging
table with ON CONFLICT straight into the keyless table; only the row writer
runs the approved index DDL. Such a merge target now declines the engine copy.
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


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


def _ep(table: str) -> EndpointConfig:
    return EndpointConfig(kind="database", format="postgresql", host="localhost", port=5432,
                          database="dataflow", username="dataflow", password="dataflow",
                          schema="public", table=table)


@pytest.fixture()
def pg(monkeypatch):
    try:
        import psycopg2

        conn = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow",
                                password="dataflow", connect_timeout=3)
        conn.autocommit = True
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    tag = uuid.uuid4().hex[:8]
    src, dst = f"r13_d101_s_{tag}", f"r13_d101_d_{tag}"
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{src}" (id int PRIMARY KEY, name text, '
                    "updated_at timestamp NOT NULL DEFAULT now())")
        cur.executemany(f'INSERT INTO public."{src}" (id, name) VALUES (%s, %s)',
                        [(i, ["n", "", "  ", None, "東京"][i % 5]) for i in range(1, 21)])
        cur.execute(f'CREATE TABLE public."{dst}" (id int, name text, updated_at timestamp)')
        cur.executemany(f'INSERT INTO public."{dst}" (id, name) VALUES (%s, %s)',
                        [(i, "old") for i in range(1, 11)])
    yield conn, src, dst
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
        cur.execute(f'DROP TABLE IF EXISTS public."{dst}"')
    conn.close()


def _run(src: str, dst: str, *, approved: bool):
    req = TransferRequest(
        source=_ep(src), destination=_ep(dst),
        mappings=[{"source": c, "target": c, "confidence": 0.99, "approved": True}
                  for c in ("id", "name", "updated_at")],
        sync_mode="incremental_deduped", validation_mode="strict", skip_preflight=False,
        ddl_changes_acknowledged=approved,
        stream_contracts=[{"name": src, "primary_key": "id", "cursor_field": "updated_at",
                           "cursor_semantics": "modification_timestamp",
                           "sync_mode": "incremental_deduped", "selected": True}],
    )
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


def _indexes(conn, dst):
    with conn.cursor() as cur:
        cur.execute("SELECT indexname FROM pg_indexes WHERE tablename = %s", (dst,))
        return [r[0] for r in cur.fetchall()]


def test_unapproved_run_discloses_the_index_and_changes_nothing(pg):
    conn, src, dst = pg
    result = _run(src, dst, approved=False)
    assert result.success is False
    assert f'CREATE UNIQUE INDEX "uidx_{dst}_id"' in (result.error or "")
    assert _indexes(conn, dst) == []


def test_approved_run_creates_the_disclosed_index_and_upserts(pg):
    conn, src, dst = pg
    result = _run(src, dst, approved=True)
    assert result.success is True, result.error
    assert _indexes(conn, dst) == [f"uidx_{dst}_id"]
    with conn.cursor() as cur:
        cur.execute(f'SELECT id, name FROM public."{dst}" ORDER BY id')
        rows = cur.fetchall()
    assert [r[0] for r in rows] == list(range(1, 21))
    assert rows[0][1] == "" and rows[1][1] == "  " and rows[2][1] is None and rows[3][1] == "東京"
    # A second run into the now-keyed table takes no DDL and stays exact.
    again = _run(src, dst, approved=False)
    assert again.success is True, again.error


def test_live_uniqueness_check_needs_the_exact_key():
    from services.upsert_ddl_gate import dest_unique_covering_columns

    # A unique index on (id, region) does not make id unique.
    assert dest_unique_covering_columns([], [{"columns": ["id", "region"]}], ["id"]) is False
    assert dest_unique_covering_columns([], [{"columns": ["ID"]}], ["id"]) is True
