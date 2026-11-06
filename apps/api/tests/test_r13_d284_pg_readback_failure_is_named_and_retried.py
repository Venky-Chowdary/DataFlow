"""D284 — a correct overwrite into PostgreSQL is not failed as "verifier unavailable".

QA: an overwrite wrote ids 1..5 exactly (CHECK and PK intact), yet the job
FAILED "Strict reconciliation requires an independent destination read-back;
verifier unavailable for 'postgresql'". PostgreSQL has a verifier: the
full-table read-back raised, ``verify_postgres_table`` swallowed the driver
error and returned ``-1``, and the reconcile, holding no reason and no retry,
blamed a missing verifier.

Now the swallowed reason is recorded, a transient read failure is retried with
the shared retry budget, and an unrecoverable one is reported by its reason.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.reconciliation as reconciliation  # noqa: E402
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
def tables(monkeypatch, tmp_path):
    try:
        import psycopg2

        conn = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow",
                                password="dataflow", connect_timeout=3)
        conn.autocommit = True
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    for prefix in ("DATAFLOW_", "DATAWRAP_"):
        monkeypatch.setenv(f"{prefix}RETRY_BASE_DELAY_SECONDS", "0.01")
        monkeypatch.setenv(f"{prefix}RETRY_JITTER", "false")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    tag = uuid.uuid4().hex[:8]
    import sqlite3

    # A cross-engine source: Gate-8 re-reads the whole PostgreSQL table
    # (a PG -> PG copy proves by in-engine digests instead).
    src = str(tmp_path / "src.db")
    lite = sqlite3.connect(src)
    lite.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, label TEXT)")
    lite.executemany("INSERT INTO t VALUES (?,?,?)",
                     [(1, 1, "a"), (2, 2, ""), (3, 3, "  "), (4, 4, None), (5, 5, "東京")])
    lite.commit()
    lite.close()
    dst = f"r13_d284_d_{tag}"
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{dst}" (id int PRIMARY KEY, n int CHECK (n > 0), label text)')
        cur.execute(f"INSERT INTO public.\"{dst}\" VALUES (99, 9, 'old')")
    yield conn, src, dst
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{dst}"')
    conn.close()


def _fail_full_readback(monkeypatch, *, times: int, error: Exception) -> dict:
    real = reconciliation.verify_postgres_table
    seen = {"n": 0}

    def _flaky(**kwargs):
        if kwargs.get("written_ids") is None and seen["n"] < times:
            seen["n"] += 1
            note = getattr(reconciliation, "note_readback_failure", None)
            if note is not None:
                note(error)
            return -1, ""
        return real(**kwargs)

    monkeypatch.setattr(reconciliation, "verify_postgres_table", _flaky)
    return seen


def _run(src: str, dst: str):
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=src, table="t"),
        destination=_ep(dst),
        mappings=[{"source": c, "target": c} for c in ("id", "n", "label")],
        sync_mode="full_refresh_overwrite", skip_preflight=True, validation_mode="strict",
    )
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


def _landed(conn, dst):
    with conn.cursor() as cur:
        cur.execute(f'SELECT id, n, label FROM public."{dst}" ORDER BY id')
        return cur.fetchall()


def test_transient_readback_failure_is_retried_and_the_correct_overwrite_completes(tables, monkeypatch):
    conn, src, dst = tables
    import psycopg2

    seen = _fail_full_readback(
        monkeypatch, times=1,
        error=psycopg2.OperationalError("server closed the connection unexpectedly"),
    )
    result = _run(src, dst)
    assert seen["n"] == 1
    assert result.success is True, result.error
    assert "verifier unavailable" not in (result.error or "")
    assert _landed(conn, dst) == [(1, 1, "a"), (2, 2, ""), (3, 3, "  "), (4, 4, None), (5, 5, "東京")]


def test_unrecoverable_readback_failure_names_its_reason(tables, monkeypatch):
    conn, src, dst = tables
    import psycopg2

    _fail_full_readback(
        monkeypatch, times=99,
        error=psycopg2.errors.InsufficientPrivilege("permission denied for table x"),
    )
    result = _run(src, dst)
    assert result.success is False
    err = result.error or ""
    assert "verifier unavailable" not in err
    assert "could not read the postgresql destination back" in err
    assert "permission denied" in err
