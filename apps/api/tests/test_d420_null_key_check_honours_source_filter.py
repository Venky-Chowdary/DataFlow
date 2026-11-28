"""D420 — the upsert NULL-key check counts the rows the source filter keeps.

QA r58-r75 (PG -> PG, PG -> SQL Server, PG -> MySQL): upsert on key ``id, d``
(or ``d``) with ``source_filter d is_not_null``, where only row 30 has a NULL
``d``, failed at 0 rows: "1 source row(s) have a NULL upsert key ... Fill the
key, filter those rows out, ..." - the filter the request already carried.

Root cause: the source probe counted NULL keys over the whole table
(``SELECT count(*) WHERE key IS NULL``), not over the rows the run reads (R3).
It now reads the NULL-key rows back with the columns the filter names and
counts only those the run's own filter predicate keeps. A filter that still
lets a NULL key through is refused, and the message names that filter instead
of recommending one.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.timeout(600)

KEEPS_NULL_KEY = {"column": "id", "op": "lte", "value": 30}
DROPS_NULL_KEY = {"column": "d", "op": "is_not_null"}


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(store, source, dest, key, source_filter):
    request = TransferRequest(
        source=source,
        destination=dest,
        sync_mode="upsert",
        validation_mode="strict",
        source_filter=source_filter,
        stream_contracts=[
            {"name": source.table, "primary_key": key, "sync_mode": "upsert", "selected": True}
        ],
    )
    job_id = "d420" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _assert_lands_29(result, dest_ids):
    assert result.success, result.error
    assert result.records_transferred == 29
    assert dest_ids() == list(range(1, 30))
    # AC-3: the job record says how many rows the filter excluded.
    assert int((result.destination_summary or {}).get("rows_source_filtered") or 0) == 1


def _assert_refused_naming_the_filter(result, dest_ids):
    assert not result.success
    text = str(result.error or "")
    assert "1 source row(s) that the source filter on id keeps have a NULL upsert key" in text, text
    assert "filter those rows out" not in text, text
    assert dest_ids() == []


def _sqlite(tmp_path: Path):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE r61 (id INTEGER PRIMARY KEY, d TEXT, v TEXT)")
    conn.executemany(
        "INSERT INTO r61 VALUES (?,?,?)",
        [(i, None if i == 30 else f"d{i}", f"v{i}") for i in range(1, 31)],
    )
    conn.commit()
    conn.close()

    def ids() -> list[int]:
        if not dst.exists():
            return []
        c = sqlite3.connect(dst)
        try:
            return [r[0] for r in c.execute("SELECT id FROM r61 ORDER BY id")]
        except sqlite3.OperationalError:
            return []
        finally:
            c.close()

    def ep(path: Path) -> EndpointConfig:
        return EndpointConfig(kind="database", format="sqlite", database=str(path), table="r61")

    return ep(src), ep(dst), ids


@pytest.mark.parametrize("key", [["id", "d"], ["d"]], ids=["id+d", "d"])
def test_sqlite_filter_that_drops_the_null_key_lets_the_upsert_run(tmp_path, store, key):
    source, dest, ids = _sqlite(tmp_path)
    _assert_lands_29(_run(store, source, dest, key, DROPS_NULL_KEY), ids)


def test_sqlite_filter_that_keeps_the_null_key_is_refused_and_named(tmp_path, store):
    source, dest, ids = _sqlite(tmp_path)
    _assert_refused_naming_the_filter(_run(store, source, dest, ["id", "d"], KEEPS_NULL_KEY), ids)


@pytest.mark.parametrize("key", [["id", "d"], ["d"]], ids=["id+d", "d"])
def test_pg_to_pg_filter_on_the_null_key(store, key):
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not available")
    src, dst = f"d420s_{uuid.uuid4().hex[:8]}", f"d420d_{uuid.uuid4().hex[:8]}"
    pg_sql(f"CREATE TABLE {src} (id INT PRIMARY KEY, d TEXT, v TEXT)")
    for i in range(1, 31):
        pg_sql(f"INSERT INTO {src} VALUES (%s, %s, %s)", (i, None if i == 30 else f"d{i}", f"v{i}"))

    def ids() -> list[int]:
        if pg_sql("SELECT to_regclass(%s)", (f"public.{dst}",), fetch=True)[0][0] is None:
            return []
        return [r[0] for r in pg_sql(f"SELECT id FROM {dst} ORDER BY id", fetch=True)]

    try:
        _assert_lands_29(_run(store, pg_endpoint(src), pg_endpoint(dst), key, DROPS_NULL_KEY), ids)
        pg_sql(f"DROP TABLE IF EXISTS {dst}")
        _assert_refused_naming_the_filter(
            _run(store, pg_endpoint(src), pg_endpoint(dst), key, KEEPS_NULL_KEY), ids
        )
    finally:
        pg_sql(f"DROP TABLE IF EXISTS {src}")
        pg_sql(f"DROP TABLE IF EXISTS {dst}")


def test_without_a_filter_the_whole_table_is_still_counted(tmp_path, store):
    """The unfiltered refusal is unchanged (D46)."""
    source, dest, ids = _sqlite(tmp_path)
    result = _run(store, source, dest, ["id", "d"], {})
    assert not result.success
    assert "1 source row(s) have a NULL upsert key" in str(result.error)
    assert ids() == []
