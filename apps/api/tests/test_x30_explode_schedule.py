"""X-30: explode_rows struct_policy must survive the schedule path end-to-end,
and the preflight sample-transform (G5 dry-run) must validate exploded rows.

Two defects fixed together:

1. ``dry_run_sample`` validated raw source rows against mappings. An explode
   mapping introduces ``<col>_elem`` / ``<col>_idx`` headers that only exist
   after ``materialize_struct_policies`` runs — the dry-run reported
   "Source column missing: tags_elem" and never validated the element values
   the writer would actually bind. The dry-run now materializes the sample
   through the same struct policies the write path uses.

2. The schedule path must land exploded rows: PG source rows carrying a
   JSON array column, mapping ``tags`` with ``struct_policy=explode_rows``
   plus ``tags_elem -> tag``. The schedule run must write one destination row
   per array element, not one row per parent with the raw JSON blob.

The PG half skips cleanly when localhost:5432 lacks the dataflow container.
"""

from __future__ import annotations

import socket
import sys
import time
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _pg_up() -> bool:
    try:
        socket.create_connection(("localhost", 5432), timeout=1).close()
    except OSError:
        return False
    try:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            connection_string="",
            ssl=False,
        )
        conn.close()
        return True
    except Exception:
        return False


def test_dry_run_sample_validates_exploded_rows():
    """G5 sees the element column + element values, not the raw parent row."""
    from services.json_intelligence import ARRAY_POLICY_EXPLODE
    from services.transform_engine import dry_run_sample

    headers = ["id", "tags"]
    rows = [["1", '["a","b","c"]'], ["2", '["x"]']]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "tags", "target": "tags", "struct_policy": ARRAY_POLICY_EXPLODE},
        {"source": "tags_elem", "target": "tag"},
    ]
    ok, errors = dry_run_sample(
        headers=headers,
        sample_rows=rows,
        mappings=mappings,
        column_types={"id": "TEXT", "tags": "TEXT"},
    )
    assert ok, errors


def test_dry_run_sample_reports_a_bad_exploded_element():
    """A cast failure on an exploded ELEMENT is named — explode cannot hide it."""
    from services.json_intelligence import ARRAY_POLICY_EXPLODE
    from services.transform_engine import dry_run_sample

    headers = ["id", "tags"]
    rows = [["1", '["1","not-a-number"]'], ["2", '["3"]']]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "tags", "target": "tags", "struct_policy": ARRAY_POLICY_EXPLODE},
        {
            "source": "tags_elem",
            "target": "tag_num",
            "transform": "cast_number",
            "target_type": "INTEGER",
        },
    ]
    ok, errors = dry_run_sample(
        headers=headers,
        sample_rows=rows,
        mappings=mappings,
        column_types={"id": "TEXT", "tags": "TEXT"},
    )
    assert not ok
    assert any("tags_elem" in e and "tag_num" in e for e in errors), errors


pg_only = pytest.mark.skipif(not _pg_up(), reason="df-pg not on localhost:5432")

_TABLE = f"x30_orders_{uuid.uuid4().hex[:8]}"


def _connect(db: str = "dataflow"):
    import psycopg2  # noqa: F401
    from connectors.postgresql_conn import get_connection

    return get_connection(
        host="localhost",
        port=5432,
        database=db,
        username="dataflow",
        password="dataflow",
        connection_string="",
        ssl=False,
    )


def _exec(sql: str, db: str = "dataflow") -> None:
    conn = _connect(db)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()


def _dst_rows(db: str, table: str) -> list[tuple]:
    conn = _connect(db)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, tag FROM {table} ORDER BY id, tag")
            return list(cur.fetchall())
    finally:
        conn.close()


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    import services.schedule_store as schedule_store

    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    return tmp_path


def _wait_job(mongo, job_id: str, timeout: float = 240.0) -> dict:
    deadline = time.time() + timeout
    job: dict = {}
    while time.time() < deadline:
        job = mongo.get_job(job_id) or {}
        if job.get("status") in {"completed", "failed", "cancelled"}:
            return job
        time.sleep(1)
    return job


@pg_only
def test_schedule_run_lands_exploded_rows(isolated):
    """Regression: a schedule with explode_rows mappings writes one dest row
    per array element — the element lands in the mapped column, the parent id
    is duplicated per element."""
    from services.connector_store import create_connector
    from services.json_intelligence import ARRAY_POLICY_EXPLODE
    from services.schedule_runner import _dispatch_transfer
    from services.schedule_store import create_schedule
    from src.services.mongodb_service import get_mongodb_service

    _exec(f"DROP TABLE IF EXISTS public.{_TABLE} CASCADE")
    _exec(
        f"CREATE TABLE public.{_TABLE} ("
        "id serial PRIMARY KEY, tags text)"
    )
    _exec(
        f"INSERT INTO public.{_TABLE} (tags) VALUES "
        "('[\"a\",\"b\",\"c\"]'), ('[\"x\"]')"
    )

    src = create_connector(
        {
            "name": "x30-pg-src",
            "type": "postgresql",
            "role": "source",
            "host": "localhost",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "ssl": False,
        }
    )
    dst = create_connector(
        {
            "name": "x30-pg-dst",
            "type": "postgresql",
            "role": "destination",
            "host": "localhost",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "ssl": False,
        }
    )
    # Structural policies need a signed Migration Risk Contract — the schedule
    # API carries the draft on the mapping and plan hydration signs it.
    def _contract(column: str) -> dict:
        return {
            "column": column,
            "source_type": "text",
            "destination_type": "text",
            "approved_by": "qa-x30",
            "reason": "explode_rows approved for schedule regression",
            "execution_policy": "CAST_AND_CONTINUE",
        }

    sched = create_schedule(
        {
            "name": "x30-explode",
            "source_connector_id": src.id,
            "source_table": _TABLE,
            "dest_connector_id": dst.id,
            "dest_table": f"{_TABLE}_dst",
            "interval": "daily",
            "sync_mode": "full_refresh_overwrite",
            "mappings": [
                {
                    "source": "id",
                    "target": "id",
                    "confidence": 1.0,
                    "target_type": "INTEGER",
                },
                {
                    "source": "tags",
                    "target": "tags",
                    "confidence": 1.0,
                    "struct_policy": ARRAY_POLICY_EXPLODE,
                    "target_type": "TEXT",
                    "risk_contract": _contract("tags"),
                },
                {
                    "source": "tags_elem",
                    "target": "tag",
                    "confidence": 1.0,
                    "struct_derived": True,
                    "struct_parent": "tags",
                    "target_type": "TEXT",
                    "risk_contract": _contract("tags_elem"),
                },
            ],
        }
    )
    assert sched.enabled is True, sched.enabled_note

    mongo = get_mongodb_service()
    try:
        job = _wait_job(mongo, _dispatch_transfer(sched.id, allow_paused=True))
        assert job.get("status") == "completed", job.get("error")
        # 2 parents -> 4 children; id duplicated per element, tag = element.
        assert _dst_rows("dataflow", f"{_TABLE}_dst") == [
            (1, "a"),
            (1, "b"),
            (1, "c"),
            (2, "x"),
        ]
    finally:
        _exec(f"DROP TABLE IF EXISTS public.{_TABLE} CASCADE")
        _exec(f"DROP TABLE IF EXISTS public.{_TABLE}_dst CASCADE")
