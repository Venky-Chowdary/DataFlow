"""D55 — a table DataFlow created could not be reloaded when column names
contained spaces, dashes or accents.

Run 1 creates the PostgreSQL table with sanitised names (``First Name`` ->
``First_Name``, ``Café`` -> ``Caf_``). Run 2 mapped the original spelling onto
those names and judged it a weak, review-grade match (the identity test only
forgave underscores), so append was blocked below the confidence floor; an
overwrite then read the original spelling as new columns, and its read-back
looked the SOURCE table up by the destination's names and digested nothing.

Live PostgreSQL, SQLite source, preflight ON, strict.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _pg():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
        conn = psycopg2.connect(
            host="127.0.0.1", dbname="dataflow", user="dataflow",
            password=os.environ.get("PGPASSWORD", "dataflow"),
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not available: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request):
    job_id = "d55" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.mark.parametrize("rerun_mode", ["full_refresh_append", "full_refresh_overwrite"])
def test_rerun_into_a_table_with_sanitised_column_names(tmp_path, isolated, rerun_mode):
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    conn.execute('CREATE TABLE t ("First Name" TEXT, "Café" TEXT, "a-b" TEXT, k INTEGER)')
    conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", [("a", "b", "c", 1), ("d", "e", "f", 2)])
    conn.commit()
    conn.close()

    pg = _pg()
    table = "d55_" + uuid.uuid4().hex[:8]
    cur = pg.cursor()
    source = EndpointConfig(kind="database", format="sqlite", database=str(src), table="t")
    dest = EndpointConfig(
        kind="database", format="postgresql", host="127.0.0.1", port=5432,
        database="dataflow", username="dataflow",
        password=os.environ.get("PGPASSWORD", "dataflow"), schema="public", table=table,
    )
    try:
        first = _run(isolated, TransferRequest(
            source=source, destination=dest,
            sync_mode="full_refresh_overwrite", validation_mode="strict",
        ))
        assert first.success, first.error
        second = _run(isolated, TransferRequest(
            source=source, destination=dest,
            sync_mode=rerun_mode, validation_mode="strict",
        ))
        assert second.success, second.error
        cur.execute(f"SELECT count(*) FROM {table}")
        expected = 4 if rerun_mode == "full_refresh_append" else 2
        assert cur.fetchone()[0] == expected
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position",
            (table,),
        )
        assert [r[0] for r in cur.fetchall()] == ["First_Name", "Caf_", "a_b", "k"]
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        pg.close()
