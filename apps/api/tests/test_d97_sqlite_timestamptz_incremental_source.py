"""D97 — incremental sync out of a DataFlow-created SQLite table always failed.

QA: Postgres (int pk, text, timestamptz) -> SQLite by overwrite, then
incremental_upsert / incremental_append SQLite -> Postgres on ``updated_at``:
preflight safe_to_start, the job FAILED "must be real number, not str", 0 rows.

Root cause: DataFlow creates the aware column on SQLite as ``TIMESTAMPTZ``
(services/type_system.py) and stores ISO-8601 text. SQLAlchemy's SQLite dialect
does not know that token and resolves it by affinity to NUMERIC, whose Decimal
result processor formats every value with ``%f``. The cursor reader
(connectors/generic_sql.py read_table_cursor_batch) selects through the
reflected table, so the first fetch raised on the text. The overwrite path
survived only because read_table_batch silently falls back to a raw SELECT.

Real SQLite -> SQLite runs through the engine.
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

_CSV = b"id,name,updated_at\n" + b"".join(
    f"{i},n{i},2024-01-0{i}T10:00:00+00:00\n".encode() for i in range(1, 6)
)


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request: TransferRequest):
    job_id = "d97" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _seed(fake, tmp_path: Path) -> Path:
    """A SQLite table DataFlow itself created (TIMESTAMPTZ column)."""
    mid = tmp_path / "mid.sqlite"
    seeded = _run(
        fake,
        TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_content=_CSV,
            source_filename="t.csv",
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(mid), table="t"
            ),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="strict",
        ),
    )
    assert seeded.success, seeded.error
    conn = sqlite3.connect(mid)
    try:
        ddl = conn.execute("SELECT sql FROM sqlite_master WHERE name='t'").fetchone()[0]
    finally:
        conn.close()
    assert "TIMESTAMPTZ" in ddl.upper(), ddl
    return mid


def _incremental(src: Path, dst: Path, mode: str) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode=mode,
        stream_contracts=[{
            "name": "t", "cursor_field": "updated_at",
            "cursor_semantics": "modification_timestamp",
            "sync_mode": mode, "selected": True, "primary_key": ["id"],
        }],
        skip_preflight=False,
        validation_mode="strict",
    )


def _rows(path: Path) -> list[tuple]:
    conn = sqlite3.connect(path)
    try:
        return conn.execute("SELECT id, name FROM t ORDER BY id").fetchall()
    finally:
        conn.close()


@pytest.mark.parametrize("mode", ["incremental_upsert", "incremental_append"])
def test_incremental_out_of_dataflow_created_sqlite_table(tmp_path, isolated, mode):
    src = _seed(isolated, tmp_path)
    dst = tmp_path / "dst.sqlite"

    first = _run(isolated, _incremental(src, dst, mode))
    assert first.success, first.error  # D97: 'must be real number, not str'
    assert first.records_transferred == 5
    assert _rows(dst) == [(i, f"n{i}") for i in range(1, 6)]

    quiet = _run(isolated, _incremental(src, dst, mode))
    assert quiet.success, quiet.error
    assert quiet.records_transferred == 0

    conn = sqlite3.connect(src)
    with conn:
        conn.execute("INSERT INTO t VALUES (6, 'n6', '2024-01-07T10:00:00+00:00')")
    conn.close()
    delta = _run(isolated, _incremental(src, dst, mode))
    assert delta.success, delta.error
    assert delta.records_transferred == 1
    assert _rows(dst)[-1] == (6, "n6") and len(_rows(dst)) == 6


def test_reflected_timestamptz_reads_as_text_under_its_own_token(tmp_path):
    import sqlalchemy as sa

    import connectors.sqlite_common  # noqa: F401 - registers the carriers

    db = tmp_path / "r.sqlite"
    conn = sqlite3.connect(db)
    with conn:
        conn.execute('CREATE TABLE "t" ("id" INTEGER, "at" TIMESTAMPTZ, "tt" timetz(6))')
        conn.execute("INSERT INTO t VALUES (1, '2024-01-01T10:00:00+00:00', '10:00:00+02:00')")
    conn.close()
    engine = sa.create_engine(f"sqlite:///{db}")
    try:
        table = sa.Table("t", sa.MetaData(), autoload_with=engine)
        assert str(table.c.at.type) == "TIMESTAMPTZ"
        assert str(table.c.tt.type) == "TIMETZ"
        with engine.connect() as c:
            row = c.execute(sa.select(table)).one()
        assert row.at == "2024-01-01T10:00:00+00:00"
        assert row.tt == "10:00:00+02:00"
    finally:
        engine.dispose()
