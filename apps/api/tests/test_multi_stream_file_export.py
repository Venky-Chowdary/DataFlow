"""A multi-stream run into a file export is refused before it reads or writes.

A file export is the buffered single-table route: it reads the request's one
source table and writes one file. A run that selected several streams exported
only the first (6 of 14 rows here, 30 of 54 in the original probe), the other
streams were never read, and the job reported success with Gate-8 green — it
checked the one file against the one table it had read. The local exports
folder and the hosted export bucket behaved the same way.

That route has no way to hold several streams, so the run is refused by name
before any read or write (the same start-of-run refusal as data residency and
the local-disk policy). A SQLite export is a database written to a file and
still lands one table per stream; a single-stream export is unchanged.
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

# Different sizes, so a run that exported one stream cannot pass for all of them.
TABLES = {"people": 6, "orders": 3, "items": 5}

FORMATS = ("csv", "tsv", "jsonl", "parquet", "excel")


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    from services import audit_log
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(dict(fields or {}))
            return True

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    return fake


def _source(tmp_path: Path) -> Path:
    path = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            for name, rows in TABLES.items():
                conn.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, label TEXT)")
                conn.executemany(
                    f"INSERT INTO {name} VALUES (?, ?)",  # noqa: S608 - fixed test names
                    [(i, f"{name}-{i}") for i in range(1, rows + 1)],
                )
    finally:
        conn.close()
    return path


def _run(store, src: Path, destination: EndpointConfig, streams=tuple(TABLES)):
    job_id = "ms2f" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table=streams[0]),
        destination=destination,
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": n, "primary_key": ["id"], "selected": True} for n in streams],
        skip_preflight=True,
        validation_mode="strict",
    )
    return job_id, UniversalTransferEngine().execute_tracked(request, job_id)


def _files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file()) if root.exists() else []


def _assert_refused(store, job_id: str, result) -> None:
    assert not result.success, "a multi-stream file export reported success"
    err = str(result.error or "")
    assert "3 streams" in err and all(n in err for n in TABLES), err
    assert int(result.records_transferred or 0) == 0
    job = store.jobs[job_id]
    assert job["status"] == "failed", job.get("status")
    assert "3 streams" in str(job.get("error") or ""), job.get("error")


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("output_path", ["ms2_named", ""], ids=["named", "generated"])
def test_multi_stream_file_export_is_refused_before_writing(tmp_path, store, fmt, output_path):
    src = _source(tmp_path)
    destination = EndpointConfig(
        kind="file_export",
        format=fmt,
        output_path=f"{output_path}.{fmt}" if output_path else "",
        table="people",
    )
    job_id, result = _run(store, src, destination)
    _assert_refused(store, job_id, result)
    assert _files(tmp_path / "exports") == [], "the refusal must come before any file is written"


def test_kind_file_spelling_is_refused_too(tmp_path, store):
    """``kind="file"`` is normalised to ``file_export`` later; the refusal reads it as one."""
    src = _source(tmp_path)
    job_id, result = _run(
        store, src, EndpointConfig(kind="file", format="csv", output_path="ms2_k.csv", table="people")
    )
    _assert_refused(store, job_id, result)
    assert _files(tmp_path / "exports") == []


def test_hosted_export_bucket_is_refused_before_upload(tmp_path, store, monkeypatch):
    moto = pytest.importorskip("moto")
    from tests.test_file_export_object_store import BUCKET, _hosted, _keys, _not_hosted

    _not_hosted(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with moto.mock_aws():
        import boto3

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        monkeypatch.setenv("DATAWRAP_EXPORT_BUCKET", BUCKET)
        _hosted(monkeypatch)
        src = _source(tmp_path)
        job_id, result = _run(store, src, EndpointConfig(kind="file_export", format="csv", table="people"))
        _assert_refused(store, job_id, result)
        assert _keys(client) == [], "nothing may be uploaded for a refused run"
    assert _files(tmp_path / "exports") == []


def test_single_stream_file_export_is_unchanged(tmp_path, store):
    src = _source(tmp_path)
    job_id, result = _run(
        store,
        src,
        EndpointConfig(kind="file_export", format="csv", output_path="ms2_one.csv", table="orders"),
        streams=("orders",),
    )
    assert result.success, result.error
    assert int(result.records_transferred or 0) == TABLES["orders"]
    assert [p.name for p in _files(tmp_path / "exports")] == ["ms2_one.csv"]


def test_multi_stream_sqlite_export_still_lands_every_stream(tmp_path, store):
    """A SQLite export is a database in a file: one table per stream, every row."""
    src = _source(tmp_path)
    job_id, result = _run(
        store, src, EndpointConfig(kind="file_export", format="sqlite", output_path="ms2_all.sqlite")
    )
    assert result.success, result.error
    assert int(result.records_transferred or 0) == sum(TABLES.values())
    (landed,) = [p for p in _files(tmp_path / "exports") if p.suffix == ".sqlite"]
    conn = sqlite3.connect(str(landed))
    try:
        counts = {n: conn.execute(f"SELECT count(*) FROM {n}").fetchone()[0] for n in TABLES}  # noqa: S608
    finally:
        conn.close()
    assert counts == TABLES
