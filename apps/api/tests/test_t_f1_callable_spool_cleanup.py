"""T-F1 family: a CALL/SELECT source run never leaves a df-proc-*.jsonl spool behind.

Before: spools were removed only by an explicit ``close_callable_spool``. The
stream closed its own at the end, but a Gate-8 / proof re-read after that
re-filled one nobody closed; a CALL that failed mid-drain left its partial file;
a spool dropped without a close stayed on disk forever. ~1,000 files / 325 MB
accumulated in the shared ``/tmp/dataflow-callable`` and filled the disk.

After: the job scope closes the job's spools on every outcome, a failed drain
removes its file, a garbage-collected or exiting spool removes its file, and
the folder honours ``DATAFLOW_CALLABLE_SPOOL_DIR`` (a suite temp dir in tests).
"""

from __future__ import annotations

import gc
import sqlite3
import uuid
from pathlib import Path

import pytest

QUERY = "SELECT id, note FROM src_rows"


def _db(tmp_path: Path) -> tuple[str, str]:
    src, dst = str(tmp_path / "src.db"), str(tmp_path / "dst.db")
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE src_rows (id INTEGER PRIMARY KEY, note TEXT)")
    conn.executemany("INSERT INTO src_rows VALUES (?, ?)", [(i, f"n{i}") for i in range(1, 51)])
    conn.commit()
    conn.close()
    sqlite3.connect(dst).close()
    return src, dst


def _job_files(job_id: str) -> list[Path]:
    from services.procedure_source import _spool_dir

    folder = _spool_dir(job_id)
    return sorted(folder.glob("df-proc-*.jsonl")) if folder.is_dir() else []


def _run(monkeypatch, tmp_path, job_id: str):
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())
    src, dst = _db(tmp_path)
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=src, table="src_rows",
            extra={"source_read_mode": "query", "source_query": QUERY},
        ),
        destination=EndpointConfig(kind="database", format="sqlite", database=dst, table="dst_rows"),
        sync_mode="full_refresh_overwrite",
        mappings=[{"source": c, "target": c} for c in ("id", "note")],
        skip_preflight=True,
    )
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_successful_query_source_job_leaves_no_spool(monkeypatch, tmp_path):
    job_id = "tf1spool" + uuid.uuid4().hex[:10]
    result = _run(monkeypatch, tmp_path, job_id)
    assert result.success is True, result.error
    assert result.records_transferred == 50
    assert _job_files(job_id) == []


def test_failed_query_source_job_leaves_no_spool(monkeypatch, tmp_path):
    from services import procedure_source

    real = procedure_source._execute_to_jsonl

    def _fails_mid_drain(cfg, spec, path):
        real(cfg, spec, path)  # the file is written ...
        raise RuntimeError("connection lost mid-drain")  # ... then the CALL fails

    monkeypatch.setattr(procedure_source, "_execute_to_jsonl", _fails_mid_drain)
    job_id = "tf1spoolfail" + uuid.uuid4().hex[:8]
    result = _run(monkeypatch, tmp_path, job_id)
    assert result.success is False
    assert _job_files(job_id) == []


def _cfg(src: str, job_id: str) -> dict:
    return {
        "type": "sqlite", "database": src, "connection_string": f"sqlite:///{src}",
        "source_read_mode": "query", "source_query": QUERY, "job_id": job_id,
    }


def test_failed_drain_removes_its_partial_file(monkeypatch, tmp_path):
    from services import procedure_source

    def _boom(cfg, spec, path):
        path.write_text('["partial"]\n')
        raise RuntimeError("boom")

    monkeypatch.setattr(procedure_source, "_execute_to_jsonl", _boom)
    src, _dst = _db(tmp_path)
    job_id = "tf1drain" + uuid.uuid4().hex[:8]
    with pytest.raises(RuntimeError):
        procedure_source.read_callable_batch(_cfg(src, job_id), offset=0, limit=10)
    assert _job_files(job_id) == []


def test_spool_dropped_without_close_is_removed_by_gc(tmp_path):
    from services import procedure_source

    src, _dst = _db(tmp_path)
    job_id = "tf1gc" + uuid.uuid4().hex[:8]
    batch = procedure_source.read_callable_batch(_cfg(src, job_id), offset=0, limit=10)
    assert len(batch.rows) == 10
    assert len(_job_files(job_id)) == 1
    with procedure_source._SPOOL_LOCK:
        for key in [k for k, s in procedure_source._SPOOLS.items() if s.job_id == job_id]:
            procedure_source._SPOOLS.pop(key)
    gc.collect()
    assert _job_files(job_id) == []


def test_spool_folder_honours_the_env_var(monkeypatch, tmp_path):
    from services import procedure_source

    monkeypatch.setenv("DATAFLOW_CALLABLE_SPOOL_DIR", str(tmp_path / "spools"))
    assert procedure_source.callable_spool_root() == tmp_path / "spools"
    assert procedure_source._spool_dir("j1") == tmp_path / "spools" / "j1"


def test_suite_spools_are_off_the_shared_tmp_and_guarded(monkeypatch, tmp_path):
    import os

    from services import procedure_source
    from tests.conftest import _orphan_callable_spools

    assert os.environ.get("DATAFLOW_CALLABLE_SPOOL_DIR")
    assert procedure_source.callable_spool_root().name != "dataflow-callable"
    monkeypatch.setenv("DATAFLOW_CALLABLE_SPOOL_DIR", str(tmp_path / "guard"))
    orphan = tmp_path / "guard" / "jobx" / "df-proc-abc.jsonl"
    orphan.parent.mkdir(parents=True)
    orphan.write_text("[]\n")
    assert _orphan_callable_spools() == ["jobx/df-proc-abc.jsonl"]
