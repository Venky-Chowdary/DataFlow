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


def _root_files() -> set[str]:
    from services.procedure_source import callable_spool_root

    root = callable_spool_root()
    return {str(p) for p in root.rglob("df-proc-*.jsonl")} if root.is_dir() else set()


def _unscoped_spools() -> list:
    from services import procedure_source

    return [s for s in procedure_source._SPOOLS.values() if not s.job_id]


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
    before = _root_files()
    result = _run(monkeypatch, tmp_path, job_id)
    assert result.success is True, result.error
    assert result.records_transferred == 50
    assert _job_files(job_id) == []
    # Nothing anywhere under the spool root either (a peek used to spool unscoped).
    assert _root_files() - before == set()
    assert _unscoped_spools() == []


def test_a_reread_after_the_stream_closed_is_released_by_the_job_scope(monkeypatch, tmp_path):
    """Gate-8 / proof re-reads after the stream's own close refill a spool; the job scope removes it."""
    import src.transfer.engine as engine_mod
    from services import procedure_source

    job_id = "tf1reread" + uuid.uuid4().hex[:8]
    refilled: list[int] = []
    real = engine_mod.run_reconciliation

    def reread_then_reconcile(*args, **kwargs):
        src = str(tmp_path / "src.db")
        batch = procedure_source.read_callable_batch(_cfg(src, job_id), offset=0, limit=5)
        refilled.append(len(_job_files(job_id)))
        assert len(batch.rows) == 5
        return real(*args, **kwargs)

    monkeypatch.setattr(engine_mod, "run_reconciliation", reread_then_reconcile)
    result = _run(monkeypatch, tmp_path, job_id)
    assert result.success is True, result.error
    assert refilled == [1], "the re-read spooled under the job"
    assert _job_files(job_id) == []


def test_a_new_column_reaches_the_next_job(monkeypatch, tmp_path):
    """Job B must not reuse job A's extract: a column added in between is delivered."""
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())
    src, _dst = _db(tmp_path)

    def job(dst: str):
        sqlite3.connect(dst).close()
        request = TransferRequest(
            source=EndpointConfig(
                kind="database", format="sqlite", database=src, table="src_rows",
                extra={"source_read_mode": "query", "source_query": "SELECT * FROM src_rows"},
            ),
            destination=EndpointConfig(kind="database", format="sqlite", database=dst, table="dst_rows"),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )
        return UniversalTransferEngine().execute_tracked(request, "tf1stale" + uuid.uuid4().hex[:8])

    first = job(str(tmp_path / "a.db"))
    assert first.success is True, first.error
    conn = sqlite3.connect(src)
    conn.execute("ALTER TABLE src_rows ADD COLUMN extra_col TEXT")
    conn.execute("UPDATE src_rows SET extra_col = 'E' || id")
    conn.commit()
    conn.close()
    second = job(str(tmp_path / "b.db"))
    assert second.success is True, second.error
    conn = sqlite3.connect(tmp_path / "b.db")
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(dst_rows)").fetchall()]
        first_row = conn.execute("SELECT id, note, extra_col FROM dst_rows ORDER BY id").fetchone()
    finally:
        conn.close()
    assert cols == ["id", "note", "extra_col"]
    assert first_row == (1, "n1", "E1")
    assert _unscoped_spools() == []


def test_an_unscoped_read_from_offset_zero_never_reuses_an_old_extract(tmp_path):
    from services import procedure_source

    src, _dst = _db(tmp_path)
    cfg = _cfg(src, "")
    first = procedure_source.read_callable_batch(cfg, offset=0, limit=100)
    conn = sqlite3.connect(src)
    conn.execute("INSERT INTO src_rows VALUES (51, 'n51')")
    conn.commit()
    conn.close()
    try:
        second = procedure_source.read_callable_batch(cfg, offset=0, limit=100)
        paged = procedure_source.read_callable_batch(cfg, offset=50, limit=10)  # paging still works
    finally:
        procedure_source.close_callable_spool()
    assert len(first.rows) == 50 and len(second.rows) == 51
    assert [r[0] for r in paged.rows] == ["51"]


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


def test_each_xdist_worker_gets_its_own_spool_folder():
    """Workers share the controller's env: one shared folder made each worker's
    orphan guard flag the others' live spools (re-review)."""
    from tests.conftest import _worker_spool_dir

    assert _worker_spool_dir("/s", "gw0") == "/s/gw0"
    assert _worker_spool_dir("/s", "gw1") != _worker_spool_dir("/s", "gw0")
    assert _worker_spool_dir("/s", None) == "/s"
    assert _worker_spool_dir("/s", "../x") == "/s/x"


def test_xdist_worker_process_uses_a_worker_subfolder(tmp_path):
    import os
    import subprocess
    import sys

    api_root = Path(__file__).resolve().parents[1]
    code = (
        "import os, sys; sys.path.insert(0, %r); import tests.conftest; "
        "print(os.environ['DATAFLOW_CALLABLE_SPOOL_DIR'])" % str(api_root)
    )
    env = {**os.environ, "PYTEST_XDIST_WORKER": "gw3", "DATAFLOW_CALLABLE_SPOOL_DIR": str(tmp_path)}
    env.pop("_DF_SPOOL_WORKER_DIR_SET", None)
    env.pop("DATAWRAP_CALLABLE_SPOOL_DIR", None)
    out = subprocess.run([sys.executable, "-c", code], cwd=api_root, env=env, capture_output=True, text=True, timeout=300)
    assert out.returncode == 0, out.stderr[-2000:]
    assert out.stdout.strip().splitlines()[-1] == str(tmp_path / "gw3")
