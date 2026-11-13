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


def _unscoped_spools() -> set[str]:
    """Paths of the unscoped (no job id) spools this process holds right now.

    Assertions compare a snapshot taken before the action with one after it:
    another test's live spool is not this action's leak (CI shard 3/6)."""
    from services import procedure_source

    return {str(s.path) for s in list(procedure_source._SPOOLS.values()) if not s.job_id}


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
    unscoped_before = _unscoped_spools()
    result = _run(monkeypatch, tmp_path, job_id)
    assert result.success is True, result.error
    assert result.records_transferred == 50
    assert _job_files(job_id) == []
    # Nothing anywhere under the spool root either (a peek used to spool unscoped).
    assert _root_files() - before == set()
    assert _unscoped_spools() - unscoped_before == set()


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
    unscoped_before = _unscoped_spools()

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
    assert _unscoped_spools() - unscoped_before == set()


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
        second = procedure_source.read_callable_batch(cfg, offset=0, limit=50)
        paged = procedure_source.read_callable_batch(cfg, offset=50, limit=10)  # paging still works
    finally:
        procedure_source.close_callable_spool()
    assert len(first.rows) == 50 and len(second.rows) == 50
    assert [r[0] for r in paged.rows] == ["51"]


def test_an_unscoped_read_that_reaches_the_end_releases_its_spool(tmp_path):
    """No job will close an unscoped spool: the read that exhausts it releases it."""
    from services import procedure_source

    src, _dst = _db(tmp_path)
    before = _unscoped_spools()
    page = procedure_source.read_callable_batch(_cfg(src, ""), offset=0, limit=20)
    held = _unscoped_spools() - before
    assert len(page.rows) == 20 and len(held) == 1, "a page short of the end keeps paging open"
    rest = procedure_source.read_callable_batch(_cfg(src, ""), offset=20, limit=100)
    assert len(rest.rows) == 30
    assert _unscoped_spools() - before == set()
    assert not any(Path(p).exists() for p in held)


@pytest.mark.parametrize("limit", [0, 7], ids=["exhausted", "stopped-early"])
def test_a_population_walk_of_a_query_source_leaves_no_spool(tmp_path, limit):
    """Preflight's population-fit walk (iter_stream_source_column_rows) of a
    CALL/SELECT source ran outside any job scope: its spool outlived the job
    (CI 3/6, test_mx3_11 left one before this file). It spools under its own
    scope now, released when the walk ends — exhausted or stopped early."""
    from src.transfer.models import EndpointConfig
    from src.transfer.source_peek import iter_stream_source_column_rows

    src, _dst = _db(tmp_path)
    source = EndpointConfig(
        kind="database", format="sqlite", database=src, table="src_rows",
        extra={"source_read_mode": "query", "source_query": QUERY},
    )
    before_unscoped, before_files = _unscoped_spools(), _root_files()
    rows = list(iter_stream_source_column_rows(source, ["id", "note"], limit=limit))
    assert len(rows) == (limit or 50)
    assert _unscoped_spools() - before_unscoped == set()
    assert _root_files() - before_files == set()


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
    orphan.unlink()  # a fake orphan; leave no df-proc-* file behind


def test_each_pytest_process_gets_its_own_spool_folder():
    """Workers and nested pytest runs inherit the parent's env: one shared folder
    made each process's orphan guard flag the others' live spools (re-review, CI)."""
    from tests.conftest import _worker_spool_dir

    assert _worker_spool_dir("/s", "gw0", 11) == "/s/gw0-p11"
    assert _worker_spool_dir("/s", "gw1", 11) != _worker_spool_dir("/s", "gw0", 11)
    assert _worker_spool_dir("/s", None, 12) == "/s/p12"
    assert _worker_spool_dir("/s", "../x", 13) == "/s/x-p13"


def test_this_process_spools_in_its_own_subfolder():
    import os

    from services.procedure_source import callable_spool_root

    root = callable_spool_root()
    assert root.name.endswith(f"p{os.getpid()}"), root
    assert str(root.parent) == os.environ["_DF_SPOOL_BASE"]


def test_a_child_process_uses_its_own_subfolder_of_the_inherited_base(tmp_path):
    import os
    import subprocess
    import sys

    api_root = Path(__file__).resolve().parents[1]
    code = (
        "import os, sys; sys.path.insert(0, %r); import tests.conftest; "
        "print(os.environ['DATAFLOW_CALLABLE_SPOOL_DIR'], os.getpid())" % str(api_root)
    )
    env = {**os.environ, "PYTEST_XDIST_WORKER": "gw3"}
    out = subprocess.run([sys.executable, "-c", code], cwd=api_root, env=env, capture_output=True, text=True, timeout=300)
    assert out.returncode == 0, out.stderr[-2000:]
    folder, pid = out.stdout.strip().splitlines()[-1].rsplit(" ", 1)
    assert folder == os.path.join(os.environ["_DF_SPOOL_BASE"], f"gw3-p{pid}")
    assert folder != os.environ["DATAFLOW_CALLABLE_SPOOL_DIR"]


def test_a_nested_pytest_passes_while_this_process_holds_a_live_spool(tmp_path):
    """The CI failure: a nested `python -m pytest` reported the parent's live spool as an orphan."""
    import os
    import subprocess
    import sys

    from services import procedure_source

    src, _dst = _db(tmp_path)
    job_id = "tf1nested" + uuid.uuid4().hex[:8]
    procedure_source.read_callable_batch(_cfg(src, job_id), offset=0, limit=5)
    try:
        assert _job_files(job_id), "the parent holds a live spool"
        api_root = Path(__file__).resolve().parents[1]
        probe = api_root / "tests" / f"test_zz_tf1_nested_probe_{uuid.uuid4().hex[:8]}.py"
        probe.write_text("def test_ok():\n    assert True\n")
        try:
            out = subprocess.run(
                [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "no:randomly",
                 f"--basetemp={tmp_path / 'nested'}", str(probe.relative_to(api_root))],
                cwd=api_root, env={**os.environ}, capture_output=True, text=True, timeout=600,
            )
        finally:
            probe.unlink(missing_ok=True)
        assert out.returncode == 0, (out.stdout[-3000:], out.stderr[-2000:])
        assert "spool" not in out.stdout.lower()
    finally:
        procedure_source.close_callable_spool(job_id=job_id)


# --- process exit, sibling processes, cancel ------------------------------------

_CHILD_SPOOL = r"""
import json, os, sqlite3, sys
sys.path.insert(0, os.getcwd())
from services import procedure_source

src, job_id = sys.argv[1], sys.argv[2]
cfg = {
    "type": "sqlite", "database": src, "connection_string": "sqlite:///" + src,
    "source_read_mode": "query", "source_query": "SELECT id, note FROM src_rows",
}
if job_id:
    cfg["job_id"] = job_id
batch = procedure_source.read_callable_batch(cfg, offset=0, limit=5)
# Held to the end on purpose: the process exits without close_callable_spool().
held = list(procedure_source._SPOOLS.values())
print(json.dumps({"rows": len(batch.rows), "files": [str(s.path) for s in held],
                  "exist": [s.path.is_file() for s in held]}))
"""


def _run_child(tmp_path: Path, spool_root: Path, job_id: str) -> dict:
    """Spool in a fresh interpreter that never imports tests.conftest (no suite rmtree)."""
    import json
    import os
    import subprocess
    import sys

    work = tmp_path / f"child-{uuid.uuid4().hex[:8]}"
    work.mkdir()
    src, _dst = _db(work)
    api_root = Path(__file__).resolve().parents[1]
    env = {
        k: v for k, v in os.environ.items()
        if k not in {"DATAWRAP_CALLABLE_SPOOL_DIR", "_DF_SPOOL_BASE", "_DF_SPOOL_PROCESS_PID"}
    }
    env["DATAFLOW_CALLABLE_SPOOL_DIR"] = str(spool_root)
    script = work / "child_spool.py"
    script.write_text(_CHILD_SPOOL)
    out = subprocess.run(
        [sys.executable, str(script), src, job_id],
        cwd=api_root, env=env, capture_output=True, text=True, timeout=300,
    )
    assert out.returncode == 0, out.stderr[-3000:]
    return json.loads(out.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize("job_id", ["tf1child-job", ""], ids=["job-scoped", "unscoped"])
def test_a_spool_created_in_a_subprocess_is_gone_after_it_exits(tmp_path, job_id):
    spool_root = tmp_path / "child-spools"
    seen = _run_child(tmp_path, spool_root, job_id)
    assert seen["rows"] == 5
    assert len(seen["files"]) == 1 and seen["exist"] == [True], seen
    spool_file = Path(seen["files"][0])
    assert spool_file.is_relative_to(spool_root)
    assert not spool_file.exists(), "process exit removed the spool"
    assert list(spool_root.rglob("df-proc-*.jsonl")) == []
    if job_id:
        assert not spool_file.parent.exists(), "the emptied job folder goes too"


def test_a_subprocess_exit_keeps_another_processes_live_spool(monkeypatch, tmp_path):
    """Exit cleanup is scoped to the exiting process's registry, not the shared folder."""
    from services import procedure_source

    shared = tmp_path / "shared-spools"
    monkeypatch.setenv("DATAFLOW_CALLABLE_SPOOL_DIR", str(shared))
    src, _dst = _db(tmp_path)
    job_id = "tf1sibling" + uuid.uuid4().hex[:8]
    procedure_source.read_callable_batch(_cfg(src, job_id), offset=0, limit=5)
    try:
        mine = _job_files(job_id)
        assert len(mine) == 1
        seen = _run_child(tmp_path, shared, "tf1sibling-child")
        assert not Path(seen["files"][0]).exists()
        assert _job_files(job_id) == mine and mine[0].is_file(), "the child left this process's spool alone"
    finally:
        procedure_source.close_callable_spool(job_id=job_id)
    assert _job_files(job_id) == []


def test_cancelled_query_source_job_leaves_no_spool(monkeypatch, tmp_path):
    """A cancel during the read, while the job's spool is on disk, leaves none behind."""
    from services import procedure_source
    from services.error_handling import TransferCancelled

    job_id = "tf1spoolcancel" + uuid.uuid4().hex[:8]
    real = procedure_source.read_callable_batch
    held: list[int] = []

    def _read_then_cancel(cfg, *args, **kwargs):
        batch = real(cfg, *args, **kwargs)
        if procedure_source.job_id_of(cfg) != job_id:
            return batch  # the pre-run peek spools under its own scope
        held.append(len(_job_files(job_id)))
        raise TransferCancelled("Transfer cancelled by user", rows_written=0)

    monkeypatch.setattr(procedure_source, "read_callable_batch", _read_then_cancel)
    result = _run(monkeypatch, tmp_path, job_id)
    assert result.success is False
    assert "cancel" in str(result.error or "").lower(), result.error
    assert held and held[0] == 1, "the cancel landed while the spool was on disk"
    assert _job_files(job_id) == []


def test_a_cancel_after_the_stream_closed_is_released_by_the_job_scope(monkeypatch, tmp_path):
    """The stream has already closed its own spool; a re-read refilled one and then
    the cancel lands. Only the job scope can remove that spool."""
    import src.transfer.engine as engine_mod
    from services import procedure_source
    from services.error_handling import TransferCancelled

    job_id = "tf1latecancel" + uuid.uuid4().hex[:8]
    refilled: list[int] = []

    def reread_then_cancel(*args, **kwargs):
        procedure_source.read_callable_batch(_cfg(str(tmp_path / "src.db"), job_id), offset=0, limit=5)
        refilled.append(len(_job_files(job_id)))
        raise TransferCancelled("Transfer cancelled by user", rows_written=50)

    monkeypatch.setattr(engine_mod, "run_reconciliation", reread_then_cancel)
    result = _run(monkeypatch, tmp_path, job_id)
    assert result.success is False
    assert refilled == [1], "the re-read spooled under the job before the cancel"
    assert _job_files(job_id) == []
