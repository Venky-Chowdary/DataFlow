"""T-F1: the test suite never writes exports or Hypothesis data into the source tree.

Before: with no ``DATAFLOW_EXPORTS_DIR`` in the environment (CI sets none),
every Query console export and every ``file_export`` destination a test ran
landed in ``apps/api/exports`` (hundreds of ``export_*``/``query_*`` files after
a long sweep), and property tests left ``apps/api/.hypothesis`` behind. The
session guard only watched ``apps/api/data``.

After: ``tests/conftest.py`` points ``DATAFLOW_EXPORTS_DIR`` and Hypothesis'
storage at suite temp folders, and the session guard also fails the run on a
new or changed file under ``apps/api/exports`` or ``apps/api/.hypothesis``.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]


def _inside(path: Path, root: Path) -> bool:
    return os.path.commonpath([str(path.resolve()), str(root.resolve())]) == str(root.resolve())


def test_exports_dir_is_outside_the_source_tree():
    from services.export_paths import exports_dir

    assert not _inside(exports_dir(create=False), _API_ROOT), exports_dir(create=False)


def test_query_console_export_lands_outside_the_source_tree(monkeypatch, tmp_path):
    """The confirmed leaking path: a Query console export with no per-test override."""
    from fastapi.testclient import TestClient

    from services import connector_store
    from src.main import app

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    connector_store._backend_choice = None
    db_path = tmp_path / "tf1.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)")
    conn.execute("INSERT INTO t VALUES (1, 'a')")
    conn.commit()
    conn.close()
    saved = connector_store.create_connector({
        "name": "T-F1 SQLite", "type": "sqlite", "role": "both",
        "connection_string": f"sqlite:///{db_path}", "workspace_id": "",
    })
    response = TestClient(app).post("/api/v1/query/export", json={
        "connector_id": saved.id, "query": "SELECT id, note FROM t", "format": "csv",
    })
    assert response.status_code == 200, response.text
    written = Path(response.json()["path"])
    try:
        assert written.is_file()
        assert not _inside(written, _API_ROOT), written
    finally:
        written.unlink(missing_ok=True)


def test_hypothesis_storage_is_outside_the_source_tree():
    """Our redirect, checked without importing hypothesis (CI's api-tests do not install it)."""
    configured = os.environ.get("HYPOTHESIS_STORAGE_DIRECTORY")
    assert configured, "conftest must point Hypothesis storage at a suite temp folder"
    assert not _inside(Path(configured), _API_ROOT), configured
    try:
        from hypothesis.configuration import storage_directory
    except ImportError:
        return  # not installed (CI api-tests): the env var above is the whole contract
    # Where hypothesis is installed, conftest also set its home dir in-process.
    assert not _inside(storage_directory("examples", intent_to_write=False).path, _API_ROOT)


def test_session_guard_reports_new_files_in_exports_and_hypothesis(tmp_path):
    from tests.conftest import _GUARDED_SOURCE_DIRS, _source_dir_writes, _tree_fingerprint

    assert set(_GUARDED_SOURCE_DIRS) == {"exports", ".hypothesis"}
    roots = {name: tmp_path / name for name in _GUARDED_SOURCE_DIRS}
    before = {name: _tree_fingerprint(root) for name, root in roots.items()}
    assert _source_dir_writes(before, before) == []

    (roots["exports"] / "_workspaces" / "ws").mkdir(parents=True)
    (roots["exports"] / "_workspaces" / "ws" / "query_abc.csv").write_text("id\n1\n")
    (roots[".hypothesis"] / "examples").mkdir(parents=True)
    (roots[".hypothesis"] / "examples" / "x").write_bytes(b"1")
    after = {name: _tree_fingerprint(root) for name, root in roots.items()}
    problems = _source_dir_writes(before, after)
    assert len(problems) == 2
    assert "apps/api/exports" in problems[0] and "query_abc.csv" in problems[0]
    assert "apps/api/.hypothesis" in problems[1]
