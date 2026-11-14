"""Re-review test quality: the GET re-read path through the real API.

Every API test job had no saved route, so ``GET /jobs/{id}/parity-report``
never re-read anything there (only the in-process SQLite e2e did). This job
carries a saved SQLite route: the route re-reads the keys the evidence names,
classifies what both sides hold now, and reuses that read while it is fresh.
"""

from __future__ import annotations

import sqlite3

import pytest

from r8_ent_harness import ent  # noqa: F401
from tests.test_parity_api import _job


def _db(path, rows):
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE pay (id INTEGER PRIMARY KEY, note TEXT)")
        conn.executemany("INSERT INTO pay VALUES (?, ?)", rows)
    conn.close()


def _clear_cache() -> None:
    try:
        from services import parity_cache
    except ImportError:  # the code before the read cache had none to clear
        return
    parity_cache.clear()


@pytest.fixture()
def route(ent, monkeypatch):  # noqa: F811
    """A saved SQLite route whose files live in the workspace's own area.

    The harness's data folder is ``tmp_path``, so its ``sqlite`` folder is the
    shared SQLite volume: a request binds the caller's workspace
    (``ConnectorAccessMiddleware``), and a file in the flat part of that volume
    with no provable owner is refused for every workspace
    (``services.embedded_volume``). A real workspace's files live in its own
    area, as here.
    """
    from services.embedded_volume import volume_workspace_dir

    _clear_cache()
    ws = ent.workspace("Parity reread")
    volume = ent.tmp / "sqlite"
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(volume))
    area = volume_workspace_dir(volume, ws)
    area.mkdir(parents=True)
    src, dst = area / "src.sqlite", area / "dst.sqlite"
    _db(src, [(i, f"n{i}") for i in range(1, 11)])
    _db(dst, [(i, "TRUE" if i == 7 else f"n{i}") for i in range(1, 11)])
    yield ws, {
        "source": {"kind": "database", "format": "sqlite", "database": str(src), "table": "pay"},
        "destination": {"kind": "database", "format": "sqlite", "database": str(dst), "table": "pay"},
        "mappings": [{"source": "id", "target": "id"},
                     {"source": "note", "target": "note", "target_type": "TEXT"}],
        "sync_mode": "full_refresh_overwrite",
    }
    _clear_cache()


def test_the_get_route_rereads_the_named_keys(ent, route, monkeypatch):  # noqa: F811
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    ws, route = route
    viewer = ent.member(email="reread-viewer@example.com", workspace_id=ws, role="viewer")
    job = _job(ws, "sqlite load", transfer_request=route, reconciliation={
        "passed": False, "source_rows": 10, "target_rows": 10,
        "culprit_keys": {"changed": ["7", "3"]},
    }, destination_summary={"table": "pay", "primary_key_columns": ["id"],
                            "sync_mode": "full_refresh_overwrite"})
    first = viewer.get(f"/api/v1/jobs/{job}/parity-report")
    assert first.status_code == 200, first.text
    report = first.json()
    assert report["evidence"]["reread"]["pay"]["status"] == "ok", report["evidence"]["reread"]
    assert report["evidence"]["reread"]["pay"]["keys"] == 2
    by_key = {k: d for d in report["differences"] for k in d["keys_sample"]}
    assert by_key["7"]["column"] == "note" and by_key["7"]["example"] == {"source": "n7", "destination": "TRUE"}
    # Key 3 reads equal now: the run's difference cannot be re-checked (never silently resolved).
    assert "cannot be re-checked" in by_key["3"]["cause"]["detail"]
    assert report["verdict"] == "unexplained"
    again = viewer.get(f"/api/v1/jobs/{job}/parity-report").json()
    assert again["evidence"]["reread"]["pay"].get("cached_at"), "the poll reused the fresh read"
    assert again["verdict"] == report["verdict"]
