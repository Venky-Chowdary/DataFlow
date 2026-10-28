"""D39 — a transform on the upsert key was never checked for collisions.

The source uniqueness probe groups the key as the source stores it, so
``A1`` and ``a1`` looked like two keys. A ``lower`` transform on the key
writes them as one: the upsert kept one row and failed the row-count check
after the write (or an overwrite died on the recreated PK). Preflight now
walks the key population through the key's own transform and refuses the run
before any row moves.
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


def test_probe_counts_only_transform_made_collisions():
    from services.transformed_key_probe import (
        key_transforms,
        probe_transformed_key_collisions,
    )

    maps = [
        {"source": "code", "target": "code", "transform": "lower"},
        {"source": "name", "target": "name", "transform": "none"},
    ]
    xforms = key_transforms(maps, ["code"])
    assert xforms == {"code": "lower"}
    rows = [
        {"code": "A1"}, {"code": "a1"},      # collide only after lower()
        {"code": "B"}, {"code": "B"},        # a plain source duplicate: not ours
        {"code": None},                       # NULL key: the null-key check's
    ]
    report = probe_transformed_key_collisions(
        rows, key_sources=["code"], transforms=xforms
    )
    assert report["collision_count"] == 1
    assert report["collisions"][0]["written_key"] == "a1"
    assert report["collisions"][0]["source_keys"] == ["A1", "a1"]


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            if rows is None:
                conn.execute(sql)
            else:
                conn.executemany(sql, rows)
    finally:
        conn.close()


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.mark.parametrize("sync_mode", ["upsert", "full_refresh_overwrite"])
def test_collision_beyond_the_sample_is_refused_before_any_write(
    tmp_path, isolated, sync_mode
):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE t (code TEXT, name TEXT)")
    rows = [(f"k{i}", "v") for i in range(20_000)] + [("A1", "x"), ("a1", "y")]
    _exec(src, "INSERT INTO t VALUES (?, ?)", rows)
    maps = [
        {"source": "code", "target": "code", "transform": "lower",
         "confidence": 1.0, "user_override": True},
        {"source": "name", "target": "name", "transform": "none",
         "confidence": 1.0, "user_override": True},
    ]
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode=sync_mode,
        validation_mode="strict",
        mappings=maps,
        stream_contracts=[
            {"name": "t", "selected": True, "sync_mode": sync_mode, "primary_key": "code"}
        ],
    )
    job_id = "d39" + uuid.uuid4().hex[:16]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)

    assert not result.success
    assert "share a key only after the key's transform" in str(result.error), result.error
    assert "'A1' vs 'a1'" in str(result.error)
    assert not dst.exists() or not sqlite3.connect(str(dst)).execute(
        "SELECT name FROM sqlite_master WHERE name='t'"
    ).fetchall()
