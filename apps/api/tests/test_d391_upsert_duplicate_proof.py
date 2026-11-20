"""D391 — an upsert that kept the last copy of a key is exact.

The load collapses duplicate keys. The transform proof used to count every
source line and report that the source changed. It now applies the same
last-wins collapse and reports the collapsed copies on their own.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.models import EndpointConfig  # noqa: E402
from src.transfer.reconcile_step import _transform_applied_proof  # noqa: E402

_RECIPE = {"steps": [{"op": "trim", "column": "name", "enabled": True}]}


def _table(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(path)
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
    conn.close()


def _endpoint(path: Path) -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlite", database=str(path), table="t")


def _prove(src: Path, dst: Path, sync_mode: str) -> dict:
    return _transform_applied_proof(
        shape_recipe=_RECIPE,
        dest_summary={
            "shape_recipe_hash": "d391",
            "sync_mode": sync_mode,
            "primary_key_columns": ["id"],
            "shape_proof": {"input_columns": ["id", "name"]},
        },
        source_endpoint=_endpoint(src),
        endpoint=_endpoint(dst),
        columns=["id", "name"],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
        ],
        source_schema={"id": "INTEGER", "name": "TEXT"},
        recon={},
    )


def test_upsert_proof_keeps_the_last_copy_and_counts_the_collapse(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "0")
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    # Two copies of id 1. The load kept the last. id 2 is unique.
    _table(src, [(1, " ada"), (1, "ADA"), (2, "bob")])
    _table(dst, [(1, "ADA"), (2, "bob")])

    proof = _prove(src, dst, "upsert")

    assert proof["status"] == "verified", proof
    assert proof["rows"] == 2, proof
    assert proof["collapsed_duplicate_rows"] == 1, proof
    assert "source changed" not in str(proof.get("reason") or "")


def test_overwrite_still_counts_every_source_line(tmp_path, monkeypatch):
    """Overwrite writes every line. Collapsing those copies would hide a row."""
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "0")
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _table(src, [(1, " ada"), (1, "ADA"), (2, "bob")])
    _table(dst, [(1, "ada"), (1, "ADA"), (2, "bob")])

    proof = _prove(src, dst, "full_refresh_overwrite")

    assert proof.get("collapsed_duplicate_rows") in (None, 0)
    assert proof["status"] == "verified", proof
    assert proof["rows"] == 3, proof
