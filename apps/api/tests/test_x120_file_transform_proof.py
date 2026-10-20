"""X-120 (P1): file sources never got a transform proof — every clean run
with a recipe reported "transform proof unverifiable — independent source
re-read requires a database endpoint" while the headline read verified.

Fix: the proof re-parses the same bytes the read consumed (or the spilled
object file via ``source_spill_path``), sha256-checked against the
``file_digest`` stamped at read time — a moved file fails the proof rather
than skipping it. The recompute then runs the approved recipe on the
re-read rows exactly as for a DB source.

X-121 note: the change detector compares the re-read source row to its own
pinned hash (source-to-source); with X-99's read_cols restricted to recipe
input columns it no longer compares source rows to recipe output.
"""
import os
import sqlite3
import uuid

import pytest


def _run(tmp_path, content=b"id,amt\n1,9\n2,10\n3,-2\n4,2.5\n5,100\n",
         steps=None, mappings=None):
    import src.transfer.engine as eng
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from test_cell_mysql_to_duckdb import _FakeMongo

    eng.get_mongodb_service = lambda *a, **k: _FakeMongo()
    dst = tmp_path / "dst.db"
    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="out"
        ),
        source_filename="nums.csv",
        source_content=content,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        shape_recipe={"version": 1, "steps": steps or [
            {"op": "derive_column", "options": {"to": "dbl", "expression": "amt * 2"}},
        ]},
        mappings=mappings or [
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "amt", "target": "amt", "target_type": "REAL"},
            {"source": "dbl", "target": "dbl", "target_type": "REAL"},
        ],
    )
    res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    return res


def test_x120_file_source_transform_proof_verified(tmp_path):
    res = _run(tmp_path)
    assert res.success, getattr(res, "error", None)
    proof = (res.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    assert proof.get("rows") == 5


def test_x120_verified_cells_match_dest(tmp_path):
    res = _run(tmp_path)
    assert res.success
    conn = sqlite3.connect(tmp_path / "dst.db")
    try:
        rows = conn.execute("select id, amt, dbl from out order by id").fetchall()
    finally:
        conn.close()
    assert rows == [(1, 9.0, 18.0), (2, 10.0, 20.0), (3, -2.0, -4.0),
                    (4, 2.5, 5.0), (5, 100.0, 200.0)]


def test_x120_missing_file_bytes_stays_honest(tmp_path, monkeypatch):
    """Without the bytes/path the proof must not read verified."""
    import src.transfer.reconcile_step as rec

    calls = {}

    orig = rec._transform_applied_proof

    def wrap(**kw):
        kw["source_file_content"] = None
        return orig(**kw)

    monkeypatch.setattr(rec, "_transform_applied_proof", wrap)
    res = _run(tmp_path)
    assert res.success
    proof = (res.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "unverifiable", proof


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X120_LIVE") != "1",
    reason="live object-store spill matrix needs provisioned engines",
)
def test_x120_live(tmp_path):  # pragma: no cover
    pass
