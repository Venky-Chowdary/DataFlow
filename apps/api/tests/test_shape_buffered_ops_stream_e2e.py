"""P1 buffered ops end-to-end: held rows reach a real destination.

dedupe keep-last and aggregate cannot emit a row until the population is
read, so the engine holds them and drains at end-of-stream. These SQLite →
SQLite runs drive the real transfer engine and re-read the destination:

* a dedupe_keep_last recipe lands one winner per key — the drain tail wrote;
* an aggregate recipe lands one group row — the drain tail wrote;
* Gate-8's source-reread digest covers the same drained tail, so the run
  does not fail alignment on its own held rows.
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
from services.shape_models import ShapeRecipe  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


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


def _seed(path: Path) -> None:
    _exec(path, "CREATE TABLE t (id TEXT, v TEXT, ts INTEGER, dept TEXT, amt INTEGER)")
    _exec(
        path,
        "INSERT INTO t (id, v, ts, dept, amt) VALUES (?, ?, ?, ?, ?)",
        [
            ("1", "old", 1, "ENG", 10),
            ("2", "b", 2, "OPS", 30),
            ("1", "new", 3, "ENG", 15),
        ],
    )


def _request(src: Path, dst: Path, steps: list[dict]) -> TransferRequest:
    payload = {"steps": steps}
    recipe = ShapeRecipe.parse(payload)
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        shape_recipe=payload,
        approved_shape_recipe_hash=recipe.recipe_hash,
        skip_preflight=True,
        validation_mode="strict",
    )


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _run(fake, src: Path, dst: Path, steps: list[dict]):
    job_id = "drain" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(src, dst, steps), job_id)


def test_dedupe_keep_last_drains_to_destination(tmp_path: Path, isolated):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src)
    result = _run(
        isolated,
        src,
        dst,
        [
            {
                "op": "dedupe_rows",
                "options": {"keys": ["id"], "keep": "last", "order_by": "ts"},
            }
        ],
    )
    assert result.success, result.error
    conn = sqlite3.connect(str(dst))
    try:
        rows = conn.execute("SELECT id, v, ts FROM t ORDER BY id").fetchall()
    finally:
        conn.close()
    # The drain tail wrote the winners: id=1 keeps ts=3, id=2 survives.
    assert sorted(rows) == [("1", "new", 3), ("2", "b", 2)]


def test_aggregate_drains_to_destination(tmp_path: Path, isolated):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src)
    result = _run(
        isolated,
        src,
        dst,
        [
            {
                "op": "aggregate",
                "options": {
                    "group_by": ["dept"],
                    "measures": [
                        {"function": "sum", "column": "amt", "as": "total"},
                        {"function": "count", "as": "n"},
                    ],
                },
            }
        ],
    )
    assert result.success, result.error
    conn = sqlite3.connect(str(dst))
    try:
        rows = conn.execute("SELECT dept, total, n FROM t ORDER BY dept").fetchall()
    finally:
        conn.close()
    # Decimal sums land as fixed-point text in the untyped SQLite column.
    assert sorted(rows) == [("ENG", "25", "2"), ("OPS", "30", "1")]


def test_sort_rows_drains_ordered_tail(tmp_path: Path, isolated):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src)
    result = _run(
        isolated,
        src,
        dst,
        [{"op": "sort_rows", "options": {"order_by": "amt", "direction": "desc"}}],
    )
    assert result.success, result.error
    conn = sqlite3.connect(str(dst))
    try:
        rows = conn.execute("SELECT amt FROM t").fetchall()
    finally:
        conn.close()
    assert [r[0] for r in rows] == [30, 15, 10]


def test_cast_divert_quarantines_the_row_not_silently(tmp_path: Path, isolated, monkeypatch):
    """Audit P0-3: a shape-diverted row is excluded from the destination AND
    persisted to the job's quarantine DLQ with step/op/reason."""
    import json

    import services.quarantine_dlq as dlq_mod

    dlq_path = tmp_path / "quarantine_dlq.jsonl"
    monkeypatch.setattr(dlq_mod, "DLQ_PATH", dlq_path)

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id TEXT, v TEXT)")
    _exec(
        src,
        "INSERT INTO t (id, v) VALUES (?, ?)",
        [("1", "10"), ("2", "NOT_A_NUMBER"), ("3", "30")],
    )
    result = _run(
        isolated,
        src,
        dst,
        [
            {
                "op": "cast_column",
                "column": "v",
                "options": {"to_type": "integer"},
                "on_error": "divert",
            }
        ],
    )
    assert result.success, result.error
    conn = sqlite3.connect(str(dst))
    try:
        rows = conn.execute("SELECT id, v FROM t ORDER BY id").fetchall()
    finally:
        conn.close()
    # The bad-cast row did not land.
    assert [r[0] for r in rows] == ["1", "3"]

    # …and it is durable quarantine, with the step/op/reason named.
    assert dlq_path.exists(), "no DLQ file written for the diverted row"
    events = [
        json.loads(line)
        for line in dlq_path.read_text().splitlines()
        if line.strip()
    ]
    details = []
    for ev in events:
        det = ev.get("details") or {}
        details.extend(det.get("rejected_details") or [])
    diverts = [d for d in details if d.get("shape_op") == "cast_column"]
    assert diverts, f"no shape_divert quarantine row in {events!r}"
    assert diverts[0]["row"]["id"] == "2"
    assert "step" in diverts[0]["reason"].lower() or diverts[0].get("shape_step")
