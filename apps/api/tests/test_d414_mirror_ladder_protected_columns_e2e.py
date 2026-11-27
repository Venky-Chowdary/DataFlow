"""D414 — a mirror with a protecting transform must not fail the ladder on an exact load.

QA (r55-r75): PG → SQL Server mirror, ``payload`` hash_pii. L1 and L3 passed;
L2 compared the raw source JSON min/max with the destination hashes and the job
ended FAILED over 30/30 box-exact rows.

Root cause: SQL Server is not a staging-merge destination, so its mirror takes
the engine's buffered path, which hands the ladder the raw in-memory records.
``source_rewrites_needed`` mapped those records to the written image only when
a column was *renamed*, so a value transform without a rename (hash_pii on
``payload``) left them raw. The same happens on any buffered route; this test
drives it through ``execute_tracked`` on SQLite with ``priority_column`` (a real
user option that also selects the buffered path). Upsert on the same path was
worse: the false L2 failure rolled the correct batch back out of the table.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine, takes_streaming_route  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def fake(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "d414-test-key")
    store = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return store


def _seed(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, payload TEXT, long_txt TEXT)"
        )
        rows = []
        for i in range(1, 31):
            payload: str | None = json.dumps({"k": i, "s": f"v{i}"})
            if i == 3 or i == 30:
                payload = None  # NULL stays NULL through the protection
            elif i == 10:
                payload = "{}"  # an empty document is a value
            rows.append((i, f" Name {i} ", payload, "x" * 40))
        conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", rows)
    conn.close()


def _request(src: Path, dst: Path, mode: str, protect: str) -> TransferRequest:
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "name", "target": "name", "transform": "trim", "transform_override": True},
        {"source": "payload", "target": "payload", "transform": protect, "transform_override": True},
        {"source": "long_txt", "target": "long_txt", "transform": "omit"},
    ]
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        mappings=mappings,
        sync_mode=mode,
        stream_contracts=[
            {"name": "t", "primary_key": ["id"], "sync_mode": mode, "selected": True}
        ],
        skip_preflight=True,
        validation_mode="strict",
        priority_column="id",
    )


def _run(fake, request: TransferRequest):
    job_id = "d414" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    return result, (fake.get_job(job_id) or {})


def _dest(path: Path) -> dict[int, tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return {r[0]: r[1:] for r in conn.execute("SELECT id, name, payload FROM t")}
    finally:
        conn.close()


def test_sqlserver_mirror_takes_the_buffered_route_this_test_drives():
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="t"),
        destination=EndpointConfig(kind="database", format="sqlserver", table="t"),
        sync_mode="full_refresh_mirror",
    )
    assert takes_streaming_route(req) is False


@pytest.mark.parametrize("protect", ["hash_pii", "mask_pii", "redact"])
@pytest.mark.parametrize("mode", ["full_refresh_mirror", "upsert"])
def test_protected_column_exact_load_completes(tmp_path: Path, fake, mode: str, protect: str):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _seed(src)
    result, job = _run(fake, _request(src, dst, mode, protect))
    ladder = (job.get("reconciliation") or {}).get("verification_ladder") or {}
    assert result.success, (result.error, ladder.get("layers", {}).get("L2"))
    l2 = ladder["layers"]["L2"]
    # Graded like with like (the recipe output), not skipped.
    assert l2["passed"] is True and not l2["details"].get("skipped"), l2
    assert ladder["assurance_level"] in {"full_checksum", "five_layer"}, ladder
    rows = _dest(dst)
    assert len(rows) == 30, "the correct batch must stay in place"
    assert rows[1][0] == "Name 1"
    assert rows[3][1] is None and rows[30][1] is None
    assert rows[2][1] not in (None, json.dumps({"k": 2, "s": "v2"}))


def test_real_difference_on_an_unprotected_column_still_fails(tmp_path: Path, fake, monkeypatch):
    """AC-3: the ladder still fails and names the column a wrong write changed."""
    import src.transfer.reconcile_step as rs

    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _seed(src)
    real_read = rs._read_verify_rows

    def _tampered(db_type, cfg, *, table, columns, **kw):
        rows = real_read(db_type, cfg, table=table, columns=columns, **kw)
        if str(cfg.get("database") or "").endswith("dst.sqlite"):
            for row in rows:
                if str(row.get("id")) == "7":
                    row["name"] = "ZZZ wrong write"
        return rows

    monkeypatch.setattr(rs, "_read_verify_rows", _tampered)
    result, job = _run(fake, _request(src, dst, "full_refresh_mirror", "hash_pii"))
    assert not result.success
    l2 = job["reconciliation"]["verification_ladder"]["layers"]["L2"]
    assert l2["passed"] is False
    assert l2["details"]["mismatched_columns"] == ["name"], l2
