"""Identity × sync-mode matrix through the real engine, read back independently.

Every cell runs a SQLite source into a destination twice — first load, then a
changed source (one key updated, one dropped, one new) — and reads the
destination back with its own driver (``tests.route_cert_readback``). The
destination population must equal the mode's documented semantics exactly:

* overwrite / mirror active rows = the latest source;
* keyless or unenforced-key Full Refresh Append keeps every copy (Airbyte
  Full Refresh Append semantics);
* ``dedupe_skip`` keeps stored keys, ``upsert`` replaces them;
* incremental reads only rows past the cursor; deduped keeps the latest;
* SCD2 keeps every version; mirror soft-deletes (``_deleted``) dropped keys;
* a keyed upsert without a key and an incremental append that collides with
  stored keys are refused with nothing written.

Composite keys (``region, id``) reuse ``id`` across regions so any path that
collapses identity to one column fails the readback. Live engines skip with
the reason when their service is not reachable.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from collections import Counter
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.route_cert_readback import read_back  # noqa: E402
from tests.test_execute_tracked_universal_matrix import (  # noqa: E402
    _build_db_endpoint,
    _endpoint_reachable,
)
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402

COLS = ["id", "region", "name", "updated_at"]
A1, B1, B2, C3 = (1, "eu", "a", 1), (2, "eu", "b", 1), (2, "eu", "B", 2), (3, "eu", "c", 2)
X1, Y1, Y2, Z2 = (1, "eu", "a", 1), (1, "us", "b", 1), (1, "us", "B", 2), (2, "eu", "c", 2)
SINGLE = ([A1, B1], [B2, C3])
COMPOSITE = ([X1, Y1], [Y2, Z2])
PAIR = ["region", "id"]

# id, mode, key, cursor, duplicate_policy, data, outcome, rows after run 2
CELLS = [
    ("overwrite_keyless", "full_refresh_overwrite", "", "", "", SINGLE, "ok", [B2, C3]),
    ("overwrite_pk", "full_refresh_overwrite", "id", "", "", SINGLE, "ok", [B2, C3]),
    ("append_keyless", "full_refresh_append", "", "", "", SINGLE, "ok", [A1, B1, B2, C3]),
    ("append_unenforced_key", "full_refresh_append", "id", "", "", SINGLE, "ok", [A1, B1, B2, C3]),
    ("append_dedupe_skip", "full_refresh_append", "id", "", "dedupe_skip", SINGLE, "ok", [A1, B1, C3]),
    ("append_upsert", "full_refresh_append", "id", "", "upsert", SINGLE, "ok", [A1, B2, C3]),
    ("incr_append_keyless", "incremental_append", "", "updated_at", "", SINGLE, "ok", [A1, B1, B2, C3]),
    ("incr_append_key_collision", "incremental_append", "id", "updated_at", "", SINGLE, "refused_r2", [A1, B1]),
    ("incr_deduped", "incremental_deduped", "id", "updated_at", "", SINGLE, "ok", [A1, B2, C3]),
    ("upsert", "upsert", "id", "", "", SINGLE, "ok", [A1, B2, C3]),
    ("upsert_keyless", "upsert", "", "", "", SINGLE, "refused_r1", None),
    ("mirror", "mirror", "id", "", "", SINGLE, "ok", [A1, B2, C3]),
    ("scd2", "scd2", "id", "", "", SINGLE, "ok", [A1, B1, B2, C3]),
    ("upsert_composite", "upsert", PAIR, "", "", COMPOSITE, "ok", [X1, Y2, Z2]),
    ("incr_deduped_composite", "incremental_deduped", PAIR, "updated_at", "", COMPOSITE, "ok", [X1, Y2, Z2]),
    ("mirror_composite", "mirror", PAIR, "", "", COMPOSITE, "ok", [X1, Y2, Z2]),
    ("append_upsert_composite", "full_refresh_append", PAIR, "", "upsert", COMPOSITE, "ok", [X1, Y2, Z2]),
]
#: Mirror keeps a dropped key as a soft-deleted row: (id, region) -> _deleted.
MIRROR_DELETED = {"mirror": {(1, "eu")}, "mirror_composite": {(1, "eu")}}
ENGINES = ["sqlite", "postgresql", "mysql", "sqlserver", "oracle"]


def _seed(
    path: Path, rows: list[tuple], id_type: str = "INTEGER", text_types: tuple = ("TEXT", "TEXT")
) -> None:
    path.unlink(missing_ok=True)
    conn = sqlite3.connect(str(path))
    region, name = text_types
    try:
        with conn:
            conn.execute(
                f"CREATE TABLE t (id {id_type}, region {region}, name {name}, updated_at INTEGER)"
            )
            conn.executemany("INSERT INTO t VALUES (?,?,?,?)", rows)
    finally:
        conn.close()


def _run(fake: _FakeMongo, src: Path, dest: EndpointConfig, cell: tuple):
    _, mode, key, cursor, policy, *_ = cell
    contract: dict = {"name": "t", "sync_mode": mode, "selected": True, "primary_key": key}
    if cursor:
        contract["cursor_field"] = cursor
        contract["cursor_semantics"] = "modification_timestamp"
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=dest,
        sync_mode=mode,
        validation_mode="strict",
        mappings=[
            {"source": c, "target": c, "confidence": 1.0, "approved": True, "user_override": True}
            for c in COLS
        ],
        stream_contracts=[contract],
        duplicate_policy=policy,
        ddl_changes_acknowledged=True,
    )
    job_id = uuid.uuid4().hex[:24]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _norm(value) -> str:
    text = str(value)
    return text[:-2] if text.endswith(".0") else text


def _population(dest: EndpointConfig, columns: list[str]) -> list[tuple]:
    rows = read_back(dest, columns)
    assert rows is not None, "destination not independently readable"
    return [tuple(row[c] if c in row else row[c.upper()] for c in columns) for row in rows]


def _as_counter(rows) -> Counter:
    return Counter(tuple(_norm(v) for v in r) for r in rows)


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("cell", CELLS, ids=[c[0] for c in CELLS])
def test_identity_sync_mode_cell(engine: str, cell: tuple, tmp_path: Path, monkeypatch) -> None:
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    dest = _build_db_endpoint(engine, tmp_path, "dst", uuid.uuid4().hex[:8])
    if engine != "sqlite" and not _endpoint_reachable(dest):
        pytest.skip(f"{engine} not reachable on its configured host/port")
    name, _mode, key, _cursor, policy, (first, second), outcome, expected = cell
    src = tmp_path / "src.db"

    _seed(src, first)
    r1 = _run(fake, src, dest, cell)
    if outcome == "refused_r1":
        assert not r1.success
        assert int(r1.records_transferred or 0) == 0
        assert "primary key" in (r1.error or "").lower()
        return
    assert r1.success, r1.error
    assert _as_counter(_population(dest, COLS)) == _as_counter(first)

    _seed(src, second)
    r2 = _run(fake, src, dest, cell)
    if outcome == "refused_r2":
        assert not r2.success
        assert "already" in (r2.error or "").lower()
    else:
        assert r2.success, r2.error
        assert (r2.reconciliation or {}).get("passed") is True
        accounting = (r2.destination_summary or {}).get("row_accounting") or {}
        assert accounting.get("balanced", True) is True
    assert _as_counter(_population(dest, COLS)) == _as_counter(expected)

    keys = [key] if isinstance(key, str) and key else list(key or [])
    if keys and (outcome == "ok") and (policy or _mode != "full_refresh_append"):
        summary = r2.destination_summary or {}
        assert [c.lower() for c in summary.get("primary_key_columns") or []] == keys

    if name in MIRROR_DELETED:
        flags = {
            (int(_norm(i)), str(r)): _norm(d) in {"1", "True", "true"}
            for i, r, d in _population(dest, ["id", "region", "_deleted"])
        }
        assert {k for k, deleted in flags.items() if deleted} == MIRROR_DELETED[name]
