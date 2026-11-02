"""QA MX3-03 — re-run Validate ignored the persisted watermark.

Real SQLite → SQLite ``incremental_append`` through the engine with preflight
on. Run 1 stores the watermark under the transfer's destination identity
(``…|host:localhost`` after ``resolve_connector_config``). Validate built its
read scope from the raw probe config, derived the unscoped key, saw no
watermark and probed the whole sample: run 2 was refused with "Append would
duplicate 10 existing destination key(s)" although only 3 rows were new.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.preflight_service as preflight_service  # noqa: E402
import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Mongo(_FakeMongo):
        def update_job_fields(self, *_a, **_kw):
            return None

    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    seen: list[tuple] = []
    probe = preflight_service.probe_append_key_collisions

    def _spy(**kw):
        result = probe(**kw)
        seen.append((
            kw.get("incremental_watermark"),
            getattr(result, "status", None),
            len(getattr(result, "findings", None) or []),
        ))
        return result

    monkeypatch.setattr(preflight_service, "probe_append_key_collisions", _spy)
    return fake, seen


def _request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        mappings=[{"source": c, "target": c, "confidence": 1.0} for c in ("CUST_ID", "seq", "v")],
        stream_contracts=[{
            "name": "t", "cursor_field": "seq", "cursor_semantics": "monotonic_sequence",
            "primary_key": ["CUST_ID"], "sync_mode": "incremental_append", "selected": True,
        }],
        skip_preflight=False,
        validation_mode="strict",
    )


def _insert(path: Path, ids) -> None:
    with sqlite3.connect(path) as conn:
        conn.executemany("INSERT INTO t VALUES (?, ?, ?)", [(i, i, f"v{i}") for i in ids])


def _count(path: Path) -> int:
    with sqlite3.connect(path) as conn:
        return conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]


def test_rerun_validate_probes_only_the_delta_past_the_watermark(tmp_path, isolated):
    fake, seen = isolated
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    with sqlite3.connect(src) as conn:
        conn.execute("CREATE TABLE t (CUST_ID INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
    _insert(src, range(1, 11))

    def run():
        job_id = "mx303" + uuid.uuid4().hex[:12]
        fake.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(_request(src, dst), job_id)

    first = run()
    assert first.success, first.error
    assert _count(dst) == 10

    _insert(src, range(11, 14))
    second = run()
    assert second.success, second.error  # MX3-03: refused on 10 stored keys here
    assert _count(dst) == 13
    watermark, status, collisions = seen[-1]
    assert watermark, "Validate did not see the persisted watermark"
    assert (status, collisions) == ("ran", 0)

    quiet = run()
    assert quiet.success, quiet.error
    assert _count(dst) == 13
    assert seen[-1][0], "quiet rerun lost the watermark"
