"""D82 — the strict load-volume guard had no override.

A load far below the route's history ("Load volume 5 is below 50% of prior
median 20") blocks a strict run, correctly by default. The operator who knows
the source legitimately shrank had no way to proceed except weakening the
whole validation mode. ``load_volume_acknowledged`` accepts the volume signal
only; every other quality signal still blocks.
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
from src.transfer.models import (  # noqa: E402
    EndpointConfig,
    TransferRequest,
    transfer_request_from_dict,
    transfer_request_to_dict,
)


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request):
    job_id = "d82" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _request(src: Path, dst: Path, **kw) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        **kw,
    )


def test_flag_survives_the_job_store_round_trip():
    request = _request(Path("a"), Path("b"), load_volume_acknowledged=True)
    assert transfer_request_from_dict(transfer_request_to_dict(request)).load_volume_acknowledged


def test_shrunk_load_is_blocked_unless_the_volume_is_acknowledged(tmp_path, isolated):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"n{i}") for i in range(1, 21)])
    conn.commit()
    conn.close()

    assert _run(isolated, _request(src, dst)).success  # history: 20 rows

    refused = _run(isolated, _request(src, dst, limit=5))
    assert not refused.success
    assert "Load volume 5 is below 50% of prior median 20" in str(refused.error)

    accepted = _run(isolated, _request(src, dst, limit=5, load_volume_acknowledged=True))
    assert accepted.success, accepted.error
    count = sqlite3.connect(str(dst)).execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert count == 5


def test_acknowledged_empty_source_overwrite_is_not_re_blocked_by_history(tmp_path, isolated):
    """D56's explicit "empty it" acknowledgement also covers the volume guard:
    an overwrite from a 0-row source is a volume of 0 by definition."""
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"n{i}") for i in range(1, 11)])
    conn.commit()
    assert _run(isolated, _request(src, dst)).success
    conn.execute("DELETE FROM t")
    conn.commit()
    conn.close()

    ack = _run(isolated, _request(src, dst, empty_source_overwrite_acknowledged=True))
    assert ack.success, ack.error
    assert sqlite3.connect(str(dst)).execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0
