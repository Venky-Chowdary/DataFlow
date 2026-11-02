"""M3 — a keyed incremental run must independently re-read its bounded
[watermark, max_seen] window and scope the destination read-back to the run's
keys, instead of reporting write-confirmed on write-pass fingerprints alone.

Owner decision (Oct 25): until this landed, an incremental run could not be
reported "verified" on the strength of write-pass readback alone. With the
bounded re-read, a clean run earns migration_proven and a destination tamper
inside the window is caught by the independent scan — not by the bytes we
just wrote.
"""

from __future__ import annotations

import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


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


def _request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t",
            connection_string=f"sqlite:///{src}", ssl=False,
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="t",
            connection_string=f"sqlite:///{dst}", ssl=False,
        ),
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "seq", "target": "seq", "confidence": 0.99},
            {"source": "v", "target": "v", "confidence": 0.99},
        ],
        sync_mode="incremental_deduped",
        stream_contracts=[
            {
                "name": "t",
                "sync_mode": "incremental_deduped",
                "cursor_field": "seq",
                "cursor_semantics": "monotonic_sequence",
                "primary_key": "id",
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="balanced",
    )


def _run(fake, src: Path, dst: Path):
    job_id = "m3" + uuid.uuid4().hex[:20]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(src, dst), job_id)


def _seed(tmp_path: Path):
    src, dst = tmp_path / "m3_src.db", tmp_path / "m3_dst.db"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(1, 1, "a"), (2, 2, "b"), (3, 3, "c")])
    _exec(dst, "CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
    return src, dst


def test_incremental_clean_run_independently_rereads_the_window(
    tmp_path: Path, isolated
) -> None:
    src, dst = _seed(tmp_path)
    first = _run(isolated, src, dst)
    assert first.success, first.error

    # New rows arrive; id=2 changes with a new seq so it lands in the window.
    _exec(src, "UPDATE t SET seq=6, v='b2' WHERE id=2")
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(4, 4, "d"), (5, 5, "e")])

    second = _run(isolated, src, dst)
    assert second.success, second.error
    recon = second.reconciliation or {}
    # The proof came from a second source scan of the bounded window, not
    # the write-pass bytes: a deduped run seeds past the stored composite
    # watermark, so the window is the written batch {2,4,5} (3 rows) while
    # the destination holds all 5.
    assert recon.get("passed") is True, recon
    assert recon.get("checksum_match") is True, recon
    assert recon.get("verification_verdict") == "verified", recon
    assert recon.get("assurance_level") == "full_checksum", recon
    assert recon.get("source_independently_reread") is True, recon
    # The destination digest is re-scoped to the batch keys; the source
    # digest is the verify-time re-read (checksum_mode carries that half).
    assert recon.get("checksum_scope") == "written_batch_keys", recon
    assert recon.get("source_rows") == 3, recon  # the (watermark, max_seen] window
    assert recon.get("target_rows") == 5, recon


def test_incremental_tamper_inside_window_is_not_verified(tmp_path: Path, isolated) -> None:
    src, dst = _seed(tmp_path)
    first = _run(isolated, src, dst)
    assert first.success, first.error

    # A destination-side tamper that fires when the incremental run rewrites
    # the row — the divergence lands between the write and the read-back,
    # inside this run's [watermark, max_seen] window.
    _exec(
        dst,
        """CREATE TRIGGER tamper_upd AFTER UPDATE ON t BEGIN
               UPDATE t SET v='TAMPERED' WHERE id=NEW.id AND v <> 'TAMPERED';
           END""",
    )
    _exec(
        dst,
        """CREATE TRIGGER tamper_ins AFTER INSERT ON t BEGIN
               UPDATE t SET v='TAMPERED' WHERE id=NEW.id AND v <> 'TAMPERED';
           END""",
    )
    _exec(src, "UPDATE t SET seq=6, v='b2' WHERE id=2")
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(4, 4, "d"), (5, 5, "e")])

    second = _run(isolated, src, dst)
    assert not second.success
    recon = second.reconciliation or {}
    assert recon.get("passed") is not True
    assert recon.get("checksum_match") is False, recon
    assert recon.get("verification_verdict") == "failed", recon
    assert recon.get("migration_proven") is not True
    # The independent re-read localized the in-window tamper itself.
    assert "TAMPERED" in str(recon.get("message") or ""), recon


def test_keyed_incremental_warehouse_route_is_reread_eligible():
    """The gate itself: a bounded-window re-read is honest proof, so the
    incremental blanket-skip is the defect. Non-warehouse destinations and
    CDC still stay on the write-pass."""
    from services.source_reread import should_reread_source

    assert should_reread_source(
        src_type="postgresql", dest_type="postgresql", incremental=True
    ) is True
    assert should_reread_source(
        src_type="mysql", dest_type="snowflake", incremental=True
    ) is True
    assert should_reread_source(
        src_type="postgresql", dest_type="s3", incremental=True
    ) is False  # not an independently-SELECTable dest
    assert should_reread_source(
        src_type="kafka", dest_type="postgresql", incremental=True
    ) is False  # not a snapshot-scan source


def test_full_refresh_reread_policy_unchanged():
    from services.source_reread import should_reread_source

    assert should_reread_source(src_type="postgresql", dest_type="sqlite") is True
    assert should_reread_source(src_type="postgresql", dest_type="s3") is False
    assert should_reread_source(
        src_type="postgresql", dest_type="s3", incremental=True,
        partial_write_pass=True,
    ) is True  # a partial write-pass always re-reads
