"""D38 regression tests — mirror delete contract.

P3.5: mirror reruns kept deleted keys behind a ``_deleted`` column — dest
502 vs source 500 while the job reported matching counts. The verifier must
state that tombstones exist instead of comparing live rows as if they were
the whole table.

Soft delete (``_deleted``) is the product contract the UI, help docs and
docs/ZERO_LOSS_PROPERTIES.md promise; QA demo 1030 found an undeclared
contract hard-deleting instead. A hard DELETE is now an explicit
``delete_mode="hard"`` on the stream contract (tests/test_r7_mirror_soft_delete_rest_e2e.py
pins the soft default end to end).
"""

from __future__ import annotations

import os
from services.brand_env import getenv_brand
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.sync_cursor import SyncContract
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _endpoint(path: Path, table: str):
    return EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=f"sqlite:///{path}",
        database=str(path),
        table=table,
    )


def _mappings() -> list[dict]:
    return [
        {
            "source": "id",
            "target": "id",
            "confidence": 1.0,
            "user_override": True,
            "transform": "none",
        },
        {
            "source": "name",
            "target": "name",
            "confidence": 1.0,
            "user_override": True,
            "transform": "none",
        },
    ]


def _contract(**extra):
    base = {"selected": True, "primary_key": "id", "sync_mode": "mirror"}
    base.update(extra)
    return [base]


def _request(db_path: str, *, contracts=None, job_suffix: str = "x"):
    return TransferRequest(
        source=_endpoint(db_path, "src"),
        destination=_endpoint(db_path, "dst"),
        sync_mode="mirror",
        stream_contracts=contracts,
        mappings=_mappings(),
        validation_mode="strict",
        skip_preflight=False,
    )


def _make_db(n: int) -> str:
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE src (id TEXT PRIMARY KEY, name TEXT)")
        conn.execute("CREATE TABLE dst (id TEXT PRIMARY KEY, name TEXT)")
        for i in range(n):
            conn.execute(
                "INSERT INTO src (id, name) VALUES (?, ?)",
                (str(i), f"Item {i}"),
            )
    return db_path


def _dst_cols(db_path: str) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        return [r[1] for r in conn.execute("PRAGMA table_info(dst)")]


def _dst_count(db_path: str) -> int:
    with sqlite3.connect(db_path) as conn:
        return conn.execute("SELECT COUNT(*) FROM dst").fetchone()[0]


@pytest.mark.skipif(getenv_brand("SKIP_SQLITE") == "1", reason="SQLite tests disabled")
def test_mirror_hard_delete_when_declared():
    """Declared delete_mode=hard DELETEs missing keys — no _deleted lattice."""
    db_path = _make_db(50)
    try:
        engine = UniversalTransferEngine()
        r1 = engine.execute_tracked(
            _request(db_path, contracts=_contract(delete_mode="hard")), "ha" * 12
        )
        assert r1.success, r1.error
        assert r1.records_transferred == 50

        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM src WHERE id IN ('0','1','2','3','4')")
            conn.commit()

        r2 = engine.execute_tracked(
            _request(db_path, contracts=_contract(delete_mode="hard")), "hb" * 12
        )
        assert r2.success, r2.error
        summary = r2.destination_summary or {}
        assert summary.get("mirror_delete_mode") == "hard", summary
        assert summary.get("hard_deleted") == 5, summary
        assert summary.get("tombstone_rows") == 0, summary
        assert _dst_count(db_path) == 45
        assert "_deleted" not in _dst_cols(db_path)
        ledger = r2.row_accounting or {}
        assert ledger.get("inferred_deletes") == 5, ledger
        assert ledger.get("dest_count") == 45, ledger
    finally:
        Path(db_path).unlink(missing_ok=True)


@pytest.mark.skipif(getenv_brand("SKIP_SQLITE") == "1", reason="SQLite tests disabled")
def test_mirror_soft_delete_declared_in_plan():
    """Declared delete_mode=soft keeps _deleted tombstones and reports them."""
    db_path = _make_db(50)
    try:
        engine = UniversalTransferEngine()
        contracts = _contract(delete_mode="soft")
        r1 = engine.execute_tracked(
            _request(db_path, contracts=contracts), "sa" * 12
        )
        assert r1.success, r1.error

        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM src WHERE id IN ('0','1','2','3','4')")
            conn.commit()

        r2 = engine.execute_tracked(
            _request(db_path, contracts=contracts), "sb" * 12
        )
        assert r2.success, r2.error
        summary = r2.destination_summary or {}
        assert summary.get("mirror_delete_mode") == "soft", summary
        assert summary.get("tombstone_rows") == 5, summary
        assert _dst_count(db_path) == 50  # physical incl. tombstones
        assert "_deleted" in _dst_cols(db_path)
    finally:
        Path(db_path).unlink(missing_ok=True)


@pytest.mark.skipif(getenv_brand("SKIP_SQLITE") == "1", reason="SQLite tests disabled")
def test_verifier_counts_tombstones():
    """Verifier reports live rows AND that tombstones exist — not 'all match'."""
    db_path = _make_db(50)
    try:
        engine = UniversalTransferEngine()
        contracts = _contract(delete_mode="soft")
        r1 = engine.execute_tracked(
            _request(db_path, contracts=contracts), "ta" * 12
        )
        assert r1.success, r1.error
        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM src WHERE id IN ('0','1')")
            conn.commit()
        r2 = engine.execute_tracked(
            _request(db_path, contracts=contracts), "tb" * 12
        )
        assert r2.success, r2.error
        summary = r2.destination_summary or {}
        # live count + tombstone census, both stated
        assert summary.get("active_rows") == 48, summary
        assert summary.get("tombstone_rows") == 2, summary
        ledger = r2.row_accounting or {}
        assert ledger.get("active_count") == 48, ledger
        assert ledger.get("dest_count") == 50, ledger  # physical population
        assert ledger.get("inferred_deletes") == 2, ledger
    finally:
        Path(db_path).unlink(missing_ok=True)


@pytest.mark.skipif(getenv_brand("SKIP_SQLITE") == "1", reason="SQLite tests disabled")
def test_mirror_hard_delete_clears_legacy_lattice():
    """A dest carrying _deleted from an older soft run: hard mirror
    physically removes missing keys even when tombstoned, and unflags
    keys the snapshot re-presents."""
    db_path = _make_db(50)
    try:
        engine = UniversalTransferEngine()
        # Seed tombstones with a declared-soft run.
        soft = _contract(delete_mode="soft")
        r1 = engine.execute_tracked(_request(db_path, contracts=soft), "la" * 12)
        assert r1.success, r1.error
        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM src WHERE id IN ('0','1','2','3','4')")
            conn.commit()
        r2 = engine.execute_tracked(_request(db_path, contracts=soft), "lb" * 12)
        assert r2.success, r2.error
        with sqlite3.connect(db_path) as conn:
            tomb = conn.execute(
                "SELECT COUNT(*) FROM dst WHERE _deleted = 1"
            ).fetchone()[0]
        assert tomb == 5
        # Re-present one tombstoned key in source.
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO src (id, name) VALUES ('0', 'Item 0')"
            )
            conn.commit()

        # Now mirror again with a declared hard delete.
        r3 = engine.execute_tracked(
            _request(db_path, contracts=_contract(delete_mode="hard")), "lc" * 12
        )
        assert r3.success, r3.error
        summary = r3.destination_summary or {}
        assert summary.get("mirror_delete_mode") == "hard", summary
        # 46 live source rows; 4 missing (incl. tombstoned) keys deleted.
        assert summary.get("hard_deleted") == 4, summary
        assert _dst_count(db_path) == 46
        with sqlite3.connect(db_path) as conn:
            tomb3 = conn.execute(
                "SELECT COUNT(*) FROM dst WHERE _deleted = 1"
            ).fetchone()[0]
            row0 = conn.execute(
                "SELECT _deleted FROM dst WHERE id = '0'"
            ).fetchone()
        assert tomb3 == 0, "no tombstones survive a hard mirror"
        assert row0[0] in (0, False)
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_sync_contract_delete_mode_parses():
    c = SyncContract.from_dict(
        {"selected": True, "sync_mode": "mirror", "primary_key": "id",
         "delete_mode": "soft"}
    )
    assert c.delete_mode == "soft"
    c2 = SyncContract.from_dict(
        {"selected": True, "sync_mode": "mirror", "primary_key": "id",
         "delete_strategy": "soft"}
    )
    assert c2.delete_mode == "soft"
    c3 = SyncContract.from_dict(
        {"selected": True, "sync_mode": "mirror", "primary_key": "id"}
    )
    assert c3.delete_mode == ""


@pytest.mark.skipif(getenv_brand("SKIP_SQLITE") == "1", reason="SQLite tests disabled")
def test_mirror_unknown_delete_mode_refused():
    db_path = _make_db(5)
    try:
        engine = UniversalTransferEngine()
        r = engine.execute_tracked(
            _request(db_path, contracts=_contract(delete_mode="archive")),
            "u" * 24,
        )
        assert not r.success
        assert "delete_mode" in str(r.error)
    finally:
        Path(db_path).unlink(missing_ok=True)
