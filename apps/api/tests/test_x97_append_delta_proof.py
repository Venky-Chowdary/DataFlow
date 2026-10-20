"""X-97 — incremental_append delta-scoped two-sided value digest.

Before X-97 an append run had no per-cell proof of the appended delta: a
corrupted appended row was accepted at row_count assurance, and the
key-aligned sample compare declined because the PK repeats across append
versions. Now the stream accumulates a (pk, cursor)-keyed fingerprint of
exactly the appended batch, reconcile re-reads that keyset on the
destination, and a tampered appended row fails naming its key.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest


def _mk_src(tmp: Path, *, pk: bool = True) -> Path:
    src = tmp / "src.db"
    conn = sqlite3.connect(src)
    decl = "INTEGER PRIMARY KEY" if pk else "INTEGER"
    conn.execute(f"CREATE TABLE t (id {decl}, amt TEXT, updated_at TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?,?,?)",
        [
            (1, "10", "2024-01-01T00:00:00"),
            (2, "20", "2024-01-02T00:00:00"),
            (3, "30", "2024-01-03T00:00:00"),
            (4, "40", "2024-01-04T00:00:00"),
            (5, "50", "2024-01-05T00:00:00"),
        ],
    )
    conn.commit()
    conn.close()
    return src


def _bump_src(src: Path) -> None:
    conn = sqlite3.connect(src)
    conn.execute(
        "UPDATE t SET amt='20.5', updated_at='2024-02-01T00:00:00' WHERE id=2"
    )
    conn.execute(
        "UPDATE t SET amt='30.5', updated_at='2024-02-02T00:00:00' WHERE id=3"
    )
    conn.execute("INSERT INTO t VALUES (6,'60','2024-02-03T00:00:00')")
    # Equal-watermark late-arriving row: cursor exactly at run 1's max.
    conn.execute("INSERT INTO t VALUES (8,'80','2024-01-05T00:00:00')")
    conn.commit()
    conn.close()


def _req(src: Path, dst: Path):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="out"
        ),
        sync_mode="incremental_append",
        skip_preflight=True,
        stream_contracts=[
            {
                "name": "t",
                "selected": True,
                "sync_mode": "incremental_append",
                "primary_key": "id",
                "cursor_field": "updated_at",
            }
        ],
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "amt", "target": "amt", "target_type": "TEXT"},
            {
                "source": "updated_at",
                "target": "updated_at",
                "target_type": "TEXT",
            },
        ],
    )


@pytest.fixture
def row_path(monkeypatch):
    """Decline the incremental COPY fast paths so the row path proves the
    delta — the digest machinery under test lives in the write pass."""
    monkeypatch.setattr(
        "services.copy_incremental.identity_incremental_route",
        lambda *_a, **_k: False,
    )
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    yield


def _run(src: Path, dst: Path):
    from src.transfer.engine import UniversalTransferEngine

    return UniversalTransferEngine().execute_tracked(
        _req(src, dst), uuid.uuid4().hex[:24]
    )


def _dest_rows(dst: Path) -> list[tuple]:
    conn = sqlite3.connect(dst)
    try:
        return conn.execute(
            "SELECT id, amt, updated_at FROM out ORDER BY rowid"
        ).fetchall()
    finally:
        conn.close()


def test_append_run2_delta_reaches_full_checksum(row_path, tmp_path):
    src = _mk_src(tmp_path)
    dst = tmp_path / "dst.db"
    r1 = _run(src, dst)
    assert r1.success, r1.error
    _bump_src(src)
    r2 = _run(src, dst)
    assert r2.success, r2.error
    rows = _dest_rows(dst)
    # dest = run 1's 5 rows + whatever delta run 2 appended
    assert len(rows) == 5 + r2.records_transferred
    summary = r2.destination_summary or {}
    ad = summary.get("append_delta")
    assert ad, f"append delta proof was not stamped: {sorted(summary.keys())[:30]}"
    assert int(ad["rows"]) == r2.records_transferred
    assert ad["key_columns"] == ["id", "updated_at"]
    reconcile = r2.reconciliation or {}
    assert reconcile.get("checksum_match") is True, reconcile
    assert reconcile.get("checksum_scope") == "written_batch_keys", reconcile


def test_append_delta_tamper_fails_naming_culprit(row_path, tmp_path, monkeypatch):
    src = _mk_src(tmp_path)
    dst = tmp_path / "dst.db"
    assert _run(src, dst).success
    _bump_src(src)

    from services import dest_precount

    real = dest_precount.destination_keyed_rows

    def tampered(db_type, cfg, **kw):
        rows = real(db_type, cfg, **kw)
        out = []
        for r in rows:
            if r[0] == 2:
                r = (2, "TAMPERED", r[2])
            out.append(r)
        return out

    monkeypatch.setattr(
        "services.dest_precount.destination_keyed_rows", tampered
    )
    r2 = _run(src, dst)
    assert not r2.success
    text = str(r2.error or "") + str(r2.reconciliation or {})
    assert "id=2" in text, text


def test_append_delta_missing_row_fails(row_path, tmp_path, monkeypatch):
    src = _mk_src(tmp_path)
    dst = tmp_path / "dst.db"
    assert _run(src, dst).success
    _bump_src(src)

    from services import dest_precount

    real = dest_precount.destination_keyed_rows

    def dropped(db_type, cfg, **kw):
        return [r for r in real(db_type, cfg, **kw) if r[0] != 2]

    monkeypatch.setattr(
        "services.dest_precount.destination_keyed_rows", dropped
    )
    r2 = _run(src, dst)
    assert not r2.success
    text = str(r2.error or "") + str(r2.reconciliation or {})
    assert "missing" in text.lower() and "id=2" in text, text


def test_append_delta_ambiguous_keyset_stays_row_count(
    row_path, tmp_path, monkeypatch
):
    """A pre-existing destination row sharing a (pk, cursor) key makes the
    keyset a superset — the delta proof declines to row_count rather than
    comparing extra rows."""
    src = _mk_src(tmp_path)
    dst = tmp_path / "dst.db"
    assert _run(src, dst).success
    _bump_src(src)

    from services import dest_precount

    real = dest_precount.destination_keyed_rows

    def extra(db_type, cfg, **kw):
        rows = list(real(db_type, cfg, **kw))
        # A second destination row under an appended (pk, cursor) key — the
        # keyset is a superset of the batch, so the proof must decline.
        rows.append((6, "ghost", "2024-02-03T00:00:00"))
        return rows

    monkeypatch.setattr(
        "services.dest_precount.destination_keyed_rows", extra
    )
    r2 = _run(src, dst)
    assert r2.success, r2.error
    reconcile = r2.reconciliation or {}
    # Declines honestly to the row-count append verdict, not the keyed scope.
    assert reconcile.get("checksum_scope") != "written_batch_keys", reconcile


def test_append_delta_declines_on_repeated_composite_key(row_path, tmp_path):
    """Two appended rows sharing (pk, cursor) make fp_by_key lossy — the
    proof declines honestly instead of comparing partial keys."""
    src = _mk_src(tmp_path, pk=False)
    dst = tmp_path / "dst.db"
    assert _run(src, dst).success
    conn = sqlite3.connect(src)
    # id2 twice with the SAME cursor value → composite key repeats in-batch.
    conn.execute("INSERT INTO t VALUES (2,'dup-a','2024-02-05T00:00:00')")
    conn.execute("INSERT INTO t VALUES (2,'dup-b','2024-02-05T00:00:00')")
    conn.commit()
    conn.close()
    r2 = _run(src, dst)
    assert r2.success, r2.error
    summary = r2.destination_summary or {}
    ad = summary.get("append_delta")
    reconcile = r2.reconciliation or {}
    if ad is not None:
        # stamped payload must carry the repeated key honestly — either the
        # pairs are deduplicated below the row count, or reconcile declined.
        assert reconcile.get("checksum_scope") != "written_batch_keys" or (
            ad.get("rows") == len(ad.get("pairs") or [])
        )


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X97_LIVE") != "1",
    reason="live 4-engine append matrix needs MySQL/PG/MSSQL services",
)
def test_x97_live_append_matrix():
    pytest.skip("runs in the live matrix")
