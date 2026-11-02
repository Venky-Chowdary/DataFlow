"""X-41b: count-only engine copies must report verification_verdict 'partial'.

A count-only engine copy carries a one-sided read-back digest on
``target_checksum`` with no source digest and ``checksum_match`` unset —
the old "has a digest" test stamped ``verified`` over a run that never
compared values. Verified is earned only by a measured two-sided digest
match (the DATAFLOW_ENGINE_DIGEST ladder promotion).
"""

import sqlite3
import uuid
from pathlib import Path

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _run(sqlite_src: Path, sqlite_dst: Path, *, digest: bool, monkeypatch):
    if digest:
        monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    else:
        monkeypatch.delenv("DATAFLOW_ENGINE_DIGEST", raising=False)
    conn = sqlite3.connect(str(sqlite_src))
    try:
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"
        )
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        conn.commit()
    finally:
        conn.close()
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(sqlite_src),
            table="items",
        ),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(sqlite_dst),
            table="items_out",
        ),
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {"source": "label", "target": "label", "transform": "none"},
        ],
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


@pytest.mark.parametrize("digest", [False, True])
def test_count_only_partial_digest_verified(tmp_path: Path, monkeypatch, digest):
    result = _run(tmp_path / "s.db", tmp_path / "d.db", digest=digest, monkeypatch=monkeypatch)
    assert result.success, result.error
    report = result.reconciliation or {}
    if digest:
        assert report.get("checksum_match") is True
        assert report.get("assurance_level") == "full_checksum"
        assert report.get("verification_verdict") == "verified"
    else:
        assert report.get("checksum_scope") == "dest_count_equals_source_snapshot_count"
        assert report.get("verification_verdict") == "partial"
