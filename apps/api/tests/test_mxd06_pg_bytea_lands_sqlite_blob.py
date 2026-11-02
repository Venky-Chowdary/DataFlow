"""MXD06 — PG BYTEA -> SQLite stores real BLOB bytes (write path not reproducible).

QA read ``avatar: "AAAAAQ=="`` through ``sample_connector_object``. That
tool returns JSON, and JSON has no bytes type: ``cell_to_string`` renders a
BLOB as base64 for display. The stored cell is a BLOB whose bytes equal the
source (``typeof = 'blob'``, ``hex = '00000001'``). This pins that contract
live so a writer that really base64-encodes into TEXT fails here.
"""

from __future__ import annotations

import sqlite3
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.test_mx2_16_overwrite_create_new_carries_keys import _engine, _pg, _pg_ep  # noqa: E402


def test_live_pg_bytea_overwrite_into_sqlite_is_blob_not_base64_text(monkeypatch, tmp_path):
    conn = _pg()
    cur = conn.cursor()
    src = f"qa_e2e_rt_bin_src_{uuid.uuid4().hex[:8]}"
    payloads = [b"\x00\x00\x00\x01", b"\x00\x00\x00\x02", bytes(range(256))]
    cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, avatar BYTEA)")
    for i, blob in enumerate(payloads, start=1):
        cur.execute(f"INSERT INTO {src} VALUES (%s, %s)", (i, blob))
    from src.transfer.models import EndpointConfig, TransferRequest

    db = tmp_path / "bin.sqlite"
    try:
        result = _engine(monkeypatch)(TransferRequest(
            mappings=[{"source": c, "target": c, "confidence": 1.0} for c in ("id", "avatar")],
            source=_pg_ep(src),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(db), table="bin_dst"),
            sync_mode="full_refresh_overwrite", validation_mode="balanced",
        ))
        assert result.success, result.error
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src}")
        conn.close()
    with sqlite3.connect(db) as lc:
        decl = {r[1]: r[2] for r in lc.execute('PRAGMA table_info("bin_dst")')}
        rows = lc.execute("SELECT typeof(avatar), avatar FROM bin_dst ORDER BY id").fetchall()
    assert decl["avatar"] == "BLOB"
    assert [r[0] for r in rows] == ["blob"] * 3
    assert [bytes(r[1]) for r in rows] == payloads
