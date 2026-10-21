"""X-5: incremental append to SFTP must add to the remote file, not replace it.

The stream writer handed object stores the sync mode but not SFTP, so the
SFTP writer saw no mode, chose "replace", and an append run overwrote the
previously delivered rows with only the new batch.
"""

from __future__ import annotations

import io
import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.sftp_test_server import start_sftp_server

FORMATS = ["csv", "jsonl", "parquet"]


def _source(tmp_path: Path, lo: int, hi: int) -> Path:
    p = tmp_path / "src.sqlite"
    conn = sqlite3.connect(p)
    conn.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
    conn.executemany("INSERT INTO t VALUES (?,?,?)", [(i, i, f"v{i}") for i in range(lo, hi)])
    conn.commit()
    conn.close()
    return p


def _ids(path: Path, fmt: str) -> list[int]:
    data = path.read_bytes()
    if fmt == "csv":
        lines = data.decode().splitlines()
        col = lines[0].split(",").index("id")
        return sorted(int(r.split(",")[col]) for r in lines[1:] if r)
    if fmt == "jsonl":
        return sorted(int(json.loads(r)["id"]) for r in data.decode().splitlines() if r)
    import pyarrow.parquet as pq

    return sorted(int(x) for x in pq.read_table(io.BytesIO(data)).column("id").to_pylist())


@pytest.mark.parametrize("fmt", FORMATS)
def test_append_to_sftp_keeps_previously_delivered_rows(fmt, tmp_path):
    root = tmp_path / "srv"
    root.mkdir()
    server, runner = start_sftp_server(str(root))
    try:
        cfg = server.endpoint_config(f"/out.{fmt}")
        dest = EndpointConfig(
            kind="database", format="sftp", host=cfg["host"], port=cfg["port"],
            username=cfg["username"], password=cfg["password"], database=cfg["database"],
            table=cfg["table"], extra={"host_key": cfg["host_key"]},
        )
        src = _source(tmp_path, 1, 11)

        def run():
            req = TransferRequest(
                source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
                destination=dest,
                sync_mode="incremental_append",
                stream_contracts=[{"name": "t", "sync_mode": "incremental_append", "selected": True,
                                   "cursor_field": "seq", "primary_key": ["id"]}],
                skip_preflight=True,
                validation_mode="strict",
            )
            return UniversalTransferEngine().execute_tracked(req, "x5-" + uuid.uuid4().hex[:12])

        r1 = run()
        assert r1.success, r1.error
        assert _ids(root / f"out.{fmt}", fmt) == list(range(1, 11))
        _source(tmp_path, 11, 13)
        r2 = run()
        assert r2.success, r2.error
        assert r2.records_transferred == 2
        assert _ids(root / f"out.{fmt}", fmt) == list(range(1, 13))
        r3 = run()  # no new rows: idempotent
        assert r3.success, r3.error
        assert _ids(root / f"out.{fmt}", fmt) == list(range(1, 13))
    finally:
        runner.stop()
