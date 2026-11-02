"""SFTP destination named without an extension: writer and readers agree on the file.

The writer stored ``out`` as ``out.csv`` (and ignored ``file_format``), while the
destination COUNT, Gate-8 read-back, sample and continuity readers opened ``out``.
The missing file read as an empty destination, so every clean append was refused
as "Destination reset since the last incremental run".
"""

from __future__ import annotations

import io
import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from connectors.sftp_common import sftp_dest_path
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.sftp_test_server import start_sftp_server

FORMATS = ["csv", "jsonl", "parquet"]


@pytest.mark.parametrize(
    ("path", "fmt", "expected"),
    [
        ("/uploads/out", "csv", "/uploads/out.csv"),
        ("/uploads/out", "jsonl", "/uploads/out.jsonl"),
        ("/uploads/out", "ndjson", "/uploads/out.jsonl"),
        ("/uploads/out", "parquet", "/uploads/out.parquet"),
        ("/uploads/out", "excel", "/uploads/out.xlsx"),
        ("uploads/out", "", "uploads/out.csv"),
        ("/out", None, "/out.csv"),
        ("out", "", "out.csv"),
        ("/uploads/out.csv", "parquet", "/uploads/out.csv"),
        ("/uploads/", "csv", "/uploads/"),
        ("", "csv", ""),
    ],
)
def test_sftp_dest_path(path, fmt, expected):
    assert sftp_dest_path(path, fmt) == expected


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


def _tamper(path: Path, fmt: str) -> None:
    if fmt == "parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        tbl = pq.read_table(path)
        v = tbl.column("v").to_pylist()
        v[0] = "TAMPERED"
        idx = tbl.schema.get_field_index("v")
        pq.write_table(tbl.set_column(idx, "v", pa.array(v, type=tbl.schema.field("v").type)), path)
        return
    text = path.read_text()
    assert "v1" in text
    path.write_text(text.replace("v1", "TAMPERED", 1))


class _Route:
    def __init__(self, tmp_path: Path, fmt: str, remote: str):
        self.tmp_path = tmp_path
        self.root = tmp_path / "srv"
        self.root.mkdir()
        self.server, self.runner = start_sftp_server(str(self.root))
        cfg = self.server.endpoint_config(remote)
        self.dest = EndpointConfig(
            kind="database", format="sftp", host=cfg["host"], port=cfg["port"],
            username=cfg["username"], password=cfg["password"], database=cfg["database"],
            table=cfg["table"], extra={"host_key": cfg["host_key"], "file_format": fmt},
        )
        self.src = _source(tmp_path, 1, 11)

    def run(self):
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(self.src), table="t"),
            destination=self.dest,
            sync_mode="incremental_append",
            stream_contracts=[{"name": "t", "sync_mode": "incremental_append", "selected": True,
                               "cursor_field": "seq", "primary_key": ["id"]}],
            skip_preflight=True,
            validation_mode="strict",
        )
        return UniversalTransferEngine().execute_tracked(req, "xl-" + uuid.uuid4().hex[:12])


@pytest.mark.parametrize("fmt", FORMATS)
def test_extensionless_sftp_append_writes_and_reads_the_same_file(fmt, tmp_path):
    route = _Route(tmp_path, fmt, "/out")
    try:
        r1 = route.run()
        assert r1.success, r1.error
        assert sorted(p.name for p in route.root.iterdir()) == [f"out.{fmt}"]
        assert _ids(route.root / f"out.{fmt}", fmt) == list(range(1, 11))
        _source(tmp_path, 11, 13)
        r2 = route.run()
        assert r2.success, r2.error
        assert r2.records_transferred == 2
        assert _ids(route.root / f"out.{fmt}", fmt) == list(range(1, 13))
        assert sorted(p.name for p in route.root.iterdir()) == [f"out.{fmt}"]
    finally:
        route.runner.stop()


@pytest.mark.parametrize("fmt", FORMATS)
def test_extensionless_sftp_tamper_between_runs_fails_closed(fmt, tmp_path):
    route = _Route(tmp_path, fmt, "/out")
    try:
        r1 = route.run()
        assert r1.success, r1.error
        target = route.root / f"out.{fmt}"
        _tamper(target, fmt)
        before = target.read_bytes()
        _source(tmp_path, 11, 13)
        r2 = route.run()
        assert not r2.success
        assert "Verification failed" in (r2.error or ""), r2.error
        assert "Destination reset" not in (r2.error or "")
        assert target.read_bytes() == before
    finally:
        route.runner.stop()


def test_extensionless_sftp_without_file_format_defaults_to_csv(tmp_path):
    route = _Route(tmp_path, "csv", "/out")
    route.dest.extra.pop("file_format")
    try:
        assert route.run().success
        _source(tmp_path, 11, 13)
        r2 = route.run()
        assert r2.success, r2.error
        assert _ids(route.root / "out.csv", "csv") == list(range(1, 13))
    finally:
        route.runner.stop()
