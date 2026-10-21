"""X-5: append to a file destination fails closed when the delivered file was tampered.

X-97's continuity baseline now covers SFTP and S3 objects: after a verified
run the file's (rows, digest) is recorded; before the next append writes,
the file is re-read and must match, or the run fails and the file is left
byte-for-byte as found.
"""

from __future__ import annotations

import io
import json
import uuid
from pathlib import Path

import pytest

from services import append_continuity as ac
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.sftp_test_server import start_sftp_server
from tests.test_x5_sftp_append_keeps_prior_rows import _ids, _source

FORMATS = ["csv", "jsonl", "parquet"]
TAMPERS = ["changed_value", "deleted_row", "stray_row"]


def _rows(path: Path, fmt: str) -> list[dict]:
    data = path.read_bytes()
    if fmt == "csv":
        import csv

        return list(csv.DictReader(io.StringIO(data.decode())))
    if fmt == "jsonl":
        return [json.loads(r) for r in data.decode().splitlines() if r]
    import pyarrow.parquet as pq

    return pq.read_table(io.BytesIO(data)).to_pylist()


def _write(path: Path, fmt: str, rows: list[dict]) -> None:
    if fmt == "csv":
        import csv

        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=list(rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
        path.write_text(buf.getvalue())
    elif fmt == "jsonl":
        path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    else:
        import pyarrow as pa
        import pyarrow.parquet as pq

        pq.write_table(pa.Table.from_pylist(rows), path)


def _tamper(path: Path, fmt: str, kind: str) -> None:
    rows = _rows(path, fmt)
    if kind == "changed_value":
        rows[2]["v"] = "tampered"
    elif kind == "deleted_row":
        rows.pop(4)
    else:
        stray = dict(rows[0])
        stray["id"] = 999 if fmt != "csv" else "999"
        rows.append(stray)
    _write(path, fmt, rows)


@pytest.fixture
def sftp(tmp_path):
    root = tmp_path / "srv"
    root.mkdir()
    server, runner = start_sftp_server(str(root))
    try:
        yield server, root
    finally:
        runner.stop()


def _dest(server, fmt: str) -> EndpointConfig:
    cfg = server.endpoint_config(f"/out.{fmt}")
    return EndpointConfig(
        kind="database", format="sftp", host=cfg["host"], port=cfg["port"],
        username=cfg["username"], password=cfg["password"], database=cfg["database"],
        table=cfg["table"], extra={"host_key": cfg["host_key"]},
    )


def _run(src: Path, dest: EndpointConfig):
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


@pytest.mark.parametrize("kind", TAMPERS)
@pytest.mark.parametrize("fmt", FORMATS)
def test_tampered_sftp_file_fails_the_next_append_and_is_left_unchanged(fmt, kind, sftp, tmp_path):
    server, root = sftp
    dest = _dest(server, fmt)
    src = _source(tmp_path, 1, 11)
    r1 = _run(src, dest)
    assert r1.success, r1.error
    out = root / f"out.{fmt}"
    _tamper(out, fmt, kind)
    before = out.read_bytes()
    _source(tmp_path, 11, 13)
    r2 = _run(src, dest)
    assert not r2.success
    assert f"Verification failed for table 'out.{fmt}'" in r2.error
    assert r2.error_details["mismatch"] in {"checksum", "row_count"}
    assert ac.REBASELINE_ENDPOINT in r2.error
    assert out.read_bytes() == before


@pytest.mark.parametrize("fmt", FORMATS)
def test_clean_sftp_appends_stay_green_and_idempotent(fmt, sftp, tmp_path):
    server, root = sftp
    dest = _dest(server, fmt)
    src = _source(tmp_path, 1, 11)
    r1 = _run(src, dest)
    assert r1.success, r1.error
    _source(tmp_path, 11, 13)
    r2 = _run(src, dest)
    assert r2.success, r2.error
    r3 = _run(src, dest)
    assert r3.success, r3.error
    assert _ids(root / f"out.{fmt}", fmt) == list(range(1, 13))


def test_file_destinations_are_covered():
    assert {"sftp", "s3"} <= ac.CONTINUITY_ENGINES


@pytest.fixture
def s3(monkeypatch):
    """S3-compatible HTTP endpoint (moto server) — a real wire protocol, not a mock client."""
    import socket

    import boto3
    from moto.server import ThreadedMotoServer

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port)
    server.start()
    url = f"http://127.0.0.1:{port}"
    import urllib.request

    # moto keeps one in-process backend across server instances; start clean.
    urllib.request.urlopen(urllib.request.Request(f"{url}/moto-api/reset", method="POST"))
    client = boto3.client(
        "s3", endpoint_url=url, aws_access_key_id="x5key",
        aws_secret_access_key="x5secret", region_name="us-east-1",
    )
    client.create_bucket(Bucket="x5bucket")
    try:
        yield client, url, port
    finally:
        server.stop()


def _s3_dest(url: str, port: int, key: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="s3", host="127.0.0.1", port=port, database="x5bucket",
        username="x5key", password="x5secret", table=key, endpoint_url=url,
        path_style=True, region="us-east-1",
    )


@pytest.mark.parametrize("kind", TAMPERS)
@pytest.mark.parametrize("fmt", FORMATS)
def test_tampered_s3_object_fails_the_next_append_and_is_left_unchanged(fmt, kind, s3, tmp_path):
    client, url, port = s3
    key = f"out.{fmt}"
    dest = _s3_dest(url, port, key)
    src = _source(tmp_path, 1, 11)
    r1 = _run(src, dest)
    assert r1.success, r1.error
    local = tmp_path / key
    local.write_bytes(client.get_object(Bucket="x5bucket", Key=key)["Body"].read())
    _tamper(local, fmt, kind)
    client.put_object(Bucket="x5bucket", Key=key, Body=local.read_bytes())
    before = local.read_bytes()
    _source(tmp_path, 11, 13)
    r2 = _run(src, dest)
    assert not r2.success
    assert f"Verification failed for table '{key}'" in r2.error
    assert ac.REBASELINE_ENDPOINT in r2.error
    assert client.get_object(Bucket="x5bucket", Key=key)["Body"].read() == before


@pytest.mark.parametrize("fmt", FORMATS)
def test_clean_s3_appends_stay_green_and_idempotent(fmt, s3, tmp_path):
    client, url, port = s3
    key = f"out.{fmt}"
    dest = _s3_dest(url, port, key)
    src = _source(tmp_path, 1, 11)
    r1 = _run(src, dest)
    assert r1.success, r1.error
    _source(tmp_path, 11, 13)
    r2 = _run(src, dest)
    assert r2.success, r2.error
    r3 = _run(src, dest)
    assert r3.success, r3.error
    out = tmp_path / key
    out.write_bytes(client.get_object(Bucket="x5bucket", Key=key)["Body"].read())
    assert _ids(out, fmt) == list(range(1, 13))
