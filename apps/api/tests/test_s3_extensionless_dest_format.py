"""S3 extensionless key honors file_format in the writer and every reader.

An extensionless key ``out`` was always written as ``out/export.json`` with JSON
bytes, whatever ``file_format`` the user picked. The object is now
``out/export.<fmt>`` in the requested format, and COUNT, Gate-8 read-back and
the continuity reader resolve the same key.
"""

from __future__ import annotations

import io
import json

import pytest

from connectors.object_store_common import normalize_object_base_key
from src.transfer.models import EndpointConfig
from tests.test_x5_file_destination_continuity import _run, _s3_dest, s3  # noqa: F401
from tests.test_x5_sftp_append_keeps_prior_rows import _source

FORMATS = ["csv", "jsonl", "parquet"]


@pytest.mark.parametrize(
    ("key", "fmt", "expected"),
    [
        ("out", "csv", "out/export.csv"),
        ("out", "jsonl", "out/export.jsonl"),
        ("out", "ndjson", "out/export.jsonl"),
        ("out", "parquet", "out/export.parquet"),
        ("out", "excel", "out/export.xlsx"),
        ("out", "", "out/export.json"),
        ("out", "CSV", "out/export.csv"),
        ("out.csv", "parquet", "out.csv"),
        ("dir/out", "csv", "dir/out/export.csv"),
    ],
)
def test_extensionless_key_takes_file_format_extension(key, fmt, expected):
    assert normalize_object_base_key(key, "", fmt) == expected


def test_schema_prefix_is_kept_with_file_format():
    assert normalize_object_base_key("out", "pre", "csv") == "pre/out/export.csv"


def _dest(url: str, port: int, fmt: str | None) -> EndpointConfig:
    dest = _s3_dest(url, port, "out")
    if fmt:
        dest.extra = {**(dest.extra or {}), "file_format": fmt}
    return dest


def _keys(client) -> list[str]:
    return sorted(o["Key"] for o in client.list_objects_v2(Bucket="x5bucket").get("Contents", []))


def _ids(data: bytes, fmt: str) -> list[int]:
    if fmt == "csv":
        lines = data.decode().splitlines()
        col = lines[0].split(",").index("id")
        return sorted(int(r.split(",")[col]) for r in lines[1:] if r)
    if fmt == "jsonl":
        return sorted(int(json.loads(r)["id"]) for r in data.decode().splitlines() if r)
    import pyarrow.parquet as pq

    assert data[:4] == b"PAR1", "parquet requested but the object is not parquet bytes"
    return sorted(int(x) for x in pq.read_table(io.BytesIO(data)).column("id").to_pylist())


def _get(client, key: str) -> bytes:
    return client.get_object(Bucket="x5bucket", Key=key)["Body"].read()


@pytest.mark.parametrize("fmt", FORMATS)
def test_extensionless_s3_key_writes_and_appends_in_requested_format(fmt, s3, tmp_path):  # noqa: F811
    client, url, port = s3
    dest = _dest(url, port, fmt)
    src = _source(tmp_path, 1, 11)
    r1 = _run(src, dest)
    assert r1.success, r1.error
    assert _keys(client) == [f"out/export.{fmt}"]
    assert _ids(_get(client, f"out/export.{fmt}"), fmt) == list(range(1, 11))

    _source(tmp_path, 11, 13)
    r2 = _run(src, dest)
    assert r2.success, r2.error
    assert _keys(client) == [f"out/export.{fmt}"]
    assert _ids(_get(client, f"out/export.{fmt}"), fmt) == list(range(1, 13))


@pytest.mark.parametrize("fmt", FORMATS)
def test_extensionless_s3_tamper_between_runs_fails_closed(fmt, s3, tmp_path):  # noqa: F811
    client, url, port = s3
    dest = _dest(url, port, fmt)
    src = _source(tmp_path, 1, 11)
    assert _run(src, dest).success
    key = f"out/export.{fmt}"
    local = tmp_path / f"o.{fmt}"
    local.write_bytes(_get(client, key))
    from tests.test_x5_file_destination_continuity import _tamper

    _tamper(local, fmt, "changed_value")
    client.put_object(Bucket="x5bucket", Key=key, Body=local.read_bytes())
    before = _get(client, key)
    _source(tmp_path, 11, 13)
    r2 = _run(src, dest)
    assert not r2.success, "a tampered object must fail the next append"
    assert _get(client, key) == before


def test_extensionless_s3_key_without_file_format_stays_json(s3, tmp_path):  # noqa: F811
    client, url, port = s3
    src = _source(tmp_path, 1, 11)
    r1 = _run(src, _dest(url, port, None))
    assert r1.success, r1.error
    assert _keys(client) == ["out/export.json"]
