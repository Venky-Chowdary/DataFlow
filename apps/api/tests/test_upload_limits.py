"""Upload and gzip-expansion byte ceilings: oversize input is refused, never inflated."""

from __future__ import annotations

import asyncio
import gzip
import io

import pytest
from fastapi.testclient import TestClient

from services import upload_limits
from services.upload_limits import (
    PayloadTooLarge,
    gunzip_capped,
    read_all_bounded,
    read_capped,
    read_upload,
    spool_capped,
)

CAP = 64 * 1024


def _bomb(size: int = 4 * CAP) -> bytes:
    return gzip.compress(b"\0" * size)


@pytest.fixture
def small_caps(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MAX_UPLOAD_BYTES", str(CAP))
    monkeypatch.setenv("DATAFLOW_MAX_DECOMPRESSED_BYTES", str(CAP))


@pytest.fixture
def client():
    from src.main import app

    return TestClient(app, raise_server_exceptions=False)


class _Upload:
    def __init__(self, data: bytes, size: int | None = None) -> None:
        self._buf = io.BytesIO(data)
        self.size = size

    async def read(self, n: int = -1) -> bytes:
        return self._buf.read(n)


def test_env_defaults_and_overrides(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MAX_UPLOAD_BYTES", raising=False)
    monkeypatch.delenv("DATAWRAP_MAX_UPLOAD_BYTES", raising=False)
    assert upload_limits.max_upload_bytes() == upload_limits.DEFAULT_MAX_UPLOAD_BYTES
    monkeypatch.setenv("DATAFLOW_MAX_UPLOAD_BYTES", "not-a-number")
    assert upload_limits.max_upload_bytes() == upload_limits.DEFAULT_MAX_UPLOAD_BYTES
    monkeypatch.setenv("DATAFLOW_MAX_UPLOAD_BYTES", "0")
    assert upload_limits.max_upload_bytes() == upload_limits.DEFAULT_MAX_UPLOAD_BYTES
    monkeypatch.setenv("DATAWRAP_MAX_DECOMPRESSED_BYTES", "12345")
    assert upload_limits.max_decompressed_bytes() == 12345


def test_payload_too_large_is_not_a_value_error():
    # ValueError handlers turn parse errors into 400s or slower fallbacks.
    assert not issubclass(PayloadTooLarge, ValueError)


@pytest.mark.parametrize("size", [0, 1, CAP - 1, CAP])
def test_read_upload_accepts_up_to_cap(size):
    data = b"x" * size
    assert asyncio.run(read_upload(_Upload(data), CAP)) == data


def test_read_upload_refuses_over_cap_without_reading_all():
    upload = _Upload(b"x" * (10 * CAP))
    with pytest.raises(PayloadTooLarge) as info:
        asyncio.run(read_upload(upload, CAP))
    assert info.value.limit == CAP
    assert upload._buf.tell() <= CAP + 1


def test_read_upload_refuses_declared_size_before_reading():
    upload = _Upload(b"x", size=CAP + 1)
    with pytest.raises(PayloadTooLarge):
        asyncio.run(read_upload(upload, CAP))
    assert upload._buf.tell() == 0


def test_read_capped_boundary():
    assert read_capped(io.BytesIO(b"y" * CAP), CAP) == b"y" * CAP
    with pytest.raises(PayloadTooLarge):
        read_capped(io.BytesIO(b"y" * (CAP + 1)), CAP)


def test_gunzip_capped_round_trip_and_multi_member():
    payload = b"a,b\n1,2\n"
    assert gunzip_capped(gzip.compress(payload), CAP) == payload
    assert gunzip_capped(gzip.compress(payload) + gzip.compress(payload), CAP) == payload * 2


def test_gunzip_capped_refuses_bomb():
    with pytest.raises(PayloadTooLarge):
        gunzip_capped(_bomb(), CAP)


def test_read_all_bounded_caps_only_gzip_streams():
    plain = b"z" * (2 * CAP)
    assert read_all_bounded(io.BytesIO(plain), CAP) == plain
    with pytest.raises(PayloadTooLarge):
        read_all_bounded(gzip.GzipFile(fileobj=io.BytesIO(_bomb())), CAP)


def test_spool_capped_is_seekable_and_bounded():
    data = b"q" * (CAP - 10)
    spool = spool_capped(io.BytesIO(data), CAP)
    assert spool.read() == data
    spool.seek(0)
    assert spool.read(3) == b"qqq"
    with pytest.raises(PayloadTooLarge):
        spool_capped(io.BytesIO(b"q" * (CAP + 1)), CAP)


def test_file_parser_refuses_gzip_bomb(small_caps):
    from services.file_parser import FileParser

    with pytest.raises(PayloadTooLarge):
        FileParser.parse(_bomb(), "rows.csv")
    ok = FileParser.parse(gzip.compress(b"a,b\n1,2\n"), "rows.csv")
    assert ok.success and ok.row_count == 1


def test_file_stream_json_peek_refuses_gzip_bomb(small_caps, tmp_path):
    """CSV/TSV stream through gzip row by row; JSON needs the whole image."""
    from src.transfer import file_stream

    bomb = tmp_path / "bomb.json.gz"
    bomb.write_bytes(_bomb())
    with pytest.raises(PayloadTooLarge):
        file_stream.peek_file_source(str(bomb), "bomb.json.gz")
    fine = tmp_path / "rows.json.gz"
    fine.write_bytes(gzip.compress(b'[{"a": 1}]'))
    headers, _schema, total, _sample = file_stream.peek_file_source(str(fine), "rows.json.gz")
    assert headers == ["a"] and total == 1


def test_copy_local_columnar_spool_refuses_bomb(small_caps, tmp_path):
    from services import copy_csv_local

    with pytest.raises(PayloadTooLarge):
        copy_csv_local._seekable_columnar_source(_bomb())
    path = tmp_path / "rows.parquet.gz"
    path.write_bytes(_bomb())
    with pytest.raises(PayloadTooLarge):
        copy_csv_local._seekable_columnar_source(path)


def test_copy_local_parquet_gz_still_reads(small_caps, tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from services import copy_csv_local

    buf = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"]}), buf)
    gz = gzip.compress(buf.getvalue())
    rows = list(copy_csv_local._iter_parquet_records(gz, ["id", "name"]))
    assert [r["name"] for r in rows] == ["a", "b"]
    path = tmp_path / "rows.parquet.gz"
    path.write_bytes(gz)
    assert len(list(copy_csv_local._iter_parquet_records(path, ["id", "name"]))) == 2


def test_copy_local_xml_and_excel_refuse_gzip_bomb(small_caps):
    from services import copy_csv_local

    with pytest.raises(PayloadTooLarge):
        list(copy_csv_local._iter_xml_records(_bomb(), ["a"]))
    with pytest.raises(PayloadTooLarge):
        list(copy_csv_local._iter_excel_records(_bomb(), "rows.xlsx", ["a"]))


def _assert_413(response):
    assert response.status_code == 413, response.text
    body = response.json()
    assert body["error"] == "payload_too_large"
    assert body["limit_bytes"] == CAP


def test_connectors_upload_refuses_oversize_body(small_caps, client):
    response = client.post(
        "/api/v1/connectors/upload",
        files={"file": ("big.csv", b"a\n" + b"1\n" * CAP, "text/csv")},
    )
    _assert_413(response)


def test_connectors_upload_refuses_gzip_bomb_under_upload_cap(small_caps, client):
    bomb = _bomb()
    assert len(bomb) < CAP
    response = client.post(
        "/api/v1/connectors/upload",
        files={"file": ("rows.csv.gz", bomb, "application/gzip")},
    )
    _assert_413(response)


def test_connectors_upload_small_gzip_still_parses(small_caps, client):
    response = client.post(
        "/api/v1/connectors/upload",
        files={"file": ("rows.csv.gz", gzip.compress(b"a,b\n1,2\n"), "application/gzip")},
    )
    assert response.status_code == 200, response.text


def test_analyze_file_refuses_oversize_and_bomb(small_caps, client):
    _assert_413(
        client.post(
            "/api/v1/transfer/analyze-file",
            files={"file": ("big.csv", b"a\n" + b"1\n" * CAP, "text/csv")},
        )
    )
    _assert_413(
        client.post(
            "/api/v1/transfer/analyze-file",
            files={"file": ("rows.json", _bomb(), "application/gzip")},
        )
    )


def test_transfer_run_refuses_oversize_body(small_caps, client):
    response = client.post(
        "/api/v1/transfer/run",
        data={"source_kind": "file"},
        files={"file": ("big.csv", b"a\n" + b"1\n" * CAP, "text/csv")},
    )
    _assert_413(response)


def test_message_names_limit_size_and_setting():
    err = PayloadTooLarge("Uploaded file", 256 * 1024 * 1024, actual=int(312.4 * 1024 * 1024))
    text = str(err)
    assert "312.4 MB" in text and "256 MB" in text
    assert "DATAFLOW_MAX_UPLOAD_BYTES" in text and "DATAWRAP_MAX_UPLOAD_BYTES" in text
    assert "268435456 bytes" in text


def test_message_without_known_size_says_larger_than_limit():
    text = str(PayloadTooLarge("Decompressed file", 1024**3, setting=upload_limits.DECOMPRESSED_SETTING))
    assert "larger than the 1024 MB limit" in text
    assert "DATAFLOW_MAX_DECOMPRESSED_BYTES" in text


def test_gzip_bomb_names_the_decompression_setting():
    with pytest.raises(PayloadTooLarge) as info:
        gunzip_capped(_bomb(), CAP)
    assert info.value.setting == upload_limits.DECOMPRESSED_SETTING
    assert "DATAFLOW_MAX_DECOMPRESSED_BYTES" in str(info.value)
    with pytest.raises(PayloadTooLarge) as info:
        spool_capped(io.BytesIO(b"q" * (CAP + 1)), CAP)
    assert info.value.setting == upload_limits.DECOMPRESSED_SETTING


def test_declared_upload_size_is_reported():
    with pytest.raises(PayloadTooLarge) as info:
        asyncio.run(read_upload(_Upload(b"x", size=3 * CAP), CAP))
    assert info.value.actual == 3 * CAP
    assert info.value.setting == upload_limits.UPLOAD_SETTING


def test_413_body_tells_the_user_limit_size_and_setting(small_caps, client):
    body = b"a\n" + b"1\n" * CAP
    response = client.post(
        "/api/v1/connectors/upload", files={"file": ("big.csv", body, "text/csv")}
    )
    assert response.status_code == 413, response.text
    payload = response.json()
    assert payload["limit_bytes"] == CAP
    assert payload["setting"] == "DATAFLOW_MAX_UPLOAD_BYTES"
    assert payload["actual_bytes"] >= len(body)
    assert f"{CAP} bytes" in payload["detail"]
    assert "DATAFLOW_MAX_UPLOAD_BYTES" in payload["detail"]


def test_413_for_gzip_bomb_points_at_the_decompression_setting(small_caps, client):
    response = client.post(
        "/api/v1/connectors/upload",
        files={"file": ("rows.csv.gz", _bomb(), "application/gzip")},
    )
    assert response.status_code == 413, response.text
    payload = response.json()
    assert payload["setting"] == "DATAFLOW_MAX_DECOMPRESSED_BYTES"
    assert "actual_bytes" not in payload
    assert "DATAFLOW_MAX_DECOMPRESSED_BYTES" in payload["detail"]
