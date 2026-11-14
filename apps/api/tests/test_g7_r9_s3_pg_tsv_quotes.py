"""G7 re-review B1 (reader check): S3→PostgreSQL COPY and a '"' inside a TSV object.

PostgreSQL ``COPY ... (FORMAT csv, DELIMITER E'\\t')`` treats a '"' anywhere in
a field as the start of a quoted section, so a TSV cell ``5" pipe`` (which
the product writes raw, as it always did) aborts the COPY with
"unterminated CSV quoted field". The fast path now declines a TSV object
holding a '"' to the row path, whose codec reads it literally. S3 is faked
(MinIO is not available here); PostgreSQL is the local server.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

_PG = {
    "host": "127.0.0.1", "port": 5432, "database": "dataflow",
    "username": "dataflow", "password": "dataflow", "schema": "public",
}


def _pg_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")


class _FakeS3:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def download_file(self, _bucket, _key, path):
        Path(path).write_bytes(self.body)


def _copy(monkeypatch, body: bytes, rows: int):
    from services import copy_s3_pg

    monkeypatch.setattr(copy_s3_pg, "s3_list_keys", lambda *_a, **_k: ["src.tsv"])
    monkeypatch.setattr(copy_s3_pg, "s3_dest_count", lambda *_a, **_k: rows)
    monkeypatch.setattr(copy_s3_pg, "s3_client", lambda *_a, **_k: _FakeS3(body))
    monkeypatch.setattr(copy_s3_pg, "s3_bucket", lambda *_a, **_k: "bucket")
    table = f"g7_s3pg_{uuid.uuid4().hex[:8]}"
    try:
        return copy_s3_pg.copy_s3_to_postgres(
            source_cfg={"type": "s3"},
            source_table="src.tsv",
            dest_cfg=dict(_PG),
            dest_schema="public",
            dest_table=table,
            pairs=[("id", "id"), ("note", "note")],
            pg_ddls=["TEXT", "TEXT"],
            replace_destination=True,
        ), table
    finally:
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')  # nosec B608
        conn.close()


def test_tsv_with_mid_field_quote_declines_to_the_row_path(monkeypatch):
    _pg_or_skip()
    from services.copy_fast_path import FastPathUnavailable

    with pytest.raises(FastPathUnavailable, match="quote"):
        _copy(monkeypatch, b'id\tnote\n1\t5" pipe\n', 1)


def test_plain_tsv_still_takes_the_fast_path(monkeypatch):
    _pg_or_skip()
    result, _table = _copy(monkeypatch, b"id\tnote\n1\tplain\n2\ta,b\n", 2)
    assert result.rows_copied == 2
