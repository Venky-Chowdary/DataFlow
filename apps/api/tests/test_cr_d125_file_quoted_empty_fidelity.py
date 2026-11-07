"""CR-D125 / D62 — one NULL rule for file and object sources.

C04: an unquoted empty CSV field is SQL NULL, a quoted ``""`` is ``''``.
Defects, each measured on the pre-fix tree:

- local CSV ``""`` → SQLite failed "Checksum mismatch" and dropped the table:
  the identity COPY's mapped file wrote ``''`` as a bare empty field and its
  own re-read decoded that as NULL, while the digest kept ``''``;
- local CSV → MySQL failed the read-back sample on an unquoted empty field:
  the in-memory CSV preview (the Gate-8 reconcile sample) used a plain
  ``csv.reader`` and saw ``''`` where the writer landed NULL;
- S3 CSV → PostgreSQL COPY used ``NULL '\\N'``, landing every NULL as ``''``
  under a count-only proof;
- object-store reader rows (S3/GCS/ADLS/SFTP) spelled NULL ``''``: a Parquet
  sample showed NULL as an empty string (D62).
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG = dict(host="127.0.0.1", port=5432, database="dataflow", username="dataflow", password="dataflow")
MYSQL = dict(host="127.0.0.1", port=3306, database="dataflow", username="dataflow", password="dataflow")


@pytest.fixture(autouse=True)
def _isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)


def run_transfer(source: EndpointConfig, destination: EndpointConfig, **extra):
    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        **extra,
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def file_source(name: str, body: str) -> dict:
    ext = name.rsplit(".", 1)[-1]
    return dict(
        source=EndpointConfig(kind="file", format=ext),
        source_filename=name,
        source_content=body.encode("utf-8"),
    )


def sqlite_dest(tmp_path: Path) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "dst.db"), table="t"
    )


def sqlite_rows(tmp_path: Path) -> list[tuple]:
    conn = sqlite3.connect(str(tmp_path / "dst.db"))
    try:
        return conn.execute("SELECT id, name FROM t ORDER BY id").fetchall()
    finally:
        conn.close()


def pg_dest(table: str) -> EndpointConfig:
    return EndpointConfig(kind="database", format="postgresql", schema="public", table=table, **PG)


def pg_rows_and_drop(table: str) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"], port=PG["port"], dbname=PG["database"],
        user=PG["username"], password=PG["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, name FROM public."{table}" ORDER BY id')
            rows = [(int(r[0]), r[1]) for r in cur.fetchall()]
            return rows
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()


def mysql_dest(table: str) -> EndpointConfig:
    return EndpointConfig(kind="database", format="mysql", table=table, **MYSQL)


def mysql_rows_and_drop(table: str) -> list[tuple]:
    import pymysql

    conn = pymysql.connect(
        host=MYSQL["host"], port=MYSQL["port"], user=MYSQL["username"],
        password=MYSQL["password"], database=MYSQL["database"], autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, name FROM `{table}` ORDER BY id")
            return [(int(r[0]), r[1]) for r in cur.fetchall()]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()


def s3_source(bucket: str, key: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="s3", host="s3.amazonaws.com", port=443,
        database=bucket, username="testing", password="testing",
        table=key, region="us-east-1",
    )


def put_s3_object(bucket: str, key: str, body: str) -> None:
    import boto3

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=bucket)
    client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def assert_verified(result) -> None:
    assert result.success is True, result.error
    recon = result.reconciliation or {}
    if recon:
        assert recon.get("passed") is True, recon.get("message")


EMPTY_CSV = 'id,name\n1,""\n2,\n3,x\n'
EMPTY_EXPECTED = [(1, ""), (2, None), (3, "x")]


@pytest.mark.parametrize("fast_path", ["1", "0"])
def test_local_csv_quoted_empty_lands_empty_string_in_sqlite(tmp_path, monkeypatch, fast_path):
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", fast_path)
    result = run_transfer(destination=sqlite_dest(tmp_path), **file_source("t.csv", EMPTY_CSV))
    assert_verified(result)
    assert sqlite_rows(tmp_path) == EMPTY_EXPECTED


def test_local_csv_unquoted_empty_is_null_in_mysql():
    table = f"crd125_{uuid.uuid4().hex[:8]}"
    result = run_transfer(destination=mysql_dest(table), **file_source("t.csv", EMPTY_CSV))
    rows = mysql_rows_and_drop(table)
    assert_verified(result)
    assert rows == EMPTY_EXPECTED


def test_in_memory_csv_preview_keeps_the_quoting_distinction():
    from src.transfer.file_stream import peek_file_source

    _headers, _schema, total, sample = peek_file_source(EMPTY_CSV.encode(), "t.csv")
    assert total == 3
    assert [row["name"] for row in sample] == ["", None, "x"]


def test_s3_csv_to_postgres_copy_keeps_null_and_empty_distinct():
    import moto

    table = f"crd125_{uuid.uuid4().hex[:8]}"
    bucket = f"crd125-{uuid.uuid4().hex[:8]}"
    with moto.mock_aws():
        put_s3_object(bucket, "in/t.csv", EMPTY_CSV)
        result = run_transfer(source=s3_source(bucket, "in/t.csv"), destination=pg_dest(table))
    rows = pg_rows_and_drop(table)
    assert_verified(result)
    assert rows == EMPTY_EXPECTED


def test_s3_csv_legacy_backslash_n_null_leaves_the_copy_to_the_codec(tmp_path):
    """``\\N`` is NULL to the codec but text to a CSV ``NULL ''`` COPY."""
    import moto

    from services.copy_s3_common import s3_file_has_legacy_null

    legacy = tmp_path / "legacy.csv"
    legacy.write_text('id,name\n1,\\N\n2,""\n', encoding="utf-8")
    plain = tmp_path / "plain.csv"
    plain.write_text(EMPTY_CSV, encoding="utf-8")
    assert s3_file_has_legacy_null(str(legacy)) is True
    assert s3_file_has_legacy_null(str(plain)) is False
    # The scan straddles read chunks.
    assert s3_file_has_legacy_null(str(legacy), chunk_size=10) is True

    table = f"crd125_{uuid.uuid4().hex[:8]}"
    bucket = f"crd125-{uuid.uuid4().hex[:8]}"
    with moto.mock_aws():
        put_s3_object(bucket, "in/t.csv", legacy.read_text(encoding="utf-8"))
        result = run_transfer(source=s3_source(bucket, "in/t.csv"), destination=pg_dest(table))
    rows = pg_rows_and_drop(table)
    assert_verified(result)
    assert rows == [(1, None), (2, "")]


def test_object_reader_rows_spell_null_as_the_wire_sentinel(tmp_path):
    """D62: reader rows carry NULL as the sentinel the UI renders as NULL."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from services.object_streaming import read_rows_from_spill
    from services.value_serializer import SQL_NULL_SENTINEL

    parquet = tmp_path / "t.parquet"
    pq.write_table(pa.table({"id": [1, 2], "name": [None, ""]}), parquet)
    _headers, rows, total = read_rows_from_spill(parquet, "t.parquet")
    assert total == 2
    assert rows == [["1", SQL_NULL_SENTINEL], ["2", ""]]

    csv_path = tmp_path / "t.csv"
    csv_path.write_text(EMPTY_CSV, encoding="utf-8")
    _headers, rows, _total = read_rows_from_spill(csv_path, "t.csv")
    assert [r[1] for r in rows] == ["", SQL_NULL_SENTINEL, "x"]
