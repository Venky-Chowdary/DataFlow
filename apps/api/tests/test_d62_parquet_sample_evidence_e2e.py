"""D62 - sample_connector_object on a Parquet object shows what the file holds.

QA (prod ac612203): Parquet -> SQLite data was correct, but the copilot sample
of the same object

1. showed SQL NULL as the literal ``__DF_SQL_NULL__``,
2. counted ``''`` as NULL in its column profile,
3. labelled ``amt`` ``DECIMAL(3,2)`` although it holds 123456789.12345.

Root causes:
1. ``query_tools._sample_batch_source`` handed the reader's *wire* cells to
   the operator. One renderer now owns operator cells
   (``value_serializer.operator_cell``): NULL -> JSON null, text verbatim.
2. ``_is_nullish`` used ``is_null_evidence`` (NULL *or blank*), a type-
   evidence rule, for a null count. NULL is now NULL only; blanks are counted
   as ``empty_strings``.
3. The object/SFTP readers typed every column from the sampled page
   (``sampled_native_types_meta``) and threw away the schema the Parquet file
   declares. ``read_rows_and_schema_from_spill`` now returns that schema and
   it wins (authority ``declared``).

Routes: the real SFTP protocol (tests/sftp_test_server.py) and S3 through
boto3 (moto - an emulator; QA's live object store is theirs to confirm).
"""

from __future__ import annotations

import io
import uuid
from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from tests import value_fidelity_route as r  # noqa: E402

_NAMES = ["x", None, "", "   ", "Łukasz 山田 🎉", "y"]
_AMTS = [1.25, None, 2.5, 3.75, 123456789.12345, 0.5]
_DECS = [Decimal("1.25"), None, Decimal("2.50"), Decimal("0.00"),
         Decimal("123456789.12345"), Decimal("0.50")]


def _parquet() -> bytes:
    table = pa.table({
        "id": pa.array(list(range(1, 7)), pa.int64()),
        "name": pa.array(_NAMES, pa.string()),
        "amt": pa.array(_AMTS, pa.float64()),
        "dec": pa.array(_DECS, pa.decimal128(20, 5)),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@pytest.fixture
def client(tmp_path, monkeypatch):
    with r.make_client(tmp_path, monkeypatch) as c:
        yield c


def _assert_sample(out: dict, limit: int) -> None:
    rows = out["rows"]
    assert [row["name"] for row in rows] == _NAMES[:limit]
    assert all("__DF_" not in str(v) for row in rows for v in row.values()), rows
    assert rows[1]["amt"] is None and rows[1]["dec"] is None
    schema = out["column_schema"]
    # The file's declared types, however few rows were sampled.
    assert schema["amt"] == "DOUBLE", schema
    assert schema["dec"] == "DECIMAL(20,5)", schema
    prof = {c["column"]: c for c in out["analysis"]["columns"]}
    assert prof["name"]["nulls"] == _NAMES[:limit].count(None)
    assert prof["name"]["empty_strings"] == sum(
        1 for v in _NAMES[:limit] if v is not None and not v.strip()
    )
    assert prof["amt"]["nulls"] == _AMTS[:limit].count(None)


@pytest.mark.parametrize("limit", [3, 6])
def test_sftp_parquet_sample_shows_null_empty_and_declared_types(client, tmp_path, limit):
    from services.connector_store import create_connector
    from src.ai.copilot.query_tools import sample_connector_object
    from tests.sftp_test_server import start_sftp_server

    root = tmp_path / "sftp"
    root.mkdir()
    (root / "p.parquet").write_bytes(_parquet())
    server, runner = start_sftp_server(str(root))
    try:
        cfg = server.endpoint_config("/p.parquet")
        conn = create_connector({
            "name": f"sftp-{uuid.uuid4().hex[:6]}", "type": "sftp", "role": "source",
            "host": cfg["host"], "port": cfg["port"], "username": cfg["username"],
            "password": cfg["password"], "database": cfg["database"],
            "host_key": cfg["host_key"],
        })
        res = sample_connector_object(connector_id=conn.id, table="p.parquet", limit=limit)
        assert res.success, res.error
        _assert_sample(res.output, limit)
    finally:
        runner.stop()


@pytest.mark.parametrize("limit", [3, 6])
def test_s3_parquet_sample_shows_null_empty_and_declared_types(client, monkeypatch, limit):
    moto = pytest.importorskip("moto")
    import boto3

    from services.connector_store import create_connector
    from src.ai.copilot.query_tools import sample_connector_object

    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                 "AWS_DEFAULT_REGION": "us-east-1"}.items():
        monkeypatch.setenv(k, v)
    bucket = f"d62-{uuid.uuid4().hex[:8]}"
    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="p.parquet", Body=_parquet())
        conn = create_connector({
            "name": f"s3-{bucket}", "type": "s3", "role": "source", "workspace_id": "",
            "host": "s3.amazonaws.com", "port": 443, "database": bucket,
            "username": "testing", "password": "testing", "region": "us-east-1",
        })
        res = sample_connector_object(connector_id=conn.id, table="p.parquet", limit=limit)
        assert res.success, res.error
        _assert_sample(res.output, limit)


def test_upload_preview_keeps_parquet_null_distinct_from_empty():
    from services.file_parser import _parse_parquet_preview

    _headers, rows, _n, _schema = _parse_parquet_preview(_parquet())
    assert [row[1] for row in rows] == _NAMES


@pytest.mark.parametrize(
    "wire,shown",
    [("__DF_SQL_NULL__", None), (None, None), ("", ""), ("   ", "   "),
     ("Łukasz", "Łukasz"), ("__DF_ESC____DF_SQL_NULL__", "__DF_SQL_NULL__"), (5, 5)],
)
def test_operator_cell_is_the_one_sample_renderer(wire, shown):
    from services.value_serializer import operator_cell

    assert operator_cell(wire) == shown
