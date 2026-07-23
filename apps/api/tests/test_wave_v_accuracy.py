"""Wave V accuracy: CDC watermark honesty, SaaS schema, object fail policy, files."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_oracle_full_cdc_page_refuses_unsafe_scn_advance():
    from connectors.oracle_change_stream import OracleFlashbackCdc, encode_oracle_resume_token

    cdc = OracleFlashbackCdc(
        {"host": "localhost", "username": "APP"},
        table="orders",
        primary_key="ID",
        schema="APP",
        batch_size=2,
        resume_token=encode_oracle_resume_token(100, table="orders", phase="streaming"),
    )
    prior_scn = cdc.scn
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("ID",), ("AMOUNT",), ("DF_OP",), ("DF_SCN",)]
    cur.fetchone.return_value = (500,)
    # Full page of changes — advancing to head_scn would skip remainder.
    cur.fetchall.return_value = [
        ("1", "10", "U", 101),
        ("2", "20", "U", 102),
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        with pytest.raises(RuntimeError, match="refusing to advance the SCN"):
            list(cdc.poll())

    assert cdc.scn == prior_scn


def test_sqlserver_full_ct_page_refuses_unsafe_version_advance():
    from connectors.sqlserver_change_stream import (
        SqlServerChangeTrackingCdc,
        encode_sqlserver_resume_token,
    )

    cdc = SqlServerChangeTrackingCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        batch_size=2,
        resume_token=encode_sqlserver_resume_token(5, table="orders", phase="streaming"),
    )
    prior = cdc.version
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = [(6, "U", "1"), (6, "U", "2")]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        with pytest.raises(RuntimeError, match="refusing to advance"):
            list(cdc.poll())

    assert cdc.version == prior


@pytest.mark.parametrize(
    "module_path,bucket_kw",
    [
        ("connectors.s3_writer", {"database": "bucket"}),
        ("connectors.gcs_writer", {"database": "bucket"}),
        ("connectors.adls_writer", {"database": "container"}),
    ],
)
def test_object_writers_honor_fail_error_policy(module_path, bucket_kw):
    import importlib

    mod = importlib.import_module(module_path)
    result = mod.write_mapped_rows(
        host="localhost",
        port=443,
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=True,
        table_name="exports/out.json",
        headers=["id", "amount"],
        data_rows=[["1", "not-a-decimal"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
        ],
        column_types={"id": "INTEGER", "amount": "DECIMAL"},
        error_policy="fail",
        **bucket_kw,
    )
    assert result.ok is False
    assert "Transform errors" in (result.error or "")
    assert result.rows_written == 0


def test_legacy_get_file_chunks_refuses_binary_formats(tmp_path):
    from services import file_parser as fp

    path = tmp_path / "orders.avro"
    path.write_bytes(b"Obj\x01binary")
    record = {
        "id": "f1",
        "path": str(path),
        "format": "avro",
        "encoding": "utf-8",
        "delimiter": ",",
    }
    with patch.object(fp, "get_file", return_value=record):
        with pytest.raises(ValueError, match="legacy CSV chunker"):
            list(fp.get_file_chunks("f1"))


def test_legacy_get_file_chunks_refuses_parquet(tmp_path):
    from services import file_parser as fp

    path = tmp_path / "orders.parquet"
    path.write_bytes(b"PAR1")
    record = {
        "id": "f2",
        "path": str(path),
        "format": "parquet",
        "encoding": "utf-8",
        "delimiter": ",",
    }
    with patch.object(fp, "get_file", return_value=record):
        with pytest.raises(ValueError, match="native file-stream"):
            list(fp.get_file_chunks("f2"))
