"""Redshift: a batch the TSV stage cannot carry is staged as typed Parquet, not INSERTed.

6d5e26614 sent any batch holding a backslash, tab, CR, LF, bytea or a text \\N
to row-wise INSERT (Redshift COPY without ESCAPE takes the TSV escapes
literally) — correct, but orders of magnitude slower on ordinary text. Such a
batch now stages as Parquet and runs ``COPY … FORMAT AS PARQUET``; INSERT is
left only for when Parquet staging is impossible (no pyarrow), with a warning.
A plain batch still stages the same TSV bytes as before.
"""

from __future__ import annotations

import io
import logging

import pytest

_ROLE = "arn:aws:iam::123456789012:role/redshift-copy"
COLS = ["id", "note", "blob"]
UNSAFE = [
    (1, "C:\\temp\\x", b"\x00\x01"),
    (2, "a\tb", None),
    (3, "line1\nline2\rend", b"hello"),
    (4, "\\N", b""),
    (5, None, None),
]


class _S3:
    def __init__(self) -> None:
        self.puts: list[dict] = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)

    def delete_object(self, **_kwargs):
        return None


class _Cur:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, sql, params=None):
        self.sql.append(str(sql))


def _copy(rows, stage_format="tsv", dest_types=None):
    from connectors.redshift_copy import RedshiftCopyConfig, copy_redshift_rows_from_s3

    s3, cur = _S3(), _Cur()
    copy_redshift_rows_from_s3(
        cur, schema="public", table="t", columns=list(COLS), rows=rows,
        config=RedshiftCopyConfig(bucket="dw-stage", iam_role=_ROLE, region="us-east-1"),
        s3_client=s3, job_id="j1", chunk_idx=0, dest_types=dest_types, stage_format=stage_format,
    )
    return s3.puts[0], cur.sql[0]


def test_unsafe_batch_stages_as_parquet_and_round_trips():
    import pyarrow.parquet as pq

    from connectors.redshift_copy import redshift_batch_stage_format

    assert redshift_batch_stage_format(UNSAFE) == "parquet"
    put, sql = _copy(UNSAFE, dest_types={"id": "BIGINT", "note": "VARCHAR(256)", "blob": "VARBYTE"})
    assert put["Key"].endswith(".parquet")
    assert "FORMAT AS PARQUET" in sql and "DELIMITER" not in sql
    table = pq.read_table(io.BytesIO(put["Body"]))
    got = [tuple(r[c] for c in COLS) for r in table.to_pylist()]
    assert got == UNSAFE


def test_plain_batch_still_uses_the_same_tsv_bytes():
    from connectors.redshift_copy import redshift_batch_stage_format

    rows = [(1, "ok", "x"), (2, None, "y z"), (3, "", "")]
    assert redshift_batch_stage_format(rows) == "tsv"
    put, sql = _copy(rows)
    assert put["Key"].endswith(".tsv")
    assert put["Body"] == b"1\tok\tx\n2\t\\N\ty z\n3\t\t\n"
    assert "DELIMITER '\\t' NULL AS '\\\\N'" in sql and "PARQUET" not in sql


def test_requested_parquet_is_kept():
    from connectors.redshift_copy import redshift_batch_stage_format

    assert redshift_batch_stage_format([(1, "ok", "x")], "parquet") == "parquet"


def test_insert_only_when_parquet_staging_is_impossible(monkeypatch, caplog):
    from connectors import postgresql_writer, redshift_copy

    monkeypatch.setattr(redshift_copy, "_parquet_staging_available", lambda: False)
    assert redshift_copy.redshift_batch_stage_format(UNSAFE) == ""
    assert redshift_copy.redshift_batch_stage_format([(1, "ok", "x")]) == "tsv"
    with caplog.at_level(logging.WARNING):
        assert postgresql_writer._redshift_batch_stage(UNSAFE, "tsv") == ""
    assert "much slower" in caplog.text and "pyarrow" in caplog.text
    with pytest.raises(ValueError, match="pyarrow"):
        _copy(UNSAFE)


def test_the_renderer_still_refuses_an_unsafe_batch():
    from connectors.redshift_copy import render_redshift_copy_text

    with pytest.raises(ValueError, match="INSERT path"):
        render_redshift_copy_text([(1, "a\tb", None)])
