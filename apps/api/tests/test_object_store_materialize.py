"""Chunked object-store materialize — one algorithm, five writers.

Proves: bounded accepted-row bundles, byte-identical encode vs a full-list
serialize, source-row quarantine stamps across batch boundaries, fail-closed
does not finish an export, checksum matches the full accepted image.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.object_store_common import serialize_object_store_body  # noqa: E402
from connectors.object_store_materialize import (  # noqa: E402
    ObjectStoreEncoder,
    materialize_object_store_export,
    resolve_materialize_batch,
)
from connectors.writer_common import (  # noqa: E402
    apply_write_quarantine_matrix,
    build_mapped_rows_with_details,
    row_checksum,
)


def _common(**overrides):
    base = dict(
        headers=["id", "note"],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
        ],
        target_cols=["id", "note"],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_types={"id": "TEXT", "note": "TEXT"},
        error_policy="quarantine",
        dest_kind="s3",
        dialect_label="S3",
        spill_max_size=8 * 1024 * 1024,
        dest_db_type="s3",
    )
    base.update(overrides)
    return base


def test_resolve_materialize_batch_reads_dest_extra_and_floor():
    assert resolve_materialize_batch({}) == 1024
    assert resolve_materialize_batch({"materialize_batch": 3}) == 3
    assert resolve_materialize_batch({"materialize_batch": 0}) == 1


def test_materialize_json_matches_full_list_serialize_and_bounds_bundles():
    rows = [[str(i), f"n{i}"] for i in range(7)]
    sizes: list[int] = []
    orig = ObjectStoreEncoder.append_rows

    def _append(self, mapped):
        sizes.append(len(mapped))
        return orig(self, mapped)

    with patch.object(ObjectStoreEncoder, "append_rows", _append):
        mat = materialize_object_store_export(
            key="exports/a.json",
            data_rows=rows,
            batch_size=3,
            **_common(),
        )
    assert mat.abort_error is None
    assert mat.rows_written == 7
    assert mat.batch_sizes == [3, 3, 1]
    assert max(sizes) <= 3
    assert mat.export is not None
    try:
        body = mat.export.read_all()
    finally:
        mat.export.close()
    expected, mime = serialize_object_store_body(
        key="exports/a.json",
        mapped_rows=[(str(i), f"n{i}") for i in range(7)],
        target_cols=["id", "note"],
        dest_types={"id": "TEXT", "note": "TEXT"},
    )
    assert mime == "application/json"
    assert body == expected
    assert mat.meta["source_row_count"] == 7
    assert len(mat.meta["reconcile_sample"]) == 7


def test_materialize_jsonl_csv_tsv_match_full_list_serialize():
    rows = [["1", "ok"], ["2", "next"], ["3", "last"]]
    mapped = [("1", "ok"), ("2", "next"), ("3", "last")]
    for ext in (".jsonl", ".csv", ".tsv"):
        mat = materialize_object_store_export(
            key=f"exports/a{ext}",
            data_rows=rows,
            batch_size=2,
            **_common(),
        )
        assert mat.abort_error is None, mat.abort_error
        try:
            body = mat.export.read_all()
        finally:
            mat.export.close()
        expected, _ = serialize_object_store_body(
            key=f"exports/a{ext}",
            mapped_rows=mapped,
            target_cols=["id", "note"],
            dest_types={"id": "TEXT", "note": "TEXT"},
        )
        assert body == expected, ext


def test_materialize_checksum_matches_full_accepted_image():
    rows = [[str(i), f"n{i}"] for i in range(11)]
    mat = materialize_object_store_export(
        key="exports/a.jsonl",
        data_rows=rows,
        batch_size=4,
        **_common(),
    )
    assert mat.export is not None
    mat.export.close()
    mapped = [(str(i), f"n{i}") for i in range(11)]
    assert mat.checksum == row_checksum(mapped, ["id", "note"], dest_db_type="s3")


def test_materialize_quarantine_keeps_source_row_numbers_across_batches():
    rows = [
        ["1", "10"],
        ["2", "20"],
        ["3", "bad"],
        ["4", "40"],
        ["5", "bad"],
    ]
    mat = materialize_object_store_export(
        key="exports/a.jsonl",
        data_rows=rows,
        batch_size=2,
        **_common(
            column_types={"id": "TEXT", "note": "INTEGER"},
            dest_types={"id": "TEXT", "note": "INTEGER"},
        ),
    )
    assert mat.abort_error is None
    assert mat.rows_written == 3
    held = sorted({int(d["row"]) for d in mat.rejected_details if d.get("row")})
    assert held == [3, 5]
    mat.export.close()


def test_materialize_fail_policy_discards_export_and_reports_every_reject():
    rows = [["1", "10"], ["2", "bad"], ["3", "also-bad"], ["4", "40"]]
    mat = materialize_object_store_export(
        key="exports/a.json",
        data_rows=rows,
        batch_size=2,
        **_common(
            error_policy="fail",
            column_types={"id": "TEXT", "note": "INTEGER"},
            dest_types={"id": "TEXT", "note": "INTEGER"},
        ),
    )
    assert mat.export is None
    assert mat.abort_error
    assert mat.rows_written == 0
    held = {int(d["row"]) for d in mat.rejected_details if d.get("row")}
    assert held == {2, 3}


def test_materialize_empty_json_is_empty_array():
    mat = materialize_object_store_export(
        key="exports/a.json",
        data_rows=[],
        batch_size=2,
        **_common(),
    )
    try:
        assert mat.export.read_all() == b"[]"
    finally:
        mat.export.close()


def test_s3_writer_uses_materialize_batch(monkeypatch):
    from connectors import s3_writer

    seen: list[int] = []

    def _mat(**kwargs):
        seen.append(int(kwargs["batch_size"]))

        class _Exp:
            content_type = "application/json"

            def close(self):
                return None

        class _Mat:
            export = _Exp()
            rows_written = 1
            transform_errors = []
            rejected_details = []
            abort_error = None
            checksum = "abc"
            meta = {"source_row_count": 1}
            rejected_rows = 0
            coerced_null_rows = 0

        return _Mat()

    monkeypatch.setattr(
        "connectors.s3_writer.materialize_object_store_export", _mat
    )
    monkeypatch.setattr(
        "connectors.s3_writer.land_object_store_export", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "connectors.s3_writer.boto3_client", lambda *a, **k: type(
            "C", (), {"head_bucket": lambda self, **k: None, "delete_object": lambda self, **k: None}
        )()
    )
    result = s3_writer.write_mapped_rows(
        host="",
        port=0,
        database="bucket",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="exports/out.json",
        headers=["id", "note"],
        data_rows=[["1", "ok"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
        ],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_extra={"materialize_batch": 2},
        create_table=False,
    )
    assert result.ok, result.error
    assert seen == [2]


def test_gcs_adls_sftp_email_forward_materialize_batch(monkeypatch):
    seen: dict[str, int] = {}

    def _factory(name):
        def _mat(**kwargs):
            seen[name] = int(kwargs["batch_size"])

            class _Exp:
                content_type = "text/csv"

                def read_all(self):
                    return b"id,note\n1,ok\n"

                def close(self):
                    return None

                def copy_to(self, dest, chunk_size=1024):
                    dest.write(b"id,note\n1,ok\n")
                    return 10

            class _Mat:
                export = _Exp()
                rows_written = 1
                transform_errors = []
                rejected_details = []
                abort_error = None
                checksum = "abc"
                meta = {"source_row_count": 1, "reconcile_sample": [{"id": "1"}]}
                rejected_rows = 0
                coerced_null_rows = 0

            return _Mat()

        return _mat

    from connectors import adls_writer, email as email_mod, gcs_writer, sftp_writer

    monkeypatch.setattr("connectors.gcs_writer.materialize_object_store_export", _factory("gcs"))
    monkeypatch.setattr("connectors.adls_writer.materialize_object_store_export", _factory("adls"))
    monkeypatch.setattr("connectors.sftp_writer.materialize_object_store_export", _factory("sftp"))
    monkeypatch.setattr("connectors.email.materialize_object_store_export", _factory("email"))

    class _Bucket:
        def exists(self):
            return True

        def blob(self, _k):
            return type("B", (), {"delete": lambda self: None})()

    monkeypatch.setattr(
        "connectors.gcs_writer.gcs_client",
        lambda cfg: type("C", (), {"bucket": lambda self, n: _Bucket()})(),
    )
    monkeypatch.setattr("connectors.gcs_writer.land_object_store_export", lambda *a, **k: None)

    gcs = gcs_writer.write_mapped_rows(
        host="",
        port=0,
        database="bkt",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="exports/out.json",
        headers=["id", "note"],
        data_rows=[["1", "ok"]],
        mappings=[{"source": "id", "target": "id"}, {"source": "note", "target": "note"}],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_extra={"materialize_batch": 5},
        create_table=False,
    )
    assert gcs.ok, gcs.error

    class _Container:
        def exists(self):
            return True

        def create_container(self):
            return None

    class _Blob:
        def delete_blob(self):
            return None

    class _Client:
        def get_container_client(self, _n):
            return _Container()

        def get_blob_client(self, *_a):
            return _Blob()

    monkeypatch.setattr("connectors.adls_writer.blob_service_client", lambda cfg: _Client())
    monkeypatch.setattr("connectors.adls_writer.land_object_store_export", lambda *a, **k: None)
    adls = adls_writer.write_mapped_rows(
        host="",
        port=0,
        database="ctr",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="exports/out.json",
        headers=["id", "note"],
        data_rows=[["1", "ok"]],
        mappings=[{"source": "id", "target": "id"}, {"source": "note", "target": "note"}],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_extra={"materialize_batch": 6},
        create_table=False,
    )
    assert adls.ok, adls.error

    transport = type("T", (), {"close": lambda self: None})()
    handle = type("H", (), {})()
    handle.__enter__ = lambda self: self
    handle.__exit__ = lambda self, *a: False
    handle.write = lambda self, b: None
    handle.flush = lambda self: None
    sftp = type("S", (), {})()
    sftp.file = lambda self, *a, **k: handle
    sftp.stat = lambda self, p: None
    sftp.posix_rename = lambda self, a, b: None
    sftp.close = lambda self: None
    monkeypatch.setattr(
        "connectors.sftp_writer.connect_sftp", lambda cfg: (transport, sftp)
    )
    sftp_res = sftp_writer.write_mapped_rows(
        connection_string="sftp://u:p@host/data/out.csv",
        headers=["id", "note"],
        data_rows=[["1", "ok"]],
        mappings=[{"source": "id", "target": "id"}, {"source": "note", "target": "note"}],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_extra={"materialize_batch": 7},
    )
    assert sftp_res.ok, sftp_res.error

    server = type("Srv", (), {"ehlo": lambda self: None, "login": lambda self, u, p: None, "sendmail": lambda self, *a: None, "starttls": lambda self, context=None: None})()
    smtp_cm = type("CM", (), {"__enter__": lambda self: server, "__exit__": lambda self, *a: False})()
    monkeypatch.setattr(
        "connectors.email.smtplib.SMTP", lambda *a, **k: smtp_cm
    )
    email_res = email_mod.write_mapped_rows(
        host="localhost",
        port=1025,
        username="u",
        password="p",
        database="to@example.com",
        headers=["id", "note"],
        data_rows=[["1", "ok"]],
        mappings=[{"source": "id", "target": "id"}, {"source": "note", "target": "note"}],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_extra={"materialize_batch": 8},
    )
    assert email_res.ok, email_res.error
    assert seen == {"gcs": 5, "adls": 6, "sftp": 7, "email": 8}


def test_parquet_row_groups_round_trip():
    pytest.importorskip("pyarrow.parquet")
    import io

    import pyarrow.parquet as pq

    rows = [[str(i), f"n{i}"] for i in range(5)]
    mat = materialize_object_store_export(
        key="exports/a.parquet",
        data_rows=rows,
        batch_size=2,
        **_common(
            column_types={"id": "INTEGER", "note": "TEXT"},
            dest_types={"id": "INTEGER", "note": "TEXT"},
        ),
    )
    assert mat.abort_error is None
    try:
        table = pq.read_table(io.BytesIO(mat.export.read_all()))
    finally:
        mat.export.close()
    assert table.num_rows == 5


def test_apply_matrix_source_row_numbers_require_alignment():
    with pytest.raises(ValueError, match="source_row_numbers"):
        apply_write_quarantine_matrix(
            [("1", "ok")],
            ["id", "note"],
            ["TEXT", "TEXT"],
            [],
            "quarantine",
            source_row_numbers=[1, 2],
        )


def test_build_mapped_rows_row_number_start_and_accepted_source_rows():
    accepted: list[int] = []
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["id", "note"],
        data_rows=[["1", "ok"], ["2", "bad"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
        ],
        target_cols=["id", "note"],
        column_types={"id": "TEXT", "note": "INTEGER"},
        dest_types={"id": "TEXT", "note": "INTEGER"},
        error_policy="quarantine",
        row_number_start=10,
        accepted_source_rows=accepted,
        struct_already_materialized=True,
    )
    assert accepted == [10]
    assert len(mapped) == 1
    assert details[0]["row"] == 11
