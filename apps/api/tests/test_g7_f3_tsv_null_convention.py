"""G7-F3: an opt-in ``\\N`` NULL convention keeps '' and NULL apart in TSV.

Before: an object-store TSV (S3/GCS/ADLS/SFTP) wrote SQL NULL and ``''`` as the
same empty cell, so the read-back landed the source ``''`` as NULL and Gate-8
failed ``source='' dest='null'``; a file_export TSV kept them apart only by
quoting ``''`` as ``""``, which a text-format loader (MySQL ``LOAD DATA``,
PostgreSQL ``COPY ... TEXT``) reads as two quote characters.

After: a TSV destination may opt in with ``tsv_null: "\\N"`` (the MySQL /
PostgreSQL text convention): NULL is ``\\N``, ``''`` an empty cell, and a text
value that is literally ``\\N`` is quoted. The writers, Gate-8 and the TSV
readers all honour it. The default (``"empty"``) output is unchanged.
"""

from __future__ import annotations

import csv
import io
import uuid
from pathlib import Path

import pytest

TEXT_NULL = "\\N"
COLS = ["id", "note"]
RECORDS = [
    {"id": "1", "note": ""},
    {"id": "2", "note": None},
    {"id": "3", "note": TEXT_NULL},  # the two-character text, not NULL
    {"id": "4", "note": "a\tb"},
    {"id": "5", "note": "plain"},
]
EXPECTED_TSV = (
    "id\tnote\r\n"
    "1\t\r\n"
    "2\t\\N\r\n"
    '3\t"\\N"\r\n'
    '4\t"a\tb"\r\n'
    "5\tplain\r\n"
).encode("utf-8")


def _write(extra: dict | None = None, fmt: str = "tsv"):
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig

    return write_destination_file(
        EndpointConfig(kind="file_export", format=fmt, extra=dict(extra or {})),
        [dict(r) for r in RECORDS],
        list(COLS),
        source_format="csv",
    )


def _decoded(body: bytes, empty_cell: str) -> list[list[str | None]]:
    from services.csv_codec import iter_csv_records

    return list(iter_csv_records(io.StringIO(body.decode("utf-8"), newline=""), "\t", empty_cell=empty_cell))[1:]


# ── the vocabulary ───────────────────────────────────────────────────────────


def test_tsv_null_vocabulary():
    from services.export_dialect import tsv_null_from, tsv_null_marker, tsv_null_mode

    assert tsv_null_mode(None) == tsv_null_mode("") == tsv_null_mode("EMPTY") == "empty"
    assert tsv_null_mode(TEXT_NULL) == TEXT_NULL
    with pytest.raises(ValueError, match="tsv_null"):
        tsv_null_mode("NULL")
    assert tsv_null_from({"tsv_null": TEXT_NULL}) == TEXT_NULL
    assert tsv_null_from({"extra": {"tsv_null": TEXT_NULL}}) == TEXT_NULL
    assert tsv_null_from(None) == "empty"
    assert tsv_null_marker("tsv", TEXT_NULL) == TEXT_NULL
    assert tsv_null_marker("csv", None) == ""
    with pytest.raises(ValueError, match="TSV files only"):
        tsv_null_marker("csv", TEXT_NULL)


def test_endpoint_dto_validates_tsv_null():
    from pydantic import ValidationError

    from src.routers.transfer_router import EndpointDTO

    from src.routers.transfer_router import AnalyzeRequest

    def dest(**kw):
        return AnalyzeRequest(source={"kind": "file"}, destination={"kind": "file_export", "format": "tsv", **kw})

    assert dest(tsv_null=TEXT_NULL)
    assert dest(extra={"tsv_null": "empty"})
    with pytest.raises(ValidationError):
        dest(extra={"tsv_null": "NULL"})
    assert "tsv_null" in (EndpointDTO.model_fields["extra"].description or "")


def test_quoted_backslash_n_reads_as_text_unquoted_as_null():
    from services.csv_codec import decode_cell

    assert decode_cell(TEXT_NULL, False) is None
    assert decode_cell(TEXT_NULL, None) is None
    assert decode_cell(TEXT_NULL, True) == TEXT_NULL


# ── file_export ──────────────────────────────────────────────────────────────


def test_file_export_tsv_default_is_unchanged():
    content, _name, summary = _write()
    assert content == (
        'id\tnote\r\n1\t""\r\n2\t\r\n3\t"\\N"\r\n4\t"a\tb"\r\n5\tplain\r\n'
    ).encode("utf-8")
    assert "tsv_null" not in summary


def test_file_export_tsv_opt_in_writes_the_text_null_convention():
    content, _name, summary = _write({"tsv_null": TEXT_NULL})
    assert content == EXPECTED_TSV
    assert summary["tsv_null"] == TEXT_NULL
    assert _decoded(content, "keep") == [["1", ""], ["2", None], ["3", TEXT_NULL], ["4", "a\tb"], ["5", "plain"]]


def test_file_export_tsv_null_is_refused_for_csv():
    with pytest.raises(ValueError, match="TSV files only"):
        _write({"tsv_null": TEXT_NULL}, fmt="csv")


def test_gate8_file_export_readback_green_with_text_null(tmp_path):
    from services.dest_precount import _iter_artifact_records
    from src.transfer.reconcile_step import _file_export_cell_readback

    content, _name, summary = _write({"tsv_null": TEXT_NULL})
    path = tmp_path / "export.tsv"
    path.write_bytes(content)
    with open(path, "rb") as fh:
        rows = list(_iter_artifact_records(fh, name=path.name, fmt="tsv", written_here=True, tsv_null=TEXT_NULL))
    assert [r["note"] for r in rows] == [r["note"] for r in RECORDS]
    mappings = [{"source": c, "target": c, "target_type": "text"} for c in COLS]
    compare = _file_export_cell_readback(
        [dict(r) for r in RECORDS], list(COLS), mappings,
        {**summary, "path": str(path), "primary_key_columns": ["id"]}, fmt="tsv",
    )
    assert compare is not None and compare.get("passed") is True, compare
    assert compare.get("rows_compared") == len(RECORDS), compare


def test_transfer_job_file_export_tsv_text_null_end_to_end(monkeypatch, tmp_path):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    source = b'id,note\n1,""\n2,\n3,plain\n'
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig.from_dict("file_export", {"format": "tsv", "tsv_null": TEXT_NULL}),
        source_filename="g7f3.csv",
        source_content=source,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    written = Path(result.destination_summary["path"]).read_bytes()
    assert written == b"id\tnote\r\n1\t\r\n2\t\\N\r\n3\tplain\r\n"
    assert (result.reconciliation or {}).get("passed") is True, result.reconciliation


# ── object store (S3 / GCS / ADLS / SFTP share this encoder and reader) ──────


def _object_body(tsv_null: str | None) -> bytes:
    from connectors.object_store_common import serialize_object_store_export

    export = serialize_object_store_export(
        key="out/data.tsv",
        mapped_rows=[(r["id"], r["note"]) for r in RECORDS],
        target_cols=list(COLS),
        dest_types={"id": "TEXT", "note": "TEXT"},
        **({"tsv_null": tsv_null} if tsv_null else {}),
    )
    try:
        return export.read_all()
    finally:
        export.close()


def test_object_store_tsv_default_bytes_unchanged_and_collapse_documented():
    body = _object_body(None)
    assert body == b'id\tnote\n1\t\n2\t\n3\t"\\N"\n4\t"a\tb"\n5\tplain\n'
    # The default codec cannot tell '' from NULL — why the opt-in exists.
    assert _decoded(body, "null")[0][1] is None


def test_object_store_tsv_opt_in_round_trips_empty_and_null():
    from services.dest_precount import _iter_artifact_records

    body = _object_body(TEXT_NULL)
    assert body == EXPECTED_TSV.replace(b"\r\n", b"\n")
    rows = list(_iter_artifact_records(io.BytesIO(body), name="out/data.tsv", tsv_null=TEXT_NULL))
    assert [r["note"] for r in rows] == [r["note"] for r in RECORDS]
    # A plain RFC-4180 reader still sees one cell per column.
    assert all(len(r) == 2 for r in csv.reader(io.StringIO(body.decode("utf-8"), newline=""), delimiter="\t"))


def test_object_store_tsv_null_refused_for_non_tsv_key():
    from connectors.object_store_common import serialize_object_store_export

    with pytest.raises(ValueError, match="TSV files only"):
        serialize_object_store_export(
            key="out/data.csv", mapped_rows=[("1", None)], target_cols=list(COLS), tsv_null=TEXT_NULL,
        )


def _fake_bucket(monkeypatch, body: bytes) -> None:
    from services import dest_precount, object_streaming

    monkeypatch.setattr(dest_precount, "_object_store_list_keys", lambda *_a, **_k: ["out/data.tsv"])
    monkeypatch.setattr(object_streaming, "open_object_store_binary", lambda *_a, **_k: (io.BytesIO(body), None))


@pytest.mark.parametrize("tsv_null, green", [(TEXT_NULL, True), (None, False)])
def test_gate8_object_store_checksum_and_sample(monkeypatch, tsv_null, green):
    """Gate-8 on a bucket: the read-back equals what was sent only with the convention."""
    from services.dest_precount import _checksum_records, checksum_object_store, sample_object_store

    _fake_bucket(monkeypatch, _object_body(tsv_null))
    cfg = {"database": "bucket", **({"tsv_null": tsv_null} if tsv_null else {})}
    expected = _checksum_records(iter([dict(r) for r in RECORDS]), columns=list(COLS))
    got = checksum_object_store("s3", cfg, table_name="out/data.tsv", columns=list(COLS))
    assert got[0] == len(RECORDS)
    assert (got == expected) is green, (got, expected)
    sample = sample_object_store("s3", cfg, table_name="out/data.tsv", limit=10)
    assert ([r["note"] for r in sample] == [r["note"] for r in RECORDS]) is green


def test_verify_target_threads_tsv_null_to_the_bucket_read(monkeypatch):
    from services import reconciliation_readback

    seen = {}

    def fake_checksum(db_type, cfg, **_k):
        seen.update(cfg)
        return 1, "x"

    from services import dest_precount

    monkeypatch.setattr(dest_precount, "checksum_object_store", fake_checksum)
    reconciliation_readback.verify_s3_object(
        bucket="b", key="out/data.tsv", host="", port=0, username="", password="",
        connection_string="", ssl=False, file_format="tsv", tsv_null=TEXT_NULL,
    )
    assert seen.get("tsv_null") == TEXT_NULL


def test_object_store_key_merge_keeps_the_convention():
    from connectors.object_store_common import serialize_object_store_export
    from connectors.object_store_incremental import merge_object_bytes

    existing = _object_body(TEXT_NULL)
    new_export = serialize_object_store_export(
        key="out/data.tsv", mapped_rows=[("6", ""), ("2", "now set")], target_cols=list(COLS),
        dest_types={"id": "TEXT", "note": "TEXT"}, tsv_null=TEXT_NULL,
    )
    merged = merge_object_bytes(
        key="out/data.tsv", existing=existing, new_export=new_export, target_cols=list(COLS),
        dest_types={"id": "TEXT", "note": "TEXT"}, conflict_columns=["id"], strategy="key",
        spill_max_size=1 << 20, tsv_null=TEXT_NULL,
    )
    try:
        body = merged.read_all()
    finally:
        merged.close()
    by_id = {r[0]: r[1] for r in _decoded(body, "keep")}
    assert by_id == {"1": "", "2": "now set", "3": TEXT_NULL, "4": "a\tb", "5": "plain", "6": ""}


def test_copy_fast_paths_are_declined_for_the_text_null_convention():
    """Engine COPY routes write the default codec only; the row path honours tsv_null."""
    import inspect

    from src.transfer import stream

    assert 'tsv_null_from(run.cfg.dest_cfg) != "empty"' in inspect.getsource(stream)
