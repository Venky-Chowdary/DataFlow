"""Independent dest COUNT(*) closes conservation — writer ack never does.

AWS DMS Full Load can succeed while validation later reports MISSING_TARGET:
the writer counted rows the dest engine does not hold. This module is the
named identity so the certificate cannot circularly balance a short write
against itself.
"""

from __future__ import annotations

import gzip
import io
from pathlib import Path

import pytest

from services.dest_precount import (
    ARTIFACT_COUNT_KEY,
    CURRENT_ROWS_KEY,
    DEST_COUNT_ARTIFACT,
    DEST_COUNT_CURRENT,
    DEST_COUNT_IDENTITY,
    EXTRA_KEYS_KEY,
    HISTORY_ROWS_KEY,
    IDENTITY_COUNT_KEY,
    MISSING_KEYS_KEY,
    SOURCE_ID_SCAN_COMPLETE,
    SOURCE_ID_SCAN_MISSING,
    SOURCE_ID_SCAN_NO_FIELD,
    SOURCE_ID_SCAN_TRUNCATED,
    SOURCE_ID_SCAN_UNMEASURED,
    VECTOR_IDENTITY_ENGINES,
    VECTOR_ROWS_KEY,
    count_artifact_rows,
    count_scd2_current,
    count_scd2_populations,
    destination_key_list,
    destination_keyset_census,
    destination_row_count,
    identity_count_from_source_id_scan,
    records_to_key_tuples,
    stamp_artifact_census,
    stamp_keyset_census,
    stamp_scd2_census,
    stamp_vector_census,
)
from services.row_conservation import (
    DEST_ACTIVE_READBACK,
    DEST_ARTIFACT_READBACK,
    DEST_CURRENT_READBACK,
    DEST_IDENTITY_READBACK,
    DEST_PER_STREAM,
    DEST_READBACK,
    DEST_UNMEASURED,
    KIND_APPEND_DELTA,
    KIND_EMPTY_PASS,
    KIND_JOB,
    KIND_KEYED,
    KIND_MIRROR,
    KIND_OVERWRITE,
    KIND_SCD2,
    KIND_VECTOR,
    account_job,
    account_job_streams,
    account_population,
    conservation_kind,
    dest_count_from_recon,
    hold_outs,
    apply_inferred_leftover_deletes,
)


def test_hold_outs_exclude_coerced_null_rows_that_landed():
    assert hold_outs(rejected_rows=5, coerced_null_rows=2) == 3
    assert hold_outs(rejected_rows=2, coerced_null_rows=2) == 0
    assert hold_outs(rejected_rows=0, coerced_null_rows=3) == 0


def test_writer_ack_phase_without_dest_digest_is_not_a_dest_count():
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "phase": "post_write_writer_ack",
            "coverage": "writer_ack",
            "assurance_level": "writer_ack",
            "message": "verified by writer checksum",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_writer_ack_source_digest_still_exposes_independent_dest_count():
    """Streaming Gate-8: source digest is writer ack; dest COUNT(*) is dest."""
    count, source = dest_count_from_recon(
        {
            "passed": True,
            "phase": "post_write_writer_ack",
            "coverage": "writer_ack",
            "assurance_level": "writer_ack",
            "source_rows": 4,
            "target_rows": 4,
            "target_checksum": "abc123",
            "source_checksum": "abc123",
            "source_checksum_provenance": "writer_ack",
            "message": "Row fidelity verified — source and target checksums match (4 rows)",
        }
    )
    assert count == 4
    assert source == DEST_READBACK


def test_skipped_readback_stuffs_writer_ack_and_is_refused():
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "skipped_readback": True,
            "unproven": True,
            "message": "File/object export wrote successfully",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_artifact_readback_closes_on_file_count_not_writer_ack():
    """DMS hole for files: writer rows never close dest; re-opened records do."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "skipped_readback": True,
            "unproven": True,
            "migration_proven": False,
            "dest_count_source": DEST_ARTIFACT_READBACK,
            ARTIFACT_COUNT_KEY: 3,
            "message": "File/object export wrote successfully — Gate-8 cell fidelity unproven",
        }
    )
    assert count == 3
    assert source == DEST_ARTIFACT_READBACK


def test_artifact_source_without_artifact_count_is_unmeasured():
    """Forged dest_count_source + stuffed target_rows is still writer ack."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "skipped_readback": True,
            "dest_count_source": DEST_ARTIFACT_READBACK,
            "message": "File/object export wrote successfully",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_artifact_overwrite_balances_on_file_count_not_writer_ack():
    ledger = account_population(
        rows_read=3,
        dest_count=3,
        dest_count_source=DEST_ARTIFACT_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="incremental_append",
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.rows_written == 3
    assert ledger.rows_written_source == DEST_ARTIFACT_READBACK
    assert ledger.writer_ack == 10_000
    assert ledger.unaccounted == 0
    assert ledger.balanced is True
    assert ledger.writer_ack_delta == -9997
    assert "artifact" in ledger.note.lower()
    assert "destination table" not in ledger.note.lower()


def test_count_artifact_rows_csv_jsonl_json_independent_of_writer(tmp_path: Path):
    csv_path = tmp_path / "export.csv"
    csv_path.write_text("id,name\n1,a\n2,b\n3,c\n", encoding="utf-8")
    assert count_artifact_rows(csv_path, fmt="csv") == 3

    empty = tmp_path / "empty.csv"
    empty.write_text("id,name\n", encoding="utf-8")
    assert count_artifact_rows(empty, fmt="csv") == 0
    tsv_path = tmp_path / "export.tsv"
    tsv_path.write_text("id\tname\n1\ta\n2\tb\n", encoding="utf-8")
    assert count_artifact_rows(tsv_path, fmt="tsv") == 2

    jsonl_path = tmp_path / "export.jsonl"
    jsonl_path.write_text('{"id":1}\n{"id":2}\n', encoding="utf-8")
    assert count_artifact_rows(jsonl_path, fmt="jsonl") == 2
    empty_jsonl = tmp_path / "empty.jsonl"
    empty_jsonl.write_text("\n\n", encoding="utf-8")
    assert count_artifact_rows(empty_jsonl, fmt="jsonl") == 0
    ndjson_path = tmp_path / "export.ndjson"
    ndjson_path.write_text('{"id":1}\n{"id":2}\n{"id":3}\n', encoding="utf-8")
    assert count_artifact_rows(ndjson_path, fmt="ndjson") == 3

    import gzip

    gz_jsonl = tmp_path / "export.jsonl.gz"
    gz_jsonl.write_bytes(gzip.compress(b'{"id":1}\n{"id":2}\n'))
    assert count_artifact_rows(gz_jsonl, fmt="jsonl") == 2

    json_path = tmp_path / "export.json"
    json_path.write_text('[{"id":1},{"id":2},{"id":3}]', encoding="utf-8")
    assert count_artifact_rows(json_path, fmt="json") == 3
    wrap = tmp_path / "wrap.json"
    wrap.write_text('{"records":[{"id":1},{"id":2}]}', encoding="utf-8")
    assert count_artifact_rows(wrap, fmt="json") == 2
    empty_json = tmp_path / "empty.json"
    empty_json.write_text("[]", encoding="utf-8")
    assert count_artifact_rows(empty_json, fmt="json") == 0
    scalar_json = tmp_path / "scalars.json"
    scalar_json.write_text("[1,2,3]", encoding="utf-8")
    assert count_artifact_rows(scalar_json, fmt="json") is None

    gz_path = tmp_path / "export.csv.gz"
    gz_path.write_bytes(gzip.compress(b"id\n1\n2\n"))
    assert count_artifact_rows(gz_path, fmt="csv") == 2

    bad = tmp_path / "bad.jsonl"
    bad.write_text("{not json\n", encoding="utf-8")
    assert count_artifact_rows(bad, fmt="jsonl") is None

    missing = tmp_path / "nope.csv"
    assert count_artifact_rows(missing, fmt="csv") is None
    assert count_artifact_rows("s3://bucket/key.csv", fmt="csv") is None


def test_count_artifact_rows_excel_excludes_phantom_used_range(tmp_path: Path):
    """Finance dumps: formatting inflates openpyxl max_row. Dest is value rows."""
    openpyxl = pytest.importorskip("openpyxl")
    import io

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["id", "name"])
    ws.append(["1", "a"])
    ws.append(["2", "b"])
    for r in range(5, 22):
        ws.cell(row=r, column=1).number_format = "0.00"
    buf = io.BytesIO()
    wb.save(buf)
    path = tmp_path / "export.xlsx"
    path.write_bytes(buf.getvalue())
    assert ws.max_row > 3
    assert count_artifact_rows(path, fmt="excel") == 2
    assert count_artifact_rows(path) == 2
    empty = tmp_path / "header_only.xlsx"
    from services.format_converter import convert_rows

    content, _mime = convert_rows(["id", "name"], [], source_format="csv", target_format="excel")
    empty.write_bytes(content)
    assert count_artifact_rows(empty, fmt="excel") == 0
    stamped = stamp_artifact_census(
        {"target_rows": 10_000, "skipped_readback": True},
        {"path": str(path), "format": "excel"},
    )
    assert stamped[ARTIFACT_COUNT_KEY] == 2
    assert stamped["dest_count_source"] == DEST_COUNT_ARTIFACT
    assert stamped["target_rows"] == 2
    assert stamped["target_rows_before"] == 0


def test_count_artifact_rows_avro_streams_records_not_writer_ack(tmp_path: Path):
    pytest.importorskip("fastavro")
    from services.format_converter import convert_rows

    content, _mime = convert_rows(
        ["id", "name"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="avro",
    )
    path = tmp_path / "export.avro"
    path.write_bytes(content)
    assert count_artifact_rows(path, fmt="avro") == 3
    assert count_artifact_rows(path) == 3
    empty_bytes, _ = convert_rows(["id"], [], source_format="csv", target_format="avro")
    empty = tmp_path / "empty.avro"
    empty.write_bytes(empty_bytes)
    assert count_artifact_rows(empty, fmt="avro") == 0
    bad = tmp_path / "bad.avro"
    bad.write_bytes(b"not-avro")
    assert count_artifact_rows(bad, fmt="avro") is None
    stamped = stamp_artifact_census(
        {"target_rows": 10_000, "skipped_readback": True},
        {"path": str(path), "format": "avro"},
    )
    assert stamped[ARTIFACT_COUNT_KEY] == 3
    assert stamped["target_rows"] == 3


def test_count_artifact_rows_orc_footer_not_writer_ack(tmp_path: Path):
    pytest.importorskip("pyarrow.orc")
    from services.format_converter import convert_rows

    content, _mime = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="orc",
    )
    path = tmp_path / "export.orc"
    path.write_bytes(content)
    assert count_artifact_rows(path, fmt="orc") == 2
    assert count_artifact_rows(path) == 2
    empty_bytes, _ = convert_rows(["id"], [], source_format="csv", target_format="orc")
    empty = tmp_path / "empty.orc"
    empty.write_bytes(empty_bytes)
    assert count_artifact_rows(empty, fmt="orc") == 0
    stamped = stamp_artifact_census(
        {"target_rows": 10_000, "skipped_readback": True},
        {"path": str(path), "format": "orc"},
    )
    assert stamped[ARTIFACT_COUNT_KEY] == 2
    assert stamped["target_rows"] == 2


def test_count_artifact_rows_xml_unique_record_path_not_ingest_cap(tmp_path: Path):
    """XML dest COUNT is the unique repeating record-path, never writer ack.

    parse_xml ingest may refuse max_rows or treat a document as one row.
    Dest COUNT of the export we wrote does neither.
    """
    from services.file_parser import count_xml_records
    from services.format_converter import convert_rows

    content, _mime = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="xml",
    )
    path = tmp_path / "export.xml"
    path.write_bytes(content)
    assert count_xml_records(content) == 3
    assert count_artifact_rows(path, fmt="xml") == 3
    one, _ = convert_rows(
        ["id", "v"],
        [["1", "a"]],
        source_format="csv",
        target_format="xml",
    )
    assert count_xml_records(one) == 1
    empty, _ = convert_rows(["id", "v"], [], source_format="csv", target_format="xml")
    assert count_xml_records(empty) == 0
    empty_path = tmp_path / "empty.xml"
    empty_path.write_bytes(empty)
    assert count_artifact_rows(empty_path, fmt="xml") == 0
    assert count_xml_records(b"not-xml") is None
    document = b"<note><to>T</to><from>F</from></note>"
    assert count_xml_records(document) is None
    ambiguous = (
        b"<root><orders><o><id>1</id></o><o><id>2</id></o></orders>"
        b"<items><i><id>a</id></i><i><id>b</id></i></items></root>"
    )
    assert count_xml_records(ambiguous) is None
    stamped = stamp_artifact_census(
        {"target_rows": 10_000, "skipped_readback": True},
        {"path": str(path), "format": "xml"},
    )
    assert stamped[ARTIFACT_COUNT_KEY] == 3
    assert stamped["target_rows"] == 3


def test_count_xml_records_stax_unique_path_not_dom_or_inner_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Dest COUNT streams the outer record-path. Inner items do not win.

    parse_xml ingest is unchanged (still DOM + max_rows refuse). xmltodict
    must not run when defusedxml iterparse is available — a GB export is
    not two in-memory trees. Missing defusedxml+xmltodict stays unmeasured.
    """
    pytest.importorskip("defusedxml.ElementTree")
    from services.file_parser import count_xml_records

    nested = (
        b"<records>"
        b"<record><id>1</id><items><item><sku>a</sku></item>"
        b"<item><sku>b</sku></item></items></record>"
        b"<record><id>2</id><items><item><sku>c</sku></item>"
        b"<item><sku>d</sku></item></items></record>"
        b"<record><id>3</id><items><item><sku>e</sku></item>"
        b"<item><sku>f</sku></item></items></record>"
        b"</records>"
    )
    assert count_xml_records(nested) == 3
    assert count_xml_records(b"<records><record/></records>") == 1
    assert count_xml_records(b"<root>hello</root>") is None
    namespaced = (
        b'<records xmlns="http://example.com/ns">'
        b"<record><id>1</id></record><record><id>2</id></record>"
        b"</records>"
    )
    assert count_xml_records(namespaced) == 2
    xxe = (
        b'<?xml version="1.0"?>'
        b'<!DOCTYPE r [<!ENTITY e SYSTEM "file:///etc/passwd">]>'
        b"<records><record>&e;</record></records>"
    )
    assert count_xml_records(xxe) is None

    wide = tmp_path / "wide.xml"
    with wide.open("w", encoding="utf-8") as handle:
        handle.write("<records>")
        for i in range(5000):
            handle.write(f"<record><id>{i}</id></record>")
        handle.write("</records>")
    assert count_xml_records(wide) == 5000
    assert count_artifact_rows(wide, fmt="xml") == 5000

    def _dom_forbidden(*_a, **_k):
        raise AssertionError("xmltodict DOM must not run when StAX COUNT is available")

    monkeypatch.setattr("xmltodict.parse", _dom_forbidden)
    assert count_xml_records(nested) == 3


def test_count_jsonl_records_streams_objects_not_prefix_or_ingest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Dest COUNT streams one object per line. Ingest parse_jsonl stays ingest.

    A scalar / array / malformed line is unmeasured — never dest=prefix.
    Empty / blank-only is 0. parse_jsonl still raises on empty and still
    materializes; this COUNT must not decode the whole path as one string.
    """
    from services.file_parser import count_jsonl_records, parse_jsonl
    from services.format_converter import convert_rows

    content, _mime = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="jsonl",
    )
    path = tmp_path / "export.jsonl"
    path.write_bytes(content)
    assert count_jsonl_records(content) == 3
    assert count_artifact_rows(path, fmt="jsonl") == 3
    one, _ = convert_rows(
        ["id", "v"],
        [["1", "a"]],
        source_format="csv",
        target_format="jsonl",
    )
    assert count_jsonl_records(one) == 1
    empty, _ = convert_rows(["id", "v"], [], source_format="csv", target_format="jsonl")
    assert count_jsonl_records(empty) == 0
    empty_path = tmp_path / "empty.jsonl"
    empty_path.write_bytes(empty)
    assert count_artifact_rows(empty_path, fmt="jsonl") == 0
    assert count_jsonl_records(b"") == 0
    assert count_jsonl_records(b"\n\n  \n") == 0
    assert count_jsonl_records(b'{"id":1}\r\n{"id":2}\r\n') == 2
    assert count_jsonl_records(b'  {"id":1}\n\n{"id":2}\n') == 2
    assert count_jsonl_records(b'{"id":1}\n42\n{"id":3}\n') is None
    assert count_jsonl_records(b'{"id":1}\n[1,2]\n') is None
    assert count_jsonl_records(b'{"id":1}\nnull\n') is None
    assert count_jsonl_records(b'{"id":1}\n{not json\n') is None
    assert count_jsonl_records(b'{"id": 1}\n{"id": "\xff"}') is None
    with pytest.raises(ValueError, match="at least one"):
        parse_jsonl(b"")
    with pytest.raises(ValueError, match="JSON object"):
        parse_jsonl(b'{"id":1}\n42\n')

    wide = tmp_path / "wide.jsonl"
    with wide.open("w", encoding="utf-8") as handle:
        for i in range(5000):
            handle.write(f'{{"id":{i}}}\n')
    assert count_jsonl_records(wide) == 5000
    assert count_artifact_rows(wide, fmt="jsonl") == 5000

    stamped = stamp_artifact_census(
        {"target_rows": 10_000, "skipped_readback": True},
        {"path": str(path), "format": "jsonl"},
    )
    assert stamped[ARTIFACT_COUNT_KEY] == 3
    assert stamped["target_rows"] == 3

    orig_read_bytes = Path.read_bytes
    orig_read_text = Path.read_text

    def _no_read_bytes(self, *args, **kwargs):
        if Path(self).resolve() == wide.resolve():
            raise AssertionError(
                "JSONL COUNT must not read_bytes the whole export"
            )
        return orig_read_bytes(self, *args, **kwargs)

    def _no_read_text(self, *args, **kwargs):
        if Path(self).resolve() == wide.resolve():
            raise AssertionError(
                "JSONL COUNT must not read_text the whole export"
            )
        return orig_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", _no_read_bytes)
    monkeypatch.setattr(Path, "read_text", _no_read_text)
    assert count_jsonl_records(wide) == 5000
    assert count_artifact_rows(wide, fmt="jsonl") == 5000


def test_count_csv_rows_streams_rfc4180_not_line_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Dest COUNT is csv.reader records from disk, not wc -l.

    A quoted embedded newline is one row. Blank / delimiter-only lines
    are not dest rows. Header-only is 0. parse_csv_preview ingest stays
    ingest. Path COUNT must not slurp the whole export.
    """
    from services.csv_profiler import count_csv_rows, parse_csv_preview
    from services.format_converter import convert_rows

    content, _mime = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="csv",
    )
    path = tmp_path / "export.csv"
    path.write_bytes(content)
    assert count_csv_rows(content) == 3
    assert count_artifact_rows(path, fmt="csv") == 3
    one, _ = convert_rows(
        ["id", "v"],
        [["1", "a"]],
        source_format="csv",
        target_format="csv",
    )
    assert count_csv_rows(one) == 1
    empty, _ = convert_rows(["id", "v"], [], source_format="csv", target_format="csv")
    assert count_csv_rows(empty) == 0
    empty_path = tmp_path / "empty.csv"
    empty_path.write_bytes(empty)
    assert count_artifact_rows(empty_path, fmt="csv") == 0
    assert count_csv_rows(b"") == 0
    assert count_csv_rows(b"a,b\n1,2\n\n,\n3,4\n\n") == 2
    quoted, _ = convert_rows(
        ["id", "note"],
        [["1", "hello\nworld"], ["2", "b"]],
        source_format="csv",
        target_format="csv",
    )
    assert quoted.count(b"\n") > 3
    assert count_csv_rows(quoted) == 2
    quoted_path = tmp_path / "quoted.csv"
    quoted_path.write_bytes(quoted)
    assert count_artifact_rows(quoted_path, fmt="csv") == 2
    tsv, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="tsv",
    )
    assert count_csv_rows(tsv) == 2
    bom = b"\xef\xbb\xbfid,name\n1,a\n2,b\n"
    assert count_csv_rows(bom) == 2
    headers, preview, _enc, _delim = parse_csv_preview(content)
    assert headers == ["id", "v"]
    assert preview == [["1", "a"], ["2", "b"], ["3", "c"]]

    wide = tmp_path / "wide.csv"
    with wide.open("w", encoding="utf-8", newline="") as handle:
        handle.write("id,v\n")
        for i in range(5000):
            handle.write(f"{i},x\n")
    assert count_csv_rows(wide) == 5000
    assert count_artifact_rows(wide, fmt="csv") == 5000

    stamped = stamp_artifact_census(
        {"target_rows": 10_000, "skipped_readback": True},
        {"path": str(path), "format": "csv"},
    )
    assert stamped[ARTIFACT_COUNT_KEY] == 3
    assert stamped["target_rows"] == 3

    orig_read_bytes = Path.read_bytes
    orig_read_text = Path.read_text

    def _no_read_bytes(self, *args, **kwargs):
        if Path(self).resolve() == wide.resolve():
            raise AssertionError(
                "CSV COUNT must not read_bytes the whole export"
            )
        return orig_read_bytes(self, *args, **kwargs)

    def _no_read_text(self, *args, **kwargs):
        if Path(self).resolve() == wide.resolve():
            raise AssertionError(
                "CSV COUNT must not read_text the whole export"
            )
        return orig_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", _no_read_bytes)
    monkeypatch.setattr(Path, "read_text", _no_read_text)
    assert count_csv_rows(wide) == 5000
    assert count_artifact_rows(wide, fmt="csv") == 5000


def test_count_artifact_rows_gzip_streams_not_slurp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Local gzip CSV/JSON/JSONL/XML COUNT streams. Never decompress-then-slurp.

    Excel/Avro/Parquet/ORC gzip still decompresses first (byte-image parsers).
    Object-store GET gzip of the same kinds streams through ``GzipFile``
    (see ``test_count_artifact_payload_gzip_streams_not_decompress_slurp``).
    """
    import gzip

    from services.csv_profiler import count_csv_rows
    from services.file_parser import count_jsonl_records, count_xml_records
    from services.format_converter import convert_rows
    from services.json_tabular import count_json_records

    csv_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="csv",
    )
    csv_gz = tmp_path / "export.csv.gz"
    csv_gz.write_bytes(gzip.compress(csv_body))
    assert count_csv_rows(csv_gz) == 3
    assert count_artifact_rows(csv_gz, fmt="csv") == 3

    jsonl_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="jsonl",
    )
    jsonl_gz = tmp_path / "export.jsonl.gz"
    jsonl_gz.write_bytes(gzip.compress(jsonl_body))
    assert count_jsonl_records(jsonl_gz) == 2
    assert count_artifact_rows(jsonl_gz, fmt="jsonl") == 2

    json_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="json",
    )
    json_gz = tmp_path / "export.json.gz"
    json_gz.write_bytes(gzip.compress(json_body))
    assert count_json_records(json_gz) == 3
    assert count_artifact_rows(json_gz, fmt="json") == 3

    xml_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="xml",
    )
    xml_gz = tmp_path / "export.xml.gz"
    xml_gz.write_bytes(gzip.compress(xml_body))
    assert count_xml_records(xml_gz) == 2
    assert count_artifact_rows(xml_gz, fmt="xml") == 2

    quoted, _ = convert_rows(
        ["id", "note"],
        [["1", "hello\nworld"], ["2", "b"]],
        source_format="csv",
        target_format="csv",
    )
    quoted_gz = tmp_path / "quoted.csv.gz"
    quoted_gz.write_bytes(gzip.compress(quoted))
    assert count_csv_rows(quoted_gz) == 2
    assert count_artifact_rows(quoted_gz, fmt="csv") == 2

    bad = tmp_path / "bad.csv.gz"
    bad.write_bytes(b"not-gzip")
    assert count_artifact_rows(bad, fmt="csv") is None

    guarded = {p.resolve() for p in (csv_gz, jsonl_gz, json_gz, xml_gz, quoted_gz)}
    orig_read_bytes = Path.read_bytes

    def _no_read_bytes(self, *args, **kwargs):
        if Path(self).resolve() in guarded:
            raise AssertionError("gzip COUNT must not read_bytes the compressed export")
        return orig_read_bytes(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", _no_read_bytes)
    assert count_artifact_rows(csv_gz, fmt="csv") == 3
    assert count_artifact_rows(jsonl_gz, fmt="jsonl") == 2
    assert count_artifact_rows(json_gz, fmt="json") == 3
    assert count_artifact_rows(xml_gz, fmt="xml") == 2


def test_count_artifact_payload_gzip_streams_not_decompress_slurp(
    monkeypatch: pytest.MonkeyPatch,
):
    """Object-store GET gzip COUNT streams GzipFile. Never gzip.decompress.

    The GET body is already compressed in RAM. A second decompressed copy
    is the same hole local *.gz had. Excel/Avro/Parquet/ORC GET gzip still
    decompresses (byte-image parsers).
    """
    import gzip

    from services.dest_precount import _count_artifact_payload
    from services.format_converter import convert_rows

    csv_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="csv",
    )
    jsonl_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="jsonl",
    )
    json_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="json",
    )
    xml_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="xml",
    )
    quoted, _ = convert_rows(
        ["id", "note"],
        [["1", "hello\nworld"], ["2", "b"]],
        source_format="csv",
        target_format="csv",
    )

    def _no_decompress(*_a, **_k):
        raise AssertionError("GET gzip COUNT must not gzip.decompress the whole body")

    monkeypatch.setattr("services.dest_precount.gzip.decompress", _no_decompress)
    assert _count_artifact_payload(gzip.compress(csv_body), name="export.csv.gz") == 3
    assert _count_artifact_payload(gzip.compress(jsonl_body), name="export.jsonl.gz") == 2
    assert _count_artifact_payload(gzip.compress(json_body), name="export.json.gz") == 3
    assert _count_artifact_payload(gzip.compress(xml_body), name="export.xml.gz") == 2
    assert _count_artifact_payload(gzip.compress(quoted), name="quoted.csv.gz") == 2
    assert _count_artifact_payload(b"not-gzip", name="bad.csv.gz") is None
    assert _count_artifact_payload(csv_body, name="export.csv") == 3


def _oneshot_gzip(compressed: bytes) -> gzip.GzipFile:
    """``GzipFile`` over a compressed GET body that cannot rewind.

    boto3 ``StreamingBody`` / GCS / ADLS download streams are one-shot.
    CSV COUNT used to ``seek(0)`` after the encoding prefix; that only
    worked because the GET had already been ``read()`` into ``BytesIO``.
    """

    class _NoSeek(io.BytesIO):
        def seekable(self) -> bool:
            return False

        def seek(self, *args: object, **kwargs: object) -> int:
            raise AssertionError("compressed GET body is not rewindable")

    return gzip.GzipFile(fileobj=_NoSeek(compressed), mode="rb")


def test_count_csv_rows_one_shot_gzip_stream_no_seek() -> None:
    """CSV COUNT on a non-rewindable gzip stream. Spark/Hadoop do this.

    Encoding sniff consumes a prefix. ``seek(0)`` would fail on an HTTP
    GET body. Prefix-then-rest is the COUNT from byte 0 without rewind.
    JSON/XML/JSONL already walk forward-only; this locks the same
    contract on those COUNT openers.
    """
    import gzip as gzip_mod

    from services.csv_profiler import count_csv_rows
    from services.file_parser import count_jsonl_records, count_xml_records
    from services.format_converter import convert_rows
    from services.json_tabular import count_json_records

    csv_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="csv",
    )
    quoted, _ = convert_rows(
        ["id", "note"],
        [["1", "hello\nworld"], ["2", "b"]],
        source_format="csv",
        target_format="csv",
    )
    jsonl_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="jsonl",
    )
    json_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="json",
    )
    xml_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="xml",
    )

    assert count_csv_rows(_oneshot_gzip(gzip_mod.compress(csv_body))) == 3
    assert count_csv_rows(_oneshot_gzip(gzip_mod.compress(quoted))) == 2
    assert count_jsonl_records(_oneshot_gzip(gzip_mod.compress(jsonl_body))) == 2
    assert count_json_records(_oneshot_gzip(gzip_mod.compress(json_body))) == 3
    assert count_xml_records(_oneshot_gzip(gzip_mod.compress(xml_body))) == 2

    with pytest.raises((OSError, EOFError, gzip_mod.BadGzipFile, ValueError)):
        count_csv_rows(_oneshot_gzip(b"not-gzip"))


def test_count_artifact_rows_missing_parser_is_unmeasured_not_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """Absent openpyxl/fastavro/pyarrow must not close overwrite as dest=0."""
    xlsx = tmp_path / "export.xlsx"
    xlsx.write_bytes(b"not-a-workbook")
    monkeypatch.setattr(
        "services.excel_parser.count_excel_rows",
        lambda *_a, **_k: (_ for _ in ()).throw(ValueError("Excel import is not ready")),
    )
    assert count_artifact_rows(xlsx, fmt="excel") is None

    import sys

    avro = tmp_path / "export.avro"
    avro.write_bytes(b"not-avro")
    monkeypatch.setitem(sys.modules, "fastavro", None)
    assert count_artifact_rows(avro, fmt="avro") is None

    orc = tmp_path / "export.orc"
    orc.write_bytes(b"not-orc")
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    assert count_artifact_rows(orc, fmt="orc") is None

    xml = tmp_path / "export.xml"
    xml.write_bytes(b"<records><record><id>1</id></record></records>")
    monkeypatch.setitem(sys.modules, "defusedxml", None)
    monkeypatch.setitem(sys.modules, "defusedxml.ElementTree", None)
    monkeypatch.setitem(sys.modules, "xmltodict", None)
    assert count_artifact_rows(xml, fmt="xml") is None


def test_stamp_artifact_census_never_keeps_writer_target_rows(tmp_path: Path):
    csv_path = tmp_path / "out.csv"
    csv_path.write_text("id\n1\n2\n", encoding="utf-8")
    stamped = stamp_artifact_census(
        {"target_rows": 10_000, "skipped_readback": True},
        {"path": str(csv_path), "format": "csv"},
    )
    assert stamped[ARTIFACT_COUNT_KEY] == 2
    assert stamped["dest_count_source"] == DEST_COUNT_ARTIFACT
    assert stamped["target_rows"] == 2
    assert stamped["target_rows_before"] == 0

    unmeasured = stamp_artifact_census(
        {"target_rows": 10_000, "skipped_readback": True},
        {"path": "s3://bucket/export.csv", "format": "csv"},
    )
    assert ARTIFACT_COUNT_KEY not in unmeasured
    assert unmeasured["target_rows"] is None


def test_identity_readback_closes_on_distinct_source_id_not_vector_count():
    """RAG hole: 2 documents / 5 chunks / writer 10,000 never closes dest as 5."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 5,
            "target_checksum": "abc123",
            "skipped_readback": True,
            "unproven": True,
            "migration_proven": False,
            "dest_count_source": DEST_IDENTITY_READBACK,
            IDENTITY_COUNT_KEY: 2,
            VECTOR_ROWS_KEY: 5,
            "message": "pgvector write completed — Gate-8 embedding cell fidelity unproven",
        }
    )
    assert count == 2
    assert source == DEST_IDENTITY_READBACK


def test_identity_source_without_identity_rows_is_unmeasured():
    """Forged dest_count_source + stuffed vector COUNT(*) is still not dest."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "target_checksum": "abc123",
            "skipped_readback": True,
            "dest_count_source": DEST_IDENTITY_READBACK,
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_skipped_identity_readback_refuses_physical_vector_count():
    count, source = dest_count_from_recon(
        {
            "target_rows": 5,
            "target_checksum": "abc123",
            "dest_count_source": "skipped_identity_readback",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_vector_overwrite_balances_on_identities_not_chunks_or_writer_ack():
    ledger = account_population(
        rows_read=2,
        dest_count=2,
        dest_count_source=DEST_IDENTITY_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="full_refresh_overwrite",
        vector={"identity_rows": 2, "vector_rows": 5},
    )
    assert ledger.conservation_kind == KIND_VECTOR
    assert ledger.rows_written == 2
    assert ledger.rows_written_source == DEST_IDENTITY_READBACK
    assert ledger.identity_count == 2
    assert ledger.vector_rows == 5
    assert ledger.writer_ack == 10_000
    assert ledger.unaccounted == 0
    assert ledger.balanced is True
    assert ledger.writer_ack_delta == -9998
    assert "identity" in ledger.note.lower() or "source_id" in ledger.note.lower()
    assert "chunk" in ledger.note.lower() or "vector" in ledger.note.lower()


def test_vector_physical_count_does_not_close_as_overwrite_surplus():
    """If dest_count were physical 5 against reader 2, overwrite would invent dupes."""
    ledger = account_population(
        rows_read=2,
        dest_count=5,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=5,
        sync_mode="full_refresh_overwrite",
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.balanced is False
    assert ledger.unaccounted == -3


def test_vector_nonempty_dest_stays_unproven_without_source_id_census():
    ledger = account_population(
        rows_read=2,
        dest_count=12,
        dest_count_source=DEST_IDENTITY_READBACK,
        dest_count_before=10,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=5,
        sync_mode="incremental_append",
        vector={"identity_rows": 12, "vector_rows": 40},
    )
    assert ledger.conservation_kind == KIND_VECTOR
    assert ledger.balanced is False
    assert ledger.unaccounted is None
    assert ledger.dest_count == 12
    assert ledger.dest_count_before == 10
    assert ledger.dest_delta == 2
    assert "unproven" in ledger.note.lower()


def test_vector_dest_before_unmeasured_does_not_close():
    ledger = account_population(
        rows_read=2,
        dest_count=2,
        dest_count_source=DEST_IDENTITY_READBACK,
        dest_count_before=None,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=5,
        sync_mode="full_refresh_overwrite",
        vector={"identity_rows": 2, "vector_rows": 5},
    )
    assert ledger.conservation_kind == KIND_VECTOR
    assert ledger.balanced is False
    assert ledger.unaccounted is None


def test_stamp_vector_census_milvus_unreachable_is_skipped_identity_not_rowcount():
    """Unreachable Milvus must not close dest as collection rowCount."""
    stamped = stamp_vector_census(
        {"target_rows": 5, "target_checksum": "abc"},
        {},
        schema="public",
        table_name="docs",
        dest_engine="milvus",
    )
    assert IDENTITY_COUNT_KEY not in stamped
    assert stamped.get("dest_count_source") == "skipped_identity_readback"
    assert stamped["target_rows"] == 5


def test_stamp_vector_census_pinecone_closes_on_distinct_source_id(monkeypatch):
    """Pinecone identity is DISTINCT source_id, never describe_index_stats vectorCount."""

    def fake_scan(cfg, *, table_name, max_entities):
        assert table_name == "docs"
        return SOURCE_ID_SCAN_COMPLETE, ["doc-1", "doc-1", "doc-1", "doc-2", "doc-2"]

    monkeypatch.setattr("connectors.pinecone_writer.scan_source_ids", fake_scan)
    stamped = stamp_vector_census(
        {"target_rows": 10_000, "target_checksum": "writer"},
        {"host": "https://idx.svc.pinecone.io", "api_key": "k"},
        schema="",
        table_name="docs",
        dest_engine="pinecone",
    )
    assert stamped[IDENTITY_COUNT_KEY] == 2
    assert stamped["dest_count_source"] == DEST_COUNT_IDENTITY
    assert stamped[VECTOR_ROWS_KEY] == 10_000
    count, source = dest_count_from_recon(stamped)
    assert count == 2
    assert source == DEST_IDENTITY_READBACK
    assert "pinecone" in VECTOR_IDENTITY_ENGINES
    assert "weaviate" in VECTOR_IDENTITY_ENGINES


def test_stamp_vector_census_weaviate_truncated_scan_is_unmeasured(monkeypatch):
    def fake_scan(cfg, *, table_name, max_entities):
        return SOURCE_ID_SCAN_TRUNCATED, []

    monkeypatch.setattr("connectors.weaviate_writer.scan_source_ids", fake_scan)
    stamped = stamp_vector_census(
        {"target_rows": 5, "target_checksum": "abc"},
        {"host": "127.0.0.1", "port": 8080},
        schema="",
        table_name="docs",
        dest_engine="weaviate",
    )
    assert IDENTITY_COUNT_KEY not in stamped
    assert stamped.get("dest_count_source") == "skipped_identity_readback"
    assert stamped["target_rows"] == 5


def test_stamp_vector_census_pinecone_unreachable_is_skipped_identity_not_vectorcount(monkeypatch):
    def fake_scan(cfg, *, table_name, max_entities):
        return SOURCE_ID_SCAN_UNMEASURED, []

    monkeypatch.setattr("connectors.pinecone_writer.scan_source_ids", fake_scan)
    stamped = stamp_vector_census(
        {"target_rows": 5, "target_checksum": "abc"},
        {"host": "https://idx.svc.pinecone.io"},
        schema="",
        table_name="docs",
        dest_engine="pinecone",
    )
    assert IDENTITY_COUNT_KEY not in stamped
    assert stamped.get("dest_count_source") == "skipped_identity_readback"
    assert stamped["target_rows"] == 5


def test_identity_count_from_source_id_scan_is_distinct_not_chunk_count():
    """5 chunks / 2 documents / empty ids / truncated prefix — SQL COUNT DISTINCT."""
    assert identity_count_from_source_id_scan(SOURCE_ID_SCAN_MISSING, None) == 0
    assert identity_count_from_source_id_scan(
        SOURCE_ID_SCAN_COMPLETE,
        ["doc-1", "doc-1", "doc-1", "doc-2", "doc-2"],
    ) == 2
    assert identity_count_from_source_id_scan(SOURCE_ID_SCAN_COMPLETE, []) == 0
    assert identity_count_from_source_id_scan(
        SOURCE_ID_SCAN_COMPLETE, ["doc-1", "", None, "  "]
    ) == 1
    assert identity_count_from_source_id_scan(
        SOURCE_ID_SCAN_TRUNCATED, ["doc-1"] * 20_000
    ) is None
    assert identity_count_from_source_id_scan(SOURCE_ID_SCAN_NO_FIELD, ["x"]) is None
    assert identity_count_from_source_id_scan(SOURCE_ID_SCAN_UNMEASURED, ["x"]) is None


def test_stamp_vector_census_milvus_closes_on_distinct_source_id(monkeypatch):
    def fake_scan(cfg, *, table_name, max_entities):
        assert table_name == "docs"
        assert max_entities >= 5
        return SOURCE_ID_SCAN_COMPLETE, ["doc-1", "doc-1", "doc-1", "doc-2", "doc-2"]

    monkeypatch.setattr("connectors.milvus_writer.scan_source_ids", fake_scan)
    stamped = stamp_vector_census(
        {"target_rows": 10_000, "target_checksum": "writer"},
        {"host": "127.0.0.1", "port": 19530},
        schema="",
        table_name="docs",
        dest_engine="milvus",
    )
    assert stamped[IDENTITY_COUNT_KEY] == 2
    assert stamped["dest_count_source"] == DEST_COUNT_IDENTITY
    assert stamped[VECTOR_ROWS_KEY] == 10_000
    count, source = dest_count_from_recon(stamped)
    assert count == 2
    assert source == DEST_IDENTITY_READBACK


def test_stamp_vector_census_qdrant_truncated_scan_is_unmeasured(monkeypatch):
    def fake_scan(cfg, *, table_name, max_entities):
        return SOURCE_ID_SCAN_TRUNCATED, []

    monkeypatch.setattr("connectors.qdrant_writer.scan_source_ids", fake_scan)
    stamped = stamp_vector_census(
        {"target_rows": 5, "target_checksum": "writer"},
        {"host": "127.0.0.1", "port": 6333},
        schema="",
        table_name="docs",
        dest_engine="qdrant",
    )
    assert IDENTITY_COUNT_KEY not in stamped
    assert stamped.get("dest_count_source") == "skipped_identity_readback"
    count, source = dest_count_from_recon(stamped)
    assert count is None
    assert source == DEST_UNMEASURED


def test_current_readback_closes_on_is_current_not_history_count():
    """SCD2 hole: 2 current / 3 history / writer 10,000 never closes dest as 3."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 3,
            "target_checksum": "writer-active",
            "skipped_readback": True,
            "unproven": True,
            "migration_proven": False,
            "dest_count_source": DEST_CURRENT_READBACK,
            CURRENT_ROWS_KEY: 2,
            HISTORY_ROWS_KEY: 3,
            "message": "SCD2 merge — Gate-8 stuffed active_rows is writer-path",
        }
    )
    assert count == 2
    assert source == DEST_CURRENT_READBACK


def test_current_source_without_current_rows_is_unmeasured():
    """Forged dest_count_source + stuffed history COUNT(*) is still not dest."""
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "target_checksum": "abc123",
            "skipped_readback": True,
            "dest_count_source": DEST_CURRENT_READBACK,
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_skipped_current_readback_refuses_physical_history_count():
    count, source = dest_count_from_recon(
        {
            "target_rows": 3,
            "target_checksum": "abc123",
            "dest_count_source": "skipped_current_readback",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_scd2_first_load_closes_on_current_not_history_or_writer_ack():
    ledger = account_population(
        rows_read=2,
        dest_count=2,
        dest_count_source=DEST_CURRENT_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="scd2",
        scd2={"current_rows": 2, "history_rows": 2},
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.rows_written == 2
    assert ledger.rows_written_source == DEST_CURRENT_READBACK
    assert ledger.current_count == 2
    assert ledger.history_rows == 2
    assert ledger.dest_count == 2
    assert ledger.active_count is None
    assert ledger.dest_delta is None
    assert ledger.writer_ack == 10_000
    assert ledger.unaccounted == 0
    assert ledger.balanced is True
    assert ledger.writer_ack_delta == -9998
    assert "is_current" in ledger.note.lower() or "current" in ledger.note.lower()
    assert "history" in ledger.note.lower()


def test_scd2_physical_history_does_not_close_as_overwrite_surplus():
    """2 identities / 3 history rows must not close overwrite as dest=3."""
    ledger = account_population(
        rows_read=2,
        dest_count=3,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="scd2",
        scd2={"current_rows": 2, "history_rows": 3},
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.balanced is False
    assert ledger.rows_written is None
    assert ledger.rows_written_source == DEST_UNMEASURED
    assert ledger.active_count is None


def test_scd2_incremental_change_batch_stays_unproven():
    """Watermarked SCD2: reader=1 changed row, current=2, history=3."""
    ledger = account_population(
        rows_read=1,
        dest_count=2,
        dest_count_source=DEST_CURRENT_READBACK,
        dest_count_before=3,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=1,
        sync_mode="scd2",
        scd2={"current_rows": 2, "history_rows": 3},
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.balanced is False
    assert ledger.unaccounted is None
    assert ledger.rows_written_source == DEST_CURRENT_READBACK
    assert ledger.dest_count == 2
    assert ledger.history_rows == 3
    assert ledger.dest_delta is None
    assert "incremental" in ledger.note.lower()


def test_scd2_full_snapshot_resync_closes_when_reader_equals_current():
    ledger = account_population(
        rows_read=2,
        dest_count=2,
        dest_count_source=DEST_CURRENT_READBACK,
        dest_count_before=3,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=1,
        sync_mode="scd2",
        scd2={"current_rows": 2, "history_rows": 3},
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.balanced is True
    assert ledger.unaccounted == 0
    assert ledger.dest_count == 2
    assert ledger.history_rows == 3
    assert ledger.dest_delta is None
    assert "re-sync" in ledger.note.lower() or "resync" in ledger.note.lower()


def test_scd2_writer_active_checksum_is_not_mirror_and_does_not_close():
    """SCD2 dest_summary stamps active_rows + active_checksum — that is not _deleted."""
    ledger = account_job(
        {
            "sync_mode": "scd2",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 2,
                "target_rows": 2,
                "target_checksum": "writer-active",
            },
            "destination_summary": {
                "active_rows": 2,
                "active_checksum": "writer-active",
                "scd2": {"active_rows": 2, "rows_written": 1},
            },
        }
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.balanced is False
    assert ledger.rows_written is None
    assert ledger.rows_written_source == DEST_UNMEASURED
    assert ledger.active_count is None


def test_scd2_kind_is_not_overwrite_or_keyed_even_when_dest_is_empty():
    assert conservation_kind("scd2", dest_count_before=0) == KIND_SCD2
    assert conservation_kind("scd2", dest_count_before=3) == KIND_SCD2
    assert conservation_kind("slowly_changing_dimension", dest_count_before=0) == KIND_SCD2


def test_stamp_scd2_census_pgvector_is_a_noop():
    stamped = stamp_scd2_census(
        {"target_rows": 5, "target_checksum": "abc"},
        {"host": "127.0.0.1"},
        schema="public",
        table_name="docs",
        dest_engine="pgvector",
    )
    assert CURRENT_ROWS_KEY not in stamped
    assert stamped.get("dest_count_source") != DEST_COUNT_CURRENT


def test_scd2_live_sqlite_current_not_history(tmp_path: Path):
    """apply_scd2 twice: current=2, history=3; conservation uses 2 not 3."""
    from src.transfer.models import EndpointConfig

    from services.scd2_engine import apply_scd2

    db = tmp_path / "scd2_current.db"
    endpoint = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=f"sqlite:///{db}",
        database=str(db),
        table="products",
    )
    columns = ["id", "name", "price"]
    schema = {"id": "string", "name": "string", "price": "decimal"}
    first = [
        {"id": "1", "name": "A", "price": "10.00"},
        {"id": "2", "name": "B", "price": "20.00"},
    ]
    apply_scd2(endpoint, first, columns, schema, None, ["id"])
    missing = count_scd2_current("sqlite", {"database": str(db)}, schema="", table_name="gone")
    assert missing == 0
    first_current = count_scd2_current(
        "sqlite", {"database": str(db)}, schema="", table_name="products"
    )
    assert first_current == 2
    changed = [
        {"id": "1", "name": "A-updated", "price": "10.00"},
        {"id": "2", "name": "B", "price": "20.00"},
    ]
    apply_scd2(endpoint, changed, columns, schema, None, ["id"])
    stamped = stamp_scd2_census(
        {
            "source_rows": 2,
            "target_rows": 10_000,
            "target_checksum": "writer-active",
            "skipped_readback": True,
        },
        {"database": str(db), "type": "sqlite"},
        schema="",
        table_name="products",
        dest_engine="sqlite",
    )
    assert stamped[CURRENT_ROWS_KEY] == 2
    assert stamped[HISTORY_ROWS_KEY] == 3
    assert stamped["dest_count_source"] == DEST_COUNT_CURRENT
    assert stamped["target_rows"] == 10_000
    count, source = dest_count_from_recon(stamped)
    assert count == 2
    assert source == DEST_CURRENT_READBACK
    ledger = account_population(
        rows_read=2,
        dest_count=count,
        dest_count_source=source,
        dest_count_before=2,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="scd2",
        scd2={"current_rows": 2, "history_rows": 3},
    )
    assert ledger.balanced is True
    assert ledger.dest_count == 2
    assert ledger.history_rows == 3
    assert ledger.writer_ack == 10_000

    bare = stamp_scd2_census(
        {"target_rows": 4},
        {"database": str(db), "type": "sqlite"},
        schema="",
        table_name="products",
        dest_engine="sqlite",
    )
    # Table without is_current: create a non-SCD2 table.
    import sqlite3

    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE plain (id TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO plain (id) VALUES ('x')")
        conn.commit()
    none = count_scd2_current(
        "sqlite", {"database": str(db)}, schema="", table_name="plain"
    )
    assert none is None
    skipped = stamp_scd2_census(
        {"target_rows": 1},
        {"database": str(db)},
        schema="",
        table_name="plain",
        dest_engine="sqlite",
    )
    assert skipped.get("dest_count_source") == "skipped_current_readback"
    assert CURRENT_ROWS_KEY not in skipped
    assert bare[CURRENT_ROWS_KEY] == 2


def test_job_rollup_two_scd2_streams_sums_current_not_history():
    def _scd2(name: str, current: int, history: int) -> dict:
        return {
            "name": name,
            "row_accounting": account_population(
                rows_read=current,
                dest_count=current,
                dest_count_source=DEST_CURRENT_READBACK,
                dest_count_before=0,
                rejected_rows=0,
                coerced_null_rows=0,
                rows_skipped=0,
                writer_ack=history * 100,
                sync_mode="scd2",
                scd2={"current_rows": current, "history_rows": history},
            ).to_dict(),
        }

    job = {
        "records_processed": 10_000,
        "destination_summary": {
            "streams": [
                _scd2("dim_a", 2, 3),
                _scd2("dim_b", 3, 5),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is True
    assert ledger.summable is True
    assert ledger.dest_count == 5
    assert ledger.rows_written == 5
    assert ledger.rows_written_source == DEST_CURRENT_READBACK
    assert ledger.active_count is None
    assert ledger.per_stream[0]["dest_count"] == 2
    assert ledger.per_stream[1]["dest_count"] == 3


def test_account_job_scd2_recon_never_uses_writer_or_history_count():
    ledger = account_job(
        {
            "sync_mode": "scd2",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 2,
                "target_rows": 3,
                "target_checksum": "history",
                "skipped_readback": True,
                "dest_count_source": DEST_CURRENT_READBACK,
                CURRENT_ROWS_KEY: 2,
                HISTORY_ROWS_KEY: 3,
                "target_rows_before": 0,
            },
        }
    )
    assert ledger.conservation_kind == KIND_SCD2
    assert ledger.dest_count == 2
    assert ledger.current_count == 2
    assert ledger.history_rows == 3
    assert ledger.writer_ack == 10_000
    assert ledger.balanced is True
    assert ledger.active_count is None


def test_count_star_nets_missing_and_extra_keys_to_a_false_balance():
    """DMS hole: dest {2,3,99} vs source {1,2,3} is COUNT(*)=3 but not the same keys."""
    ledger = account_population(
        rows_read=3,
        dest_count=3,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="full_refresh_overwrite",
        keyset={MISSING_KEYS_KEY: 1, EXTRA_KEYS_KEY: 1},
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.unaccounted == 0
    assert ledger.balanced is False
    assert ledger.missing_keys == 1
    assert ledger.extra_keys == 1
    assert ledger.writer_ack == 10_000
    assert "MISSING_TARGET" in ledger.note
    assert "EXTRA_TARGET" in ledger.note
    assert "inferred" in ledger.note.lower()


def test_keyset_closed_when_every_source_key_is_on_dest_and_no_extras():
    ledger = account_population(
        rows_read=3,
        dest_count=3,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=3,
        sync_mode="full_refresh_overwrite",
        keyset={MISSING_KEYS_KEY: 0, EXTRA_KEYS_KEY: 0},
    )
    assert ledger.balanced is True
    assert ledger.missing_keys == 0
    assert ledger.extra_keys == 0
    assert "keyset closed" in ledger.note.lower()


def test_incremental_without_keyset_does_not_invent_leftover_from_batch():
    """A CDC batch is not S. dest_count − hits(batch) would be almost every dest row."""
    ledger = account_population(
        rows_read=3,
        dest_count=3,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=3,
        sync_mode="full_refresh_overwrite",
    )
    assert ledger.balanced is True
    assert ledger.missing_keys is None
    assert ledger.extra_keys is None


def test_records_to_key_tuples_requires_one_row_one_key():
    assert records_to_key_tuples(
        [{"id": 1}, {"id": 2}, {"id": 3}],
        ["id"],
    ) == [(1,), (2,), (3,)]
    assert records_to_key_tuples([{"id": 1}, {"id": 1}], ["id"]) is None
    assert records_to_key_tuples([{"id": 1}, {"name": "x"}], ["id"]) is None
    assert records_to_key_tuples([], ["id"]) is None


def test_stamp_keyset_census_does_not_run_on_pgvector():
    stamped = stamp_keyset_census(
        {"target_rows": 3},
        {},
        schema="public",
        table_name="docs",
        dest_engine="pgvector",
        key_columns=["id"],
        keys=[(1,), (2,)],
    )
    assert MISSING_KEYS_KEY not in stamped
    assert EXTRA_KEYS_KEY not in stamped


def test_failed_gate8_still_exposes_independent_dest_count():
    """MISSING_TARGET class: dest COUNT is 9997 even though the write 'succeeded'."""
    count, source = dest_count_from_recon(
        {
            "passed": False,
            "phase": "post_write_failed",
            "target_rows": 9997,
            "source_rows": 10_000,
            "message": "Row count mismatch",
        }
    )
    assert count == 9997
    assert source == DEST_READBACK


def test_overwrite_balances_on_dest_count_not_writer_ack():
    ledger = account_population(
        rows_read=10_000,
        dest_count=9997,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="full_refresh_overwrite",
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.rows_written == 9997
    assert ledger.writer_ack == 10_000
    assert ledger.unaccounted == 3
    assert ledger.balanced is False
    assert ledger.writer_ack_delta == -3


def test_coerced_null_rows_are_on_the_destination():
    ledger = account_population(
        rows_read=10,
        dest_count=10,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=2,
        coerced_null_rows=2,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="overwrite",
    )
    assert ledger.rows_quarantined == 0
    assert ledger.unaccounted == 0
    assert ledger.balanced is True


def test_true_quarantine_hold_outs_close_with_dest_count():
    ledger = account_population(
        rows_read=10,
        dest_count=8,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=2,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=8,
        sync_mode="overwrite",
    )
    assert ledger.rows_quarantined == 2
    assert ledger.balanced is True


def test_append_uses_dest_delta_not_whole_table_count():
    ledger = account_population(
        rows_read=10,
        dest_count=40,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="full_refresh_append",
    )
    assert ledger.conservation_kind == KIND_APPEND_DELTA
    assert ledger.rows_written == 10
    assert ledger.dest_delta == 10
    assert ledger.dest_count == 40
    assert ledger.dest_count_before == 30
    assert ledger.balanced is True
    assert "Pre-existing dest rows remain" in ledger.note
    assert "Every source row is in the destination" not in ledger.note


def test_append_without_precount_is_unmeasured():
    ledger = account_population(
        rows_read=10,
        dest_count=40,
        dest_count_source=DEST_READBACK,
        dest_count_before=None,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="append",
    )
    assert ledger.balanced is False
    assert ledger.rows_written is None
    assert "Append delta unverified" in ledger.note


def test_upsert_into_nonempty_dest_has_no_count_identity():
    ledger = account_population(
        rows_read=10,
        dest_count=35,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="upsert",
    )
    assert ledger.conservation_kind == KIND_KEYED
    assert ledger.balanced is False
    assert ledger.rows_written is None


def test_upsert_into_empty_dest_is_insert_cardinality():
    ledger = account_population(
        rows_read=10,
        dest_count=10,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="upsert",
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.balanced is True


def test_incremental_empty_pass_is_measured_zero():
    ledger = account_population(
        rows_read=0,
        dest_count=None,
        dest_count_source=DEST_UNMEASURED,
        dest_count_before=None,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=0,
        sync_mode="incremental_append",
    )
    assert ledger.conservation_kind == KIND_EMPTY_PASS
    assert ledger.balanced is True
    assert ledger.unaccounted == 0


def test_account_job_ignores_records_processed_when_dest_count_exists():
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "reconciliation": {
            "phase": "post_write_verified",
            "source_rows": 10_000,
            "target_rows": 9997,
            "rejected_rows": 0,
            "rows_skipped": 0,
            "target_checksum": "deadbeef",
            "message": "Verified",
        },
        "destination_summary": {"rows": 10_000, "rejected": 50},
    }
    ledger = account_job(job)
    assert ledger.rows_written == 9997
    assert ledger.writer_ack == 10_000
    assert ledger.rows_quarantined == 0
    assert ledger.balanced is False


def test_extract_batch_keys_are_distinct_and_skip_nulls():
    from services.row_conservation import extract_batch_keys

    keys = extract_batch_keys(
        [
            {"id": 1, "label": "a"},
            {"id": 1, "label": "a2"},
            {"id": 2, "label": "b"},
            {"id": None, "label": "x"},
        ],
        ["id"],
    )
    assert keys == [(1,), (2,)]


def test_key_census_splits_inserts_from_updates():
    from services.row_conservation import KeyCensus

    census = KeyCensus(unique_batch_keys=10, dest_preexisting=7)
    assert census.inserts == 3
    assert census.updates == 7
    assert census.deletes == 0
    assert census.expected_delta == 3
    assert KeyCensus.from_mapping({"unique_batch_keys": 2, "dest_preexisting": 5}) is None


def test_keyed_census_closes_on_dest_delta_not_writer_ack():
    from services.row_conservation import KeyCensus

    census = KeyCensus(unique_batch_keys=10, dest_preexisting=9)
    ledger = account_population(
        rows_read=10,
        dest_count=31,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="upsert",
        census=census,
    )
    assert ledger.conservation_kind == KIND_KEYED
    assert ledger.inserts == 1
    assert ledger.updates == 9
    assert ledger.dest_delta == 1
    assert ledger.rows_written == 1
    assert ledger.unaccounted == 0
    assert ledger.balanced is True
    assert ledger.writer_ack == 10
    assert ledger.writer_ack_delta == -9


def test_keyed_census_detects_dest_shortfall():
    from services.row_conservation import KeyCensus

    census = KeyCensus(unique_batch_keys=4, dest_preexisting=1)
    ledger = account_population(
        rows_read=4,
        dest_count=32,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=4,
        sync_mode="upsert",
        census=census,
    )
    assert ledger.inserts == 3
    assert ledger.dest_delta == 2
    assert ledger.unaccounted == 1
    assert ledger.balanced is False


def test_keyed_census_with_quarantine_stays_unproven():
    from services.row_conservation import KeyCensus

    census = KeyCensus(unique_batch_keys=4, dest_preexisting=3)
    ledger = account_population(
        rows_read=4,
        dest_count=31,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=1,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=3,
        sync_mode="upsert",
        census=census,
    )
    assert ledger.balanced is False
    assert ledger.rows_written is None
    assert "quarantined" in ledger.note


def test_account_job_reads_keyed_census_from_dest_summary():
    job = {
        "records_processed": 10,
        "sync_mode": "upsert",
        "reconciliation": {
            "phase": "post_write_verified",
            "source_rows": 10,
            "target_rows": 31,
            "rejected_rows": 0,
            "rows_skipped": 0,
            "target_checksum": "deadbeef",
            "message": "Verified",
        },
        "destination_summary": {
            "target_rows_before": 30,
            "keyed_census": {"unique_batch_keys": 10, "dest_preexisting": 9},
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_KEYED
    assert ledger.balanced is True
    assert ledger.inserts == 1
    assert ledger.updates == 9
    assert ledger.rows_written == 1
    assert ledger.writer_ack == 10


def test_sqlite_destination_key_hits_are_dest_engine_distinct(tmp_path: Path):
    import sqlite3

    from services.dest_precount import destination_key_hits

    path = tmp_path / "p9_hits.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        conn.commit()
    finally:
        conn.close()
    hits = destination_key_hits(
        "sqlite",
        {"database": str(path)},
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (9,), (1,)],
    )
    assert hits == 2
    empty = destination_key_hits(
        "sqlite",
        {"database": str(path)},
        schema="",
        table_name="missing",
        key_columns=["id"],
        keys=[(1,)],
    )
    assert empty == 0


def test_stream_accumulator_reconstructs_preexisting_from_per_batch_hits():
    from services.row_conservation import KeyCensusAccumulator

    acc = KeyCensusAccumulator()
    acc.add_batch([(1,), (2,)], dest_hits=2)
    acc.add_batch([(3,), (4,)], dest_hits=1)
    census = acc.to_census()
    assert census is not None
    assert census.unique_batch_keys == 4
    assert census.dest_preexisting == 3
    assert census.inserts == 1
    assert census.updates == 3
    assert census.deletes == 0


def test_partition_last_op_wins_delete_then_insert_is_live():
    from services.row_conservation import partition_keyed_records

    part = partition_keyed_records(
        [
            {"id": 1, "label": "gone", "__deleted": True},
            {"id": 1, "label": "back", "__deleted": False},
            {"id": 2, "label": "x", "__deleted": False},
            {"id": 2, "label": "x2", "__op": "d"},
        ],
        ["id"],
    )
    assert part.live_keys == [(1,)]
    assert part.tombstone_keys == [(2,)]
    assert part.live_records[0]["label"] == "back"


def test_keyed_census_tombstone_of_missing_key_is_not_an_insert():
    from services.row_conservation import KeyCensus

    # 3 live keys dest already holds + 1 tombstone dest does not hold.
    census = KeyCensus(
        unique_batch_keys=3,
        dest_preexisting=3,
        tombstones=0,
        unique_tombstone_keys=1,
    )
    assert census.inserts == 0
    assert census.deletes == 0
    assert census.expected_delta == 0


def test_keyed_census_closes_on_insert_minus_dest_held_delete():
    from services.row_conservation import KeyCensus

    census = KeyCensus(
        unique_batch_keys=3,
        dest_preexisting=2,
        tombstones=1,
        unique_tombstone_keys=1,
    )
    assert census.inserts == 1
    assert census.updates == 2
    assert census.deletes == 1
    assert census.expected_delta == 0
    ledger = account_population(
        rows_read=4,
        dest_count=30,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="upsert",
        census=census,
    )
    assert ledger.balanced is True
    assert ledger.dest_delta == 0
    assert ledger.rows_written == 0
    assert ledger.deletes == 1
    assert ledger.inserts == 1
    assert ledger.writer_ack_delta == -10_000


def test_stream_accumulator_delete_only_batch_is_a_census():
    from services.row_conservation import KeyCensusAccumulator

    acc = KeyCensusAccumulator()
    acc.add_batch([], dest_hits=0)
    acc.add_tombstones(2, unique_keys=3)
    census = acc.to_census()
    assert census is not None
    assert census.unique_batch_keys == 0
    assert census.inserts == 0
    assert census.deletes == 2
    assert census.unique_tombstone_keys == 3
    assert census.expected_delta == -2


def test_sqlite_prepare_keyed_upsert_hard_deletes_dest_held_keys(tmp_path: Path):
    import sqlite3

    from services.row_conservation import prepare_keyed_upsert

    path = tmp_path / "p9_tomb.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        conn.commit()
    finally:
        conn.close()

    live, payload = prepare_keyed_upsert(
        [
            {"id": 1, "label": "A", "is_deleted": 0},
            {"id": 2, "label": "b", "is_deleted": 1},
            {"id": 4, "label": "d", "is_deleted": 0},
            {"id": 9, "label": "ghost", "is_deleted": 1},
        ],
        key_columns=["id"],
        mappings=None,
        db_type="sqlite",
        cfg={"database": str(path)},
        schema="",
        table_name="items",
        dest_nonempty=True,
    )
    assert [r["id"] for r in live] == [1, 4]
    assert payload is not None
    assert payload["inserts"] == 1
    assert payload["updates"] == 1
    assert payload["deletes"] == 1
    assert payload["unique_tombstone_keys"] == 2
    assert payload["expected_delta"] == 0

    conn = sqlite3.connect(str(path))
    try:
        rows = list(conn.execute("SELECT id, label FROM items ORDER BY id"))
        count = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    finally:
        conn.close()
    # Hard-DELETE of dest-held key 2; key 9 was never present (no-op).
    # Live upserts have not run yet — prepare only strips + deletes.
    assert count == 2
    assert rows == [(1, "a"), (3, "c")]


def test_mysql_hard_delete_survives_connection_close():
    """PyMySQL ``autocommit = True`` assignment used to roll back DELETE on close."""
    import socket

    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=0.4):
            pass
    except OSError:
        import pytest

        pytest.skip("MariaDB not listening")

    import uuid

    import pymysql

    from services.row_conservation import apply_hard_deletes

    cfg = {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }
    table = f"p9_mysql_del_{uuid.uuid4().hex[:8]}"
    conn = pymysql.connect(
        host=cfg["host"], port=cfg["port"], database=cfg["database"],
        user=cfg["username"], password=cfg["password"], autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, label VARCHAR(8))"
            )
            cur.execute(
                f"INSERT INTO `{table}` (id, label) VALUES (1,'a'),(2,'b'),(3,'c')"
            )
        deleted = apply_hard_deletes(
            db_type="mysql",
            cfg=cfg,
            schema="",
            table_name=table,
            key_columns=["id"],
            keys=[(2,)],
        )
        assert deleted == 1
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM `{table}` ORDER BY id")
            left = [int(r[0]) for r in cur.fetchall()]
        assert left == [1, 3]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()


def test_attach_conservation_only_on_terminal():
    from services.row_conservation import attach_conservation_to_updates

    running = {"records_processed": 10}
    attach_conservation_to_updates("running", running)
    assert "row_accounting" not in running

    done = {
        "records_processed": 10_000,
        "sync_mode": "full_refresh_overwrite",
        "reconciliation": {
            "source_rows": 4,
            "target_rows": 4,
            "target_checksum": "abc",
            "source_checksum": "abc",
            "phase": "post_write_verified",
            "coverage": "full",
            "assurance_level": "full_checksum",
        },
    }
    attach_conservation_to_updates("completed", done)
    ledger = done["row_accounting"]
    assert ledger["dest_count"] == 4
    assert ledger["rows_written"] == 4
    assert ledger["writer_ack"] == 10_000
    assert ledger["writer_ack_delta"] != 0
    assert ledger["rows_written_source"] == DEST_READBACK
    assert ledger["balanced"] is True


def test_ledger_from_transfer_result_does_not_close_on_writer_ack():
    from dataclasses import dataclass, field

    from services.row_conservation import ledger_from_transfer_result

    @dataclass
    class _Result:
        records_transferred: int = 10_000
        operation: str = "full_refresh_overwrite"
        destination_summary: dict = field(default_factory=dict)
        reconciliation: dict = field(default_factory=dict)

    result = _Result(
        reconciliation={
            "source_rows": 4,
            "target_rows": 4,
            "target_checksum": "abc",
            "source_checksum": "abc",
            "phase": "post_write_verified",
            "coverage": "full",
            "assurance_level": "full_checksum",
        },
    )
    ledger = ledger_from_transfer_result(result, sync_mode="full_refresh_overwrite")
    assert ledger["dest_count"] == 4
    assert ledger["writer_ack"] == 10_000
    assert ledger["rows_written"] != ledger["writer_ack"]
    assert ledger["balanced"] is True


def test_mirror_kind_is_not_overwrite_even_on_empty_dest():
    assert conservation_kind("full_refresh_mirror", dest_count_before=0) == KIND_MIRROR
    assert conservation_kind("mirror", dest_count_before=3) == KIND_MIRROR


def test_mirror_closes_on_active_population_not_physical_or_writer_ack():
    """Gate-8 stuffs target_rows with COUNT(*) WHERE NOT _deleted.

    Physical COUNT(*) stays (Fivetran _fivetran_deleted hole). Writer ack
    of 10,000 must not close the identity or hide leftover dest keys.
    """
    ledger = account_job(
        {
            "sync_mode": "full_refresh_mirror",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "active-digest",
                "source_checksum": "source-digest",
                "phase": "post_write_verified",
                "coverage": "full",
            },
            "destination_summary": {
                "mirror": {
                    "mode": "mirror",
                    "active_rows": 3,
                    "soft_deleted": 1,
                    "reactivated": 0,
                    "rows_scanned": 4,
                    "soft_delete_column": "_deleted",
                }
            },
        }
    ).to_dict()
    assert ledger["conservation_kind"] == KIND_MIRROR
    assert ledger["balanced"] is True
    assert ledger["active_count"] == 3
    assert ledger["rows_written"] == 3
    assert ledger["dest_count"] == 4
    assert ledger["inferred_deletes"] == 1
    assert ledger["reactivated"] == 0
    assert ledger["writer_ack"] == 10_000
    assert ledger["writer_ack_delta"] != 0
    assert ledger["rows_written_source"] == DEST_ACTIVE_READBACK
    assert ledger["unaccounted"] == 0


def test_mirror_stream_path_closes_on_top_level_active_rows():
    """Stream path stamps active_rows at dest_summary top-level; no rows_scanned."""
    ledger = account_job(
        {
            "sync_mode": "mirror",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "active-digest",
            },
            "destination_summary": {
                "active_rows": 3,
                "active_checksum": "active-digest",
                "soft_delete_column": "_deleted",
            },
        }
    ).to_dict()
    assert ledger["conservation_kind"] == KIND_MIRROR
    assert ledger["balanced"] is True
    assert ledger["active_count"] == 3
    assert ledger["rows_written"] == 3
    assert ledger["dest_count"] is None
    assert ledger["inferred_deletes"] is None
    assert ledger["writer_ack"] == 10_000
    assert ledger["rows_written_source"] == DEST_ACTIVE_READBACK


def test_mirror_stream_path_this_run_census_is_dest_engine_transitions():
    """This-run inferred deletes are dest-engine transitions, not driver rowcount."""
    ledger = account_job(
        {
            "sync_mode": "mirror",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "active-digest",
            },
            "destination_summary": {
                "sync_mode": "mirror",
                "active_rows": 3,
                "active_checksum": "active-digest",
                "soft_delete_column": "_deleted",
                "soft_deleted": 1,
                "reactivated": 0,
            },
        }
    ).to_dict()
    assert ledger["conservation_kind"] == KIND_MIRROR
    assert ledger["inferred_deletes"] == 1
    assert ledger["reactivated"] == 0
    assert "this run inferred 1 delete" in ledger["note"].lower()


def test_stream_scd2_top_level_active_is_not_a_mirror_payload():
    from services.row_conservation import extract_mirror_payload

    assert extract_mirror_payload(
        {
            "sync_mode": "scd2",
            "active_rows": 2,
            "active_checksum": "current-digest",
        }
    ) == {}


def test_mirror_without_active_census_is_unmeasured():
    ledger = account_job(
        {
            "sync_mode": "mirror",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "stuffed",
            },
            "destination_summary": {},
        }
    ).to_dict()
    assert ledger["conservation_kind"] == KIND_MIRROR
    assert ledger["balanced"] is False
    assert ledger["rows_written"] is None
    assert ledger["rows_written_source"] == DEST_UNMEASURED
    assert ledger["active_count"] is None


def test_accumulator_redelivery_of_same_key_is_not_a_second_insert():
    from services.row_conservation import KeyCensusAccumulator

    acc = KeyCensusAccumulator()
    acc.add_events(5)
    acc.add_batch([(1,)], dest_hits=0)
    acc.add_events(5)
    acc.add_batch([(1,)], dest_hits=0)
    census = acc.to_census()
    assert census is not None
    assert census.unique_batch_keys == 1
    assert census.inserts == 1
    assert census.dest_preexisting == 0
    assert census.events_read == 10
    assert census.expected_delta == 1


def test_keyed_ledger_closes_on_keys_not_duplicate_events():
    from services.row_conservation import KeyCensus

    census = KeyCensus(
        unique_batch_keys=3,
        dest_preexisting=3,
        events_read=10,
    )
    ledger = account_population(
        rows_read=10,
        dest_count=30,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="cdc",
        census=census,
    )
    assert ledger.balanced is True
    assert ledger.dest_delta == 0
    assert ledger.inserts == 0
    assert ledger.updates == 3
    assert ledger.events_read == 10
    assert ledger.unique_batch_keys == 3
    assert ledger.writer_ack == 10_000
    assert "10 event" in ledger.note
    assert "3 live key" in ledger.note


def test_sqlite_duplicate_events_census_is_keys_not_rowcount(tmp_path: Path):
    import sqlite3

    from services.row_conservation import prepare_keyed_upsert

    path = tmp_path / "p9_events.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        conn.commit()
    finally:
        conn.close()

    live, payload = prepare_keyed_upsert(
        [
            {"id": 1, "label": "A"},
            {"id": 1, "label": "A2"},
            {"id": 2, "label": "B"},
            {"id": 2, "label": "B2"},
            {"id": 3, "label": "C"},
            {"id": 3, "label": "C2"},
        ],
        key_columns=["id"],
        mappings=None,
        db_type="sqlite",
        cfg={"database": str(path)},
        schema="",
        table_name="items",
        dest_nonempty=True,
    )
    assert payload is not None
    assert payload["events_read"] == 6
    assert payload["unique_batch_keys"] == 3
    assert payload["dest_preexisting"] == 3
    assert payload["inserts"] == 0
    assert payload["expected_delta"] == 0
    assert len(live) == 3

    from services.dest_precount import PRECOUNT_KEY

    ledger = account_job(
        {
            "sync_mode": "cdc",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 6,
                "target_rows": 3,
                "target_checksum": "dest-digest",
                PRECOUNT_KEY: 3,
            },
            "destination_summary": {
                PRECOUNT_KEY: 3,
                "keyed_census": payload,
            },
        }
    )
    assert ledger.conservation_kind == KIND_KEYED
    assert ledger.balanced is True
    assert ledger.events_read == 6
    assert ledger.unique_batch_keys == 3
    assert ledger.dest_delta == 0
    assert ledger.writer_ack == 10_000


def _overwrite_stream(name: str, dest_count: int, *, writer_ack: int | None = None) -> dict:
    ack = dest_count if writer_ack is None else writer_ack
    return {
        "name": name,
        "records_processed": ack,
        "row_accounting": account_job(
            {
                "records_processed": ack,
                "sync_mode": "overwrite",
                "reconciliation": {
                    "source_rows": dest_count,
                    "target_rows": dest_count,
                    "target_checksum": f"digest-{name}",
                    "phase": "post_write_row_count",
                    "coverage": "row_count",
                },
            }
        ).to_dict(),
    }


def test_job_rollup_sums_overwrite_streams_not_last_table():
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "reconciliation": {
            "source_rows": 3,
            "target_rows": 3,
            "target_checksum": "last-table-only",
            "coverage": "row_count",
        },
        "destination_summary": {
            "streams": [
                _overwrite_stream("customers", 2, writer_ack=10_000),
                _overwrite_stream("orders", 3, writer_ack=10_000),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.dest_count == 5
    assert ledger.rows_read == 5
    assert ledger.balanced is True
    assert ledger.summable is True
    assert ledger.stream_count == 2
    assert ledger.measured_streams == 2
    assert ledger.writer_ack == 20_000
    assert ledger.writer_ack_delta == 5 - 20_000
    assert ledger.per_stream[0]["stream"] == "customers"
    assert ledger.per_stream[1]["dest_count"] == 3


def test_job_rollup_open_when_first_stream_unmeasured():
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "reconciliation": {
            "source_rows": 3,
            "target_rows": 3,
            "target_checksum": "last-table-only",
            "coverage": "row_count",
        },
        "destination_summary": {
            "streams": [
                {"name": "customers", "records_processed": 2},
                _overwrite_stream("orders", 3),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is False
    assert ledger.dest_count is None
    assert ledger.measured_streams == 1
    assert ledger.stream_count == 2


def test_job_rollup_open_when_first_stream_unbalanced():
    short = account_job(
        {
            "records_processed": 2,
            "sync_mode": "overwrite",
            "reconciliation": {
                "source_rows": 4,
                "target_rows": 2,
                "target_checksum": "short",
                "coverage": "row_count",
            },
        }
    ).to_dict()
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "destination_summary": {
            "streams": [
                {"name": "customers", "row_accounting": short},
                _overwrite_stream("orders", 3),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is False
    assert ledger.summable is True
    assert ledger.dest_count == 5


def test_job_rollup_does_not_sum_mixed_kinds():
    keyed = account_job(
        {
            "sync_mode": "cdc",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 4,
                "target_checksum": "k",
                "target_rows_before": 3,
            },
            "destination_summary": {
                "target_rows_before": 3,
                "keyed_census": {
                    "unique_batch_keys": 4,
                    "dest_preexisting": 3,
                    "tombstones": 0,
                    "unique_tombstone_keys": 0,
                    "events_read": 10,
                },
            },
        }
    ).to_dict()
    job = {
        "records_processed": 10_000,
        "destination_summary": {
            "streams": [
                _overwrite_stream("customers", 2),
                {"name": "orders", "row_accounting": keyed},
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is True
    assert ledger.summable is False
    assert ledger.dest_count is None
    assert ledger.rows_written_source == DEST_PER_STREAM


def test_single_stream_still_uses_table_identity():
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "reconciliation": {
            "source_rows": 4,
            "target_rows": 4,
            "target_checksum": "one",
            "coverage": "row_count",
        },
        "destination_summary": {
            "streams": [_overwrite_stream("items", 4)],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.dest_count == 4
    assert account_job_streams(job["destination_summary"]["streams"]) is None


def test_dest_before_census_counts_once_per_table(tmp_path: Path):
    """Second capture must not observe dest-after (that would close a false delta)."""
    import sqlite3

    from src.transfer.models import EndpointConfig
    from services.dest_precount import DestBeforeCensus, count_endpoint_rows

    path = tmp_path / "before.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO items (id) VALUES (1), (2), (3)")
        conn.commit()
    finally:
        conn.close()
    endpoint = EndpointConfig(kind="database", format="sqlite", database=str(path), table="items")
    census = DestBeforeCensus()
    first = census.capture(endpoint, table_name="items", aliases=("items_alias",))
    assert first == 3
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("INSERT INTO items (id) VALUES (4)")
        conn.commit()
    finally:
        conn.close()
    second = census.capture(endpoint, table_name="items")
    assert second == 3
    assert census.get("items_alias") == 3
    assert count_endpoint_rows(endpoint, table_name="items") == 4
    summary: dict = {}
    assert census.stamp(summary, "items")
    assert summary["target_rows_before"] == 3


def _iceberg_cfg(warehouse: Path) -> dict:
    return {
        "connection_string": str(warehouse),
        "database": str(warehouse),
        "host": "",
        "schema": "",
    }


def test_iceberg_missing_table_is_measured_zero(tmp_path: Path):
    """Create-on-first-write is dest-before 0, not unmeasured."""
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "iceberg", _iceberg_cfg(tmp_path / "wh"), schema="", table_name="orders"
    )
    assert n == 0


def test_iceberg_filesystem_dest_count_and_key_hits_independent_of_writer(tmp_path: Path):
    """Lakehouse MERGE conservation: dest snapshot COUNT and key hits, not upsert ack."""
    from connectors.iceberg_writer import write_mapped_rows
    from services.dest_precount import (
        DestBeforeCensus,
        count_endpoint_rows,
        destination_key_hits,
        destination_row_count,
    )
    from src.transfer.models import EndpointConfig

    warehouse = tmp_path / "wh"
    cfg = _iceberg_cfg(warehouse)
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
        {"source": "_df_lsn", "target": "_df_lsn", "transform": "direct"},
    ]
    first = write_mapped_rows(
        connection_string=str(warehouse),
        table_name="orders",
        headers=["id", "v", "_df_lsn"],
        data_rows=[["1", "a", "0/10"], ["2", "b", "0/10"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert first.ok, first.error
    assert destination_row_count("iceberg", cfg, schema="", table_name="orders") == 2
    assert (
        destination_key_hits(
            "iceberg",
            cfg,
            schema="",
            table_name="orders",
            key_columns=["id"],
            keys=[("1",), ("9",)],
        )
        == 1
    )

    endpoint = EndpointConfig(
        kind="database",
        format="iceberg",
        connection_string=str(warehouse),
        database=str(warehouse),
        table="orders",
    )
    census = DestBeforeCensus()
    before = census.capture(endpoint, table_name="orders")
    assert before == 2
    second = write_mapped_rows(
        connection_string=str(warehouse),
        table_name="orders",
        headers=["id", "v", "_df_lsn"],
        data_rows=[["1", "a2", "0/20"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert second.ok, second.error
    assert census.capture(endpoint, table_name="orders") == 2
    assert count_endpoint_rows(endpoint, table_name="orders") == 2
    summary: dict = {}
    census.stamp(summary, "orders")
    assert summary["target_rows_before"] == 2


def test_write_destination_database_stamps_iceberg_dest_before(tmp_path: Path):
    """Adapters precount uses dest-engine Iceberg COUNT — missing table is 0."""
    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig

    warehouse = tmp_path / "wh"
    dest = EndpointConfig(
        kind="database",
        format="iceberg",
        database=str(warehouse),
        table="orders",
        connection_string=str(warehouse),
    )
    records = [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}]
    columns = ["id", "v"]
    mappings = [{"source": c, "target": c} for c in columns]
    schema = {"id": "string", "v": "string"}
    written, _ddl, summary = write_destination_database(
        dest, records, columns, schema, mappings
    )
    assert written == 2, summary
    assert summary.get("target_rows_before") == 0
    written2, _ddl2, summary2 = write_destination_database(
        dest, records, columns, schema, mappings
    )
    assert written2 == 2
    assert summary2.get("target_rows_before") == 2


def test_s3_missing_object_is_measured_zero():
    """Missing object is dest-before 0 (create-on-first-write), not writer ack."""
    moto = pytest.importorskip("moto")
    import boto3

    from services.dest_precount import destination_row_count

    with moto.mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="df-count")
        n = destination_row_count(
            "s3",
            {"database": "df-count", "host": "us-east-1"},
            schema="",
            table_name="exports/missing.json",
        )
        assert n == 0
        boto3.client("s3", region_name="us-east-1").put_object(
            Bucket="df-count",
            Key="exports/data.json",
            Body=b'[{"id":1},{"id":2}]',
        )
        assert (
            destination_row_count(
                "s3",
                {"database": "df-count", "host": "us-east-1"},
                schema="",
                table_name="exports/data.json",
            )
            == 2
        )


def test_object_store_dest_count_live_get(local_object_store: str):
    """Dest COUNT GETs bodies from a live S3 API, not a monkeypatched payload list."""
    if not local_object_store:
        pytest.skip(
            "no local object store endpoint (install moto or set DATAFLOW_TEST_S3_ENDPOINT)"
        )
    import boto3

    from tests.conftest import LOCAL_OBJECT_STORE_BUCKET

    client = boto3.client(
        "s3",
        endpoint_url=local_object_store,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    key = "proof/live_get.json"
    client.put_object(
        Bucket=LOCAL_OBJECT_STORE_BUCKET,
        Key=key,
        Body=b'[{"id":1},{"id":2},{"id":3}]',
    )
    cfg = {
        "database": LOCAL_OBJECT_STORE_BUCKET,
        "connection_string": local_object_store,
        "username": "test",
        "password": "test",
        "path_style": True,
    }
    assert destination_row_count("s3", cfg, schema="", table_name=key) == 3
    assert (
        destination_row_count("s3", cfg, schema="", table_name="proof/missing.json")
        == 0
    )


def _patch_object_store_payloads(
    monkeypatch: pytest.MonkeyPatch, payloads: list[tuple[str, bytes]] | None
) -> None:
    """List keys + open a stream per key. Never a list of GET bodies."""
    if payloads is None:
        monkeypatch.setattr(
            "services.dest_precount._object_store_list_keys",
            lambda *_a, **_k: None,
        )
        return
    bodies = {str(k): v for k, v in payloads}
    monkeypatch.setattr(
        "services.dest_precount._object_store_list_keys",
        lambda *_a, **_k: [str(k) for k, _ in payloads],
    )

    def _open(_kind: str, _cfg: dict, _bucket: str, key: str):
        if key not in bodies:
            return False
        buf = io.BytesIO(bodies[key])
        return buf, buf.close

    monkeypatch.setattr(
        "services.object_streaming.open_object_store_binary",
        _open,
    )


def test_object_store_parquet_count_is_footer_not_json_fallback_zero(
    monkeypatch: pytest.MonkeyPatch,
):
    """Parquet on S3 must not JSON-parse as [] and close overwrite as dest=0."""
    pytest.importorskip("pyarrow.parquet")
    from services.format_converter import convert_rows

    content, _mime = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="parquet",
    )
    _patch_object_store_payloads(monkeypatch, [("exports/data.parquet", content)])
    cfg = {"database": "df-count", "host": "us-east-1"}
    assert destination_row_count("s3", cfg, schema="", table_name="exports/data.parquet") == 2
    assert (
        destination_row_count(
            "amazon_s3", cfg, schema="", table_name="exports/data.parquet"
        )
        == 2
    )
    _patch_object_store_payloads(monkeypatch, [("exports/data.parquet", b"not-parquet")])
    assert destination_row_count("s3", cfg, schema="", table_name="exports/data.parquet") is None


def test_object_store_excel_counts_value_rows_not_used_range(monkeypatch: pytest.MonkeyPatch):
    openpyxl = pytest.importorskip("openpyxl")
    import io

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["id", "name"])
    ws.append(["1", "a"])
    ws.append(["2", "b"])
    for r in range(5, 22):
        ws.cell(row=r, column=1).number_format = "0.00"
    buf = io.BytesIO()
    wb.save(buf)
    _patch_object_store_payloads(monkeypatch, [("exports/dump.xlsx", buf.getvalue())])
    n = destination_row_count(
        "s3",
        {"database": "df-count"},
        schema="",
        table_name="exports/dump.xlsx",
    )
    assert n == 2
    assert ws.max_row > 3


def test_object_store_avro_and_orc_use_artifact_count(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("fastavro")
    pytest.importorskip("pyarrow.orc")
    from services.format_converter import convert_rows

    avro, _ = convert_rows(
        ["id"], [["1"], ["2"], ["3"]], source_format="csv", target_format="avro"
    )
    _patch_object_store_payloads(monkeypatch, [("exports/data.avro", avro)])
    assert (
        destination_row_count("s3", {"database": "b"}, schema="", table_name="exports/data.avro")
        == 3
    )
    orc, _ = convert_rows(["id"], [["1"], ["2"]], source_format="csv", target_format="orc")
    _patch_object_store_payloads(monkeypatch, [("exports/data.orc", orc)])
    assert (
        destination_row_count("gcs", {"database": "b"}, schema="", table_name="exports/data.orc")
        == 2
    )


def test_object_store_unparseable_part_does_not_sum_prefix(monkeypatch: pytest.MonkeyPatch):
    """Truncated listing is unmeasured — never CSV 2 + garbage 0."""
    _patch_object_store_payloads(
        monkeypatch,
        [
            ("exports/part-000.csv", b"id\n1\n2\n"),
            ("exports/part-001.parquet", b"not-parquet"),
        ],
    )
    assert (
        destination_row_count(
            "s3", {"database": "b"}, schema="", table_name="exports/data"
        )
        is None
    )


def test_object_store_xml_counts_record_path_not_json_empty(monkeypatch: pytest.MonkeyPatch):
    """S3 XML GET uses the same record-path COUNT as a local file. Never JSON []."""
    from services.format_converter import convert_rows

    content, _mime = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="xml",
    )
    _patch_object_store_payloads(monkeypatch, [("exports/data.xml", content)])
    assert (
        destination_row_count("s3", {"database": "b"}, schema="", table_name="exports/data.xml")
        == 2
    )
    _patch_object_store_payloads(
        monkeypatch, [("exports/data.xml", b"<not><well></formed>")]
    )
    assert (
        destination_row_count("s3", {"database": "b"}, schema="", table_name="exports/data.xml")
        is None
    )


class _NoSlurpGet(io.BytesIO):
    """Compressed GET body. ``read()`` without a size is the boto3 slurp."""

    def read(self, size: int | None = -1) -> bytes:
        if size is None or size < 0:
            raise AssertionError("object-store dest COUNT must not Body.read() the object")
        return super().read(size)


def test_object_store_gzip_csv_get_does_not_slurp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S3/GCS/ADLS dest COUNT streams gzip CSV. Never Body.read() of the GET.

    DMS S3 validation is Athena once/day and Parquet-only. Airbyte/Fivetran
    close S3 on PUT bytes. Spark feeds GzipCodec the GET stream. Our wedge
    is independent RFC 4180 COUNT of the object we wrote, one key at a
    time, without holding the compressed body as a second copy.
    """
    from services.format_converter import convert_rows

    csv_body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"], ["3", "c"]],
        source_format="csv",
        target_format="csv",
    )
    compressed = gzip.compress(csv_body)
    quoted, _ = convert_rows(
        ["id", "note"],
        [["1", "hello\nworld"], ["2", "b"]],
        source_format="csv",
        target_format="csv",
    )
    quoted_gz = gzip.compress(quoted)

    monkeypatch.setattr(
        "services.dest_precount._object_store_list_keys",
        lambda *_a, **_k: ["exports/data.csv.gz"],
    )

    def _open(_kind: str, _cfg: dict, _bucket: str, key: str):
        payload = quoted_gz if "quoted" in key else compressed
        buf = _NoSlurpGet(payload)
        return buf, buf.close

    monkeypatch.setattr("services.object_streaming.open_object_store_binary", _open)
    assert (
        destination_row_count(
            "s3", {"database": "b"}, schema="", table_name="exports/data.csv.gz"
        )
        == 3
    )
    monkeypatch.setattr(
        "services.dest_precount._object_store_list_keys",
        lambda *_a, **_k: ["exports/quoted.csv.gz"],
    )
    assert (
        destination_row_count(
            "s3", {"database": "b"}, schema="", table_name="exports/quoted.csv.gz"
        )
        == 2
    )


def test_chunk_reader_csv_count_is_one_object_not_concatenated() -> None:
    """ADLS ``chunks()`` is a file-like COUNT source, not ``b''.join(chunks)``."""
    from services.csv_profiler import count_csv_rows
    from services.object_streaming import _ChunkReader

    body = b"id,v\n1,a\n2,b\n3,c\n"
    stream = _ChunkReader(body[i : i + 5] for i in range(0, len(body), 5))
    assert count_csv_rows(io.BufferedReader(stream)) == 3


def test_job_rollup_two_keyed_streams_closed_not_summed():
    """Keyed dest COUNT(*) is not additive. Job dest stays per-stream."""
    def _keyed(name: str, *, before: int, after: int, inserts: int, updates: int) -> dict:
        return {
            "name": name,
            "row_accounting": account_job(
                {
                    "sync_mode": "cdc",
                    "records_processed": 10_000,
                    "reconciliation": {
                        "source_rows": inserts + updates,
                        "target_rows": after,
                        "target_checksum": f"k-{name}",
                        "target_rows_before": before,
                    },
                    "destination_summary": {
                        "target_rows_before": before,
                        "keyed_census": {
                            "unique_batch_keys": inserts + updates,
                            "dest_preexisting": updates,
                            "tombstones": 0,
                            "unique_tombstone_keys": 0,
                            "events_read": inserts + updates,
                        },
                    },
                }
            ).to_dict(),
        }

    job = {
        "records_processed": 10_000,
        "sync_mode": "cdc",
        "destination_summary": {
            "streams": [
                _keyed("customers", before=2, after=3, inserts=1, updates=2),
                _keyed("orders", before=3, after=3, inserts=0, updates=3),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is True
    assert ledger.summable is False
    assert ledger.dest_count is None
    assert ledger.rows_written_source == DEST_PER_STREAM
    assert ledger.per_stream[0]["conservation_kind"] == KIND_KEYED
    assert ledger.per_stream[1]["conservation_kind"] == KIND_KEYED
    assert ledger.per_stream[0]["dest_count"] == 3
    assert ledger.per_stream[1]["dest_count"] == 3


def test_job_rollup_two_vector_streams_sums_identities_not_chunks():
    def _vector(name: str, identities: int, vectors: int) -> dict:
        return {
            "name": name,
            "row_accounting": account_population(
                rows_read=identities,
                dest_count=identities,
                dest_count_source=DEST_IDENTITY_READBACK,
                dest_count_before=0,
                rejected_rows=0,
                coerced_null_rows=0,
                rows_skipped=0,
                writer_ack=vectors * 100,
                sync_mode="full_refresh_overwrite",
                vector={"identity_rows": identities, "vector_rows": vectors},
            ).to_dict(),
        }

    job = {
        "records_processed": 10_000,
        "destination_summary": {
            "streams": [
                _vector("docs_a", 2, 5),
                _vector("docs_b", 3, 9),
            ],
        },
    }
    ledger = account_job(job)
    assert ledger.conservation_kind == KIND_JOB
    assert ledger.balanced is True
    assert ledger.summable is True
    assert ledger.dest_count == 5
    assert ledger.rows_written == 5
    assert ledger.rows_written_source == DEST_IDENTITY_READBACK
    assert ledger.per_stream[0]["dest_count"] == 2
    assert ledger.per_stream[1]["dest_count"] == 3


def test_account_job_vector_recon_never_uses_writer_or_chunk_count():
    ledger = account_job(
        {
            "sync_mode": "full_refresh_overwrite",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 2,
                "target_rows": 5,
                "target_checksum": "chunks",
                "skipped_readback": True,
                "dest_count_source": DEST_IDENTITY_READBACK,
                IDENTITY_COUNT_KEY: 2,
                VECTOR_ROWS_KEY: 5,
                "target_rows_before": 0,
            },
        }
    )
    assert ledger.conservation_kind == KIND_VECTOR
    assert ledger.dest_count == 2
    assert ledger.identity_count == 2
    assert ledger.vector_rows == 5
    assert ledger.writer_ack == 10_000
    assert ledger.balanced is True


def _pg_up() -> bool:
    import socket

    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=0.4):
            return True
    except OSError:
        return False


def _pg_cfg() -> dict:
    import os

    return {
        "host": os.environ.get("P9_PG_HOST", "127.0.0.1"),
        "port": int(os.environ.get("P9_PG_PORT", "5432")),
        "database": os.environ.get("P9_PG_DB", "dataflow"),
        "username": os.environ.get("P9_PG_USER", "dataflow"),
        "password": os.environ.get("P9_PG_PASSWORD", "dataflow"),
    }


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
def test_pgvector_identity_count_distinct_source_id_not_physical_rows():
    """Identity COUNT does not require the vector extension — source_id is TEXT."""
    import psycopg2

    from services.dest_precount import DestBeforeCensus, destination_row_count, stamp_vector_census
    from src.transfer.models import EndpointConfig

    cfg = _pg_cfg()
    table = "p9_vector_identity_chunks"
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["username"],
        password=cfg["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
            cur.execute(
                f"CREATE TABLE public.{table} (id TEXT PRIMARY KEY, source_id TEXT, chunk_index INT)"
            )
            cur.executemany(
                f"INSERT INTO public.{table} (id, source_id, chunk_index) VALUES (%s, %s, %s)",
                [
                    ("d1-0", "doc-1", 0),
                    ("d1-1", "doc-1", 1),
                    ("d1-2", "doc-1", 2),
                    ("d2-0", "doc-2", 0),
                    ("d2-1", "doc-2", 1),
                ],
            )
        conn.commit()
        assert destination_row_count("pgvector", cfg, schema="public", table_name=table) == 2
        assert destination_row_count("postgresql", cfg, schema="public", table_name=table) == 5
        missing = destination_row_count(
            "pgvector", cfg, schema="public", table_name="p9_vector_identity_missing"
        )
        assert missing == 0

        stamped = stamp_vector_census(
            {"target_rows": 10_000, "target_checksum": "writer"},
            cfg,
            schema="public",
            table_name=table,
            dest_engine="pgvector",
        )
        assert stamped[IDENTITY_COUNT_KEY] == 2
        assert stamped["dest_count_source"] == DEST_IDENTITY_READBACK
        assert stamped[VECTOR_ROWS_KEY] == 10_000
        assert stamped["target_rows"] == 10_000
        count, source = dest_count_from_recon(stamped)
        assert count == 2
        assert source == DEST_IDENTITY_READBACK

        endpoint = EndpointConfig(
            kind="database",
            format="pgvector",
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            schema="public",
            table=table,
        )
        census = DestBeforeCensus()
        before = census.capture(endpoint, table_name=table)
        assert before == 2
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO public.{table} (id, source_id, chunk_index) VALUES (%s, %s, %s)",
                ("d3-0", "doc-3", 0),
            )
        conn.commit()
        assert census.capture(endpoint, table_name=table) == 2
        assert destination_row_count("pgvector", cfg, schema="public", table_name=table) == 3
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
        conn.commit()
        conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
def test_pgvector_table_without_source_id_is_unmeasured_not_physical_count():
    import psycopg2

    from services.dest_precount import destination_row_count

    cfg = _pg_cfg()
    table = "p9_vector_no_source_id"
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["username"],
        password=cfg["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
            cur.execute(f"CREATE TABLE public.{table} (id TEXT PRIMARY KEY, body TEXT)")
            cur.execute(f"INSERT INTO public.{table} (id, body) VALUES ('a', 'x'), ('b', 'y')")
        conn.commit()
        assert destination_row_count("pgvector", cfg, schema="public", table_name=table) is None
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
        conn.commit()
        conn.close()


def test_sqlite_keyset_census_splits_missing_from_extra_target(tmp_path: Path):
    """Dest {2,3,99} vs source {1,2,3}: COUNT(*)=3, missing=1, extra=1."""
    import sqlite3

    path = tmp_path / "p9_keyset.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(2, "b"), (3, "c"), (99, "ghost")],
        )
        conn.commit()
    finally:
        conn.close()
    cfg = {"database": str(path)}
    census = destination_keyset_census(
        "sqlite",
        cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert census is not None
    assert census["dest_count"] == 3
    assert census["dest_key_hits"] == 2
    assert census[MISSING_KEYS_KEY] == 1
    assert census[EXTRA_KEYS_KEY] == 1

    stamped = stamp_keyset_census(
        {"target_rows": 3, "target_checksum": "same-count"},
        cfg,
        schema="",
        table_name="items",
        dest_engine="sqlite",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert stamped[MISSING_KEYS_KEY] == 1
    assert stamped[EXTRA_KEYS_KEY] == 1
    ledger = account_job(
        {
            "sync_mode": "full_refresh_overwrite",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "same-count",
                MISSING_KEYS_KEY: 1,
                EXTRA_KEYS_KEY: 1,
            },
        }
    )
    assert ledger.balanced is False
    assert ledger.unaccounted == 0
    assert ledger.missing_keys == 1
    assert ledger.extra_keys == 1
    assert ledger.writer_ack == 10_000


def test_inferred_leftover_delete_refuses_incomplete_snapshot(tmp_path: Path):
    """Incremental CDC must not infer-delete dest keys the batch did not send."""
    import sqlite3

    path = tmp_path / "p9_no_infer.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        conn.commit()
    finally:
        conn.close()
    deleted = apply_inferred_leftover_deletes(
        db_type="sqlite",
        cfg={"database": str(path)},
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,)],
        complete_snapshot=False,
    )
    assert deleted is None
    assert destination_row_count(
        "sqlite", {"database": str(path)}, schema="", table_name="items"
    ) == 3


def test_inferred_leftover_delete_skips_pgvector():
    deleted = apply_inferred_leftover_deletes(
        db_type="pgvector",
        cfg={"host": "127.0.0.1"},
        schema="public",
        table_name="docs",
        key_columns=["id"],
        keys=[("a",), ("b",)],
        complete_snapshot=True,
    )
    assert deleted is None


def test_overwrite_merge_deletes_leftover_dest_keys_not_in_complete_s(tmp_path: Path):
    """Dest {1,2,3,99} vs S {1,2,3}: MERGE-delete 99. COUNT(*) becomes 3, extra=0.

    Fivetran would soft-flag 99 (_fivetran_deleted) so COUNT(*) stays 4.
    Airbyte incremental would leave 99. DMS EXTRA_TARGET measures 99.
    Complete overwrite snapshot hard-deletes dest \\ S, then proves extra=0.
    """
    import sqlite3

    path = tmp_path / "p9_leftover_merge.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c"), (99, "ghost")],
        )
        conn.commit()
    finally:
        conn.close()
    cfg = {"database": str(path)}
    listed = destination_key_list(
        "sqlite", cfg, schema="", table_name="items", key_columns=["id"]
    )
    assert listed is not None
    assert sorted(listed) == [(1,), (2,), (3,), (99,)]
    before = destination_keyset_census(
        "sqlite",
        cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert before is not None
    assert before["dest_count"] == 4
    assert before[EXTRA_KEYS_KEY] == 1
    assert before[MISSING_KEYS_KEY] == 0

    deleted = apply_inferred_leftover_deletes(
        db_type="sqlite",
        cfg=cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "sqlite",
        cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0
    leftover = destination_key_list(
        "sqlite", cfg, schema="", table_name="gone", key_columns=["id"]
    )
    assert leftover == []

    ledger = account_job(
        {
            "sync_mode": "full_refresh_overwrite",
            "records_processed": 10_000,
            "destination_summary": {"leftover_deleted": 1},
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 3,
                "target_checksum": "after-merge",
                MISSING_KEYS_KEY: 0,
                EXTRA_KEYS_KEY: 0,
                "leftover_deleted": 1,
            },
        }
    )
    assert ledger.balanced is True
    assert ledger.dest_count == 3
    assert ledger.extra_keys == 0
    assert ledger.missing_keys == 0
    assert ledger.leftover_deleted == 1
    assert ledger.writer_ack == 10_000
    assert "merge" in ledger.note.lower() or "leftover" in ledger.note.lower()


def test_overwrite_merge_does_not_invent_missing_source_keys(tmp_path: Path):
    """Dest {2,3,99} vs S {1,2,3}: delete 99, dest=2, missing=1 still unclosed."""
    import sqlite3

    path = tmp_path / "p9_leftover_missing.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(2, "b"), (3, "c"), (99, "ghost")],
        )
        conn.commit()
    finally:
        conn.close()
    cfg = {"database": str(path)}
    deleted = apply_inferred_leftover_deletes(
        db_type="sqlite",
        cfg=cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert deleted == 1
    census = destination_keyset_census(
        "sqlite",
        cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert census is not None
    assert census["dest_count"] == 2
    assert census[EXTRA_KEYS_KEY] == 0
    assert census[MISSING_KEYS_KEY] == 1
    ledger = account_job(
        {
            "sync_mode": "full_refresh_overwrite",
            "records_processed": 10_000,
            "reconciliation": {
                "source_rows": 3,
                "target_rows": 2,
                "target_checksum": "after-merge",
                MISSING_KEYS_KEY: 1,
                EXTRA_KEYS_KEY: 0,
                "leftover_deleted": 1,
            },
        }
    )
    assert ledger.balanced is False
    assert ledger.dest_count == 2
    assert ledger.missing_keys == 1
    assert ledger.extra_keys == 0
    assert ledger.leftover_deleted == 1
    assert ledger.unaccounted == 1


def test_iceberg_destination_key_list_missing_table_is_empty(tmp_path: Path):
    listed = destination_key_list(
        "iceberg",
        _iceberg_cfg(tmp_path / "wh"),
        schema="",
        table_name="orders",
        key_columns=["id"],
    )
    assert listed == []


def test_iceberg_overwrite_merge_deletes_leftover_snapshot_keys(tmp_path: Path):
    """Lakehouse leftover MERGE: dest {1,2,3,99} vs S {1,2,3} → CoW-delete 99.

    Same identity as SQL leftover MERGE. Metadata record-count and writer
    upsert ack never close. Incremental remains a hard no-op.
    """
    from connectors.iceberg_writer import write_mapped_rows
    from services.dest_precount import destination_keyset_census, destination_row_count

    warehouse = tmp_path / "wh"
    cfg = _iceberg_cfg(warehouse)
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    written = write_mapped_rows(
        connection_string=str(warehouse),
        table_name="orders",
        headers=["id", "v"],
        data_rows=[["1", "a"], ["2", "b"], ["3", "c"], ["99", "ghost"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert written.ok, written.error
    listed = destination_key_list(
        "iceberg", cfg, schema="", table_name="orders", key_columns=["id"]
    )
    assert listed is not None
    assert len(listed) == 4
    before = destination_keyset_census(
        "iceberg",
        cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
    )
    assert before is not None
    assert before["dest_count"] == 4
    assert before[EXTRA_KEYS_KEY] == 1
    assert before[MISSING_KEYS_KEY] == 0

    refused = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
        complete_snapshot=False,
    )
    assert refused is None
    assert destination_row_count("iceberg", cfg, schema="", table_name="orders") == 4

    deleted = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "iceberg",
        cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0
    assert destination_row_count("iceberg", cfg, schema="", table_name="orders") == 3


def test_iceberg_overwrite_merge_does_not_invent_missing_source_keys(tmp_path: Path):
    """Dest {2,3,99} vs S {1,2,3}: delete 99, dest=2, missing=1 still unclosed."""
    from connectors.iceberg_writer import write_mapped_rows
    from services.dest_precount import destination_keyset_census

    warehouse = tmp_path / "wh"
    cfg = _iceberg_cfg(warehouse)
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    written = write_mapped_rows(
        connection_string=str(warehouse),
        table_name="orders",
        headers=["id", "v"],
        data_rows=[["2", "b"], ["3", "c"], ["99", "ghost"]],
        mappings=mappings,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert written.ok, written.error
    deleted = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
        complete_snapshot=True,
    )
    assert deleted == 1
    census = destination_keyset_census(
        "iceberg",
        cfg,
        schema="",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
    )
    assert census is not None
    assert census["dest_count"] == 2
    assert census[EXTRA_KEYS_KEY] == 0
    assert census[MISSING_KEYS_KEY] == 1


def test_iceberg_dest_count_is_len_of_snapshot_population(monkeypatch):
    """Catalog and filesystem dest COUNT are |snapshot|, never scan().count()."""
    from services import dest_precount as dp

    monkeypatch.setattr(
        dp,
        "_iceberg_snapshot_rows",
        lambda *a, **k: [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
    )
    assert (
        dp.destination_row_count(
            "iceberg", {"type": "iceberg"}, schema="default", table_name="orders"
        )
        == 4
    )
    monkeypatch.setattr(dp, "_iceberg_snapshot_rows", lambda *a, **k: [])
    assert (
        dp.destination_row_count(
            "iceberg", {"type": "iceberg"}, schema="default", table_name="orders"
        )
        == 0
    )
    monkeypatch.setattr(dp, "_iceberg_snapshot_rows", lambda *a, **k: None)
    assert (
        dp.destination_row_count(
            "iceberg", {"type": "iceberg"}, schema="default", table_name="orders"
        )
        is None
    )


def _iceberg_sql_catalog(tmp_path: Path) -> tuple[str, str]:
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    return str(tmp_path / "wh"), f"sqlite:///{tmp_path / 'catalog.db'}"


def _iceberg_sql_cfg(warehouse: str, uri: str) -> dict:
    return {
        "type": "iceberg",
        "connection_string": uri,
        "warehouse": warehouse,
        "table": "orders",
        "schema": "default",
    }


def test_iceberg_sql_catalog_missing_table_is_measured_zero(tmp_path: Path):
    """Create-on-first-write for SqlCatalog is dest-before 0, not scan().count()."""
    warehouse, uri = _iceberg_sql_catalog(tmp_path)
    assert (
        destination_row_count(
            "iceberg",
            _iceberg_sql_cfg(warehouse, uri),
            schema="default",
            table_name="orders",
        )
        == 0
    )


def test_iceberg_sql_catalog_leftover_merge_deletes_extra_and_count_is_snapshot_len(
    tmp_path: Path,
):
    """SqlCatalog leftover MERGE: dest {1,2,3,99} vs S {1,2,3} → delete 99.

    Dest COUNT is len(snapshot rows), never pyiceberg scan().count()
    metadata. Incremental remains a hard no-op.
    """
    from connectors.iceberg_writer import write_mapped_rows
    from services.dest_precount import _iceberg_snapshot_rows, destination_keyset_census

    warehouse, uri = _iceberg_sql_catalog(tmp_path)
    cfg = _iceberg_sql_cfg(warehouse, uri)
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    written = write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name="default.orders",
        headers=["id", "v"],
        data_rows=[["1", "a"], ["2", "b"], ["3", "c"], ["99", "ghost"]],
        mappings=mappings,
        write_mode="append",
        create_table=True,
    )
    assert written.ok, written.error
    snapshot = _iceberg_snapshot_rows(
        cfg, schema="default", table_name="orders", cols=("id",)
    )
    assert snapshot is not None
    assert destination_row_count(
        "iceberg", cfg, schema="default", table_name="orders"
    ) == len(snapshot)
    before = destination_keyset_census(
        "iceberg",
        cfg,
        schema="default",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
    )
    assert before is not None
    assert before["dest_count"] == 4
    assert before[EXTRA_KEYS_KEY] == 1
    assert before[MISSING_KEYS_KEY] == 0

    refused = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="default",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
        complete_snapshot=False,
    )
    assert refused is None
    assert destination_row_count(
        "iceberg", cfg, schema="default", table_name="orders"
    ) == 4

    deleted = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="default",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "iceberg",
        cfg,
        schema="default",
        table_name="orders",
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0
    remaining = _iceberg_snapshot_rows(
        cfg, schema="default", table_name="orders", cols=("id",)
    )
    assert remaining is not None
    assert {str(row.get("id")) for row in remaining} == {"1", "2", "3"}
    assert destination_row_count(
        "iceberg", cfg, schema="default", table_name="orders"
    ) == len(remaining)


class _ScriptedWarehouseEngine:
    """In-process dest engine: COUNT(*) / SELECT pk / named-bind hits. No stats views."""

    def __init__(
        self,
        *,
        count: int = 0,
        rows: list[tuple] | None = None,
        error: BaseException | None = None,
        count_current: int | None = None,
        column_error: BaseException | None = None,
    ):
        self.count = count
        self.count_current = count_current
        self.rows = list(rows or [])
        self.error = error
        self.column_error = column_error
        self.sql: list[str] = []
        self.params: list[object] = []

    def connect(self):
        return self

    def dispose(self) -> None:
        return None

    def __enter__(self):
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    @staticmethod
    def _mentions_is_current(sql: str) -> bool:
        compact = (
            sql.upper()
            .replace("[", "")
            .replace("]", "")
            .replace('"', "")
            .replace("`", "")
        )
        return "IS_CURRENT" in compact

    def execute(self, stmt: object, params: object = None):
        from types import SimpleNamespace

        sql = str(stmt)
        self.sql.append(sql)
        self.params.append(params)
        if self.error is not None:
            raise self.error
        upper = sql.upper()
        if (
            "SYS.PARTITIONS" in upper
            or "DM_DB_PARTITION_STATS" in upper
            or "NUM_ROWS" in upper
            or "INFORMATION_SCHEMA" in upper
            or "__TABLES__" in upper
            or "TABLE_STORAGE" in upper
            or "SVV_TABLE_INFO" in upper
            or "STV_TBL_PERM" in upper
            or "SYSTEM.TABLES" in upper
        ):
            raise AssertionError(f"warehouse COUNT must not use stats views: {sql}")
        if self._mentions_is_current(sql):
            if self.column_error is not None:
                raise self.column_error
            n = self.count if self.count_current is None else self.count_current
            return SimpleNamespace(scalar=lambda: n, fetchall=lambda: [])
        if "COUNT(DISTINCT" in upper or "_DF_KEY_HITS" in upper:
            dest = {row[0] for row in self.rows}
            values = []
            if isinstance(params, dict):
                values = [v for k, v in params.items() if str(k).startswith("k")]
            hits = len(dest.intersection(values))
            return SimpleNamespace(scalar=lambda: hits, fetchall=lambda: [])
        if "COUNT(*)" in upper:
            return SimpleNamespace(scalar=lambda: self.count, fetchall=lambda: [])
        return SimpleNamespace(scalar=lambda: None, fetchall=lambda: list(self.rows))


def _patch_warehouse(monkeypatch: pytest.MonkeyPatch, engine: _ScriptedWarehouseEngine) -> _ScriptedWarehouseEngine:
    monkeypatch.setattr(
        "connectors.generic_sql.get_sqlalchemy_engine",
        lambda cfg: engine,
    )
    monkeypatch.setattr("services.engine_pool.release_engine", lambda eng: None)
    return engine


def test_sqlserver_dest_count_quotes_dbo_and_never_uses_partition_stats(monkeypatch: pytest.MonkeyPatch):
    """Azure SQL / SQL Server leftover MERGE listing needs [dbo].[table] COUNT(*)."""
    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(count=4, rows=[(1,), (2,), (3,), (99,)]),
    )
    cfg = {"host": "db.example", "username": "sa", "database": "app"}
    assert destination_row_count("azure_sql_database", cfg, schema="", table_name="items") == 4
    assert any("COUNT(*)" in sql.upper() for sql in engine.sql)
    assert any("[dbo].[items]" in sql for sql in engine.sql)
    assert all("sys.partitions" not in sql.lower() for sql in engine.sql)
    listed = destination_key_list(
        "sqlserver", cfg, schema="", table_name="items", key_columns=["id"]
    )
    assert listed is not None
    assert sorted(listed) == [(1,), (2,), (3,), (99,)]
    hits = destination_keyset_census(
        "amazon_rds_sql_server",
        cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert hits is not None
    assert hits["dest_count"] == 4
    assert hits[EXTRA_KEYS_KEY] == 1
    assert hits[MISSING_KEYS_KEY] == 0


def test_oracle_dest_count_folds_schema_and_missing_table_is_zero(monkeypatch: pytest.MonkeyPatch):
    from sqlalchemy.exc import ProgrammingError

    cfg = {"host": "db.example", "username": "app", "database": "ORCL"}
    missing = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=ProgrammingError("SELECT", {}, Exception("ORA-00942: table or view does not exist")),
        ),
    )
    assert destination_row_count("amazon_rds_oracle", cfg, schema="", table_name="orders") == 0
    assert any('"APP"."ORDERS"' in sql for sql in missing.sql)

    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(count=2, rows=[(10,), (20,)]),
    )
    assert destination_row_count("oracle_autonomous_warehouse", cfg, schema="hr", table_name="orders") == 2
    assert any('"HR"."ORDERS"' in sql for sql in engine.sql)
    listed = destination_key_list(
        "oracle", cfg, schema="hr", table_name="orders", key_columns=["id"]
    )
    assert listed == [(10,), (20,)]


def test_sqlserver_missing_table_is_zero_not_unmeasured(monkeypatch: pytest.MonkeyPatch):
    from sqlalchemy.exc import ProgrammingError

    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=ProgrammingError("SELECT", {}, Exception("Invalid object name 'dbo.fresh'")),
        ),
    )
    n = destination_row_count("sqlserver", {"host": "h"}, schema="dbo", table_name="fresh")
    assert n == 0
    assert engine.sql  # COUNT(*) was attempted, not skipped as unsupported


def test_sqlserver_login_failure_is_unmeasured_not_empty(monkeypatch: pytest.MonkeyPatch):
    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(error=RuntimeError("Login failed for user 'sa'")),
    )
    assert destination_row_count("sqlserver", {"host": "h"}, schema="dbo", table_name="items") is None
    assert engine.sql


def test_snowflake_dest_count_is_engine_count_not_information_schema(
    monkeypatch: pytest.MonkeyPatch,
):
    """Snowflake dest COUNT is SELECT COUNT(*), never INFORMATION_SCHEMA.ROW_COUNT."""
    from sqlalchemy.exc import ProgrammingError

    missing = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=ProgrammingError(
                "SELECT",
                {},
                Exception("SQL compilation error: Object 'T' does not exist or not authorized."),
            ),
        ),
    )
    assert destination_row_count("snowflake", {"host": "h"}, schema="PUBLIC", table_name="T") == 0
    assert any('"PUBLIC"."T"' in sql for sql in missing.sql)
    assert all("INFORMATION_SCHEMA" not in sql.upper() for sql in missing.sql)

    engine = _patch_warehouse(monkeypatch, _ScriptedWarehouseEngine(count=2, rows=[(1,), (2,)]))
    cfg = {"host": "h", "schema": "PUBLIC"}
    assert destination_row_count("snowflake", cfg, schema="PUBLIC", table_name="ORDERS") == 2
    assert any("COUNT(*)" in sql.upper() for sql in engine.sql)
    listed = destination_key_list(
        "snowflake", cfg, schema="PUBLIC", table_name="ORDERS", key_columns=["id"]
    )
    assert listed == [(1,), (2,)]


def test_snowflake_auth_failure_is_unmeasured_not_empty(monkeypatch: pytest.MonkeyPatch):
    _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(error=RuntimeError("250001: Failed to connect to DB. Incorrect username or password")),
    )
    assert destination_row_count("snowflake", {"host": "h"}, schema="PUBLIC", table_name="T") is None


def test_bigquery_dest_count_quotes_project_dataset_not_tables_row_count(
    monkeypatch: pytest.MonkeyPatch,
):
    from sqlalchemy.exc import ProgrammingError

    missing = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=ProgrammingError("SELECT", {}, Exception("Not found: Table proj:ds.fresh")),
        ),
    )
    cfg = {"project": "proj", "schema": "ds"}
    assert destination_row_count("bigquery", cfg, schema="ds", table_name="fresh") == 0
    assert any("`proj.ds.fresh`" in sql for sql in missing.sql)

    denied = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(error=RuntimeError("403 Access Denied")),
    )
    assert destination_row_count("bigquery", cfg, schema="ds", table_name="fresh") is None
    assert denied.sql


def test_databricks_missing_table_is_zero(monkeypatch: pytest.MonkeyPatch):
    from sqlalchemy.exc import ProgrammingError

    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=ProgrammingError("SELECT", {}, Exception("[TABLE_OR_VIEW_NOT_FOUND] The table or view `gone` cannot be found.")),
        ),
    )
    n = destination_row_count("databricks", {"host": "h"}, schema="default", table_name="gone")
    assert n == 0
    assert any("`default`.`gone`" in sql for sql in engine.sql)


def test_redshift_dest_count_is_engine_count_not_svv_table_info(
    monkeypatch: pytest.MonkeyPatch,
):
    """Redshift dest COUNT is SELECT COUNT(*), never SVV_TABLE_INFO.tbl_rows.

    AWS tbl_rows includes unvacuumed delete ghosts and misses Spectrum
    external tables. PG to_regclass is not a Redshift catalog.
    """
    from sqlalchemy.exc import ProgrammingError

    def _pg_conn_forbidden(**kwargs: object) -> object:
        raise AssertionError("redshift dest COUNT must not use postgresql_conn / to_regclass")

    monkeypatch.setattr("connectors.postgresql_conn.get_connection", _pg_conn_forbidden)
    missing = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=ProgrammingError(
                "SELECT",
                {},
                Exception('relation "public.gone" does not exist'),
            ),
        ),
    )
    assert destination_row_count("redshift", {"host": "h"}, schema="public", table_name="gone") == 0
    assert any('"public"."gone"' in sql for sql in missing.sql)
    assert all("to_regclass" not in sql.lower() for sql in missing.sql)
    assert all("SVV_TABLE_INFO" not in sql.upper() for sql in missing.sql)

    engine = _patch_warehouse(monkeypatch, _ScriptedWarehouseEngine(count=3, rows=[(1,), (2,), (99,)]))
    cfg = {"host": "h", "schema": "public"}
    assert destination_row_count("amazon_redshift", cfg, schema="public", table_name="orders") == 3
    listed = destination_key_list(
        "redshift_serverless", cfg, schema="public", table_name="orders", key_columns=["id"]
    )
    assert listed == [(1,), (2,), (99,)]
    census = destination_keyset_census(
        "redshift",
        cfg,
        schema="public",
        table_name="orders",
        key_columns=["id"],
        keys=[(1,), (2,)],
    )
    assert census is not None
    assert census[EXTRA_KEYS_KEY] == 1
    assert census[MISSING_KEYS_KEY] == 0


def test_redshift_permission_denied_is_unmeasured_not_empty(monkeypatch: pytest.MonkeyPatch):
    _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(error=RuntimeError('permission denied for relation "orders"')),
    )
    assert destination_row_count("redshift", {"host": "h"}, schema="public", table_name="orders") is None


def test_redshift_connect_failure_without_engine_is_unmeasured():
    """No reachable cluster: connect failure is unmeasured, not dest=0."""
    assert destination_row_count("redshift", {"host": "h"}, schema="public", table_name="T") is None


def test_clickhouse_dest_count_uses_final_not_system_tables(
    monkeypatch: pytest.MonkeyPatch,
):
    """ClickHouse dest COUNT is COUNT(*) FROM table FINAL, never total_rows.

    ReplacingMergeTree without FINAL overcounts at-least-once INSERT versions.
    Leftover MERGE stays unapplied — mutations are async and must not stamp
    leftover_deleted before COUNT(*) FINAL can see the delete.
    """
    from sqlalchemy.exc import ProgrammingError

    missing = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=ProgrammingError(
                "SELECT",
                {},
                Exception("Code: 60. DB::Exception: Table default.gone doesn't exist. (UNKNOWN_TABLE)"),
            ),
        ),
    )
    assert destination_row_count("clickhouse", {"host": "h"}, schema="default", table_name="gone") == 0
    assert any(" FINAL" in sql for sql in missing.sql)
    assert any("COUNT(*)" in sql.upper() for sql in missing.sql)
    assert all("system.tables" not in sql.lower() for sql in missing.sql)
    assert all("total_rows" not in sql.lower() for sql in missing.sql)

    engine = _patch_warehouse(monkeypatch, _ScriptedWarehouseEngine(count=3, rows=[(1,), (2,), (99,)]))
    cfg = {"host": "h", "schema": "default"}
    assert destination_row_count("clickhouse", cfg, schema="default", table_name="events") == 3
    assert any("`default`.`events` FINAL" in sql for sql in engine.sql)
    leftover = apply_inferred_leftover_deletes(
        db_type="clickhouse",
        cfg=cfg,
        schema="default",
        table_name="events",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert leftover is None


def test_clickhouse_unknown_database_is_unmeasured_not_empty(monkeypatch: pytest.MonkeyPatch):
    _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=RuntimeError("Code: 81. DB::Exception: Database analytics doesn't exist. (UNKNOWN_DATABASE)"),
        ),
    )
    assert destination_row_count("clickhouse", {"host": "h"}, schema="analytics", table_name="events") is None


def test_clickhouse_connect_failure_without_engine_is_unmeasured():
    assert destination_row_count("clickhouse", {"host": "h"}, schema="default", table_name="T") is None


def test_snowflake_connect_failure_without_engine_is_unmeasured():
    """No scripted dest: missing snowflake-sqlalchemy / account is unmeasured, not dest=0."""
    assert destination_row_count("snowflake", {"host": "h"}, schema="PUBLIC", table_name="T") is None


def test_duckdb_overwrite_leftover_merge_deletes_extra_and_dest_count(tmp_path: Path):
    """Live DuckDB: dest {1,2,3,99} vs S {1,2,3} → DELETE 99, COUNT(*)=3.

    Same warehouse COUNT(*) machine Snowflake/BQ use. Catalog stats do not exist.
    Incremental leftover MERGE stays a hard no-op.
    """
    pytest.importorskip("duckdb")
    from connectors.generic_sql import get_sqlalchemy_engine
    import sqlalchemy as sa

    path = str(tmp_path / "p9.duckdb")
    cfg = {"type": "duckdb", "database": path}
    try:
        engine = get_sqlalchemy_engine(cfg)
    except Exception as exc:
        pytest.skip(f"DuckDB engine unavailable: {exc}")
    with engine.connect() as conn:
        conn.execute(sa.text('CREATE TABLE "main"."orders" (id INTEGER PRIMARY KEY, v VARCHAR)'))
        conn.execute(
            sa.text("INSERT INTO \"main\".\"orders\" VALUES (1, 'a'), (2, 'b'), (3, 'c'), (99, 'ghost')")
        )
        conn.commit()
    assert destination_row_count("duckdb", cfg, schema="main", table_name="orders") == 4
    assert destination_row_count("duckdb", cfg, schema="main", table_name="gone") == 0
    refused = apply_inferred_leftover_deletes(
        db_type="duckdb",
        cfg=cfg,
        schema="main",
        table_name="orders",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=False,
    )
    assert refused is None
    assert destination_row_count("duckdb", cfg, schema="main", table_name="orders") == 4
    deleted = apply_inferred_leftover_deletes(
        db_type="duckdb",
        cfg=cfg,
        schema="main",
        table_name="orders",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "duckdb",
        cfg,
        schema="main",
        table_name="orders",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
def test_redshift_pg_wire_standin_leftover_merge_uses_count_star():
    """PG-wire stand-in (not a live Redshift cluster): leftover MERGE + COUNT(*).

    Real Redshift has no to_regclass; this proves the warehouse COUNT(*)
    machine against a PG-wire engine. SVV_TABLE_INFO is never consulted.
    Incremental leftover MERGE stays a hard no-op.
    """
    from connectors.generic_sql import get_sqlalchemy_engine
    import sqlalchemy as sa

    cfg = {**_pg_cfg(), "type": "redshift"}
    table = "df_p9_redshift_leftover"
    try:
        engine = get_sqlalchemy_engine(cfg)
    except Exception as exc:
        pytest.skip(f"Redshift PG-wire engine unavailable: {exc}")
    with engine.begin() as conn:
        conn.execute(sa.text(f'DROP TABLE IF EXISTS "public"."{table}"'))
        conn.execute(
            sa.text(
                f'CREATE TABLE "public"."{table}" (id INTEGER PRIMARY KEY, v VARCHAR)'
            )
        )
        conn.execute(
            sa.text(
                f"INSERT INTO \"public\".\"{table}\" VALUES (1, 'a'), (2, 'b'), (3, 'c'), (99, 'ghost')"
            )
        )
    try:
        assert destination_row_count("redshift", cfg, schema="public", table_name=table) == 4
        assert destination_row_count("redshift", cfg, schema="public", table_name="df_p9_redshift_gone") == 0
        refused = apply_inferred_leftover_deletes(
            db_type="redshift",
            cfg=cfg,
            schema="public",
            table_name=table,
            key_columns=["id"],
            keys=[(1,), (2,), (3,)],
            complete_snapshot=False,
        )
        assert refused is None
        assert destination_row_count("redshift", cfg, schema="public", table_name=table) == 4
        deleted = apply_inferred_leftover_deletes(
            db_type="redshift",
            cfg=cfg,
            schema="public",
            table_name=table,
            key_columns=["id"],
            keys=[(1,), (2,), (3,)],
            complete_snapshot=True,
        )
        assert deleted == 1
        after = destination_keyset_census(
            "redshift",
            cfg,
            schema="public",
            table_name=table,
            key_columns=["id"],
            keys=[(1,), (2,), (3,)],
        )
        assert after is not None
        assert after["dest_count"] == 3
        assert after[EXTRA_KEYS_KEY] == 0
        assert after[MISSING_KEYS_KEY] == 0
    finally:
        with engine.begin() as conn:
            conn.execute(sa.text(f'DROP TABLE IF EXISTS "public"."{table}"'))


def test_oracle_composite_key_hits_use_and_or_not_tuple_in(monkeypatch: pytest.MonkeyPatch):
    """Oracle 19c has no row-value IN; leftover MERGE composite hits must be AND/OR."""
    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(count=2, rows=[(1, "a"), (2, "b")]),
    )
    cfg = {"username": "app"}
    census = destination_keyset_census(
        "oracle",
        cfg,
        schema="app",
        table_name="pair",
        key_columns=["id", "kind"],
        keys=[(1, "a"), (2, "b")],
    )
    assert census is not None
    assert census["dest_count"] == 2
    hit_sql = [sql for sql in engine.sql if "_df_key_hits" in sql or "_DF_KEY_HITS" in sql.upper()]
    assert hit_sql
    compact = hit_sql[0].replace(" ", "")
    assert "IN((" not in compact
    assert " = :k0_0" in hit_sql[0]
    assert " AND " in hit_sql[0]


def test_azure_sql_leftover_merge_deletes_keys_not_in_complete_s(monkeypatch: pytest.MonkeyPatch):
    """Catalog SKU azure_sql_database must apply leftover = D \\ S, not return unapplied."""
    monkeypatch.setattr(
        "services.dest_precount.destination_key_list",
        lambda *a, **k: [(1,), (2,), (3,), (99,)],
    )
    deleted: list[str] = []

    def _delete(**kwargs: object) -> int:
        deleted.extend(list(kwargs["keys"]))  # type: ignore[arg-type]
        return len(kwargs["keys"])  # type: ignore[arg-type]

    monkeypatch.setattr("connectors.table_manager.delete_by_primary_keys", _delete)
    n = apply_inferred_leftover_deletes(
        db_type="azure_sql_database",
        cfg={"host": "db.example"},
        schema="dbo",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert n == 1
    assert deleted == ["99"]
    refused = apply_inferred_leftover_deletes(
        db_type="azure_sql_database",
        cfg={"host": "db.example"},
        schema="dbo",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=False,
    )
    assert refused is None


def test_amazon_rds_oracle_leftover_merge_routes_delete(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "services.dest_precount.destination_key_list",
        lambda *a, **k: [(1,), (99,)],
    )
    seen: dict[str, object] = {}

    def _delete(**kwargs: object) -> int:
        seen.update(kwargs)
        return 1

    monkeypatch.setattr("connectors.table_manager.delete_by_primary_keys", _delete)
    n = apply_inferred_leftover_deletes(
        db_type="amazon_rds_oracle",
        cfg={"username": "app"},
        schema="APP",
        table_name="ORDERS",
        key_columns=["id"],
        keys=[(1,)],
        complete_snapshot=True,
    )
    assert n == 1
    assert seen["db_type"] == "amazon_rds_oracle"
    assert seen["keys"] == ["99"]


@pytest.mark.parametrize(
    "engine",
    ["snowflake", "bigquery", "databricks", "motherduck", "redshift", "amazon_redshift"],
)
def test_cloud_warehouse_leftover_merge_routes_delete(
    monkeypatch: pytest.MonkeyPatch, engine: str
):
    """Warehouse leftover MERGE is dest-engine DELETE, never catalog stats."""
    monkeypatch.setattr(
        "services.dest_precount.destination_key_list",
        lambda *a, **k: [(1,), (99,)],
    )
    seen: dict[str, object] = {}

    def _delete(**kwargs: object) -> int:
        seen.update(kwargs)
        return 1

    monkeypatch.setattr("connectors.table_manager.delete_by_primary_keys", _delete)
    n = apply_inferred_leftover_deletes(
        db_type=engine,
        cfg={"host": "h"},
        schema="PUBLIC",
        table_name="ORDERS",
        key_columns=["id"],
        keys=[(1,)],
        complete_snapshot=True,
    )
    assert n == 1
    assert seen["db_type"] == engine
    assert seen["keys"] == ["99"]
    refused = apply_inferred_leftover_deletes(
        db_type=engine,
        cfg={"host": "h"},
        schema="PUBLIC",
        table_name="ORDERS",
        key_columns=["id"],
        keys=[(1,)],
        complete_snapshot=False,
    )
    assert refused is None


def test_sqlserver_live_leftover_merge_when_reachable():
    """Live SQL Server: dest {1,2,3,99} vs S {1,2,3} → DELETE 99, extra=0.

    Skip when :1433 does not answer or the driver cannot authenticate. Never
    invent green. COUNT(*) from dest-engine, never sys.partitions.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError:
        pytest.skip("SQL Server not listening on 1433")

    cfg = {
        "type": "sqlserver",
        "host": "127.0.0.1",
        "port": 1433,
        "database": "dataflow",
        "username": "sa",
        "password": "Datawrap_CDC_2022!",
        "schema": "dbo",
    }
    table = "df_p9_leftover_merge"
    try:
        from connectors.generic_sql import get_sqlalchemy_engine
        import sqlalchemy as sa

        engine = get_sqlalchemy_engine(cfg)
    except Exception as exc:
        pytest.skip(f"SQL Server engine unavailable: {exc}")
    try:
        with engine.connect() as conn:
            conn.execute(sa.text(f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}"))
            conn.execute(
                sa.text(f"CREATE TABLE dbo.{table} (id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)")
            )
            conn.execute(
                sa.text(f"INSERT INTO dbo.{table} (id, label) VALUES (1, N'a'), (2, N'b'), (3, N'c'), (99, N'ghost')")
            )
            conn.commit()
    except Exception as exc:
        pytest.skip(f"SQL Server setup failed: {exc}")

    assert destination_row_count("sqlserver", cfg, schema="dbo", table_name=table) == 4
    listed = destination_key_list(
        "sqlserver", cfg, schema="dbo", table_name=table, key_columns=["id"]
    )
    assert listed is not None
    assert sorted(listed) == [(1,), (2,), (3,), (99,)]
    deleted = apply_inferred_leftover_deletes(
        db_type="sqlserver",
        cfg=cfg,
        schema="dbo",
        table_name=table,
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "sqlserver",
        cfg,
        schema="dbo",
        table_name=table,
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0
    try:
        with engine.connect() as conn:
            conn.execute(sa.text(f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}"))
            conn.commit()
    except Exception:
        pass


def test_oracle_live_leftover_merge_when_reachable():
    """Live Oracle: same leftover identity. Skip when :1521 does not answer."""
    import os
    import socket

    host = os.environ.get("DATAFLOW_ORACLE_HOST", "127.0.0.1")
    port = int(os.environ.get("DATAFLOW_ORACLE_PORT", "1521"))
    try:
        socket.create_connection((host, port), timeout=1).close()
    except OSError:
        pytest.skip(f"Oracle not listening on {host}:{port}")

    cfg = {
        "type": "oracle",
        "host": host,
        "port": port,
        "database": os.environ.get("DATAFLOW_ORACLE_SERVICE", "ORCL"),
        "username": os.environ.get("DATAFLOW_ORACLE_USER", "system"),
        "password": os.environ.get("DATAFLOW_ORACLE_PASSWORD", ""),
        "schema": "",
    }
    if not cfg["password"]:
        pytest.skip("Oracle password not configured")
    table = "DF_P9_LEFTOVER"
    try:
        from connectors.generic_sql import get_sqlalchemy_engine
        import sqlalchemy as sa

        engine = get_sqlalchemy_engine(cfg)
        with engine.connect() as conn:
            conn.execute(sa.text(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"))
            conn.execute(sa.text(f'CREATE TABLE "{table}" (id NUMBER PRIMARY KEY, label VARCHAR2(32))'))
            conn.execute(sa.text(f"INSERT INTO \"{table}\" (id, label) VALUES (1, 'a')"))
            conn.execute(sa.text(f"INSERT INTO \"{table}\" (id, label) VALUES (2, 'b')"))
            conn.execute(sa.text(f"INSERT INTO \"{table}\" (id, label) VALUES (3, 'c')"))
            conn.execute(sa.text(f"INSERT INTO \"{table}\" (id, label) VALUES (99, 'ghost')"))
            conn.commit()
    except Exception as exc:
        pytest.skip(f"Oracle setup failed: {exc}")

    assert destination_row_count("oracle", cfg, schema="", table_name=table) == 4
    deleted = apply_inferred_leftover_deletes(
        db_type="oracle",
        cfg=cfg,
        schema="",
        table_name=table,
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "oracle",
        cfg,
        schema="",
        table_name=table,
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    try:
        with engine.connect() as conn:
            conn.execute(sa.text(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; EXCEPTION WHEN OTHERS THEN NULL; END;"))
            conn.commit()
    except Exception:
        pass


def test_scd2_numeric_boolean_predicate_never_emits_is_true():
    """SQL Server BIT / Oracle NUMBER(1) / SQLite INTEGER: = 1, never IS TRUE."""
    from services.scd2_engine import (
        scd2_is_current_false_sql,
        scd2_is_current_predicate,
        stores_is_current_as_numeric,
    )

    col = '"is_current"'
    for dialect in (
        "sqlite",
        "sqlserver",
        "mssql",
        "azure_sql_database",
        "amazon_rds_sql_server",
        "oracle",
        "amazon_rds_oracle",
        "oracle_autonomous_warehouse",
    ):
        assert stores_is_current_as_numeric(dialect), dialect
        pred = scd2_is_current_predicate(dialect, col)
        assert pred == f"{col} = 1", dialect
        assert "IS TRUE" not in pred.upper()
        assert scd2_is_current_false_sql(dialect) == "0"
    for dialect in ("postgresql", "mysql", "mariadb", "redshift"):
        assert not stores_is_current_as_numeric(dialect), dialect
        assert scd2_is_current_predicate(dialect, col) == f"{col} IS TRUE"
        assert scd2_is_current_false_sql(dialect) == "FALSE"


def test_sqlserver_scd2_current_not_history_and_never_is_true(monkeypatch: pytest.MonkeyPatch):
    """Azure SQL / SQL Server: current=2, history=3. BIT predicate is = 1."""
    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(count=3, count_current=2),
    )
    cfg = {"host": "db.example", "username": "sa", "database": "app"}
    pop = count_scd2_populations(
        "azure_sql_database", cfg, schema="", table_name="products"
    )
    assert pop is not None
    assert pop[CURRENT_ROWS_KEY] == 2
    assert pop[HISTORY_ROWS_KEY] == 3
    current_sql = [sql for sql in engine.sql if "IS_CURRENT" in sql.upper().replace("[", "")]
    assert current_sql
    assert "[dbo].[products]" in current_sql[0]
    assert "[is_current] = 1" in current_sql[0]
    assert all("IS TRUE" not in sql.upper() for sql in engine.sql)
    assert all("sys.partitions" not in sql.lower() for sql in engine.sql)
    stamped = stamp_scd2_census(
        {"target_rows": 10_000, "target_checksum": "writer-active"},
        cfg,
        schema="",
        table_name="products",
        dest_engine="azure_sql_database",
    )
    assert stamped[CURRENT_ROWS_KEY] == 2
    assert stamped[HISTORY_ROWS_KEY] == 3
    assert stamped["dest_count_source"] == DEST_COUNT_CURRENT
    assert stamped["target_rows"] == 10_000


def test_oracle_scd2_current_folds_is_current_and_missing_table_is_zero(
    monkeypatch: pytest.MonkeyPatch,
):
    from sqlalchemy.exc import ProgrammingError

    cfg = {"host": "db.example", "username": "app", "database": "ORCL"}
    missing = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            error=ProgrammingError("SELECT", {}, Exception("ORA-00942: table or view does not exist")),
        ),
    )
    gone = count_scd2_populations(
        "amazon_rds_oracle", cfg, schema="", table_name="products"
    )
    assert gone == {CURRENT_ROWS_KEY: 0, HISTORY_ROWS_KEY: 0}
    assert any('"APP"."PRODUCTS"' in sql for sql in missing.sql)

    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(count=3, count_current=2),
    )
    pop = count_scd2_populations(
        "oracle_autonomous_warehouse", cfg, schema="hr", table_name="products"
    )
    assert pop is not None
    assert pop[CURRENT_ROWS_KEY] == 2
    assert pop[HISTORY_ROWS_KEY] == 3
    current_sql = [sql for sql in engine.sql if "IS_CURRENT" in sql.upper().replace('"', "")]
    assert current_sql
    assert '"HR"."PRODUCTS"' in current_sql[0]
    assert '"IS_CURRENT" = 1' in current_sql[0]
    assert all("IS TRUE" not in sql.upper() for sql in engine.sql)


def test_sqlserver_scd2_missing_column_is_unmeasured_not_history(
    monkeypatch: pytest.MonkeyPatch,
):
    """Live table without is_current must not close on physical COUNT(*)."""
    from sqlalchemy.exc import ProgrammingError

    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            count=4,
            column_error=ProgrammingError(
                "SELECT", {}, Exception("Invalid column name 'is_current'")
            ),
        ),
    )
    pop = count_scd2_populations(
        "sqlserver", {"host": "h"}, schema="dbo", table_name="plain"
    )
    assert pop is None
    assert any("COUNT(*)" in sql.upper() for sql in engine.sql)
    skipped = stamp_scd2_census(
        {"target_rows": 4},
        {"host": "h"},
        schema="dbo",
        table_name="plain",
        dest_engine="sqlserver",
    )
    assert skipped.get("dest_count_source") == "skipped_current_readback"
    assert CURRENT_ROWS_KEY not in skipped


def test_oracle_scd2_missing_column_ora_00904_is_unmeasured(monkeypatch: pytest.MonkeyPatch):
    from sqlalchemy.exc import ProgrammingError

    _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            count=4,
            column_error=ProgrammingError(
                "SELECT", {}, Exception('ORA-00904: "IS_CURRENT": invalid identifier')
            ),
        ),
    )
    assert count_scd2_current("oracle", {"username": "app"}, schema="APP", table_name="plain") is None


def test_sqlserver_scd2_login_failure_is_unmeasured_not_empty(monkeypatch: pytest.MonkeyPatch):
    _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(error=RuntimeError("Login failed for user 'sa'")),
    )
    assert count_scd2_populations(
        "sqlserver", {"host": "h"}, schema="dbo", table_name="products"
    ) is None


def test_snowflake_scd2_current_is_true_not_catalog_stats(monkeypatch: pytest.MonkeyPatch):
    """Snowflake BOOLEAN is_current uses IS TRUE. Never INFORMATION_SCHEMA."""
    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(count=3, count_current=2),
    )
    pop = count_scd2_populations(
        "snowflake", {"host": "h"}, schema="PUBLIC", table_name="PRODUCTS"
    )
    assert pop is not None
    assert pop[CURRENT_ROWS_KEY] == 2
    assert pop[HISTORY_ROWS_KEY] == 3
    current_sql = [sql for sql in engine.sql if "IS_CURRENT" in sql.upper().replace('"', "")]
    assert current_sql
    assert any("IS TRUE" in sql.upper() for sql in current_sql)
    assert all("= 1" not in sql for sql in current_sql)
    assert all("INFORMATION_SCHEMA" not in sql.upper() for sql in engine.sql)


def test_snowflake_scd2_missing_column_is_unmeasured(monkeypatch: pytest.MonkeyPatch):
    from sqlalchemy.exc import ProgrammingError

    _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            count=4,
            column_error=ProgrammingError(
                "SELECT", {}, Exception("invalid identifier 'IS_CURRENT'")
            ),
        ),
    )
    assert count_scd2_current("snowflake", {"host": "h"}, schema="PUBLIC", table_name="PLAIN") is None


def test_redshift_scd2_current_is_true_not_svv_table_info(monkeypatch: pytest.MonkeyPatch):
    """Redshift BOOLEAN is_current uses IS TRUE. Never SVV_TABLE_INFO / to_regclass."""
    def _pg_conn_forbidden(**kwargs: object) -> object:
        raise AssertionError("redshift SCD2 COUNT must not use postgresql_conn / to_regclass")

    monkeypatch.setattr("connectors.postgresql_conn.get_connection", _pg_conn_forbidden)
    engine = _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(count=3, count_current=2),
    )
    pop = count_scd2_populations(
        "redshift", {"host": "h"}, schema="public", table_name="products"
    )
    assert pop is not None
    assert pop[CURRENT_ROWS_KEY] == 2
    assert pop[HISTORY_ROWS_KEY] == 3
    current_sql = [sql for sql in engine.sql if "IS_CURRENT" in sql.upper().replace('"', "")]
    assert current_sql
    assert any("IS TRUE" in sql.upper() for sql in current_sql)
    assert all("= 1" not in sql for sql in current_sql)
    assert all("SVV_TABLE_INFO" not in sql.upper() for sql in engine.sql)
    assert all("to_regclass" not in sql.lower() for sql in engine.sql)


def test_redshift_scd2_missing_column_is_unmeasured(monkeypatch: pytest.MonkeyPatch):
    from sqlalchemy.exc import ProgrammingError

    _patch_warehouse(
        monkeypatch,
        _ScriptedWarehouseEngine(
            count=4,
            column_error=ProgrammingError(
                "SELECT",
                {},
                Exception('column "is_current" of relation "plain" does not exist'),
            ),
        ),
    )
    assert count_scd2_current("redshift", {"host": "h"}, schema="public", table_name="plain") is None
    skipped = stamp_scd2_census(
        {"target_rows": 4},
        {"host": "h"},
        schema="public",
        table_name="plain",
        dest_engine="amazon_redshift",
    )
    assert skipped.get("dest_count_source") == "skipped_current_readback"
    assert CURRENT_ROWS_KEY not in skipped


def test_bigquery_scd2_connect_failure_is_unmeasured():
    assert count_scd2_current("bigquery", {"project": "p"}, schema="ds", table_name="T") is None


def test_sqlserver_live_scd2_current_when_reachable():
    """Live SQL Server: 2 current / 3 history. Skip when :1433 does not answer.

    Dest-engine COUNT(*) WHERE [is_current] = 1, never sys.partitions, never IS TRUE.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError:
        pytest.skip("SQL Server not listening on 1433")

    cfg = {
        "type": "sqlserver",
        "host": "127.0.0.1",
        "port": 1433,
        "database": "dataflow",
        "username": "sa",
        "password": "Datawrap_CDC_2022!",
        "schema": "dbo",
    }
    table = "df_p9_scd2_current"
    try:
        from connectors.generic_sql import get_sqlalchemy_engine
        import sqlalchemy as sa

        engine = get_sqlalchemy_engine(cfg)
    except Exception as exc:
        pytest.skip(f"SQL Server engine unavailable: {exc}")
    try:
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}"
                )
            )
            conn.execute(
                sa.text(
                    f"CREATE TABLE dbo.{table} ("
                    f"id BIGINT NOT NULL, is_current BIT NOT NULL)"
                )
            )
            conn.execute(
                sa.text(
                    f"INSERT INTO dbo.{table} (id, is_current) "
                    f"VALUES (1, 1), (1, 0), (2, 1)"
                )
            )
            conn.commit()
    except Exception as exc:
        pytest.skip(f"SQL Server setup failed: {exc}")

    pop = count_scd2_populations("sqlserver", cfg, schema="dbo", table_name=table)
    assert pop is not None
    assert pop[CURRENT_ROWS_KEY] == 2
    assert pop[HISTORY_ROWS_KEY] == 3
    assert count_scd2_current("sqlserver", cfg, schema="dbo", table_name="df_p9_scd2_gone") == 0
    with engine.connect() as conn:
        conn.execute(sa.text(f"CREATE TABLE dbo.{table}_plain (id BIGINT NOT NULL)"))
        conn.commit()
    assert count_scd2_current(
        "sqlserver", cfg, schema="dbo", table_name=f"{table}_plain"
    ) is None
    try:
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}"
                )
            )
            conn.execute(
                sa.text(
                    f"IF OBJECT_ID(N'dbo.{table}_plain', N'U') IS NOT NULL "
                    f"DROP TABLE dbo.{table}_plain"
                )
            )
            conn.commit()
    except Exception:
        pass


def test_oracle_live_scd2_current_when_reachable():
    """Live Oracle: NUMBER(1) current=2 / history=3. Skip when :1521 does not answer."""
    import os
    import socket

    host = os.environ.get("DATAFLOW_ORACLE_HOST", "127.0.0.1")
    port = int(os.environ.get("DATAFLOW_ORACLE_PORT", "1521"))
    try:
        socket.create_connection((host, port), timeout=1).close()
    except OSError:
        pytest.skip(f"Oracle not listening on {host}:{port}")

    cfg = {
        "type": "oracle",
        "host": host,
        "port": port,
        "database": os.environ.get("DATAFLOW_ORACLE_SERVICE", "ORCL"),
        "username": os.environ.get("DATAFLOW_ORACLE_USER", "system"),
        "password": os.environ.get("DATAFLOW_ORACLE_PASSWORD", ""),
        "schema": "",
    }
    if not cfg["password"]:
        pytest.skip("Oracle password not configured")
    table = "DF_P9_SCD2"
    try:
        from connectors.generic_sql import get_sqlalchemy_engine
        import sqlalchemy as sa

        engine = get_sqlalchemy_engine(cfg)
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; "
                    f"EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
                )
            )
            conn.execute(
                sa.text(
                    f'CREATE TABLE "{table}" (id NUMBER, is_current NUMBER(1))'
                )
            )
            conn.execute(sa.text(f'INSERT INTO "{table}" (id, is_current) VALUES (1, 1)'))
            conn.execute(sa.text(f'INSERT INTO "{table}" (id, is_current) VALUES (1, 0)'))
            conn.execute(sa.text(f'INSERT INTO "{table}" (id, is_current) VALUES (2, 1)'))
            conn.commit()
    except Exception as exc:
        pytest.skip(f"Oracle setup failed: {exc}")

    pop = count_scd2_populations("oracle", cfg, schema="", table_name=table)
    assert pop is not None
    assert pop[CURRENT_ROWS_KEY] == 2
    assert pop[HISTORY_ROWS_KEY] == 3
    assert count_scd2_current("oracle", cfg, schema="", table_name="DF_P9_SCD2_GONE") == 0
    try:
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; "
                    f"EXCEPTION WHEN OTHERS THEN NULL; END;"
                )
            )
            conn.commit()
    except Exception:
        pass

