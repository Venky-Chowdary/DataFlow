"""A file export append must add rows, never replace the operator's file."""

import pytest

from services.file_export_append import append_payload, export_append_refusal
from services.fixed_width_layout import (
    count_fixed_width_records,
    dump_fixed_width_records,
    iter_fixed_width_dicts,
)

FWF_LAYOUT = (("id", 8), ("name", 8))
FWF_RUN1 = dump_fixed_width_records(
    [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}],
    FWF_LAYOUT,
)
FWF_RUN2 = dump_fixed_width_records(
    [{"id": "3", "name": "c"}, {"id": "4", "name": "d"}],
    FWF_LAYOUT,
)

CSV_RUN1 = b"id,name\n1,a\n2,b\n"
CSV_RUN2 = b"id,name\n3,c\n"


def test_csv_append_keeps_one_header_and_all_rows(tmp_path):
    path = tmp_path / "out.csv"
    path.write_bytes(CSV_RUN1)
    with open(path, "ab") as fh:
        fh.write(append_payload("csv", str(path), CSV_RUN2))
    assert path.read_bytes() == b"id,name\n1,a\n2,b\n3,c\n"


def test_append_to_missing_file_is_a_create(tmp_path):
    path = tmp_path / "out.csv"
    assert export_append_refusal("csv", str(path)) == ""


def test_header_drift_is_refused_not_misaligned(tmp_path):
    path = tmp_path / "out.csv"
    path.write_bytes(CSV_RUN1)
    with pytest.raises(ValueError, match="misalign"):
        append_payload("csv", str(path), b"id,name,extra\n3,c,x\n")


def test_container_format_append_is_refused(tmp_path):
    path = tmp_path / "out.parquet"
    path.write_bytes(b"PAR1")
    assert "not supported" in export_append_refusal("parquet", str(path))
    with pytest.raises(ValueError, match="not supported"):
        append_payload("parquet", str(path), b"PAR1")


def test_jsonl_append_has_no_header_to_strip(tmp_path):
    path = tmp_path / "out.jsonl"
    path.write_bytes(b'{"id":1}\n')
    with open(path, "ab") as fh:
        fh.write(append_payload("ndjson", str(path), b'{"id":2}\n'))
    assert path.read_bytes() == b'{"id":1}\n{"id":2}\n'


def test_fwf_append_keeps_one_layout_header_and_all_rows(tmp_path):
    path = tmp_path / "out.fwf"
    path.write_bytes(FWF_RUN1)
    with open(path, "ab") as fh:
        fh.write(append_payload("fixed_width", str(path), FWF_RUN2))
    body = path.read_bytes()
    assert body.count(b"#layout:") == 1
    assert count_fixed_width_records(body) == 4
    assert [r["id"] for r in iter_fixed_width_dicts(body)] == ["1", "2", "3", "4"]


def test_fwf_header_drift_is_refused_not_misaligned(tmp_path):
    path = tmp_path / "out.fwf"
    path.write_bytes(FWF_RUN1)
    drifted = dump_fixed_width_records(
        [{"id": "3", "name": "c"}],
        (("id", 8), ("name", 16)),
    )
    with pytest.raises(ValueError, match="misalign"):
        append_payload("fwf", str(path), drifted)


def test_missing_trailing_newline_does_not_join_two_rows(tmp_path):
    path = tmp_path / "out.jsonl"
    path.write_bytes(b'{"id":1}')
    with open(path, "ab") as fh:
        fh.write(append_payload("jsonl", str(path), b'{"id":2}\n'))
    assert path.read_bytes() == b'{"id":1}\n{"id":2}\n'
