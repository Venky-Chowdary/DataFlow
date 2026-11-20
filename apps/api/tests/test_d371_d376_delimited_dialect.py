"""D371 / D376(b): TSV uses the CSV quoting dialect; each writer has one terminator.

NULL is an empty unquoted field. ``''`` is ``""``. A quote, tab, CR or LF
is quoted with doubled quotes. Object-store, fast-path and converter rows end
LF (as shipped); a file_export ends CRLF (PR 402). A file never mixes the two,
and a CR or LF inside quotes is data. Migration rule 9.
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.csv_codec import (  # noqa: E402
    RECORD_TERMINATOR,
    csv_cell_encode,
    format_csv_row,
    iter_csv_records,
)
from services.format_converter import convert_rows  # noqa: E402


def test_tsv_distinguishes_empty_string_from_null_and_quotes() -> None:
    cells = ["", None, 'q"uote', "a\tb", "line\r\nfeed", "  spaced  ", "NULL", "\\N"]
    line = format_csv_row([csv_cell_encode(c) for c in cells], "\t")
    assert line.startswith('""\t\t')
    parsed = next(csv.reader(io.StringIO(line + RECORD_TERMINATOR, newline=""), delimiter="\t"))
    assert parsed[0] == ""
    assert parsed[1] == ""
    assert parsed[2] == 'q"uote'
    assert parsed[3] == "a\tb"
    assert parsed[4] == "line\r\nfeed"
    assert parsed[5] == "  spaced  "
    assert parsed[6] == "NULL"
    assert parsed[7] == "\\N"


def _record_breaks(body: bytes, delimiter: str) -> tuple[int, int, int]:
    from services.delimited_scan import record_breaks

    return record_breaks(body, delimiter)


def test_csv_and_tsv_conversions_end_rows_with_lf_only() -> None:
    headers = ["id", "s"]
    rows = [["1", ""], ["2", None], ["3", 'q"uote'], ["4", "multi\r\nline"]]
    csv_bytes, _ = convert_rows(headers, rows, source_format="csv", target_format="csv")
    tsv_bytes, _ = convert_rows(headers, rows, source_format="csv", target_format="tsv")
    for body, delim in ((csv_bytes, ","), (tsv_bytes, "\t")):
        assert body.endswith(b"\n") and not body.endswith(b"\r\n")
        # Five record breaks, all LF; the only CR is the quoted value's.
        assert _record_breaks(body, delim) == (0, 5, 0)
        assert b'"multi\r\nline"' in body
    assert b'\t""\r' not in tsv_bytes and b'1\t""\n' in tsv_bytes
    text = tsv_bytes.decode("utf-8")
    rows_back = list(iter_csv_records(io.StringIO(text, newline=""), "\t"))
    # header + 4 data rows; unquoted empty is NULL, quoted "" is empty text
    assert rows_back[1] == ["1", ""]
    assert rows_back[2] == ["2", None]
    assert rows_back[3] == ["3", 'q"uote']
    assert rows_back[4] == ["4", "multi\r\nline"]
    assert RECORD_TERMINATOR == "\n"


def test_file_export_rows_end_crlf_with_the_same_quoting() -> None:
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig

    records = [{"id": "1", "s": ""}, {"id": "2", "s": None}, {"id": "3", "s": 'q"uote'}]
    content, _name, _summary = write_destination_file(
        EndpointConfig(kind="file_export", format="tsv"), records, ["id", "s"], source_format="csv"
    )
    assert content == b'id\ts\r\n1\t""\r\n2\t\r\n3\t"q""uote"\r\n'


def test_quoted_crlf_inside_a_field_is_preserved() -> None:
    raw = 'id,note\r\n1,"multi\r\nline"\r\n'
    rows = list(iter_csv_records(io.StringIO(raw, newline=""), ","))
    assert rows[1][1] == "multi\r\nline"


def test_fast_s3_export_quotes_like_csv_and_ends_lf(tmp_path) -> None:
    from services.copy_s3_common import s3_write_delimited

    path = tmp_path / "out.tsv"
    s3_write_delimited(str(path), ["id", "note"], [(1, 'q"uote'), (2, None), (3, "")], "\t")
    assert path.read_bytes() == b'id\tnote\n1\t"q""uote"\n2\t\n3\t""\n'


def test_append_onto_a_pre_d371_tsv_keeps_its_lf_and_mid_field_quote() -> None:
    from connectors.object_store_incremental import _delimited_concat

    # An object an earlier build wrote: LF, a raw mid-field quote, a quoted LF.
    existing = b'id\tnote\n1\t5" pipe\n2\t"a\nb"\n'
    incoming = b'id\tnote\n3\t"q""uote"\n4\t""\n'
    assert _delimited_concat(existing, incoming, fmt="tsv") == existing + b'3\t"q""uote"\n4\t""\n'
    # A CRLF incoming batch is re-spelled LF outside quotes only.
    incoming_crlf = b'id\tnote\r\n3\t"x\r\ny"\r\n'
    assert _delimited_concat(existing, incoming_crlf, fmt="tsv") == existing + b'3\t"x\r\ny"\n'


def test_append_refuses_an_existing_file_that_mixes_line_endings() -> None:
    import pytest

    from connectors.object_store_incremental import ObjectMergeRefused, _delimited_concat

    existing = b"id,note\n1,a\r\n2,b\n"
    with pytest.raises(ObjectMergeRefused, match="mixes LF and CRLF"):
        _delimited_concat(existing, b"id,note\n3,c\n", fmt="csv")


def test_csv_append_matches_the_existing_line_ending() -> None:
    from connectors.object_store_incremental import _delimited_concat

    existing = b"id,note\n1,a\n"
    incoming = b"id,note\r\n2,b\r\n"
    merged = _delimited_concat(existing, incoming, fmt="csv")
    assert merged == b"id,note\n1,a\n2,b\n"


def test_quoted_newlines_survive_record_terminator_rewrite() -> None:
    from services.csv_codec import rewrite_record_terminators
    from connectors.object_store_incremental import _delimited_concat

    copied = b'id,note\n1,"a\nb"\n2,"c\r\nd"\n'
    assert rewrite_record_terminators(copied, b"\r\n") == (
        b'id,note\r\n1,"a\nb"\r\n2,"c\r\nd"\r\n'
    )
    existing = b'id,note\r\n1,"a\nb"\r\n'
    incoming = b'id,note\n2,"c\r\nd"\n'
    assert _delimited_concat(existing, incoming, fmt="csv") == (
        b'id,note\r\n1,"a\nb"\r\n2,"c\r\nd"\r\n'
    )
