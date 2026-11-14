"""Appending to a CSV / TSV export: the edges review 5 found (M-4 and the append Lows).

* An LF TSV is an earlier build's file-source TSV (this build writes CRLF), and
  that build never quoted a TSV: rows appended to it are unquoted too, even
  when its first 64 KB happen to hold no bare quote to show it.
* The separator put before the appended rows (a file whose last line has no
  line ending, a header-only file) is the file's own line ending, else this
  run's, so one file never mixes line endings.
* A header behind a byte order mark (a file re-saved by Excel) is the same
  header.
"""

from __future__ import annotations

from services.file_export_append import append_payload

#: What this build writes (CRLF; NULL empty, '' quoted, a cell with a delimiter, quote or line break quoted).
RUN_TSV = b'id\tv\r\n1\t""\r\n2\t"a""b"\r\n3\t\r\n4\t"x\ty"\r\n'
RUN_CSV = b'id,v\r\n1,""\r\n2,"a""b"\r\n3,\r\n'


def _append(path, fmt, data):
    with open(path, "ab") as fh:
        fh.write(append_payload(fmt, str(path), data))
    return path.read_bytes()


def test_m4_an_lf_tsv_with_no_quote_in_sight_takes_the_append_unquoted(tmp_path):
    path = tmp_path / "old.tsv"
    path.write_bytes(b"id\tv\n9\tx\n8\t\n")  # the earlier build's TSV: LF, never quoted, no '"' to show it
    out = _append(path, "tsv", RUN_TSV)
    # Unquoted like the file; a value holding a tab still has to be quoted to stay one cell.
    assert out == b'id\tv\n9\tx\n8\t\n1\t\n2\ta"b\n3\t\n4\t"x\ty"\n'


def test_low_a_crlf_file_whose_last_row_has_no_line_ending_is_separated_in_crlf(tmp_path):
    path = tmp_path / "f.csv"
    path.write_bytes(b"id,v\r\n9,x")
    assert _append(path, "csv", RUN_CSV) == b'id,v\r\n9,x\r\n1,""\r\n2,"a""b"\r\n3,\r\n'


def test_low_a_header_only_file_without_a_line_ending_takes_this_runs_line_ending(tmp_path):
    path = tmp_path / "f.csv"
    path.write_bytes(b"id,v")
    assert _append(path, "csv", RUN_CSV) == b'id,v\r\n1,""\r\n2,"a""b"\r\n3,\r\n'


def test_low_an_lf_file_whose_last_row_has_no_line_ending_is_still_separated_in_lf(tmp_path):
    path = tmp_path / "f.csv"
    path.write_bytes(b"id,v\n9,x")
    assert _append(path, "csv", RUN_CSV) == b'id,v\n9,x\n1,""\n2,"a""b"\n3,\n'


def test_low_a_header_behind_a_byte_order_mark_is_the_same_header(tmp_path):
    path = tmp_path / "excel.csv"
    path.write_bytes(b"\xef\xbb\xbfid,v\r\n9,x\r\n")
    assert _append(path, "csv", RUN_CSV) == b'\xef\xbb\xbfid,v\r\n9,x\r\n1,""\r\n2,"a""b"\r\n3,\r\n'
