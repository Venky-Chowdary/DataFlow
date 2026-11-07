"""C04: one CSV NULL codec on every engine.

- unquoted empty cell -> SQL NULL
- quoted ``""`` -> empty string ''
- NULL writes as an empty unquoted cell; '' writes as quoted ""
- legacy ``\\N`` still reads as NULL
- '' on a non-text carrier binds NULL ('' is not a number)
"""
from __future__ import annotations

import io


def test_codec_unquoted_empty_is_null() -> None:
    from services.csv_codec import iter_csv_records

    rows = list(iter_csv_records(io.StringIO("id,name,amt\n1,,\n")))
    assert rows[1] == ["1", None, None]


def test_codec_quoted_empty_is_empty_string() -> None:
    from services.csv_codec import iter_csv_records

    rows = list(iter_csv_records(io.StringIO('id,name,amt\n1,"",2.5\n')))
    assert rows[1] == ["1", "", "2.5"]


def test_codec_legacy_null_spelling_reads_as_null() -> None:
    from services.csv_codec import iter_csv_records

    rows = list(iter_csv_records(io.StringIO("a,b\n\\N,x\n")))
    assert rows[1][0] is None


def test_codec_keep_option_holds_unquoted_empty() -> None:
    from services.csv_codec import iter_csv_records

    rows = list(iter_csv_records(io.StringIO("a,b\n,\n"), empty_cell="keep"))
    assert rows[1] == ["", ""]


def test_codec_multiline_quoted_field_intact() -> None:
    from services.csv_codec import iter_csv_records

    rows = list(iter_csv_records(io.StringIO('a,b\n"multi\nline",z\n')))
    assert rows[1] == ["multi\nline", "z"]


def test_codec_nulled_counter_counts_empty_cells() -> None:
    from services.csv_codec import iter_csv_records

    nulled = [0]
    rows = list(iter_csv_records(io.StringIO('a,b,c\n,,""\n'), nulled=nulled))
    assert rows[1] == [None, None, ""]
    assert nulled[0] == 2


def test_encode_null_is_unquoted_empty() -> None:
    from services.csv_codec import csv_cell_encode, format_csv_row

    assert format_csv_row([csv_cell_encode(None)]) == ""


def test_encode_empty_string_is_quoted() -> None:
    from services.csv_codec import csv_cell_encode, format_csv_row

    assert format_csv_row([csv_cell_encode("")]) == '""'


def test_encode_roundtrip() -> None:
    from services.csv_codec import csv_cell_encode, format_csv_row, iter_csv_records

    row = format_csv_row(
        [csv_cell_encode(v) for v in (None, "", "a,b", 42)]
    )
    assert row == ',"","a,b",42'
    back = list(iter_csv_records(io.StringIO("h1,h2,h3,h4\n" + row + "\n")))
    assert back[1] == [None, "", "a,b", "42"]


def test_bind_empty_string_on_numeric_carrier_is_null() -> None:
    from transfer.file_stream import file_empty_cell_bind

    for carrier in ("DECIMAL(12,4)", "INTEGER", "REAL", "DATE", ""):
        assert file_empty_cell_bind("", carrier) is None


def test_bind_empty_string_on_text_carrier_stays() -> None:
    from transfer.file_stream import file_empty_cell_bind

    for carrier in ("VARCHAR(50)", "TEXT", "string", "LONGTEXT"):
        assert file_empty_cell_bind("", carrier) == ""


def test_bind_non_empty_and_none_untouched() -> None:
    from transfer.file_stream import file_empty_cell_bind

    assert file_empty_cell_bind("0", "INTEGER") == "0"
    assert file_empty_cell_bind(None, "TEXT") is None


def test_apply_empty_cell_rule_counts() -> None:
    from transfer.file_stream import _apply_empty_cell_rule

    carriers = {"amt": "DECIMAL(12,4)", "name": "string", "qty": "INTEGER"}
    counter = [0]
    rows = [{"amt": "", "name": "", "qty": "5"}]
    _apply_empty_cell_rule(rows, carriers, counter)
    assert rows[0]["amt"] is None and rows[0]["name"] == "" and rows[0]["qty"] == "5"
    assert counter[0] == 1


def test_read_options_validates_empty_cell() -> None:
    import pytest
    from services.read_options import ReadOptions, ReadOptionsError

    assert ReadOptions(empty_cell="keep").empty_cell == "keep"
    assert ReadOptions().empty_cell == "null"
    with pytest.raises(ReadOptionsError):
        ReadOptions(empty_cell="drop")


def test_stream_batches_decode_codec(tmp_path) -> None:
    """The row path sees codec cells: unquoted empty -> None, quoted "" -> ''."""
    from transfer.file_stream import _iter_csv_batches

    content = b'id,name,amt\n1,,\n2,"",4.5\n'
    batches = list(_iter_csv_batches(content, 50))
    assert batches[0][0]["name"] is None
    assert batches[0][1]["name"] == ""
