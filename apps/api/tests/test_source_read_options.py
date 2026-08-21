"""The declared source read window means one population, everywhere.

A client workbook has a title row, a header on row 3, a totals row at the
bottom, and the data on the third sheet. These tests hold the two properties
that make such a file safe to migrate: the window is honoured, and *every*
consumer of it (preview, source COUNT, the streaming iterator, the buffered
parse) reports the same rows. A preview that disagrees with the writer is how a
run reconciles against rows nobody moved.
"""

from __future__ import annotations

import io

import pytest
from openpyxl import Workbook

from services.csv_profiler import count_csv_rows, iter_csv_dicts, parse_csv_preview
from services.excel_parser import (
    count_excel_rows,
    iter_excel_dicts,
    list_excel_sheets,
    parse_excel_preview,
)
from services.file_parser import FileParser
from services.read_options import ReadOptions, ReadOptionsError, parse_read_options_payload
from src.transfer.file_stream import peek_file_source


def _workbook(sheets: dict[str, list[list[object]]]) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(title=name)
        for row in rows:
            ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


PLAIN = {"Sheet1": [["id", "name"], [1, "a"], [2, "b"]]}

# Title, blank line, header on row 3, three data rows, then a totals footer —
# the shape a finance export actually arrives in.
MESSY = {
    "Cover": [["ignore me"]],
    "Data": [
        ["Quarterly export"],
        [],
        ["id", "name", "amount"],
        [1, "a", "10.5"],
        [2, "b", "20.5"],
        [3, "c", "30.5"],
        ["TOTAL", "", "61.5"],
    ],
}


# --- the declaration itself ------------------------------------------------


def test_default_options_are_todays_behaviour():
    assert ReadOptions().is_default is True
    assert ReadOptions().to_wire() == {}


def test_wire_round_trip_keeps_only_declared_fields():
    opts = ReadOptions(sheet="Data", header_row=3, skip_footer=1)
    wire = opts.to_wire()
    assert wire == {"sheet": "Data", "header_row": 3, "skip_footer": 1}
    assert ReadOptions.from_dict(wire) == opts


def test_numeric_sheet_is_read_as_a_position():
    assert ReadOptions.from_dict({"sheet": 2}).sheet_index == 2
    assert ReadOptions.from_dict({"sheet_name": "Data"}).sheet == "Data"


@pytest.mark.parametrize(
    "spelling,expected",
    [("tab", "\t"), ("\\t", "\t"), ("TAB", "\t"), ("semicolon", ";"), ("|", "|")],
)
def test_delimiter_spellings_a_form_can_carry(spelling, expected):
    assert ReadOptions.from_dict({"delimiter": spelling}).delimiter == expected


@pytest.mark.parametrize(
    "payload",
    [
        {"header_row": -1},
        {"skip_rows": -5},
        {"skip_footer": 10_000_001},
        {"sheet_index": -2},
        {"encoding": "not-a-codec"},
        {"delimiter": "||"},
        {"header_row": "three"},
        {"skip_rows": True},
    ],
)
def test_unusable_declaration_is_refused_not_defaulted(payload):
    with pytest.raises(ReadOptionsError):
        ReadOptions.from_dict(payload)


def test_multipart_json_string_and_body_dict_agree():
    as_string = parse_read_options_payload('{"sheet": "Data", "header_row": 3}')
    as_dict = parse_read_options_payload({"sheet": "Data", "header_row": 3})
    assert as_string == as_dict
    assert parse_read_options_payload("") == ReadOptions()
    with pytest.raises(ReadOptionsError):
        parse_read_options_payload("{not json")


def test_options_hash_is_stable_and_distinguishes_windows():
    assert ReadOptions(sheet="Data").options_hash == ReadOptions(sheet="Data").options_hash
    assert ReadOptions(sheet="Data").options_hash != ReadOptions(sheet="Other").options_hash


# --- Excel ------------------------------------------------------------------


def test_excel_default_reads_active_sheet_row_one():
    content = _workbook(PLAIN)
    headers, rows, _total = parse_excel_preview(content)
    assert headers == ["id", "name"]
    assert count_excel_rows(content) == 2


def test_excel_sheet_by_name_and_by_index():
    content = _workbook(MESSY)
    by_name = ReadOptions(sheet="Data", header_row=3, skip_footer=1)
    by_index = ReadOptions(sheet_index=1, header_row=3, skip_footer=1)
    assert count_excel_rows(content, by_name) == 3
    assert count_excel_rows(content, by_index) == 3


def test_excel_unknown_sheet_refuses_and_names_the_real_sheets():
    content = _workbook(MESSY)
    with pytest.raises(ReadOptionsError) as exc:
        count_excel_rows(content, ReadOptions(sheet="Nope"))
    assert "Data" in str(exc.value)


def test_excel_sheet_index_past_the_end_refuses():
    with pytest.raises(ReadOptionsError):
        count_excel_rows(_workbook(MESSY), ReadOptions(sheet_index=9))


def test_excel_preamble_above_header_is_not_data():
    content = _workbook(MESSY)
    opts = ReadOptions(sheet="Data", header_row=3)
    headers, rows, _total = parse_excel_preview(content, options=opts)
    assert headers == ["id", "name", "amount"]
    # Four value-bearing rows follow the header: three records plus the footer.
    assert count_excel_rows(content, opts) == 4


def test_excel_header_row_pointing_at_a_blank_row_refuses():
    with pytest.raises(ReadOptionsError):
        count_excel_rows(_workbook(MESSY), ReadOptions(sheet="Data", header_row=2))


def test_excel_headerless_sheet_gets_positional_names():
    content = _workbook({"S": [[1, "a"], [2, "b"]]})
    opts = ReadOptions(header_row=0)
    headers, rows, _total = parse_excel_preview(content, options=opts)
    assert headers == ["col_0", "col_1"]
    # The first row is data, not a header, so nothing is consumed to name it.
    assert count_excel_rows(content, opts) == 2


def test_excel_skip_head_and_footer_count_value_bearing_rows():
    content = _workbook(MESSY)
    opts = ReadOptions(sheet="Data", header_row=3, skip_rows=1, skip_footer=1)
    records = list(iter_excel_dicts(content, opts))
    assert [str(r["id"]) for r in records] == ["2", "3"]
    assert count_excel_rows(content, opts) == len(records)


def test_excel_footer_skip_larger_than_population_is_empty_not_negative():
    content = _workbook(MESSY)
    opts = ReadOptions(sheet="Data", header_row=3, skip_footer=99)
    assert count_excel_rows(content, opts) == 0
    assert list(iter_excel_dicts(content, opts)) == []


def test_excel_sheet_inventory_lists_positions():
    inventory = list_excel_sheets(_workbook(MESSY))
    assert [s["name"] for s in inventory] == ["Cover", "Data"]
    assert [s["index"] for s in inventory] == [0, 1]


# --- CSV --------------------------------------------------------------------

CSV_PLAIN = b"id,name\n1,a\n2,b\n"
CSV_MESSY = (
    b"Quarterly export\n"
    b"\n"
    b"id;name;amount\n"
    b"1;a;10.5\n"
    b"2;b;20.5\n"
    b"3;c;30.5\n"
    b"TOTAL;;61.5\n"
)


def test_csv_default_behaviour_unchanged():
    headers, rows, _enc, delim = parse_csv_preview(CSV_PLAIN)
    assert headers == ["id", "name"]
    assert delim == ","
    assert count_csv_rows(CSV_PLAIN) == 2


def test_csv_declared_delimiter_and_window_are_honoured():
    opts = ReadOptions(header_row=3, skip_footer=1, delimiter=";")
    headers, rows, _enc, delim = parse_csv_preview(CSV_MESSY, options=opts)
    assert headers == ["id", "name", "amount"]
    assert delim == ";"
    assert count_csv_rows(CSV_MESSY, options=opts) == 3
    assert [r["id"] for r in iter_csv_dicts(CSV_MESSY, options=opts)] == ["1", "2", "3"]


def test_csv_declared_encoding_decodes_non_utf8_bytes():
    content = "id,name\n1,café\n".encode("latin-1")
    opts = ReadOptions(encoding="latin-1")
    records = list(iter_csv_dicts(content, options=opts))
    assert records == [{"id": "1", "name": "café"}]


def test_csv_headerless_gets_positional_names():
    opts = ReadOptions(header_row=0)
    assert list(iter_csv_dicts(b"1,a\n2,b\n", options=opts)) == [
        {"col_0": "1", "col_1": "a"},
        {"col_0": "2", "col_1": "b"},
    ]


def test_csv_blank_lines_are_not_data_rows():
    content = b"id,name\n1,a\n\n,,\n2,b\n"
    opts = ReadOptions(skip_rows=1)
    assert count_csv_rows(content, options=opts) == 1
    assert [r["id"] for r in iter_csv_dicts(content, options=opts)] == ["2"]


def test_csv_quoted_newline_is_one_record_inside_the_window():
    content = b'id,note\n1,"line1\nline2"\n2,plain\n'
    opts = ReadOptions(skip_footer=1)
    records = list(iter_csv_dicts(content, options=opts))
    assert records == [{"id": "1", "note": "line1\nline2"}]


def test_csv_row_wider_than_the_header_is_refused_not_truncated():
    """A value with no column to land in is data loss, so nobody drops it."""
    content = b"id,name\n1,a\n2,b,ORPHAN\n"
    with pytest.raises(ValueError, match="refuse silent column drop"):
        list(iter_csv_dicts(content))
    buffered = FileParser.parse(content, "ragged.csv")
    assert buffered.success is False
    streamed = FileParser.parse(content, "ragged.csv")
    assert "column" in (streamed.error or "")


def test_csv_trailing_delimiter_is_not_an_extra_value():
    """``1,a,`` spells two values and an empty tail — that is not a lost cell."""
    records = list(iter_csv_dicts(b"id,name\n1,a,\n2,b,\n"))
    assert records == [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]


def test_csv_short_row_pads_rather_than_shifting_columns():
    records = list(iter_csv_dicts(b"id,name,amount\n1,a\n"))
    assert records == [{"id": "1", "name": "a", "amount": ""}]


def test_csv_duplicate_headers_are_reported_as_the_file_spells_them():
    """The profiler must not invent uniqueness; the caller decides how to resolve."""
    headers, _rows, _enc, _delim = parse_csv_preview(b"id,id,name\n1,2,a\n")
    assert headers == ["id", "id", "name"]


def test_csv_unicode_survives_the_window_intact():
    content = "id,name\nskip,ignored\n1,Ünïcodé — 東京\n".encode()
    opts = ReadOptions(skip_rows=1)
    assert list(iter_csv_dicts(content, options=opts)) == [
        {"id": "1", "name": "Ünïcodé — 東京"}
    ]


def test_csv_declared_encoding_that_cannot_decode_refuses_rather_than_replacing():
    content = "id,name\n1,café\n".encode("latin-1")
    result = FileParser.parse(
        content, "bad.csv", read_options=ReadOptions(encoding="utf-8")
    )
    assert result.success is False
    assert "not valid UTF-8" in (result.error or "")


def test_csv_large_and_high_precision_values_are_not_rounded_by_the_window():
    content = (
        b"id,amount\n"
        b"1,123456789012345678901234567890.123456789\n"
        b"2,0.000000000000000001\n"
    )
    records = list(iter_csv_dicts(content, options=ReadOptions(skip_footer=0)))
    assert records[0]["amount"] == "123456789012345678901234567890.123456789"
    assert records[1]["amount"] == "0.000000000000000001"


def test_excel_value_past_the_named_columns_gets_a_positional_column():
    """A workbook's used range is rectangular, so the orphan value is carried
    under a positional name rather than dropped on the floor."""
    content = _workbook({"S": [["id", "name"], [1, "a"], [2, "b", "ORPHAN"]]})
    assert list(iter_excel_dicts(content)) == [
        {"id": "1", "name": "a", "col_2": ""},
        {"id": "2", "name": "b", "col_2": "ORPHAN"},
    ]


# --- one declaration, one population ---------------------------------------


def test_csv_preview_count_iterator_and_buffered_parse_agree():
    opts = ReadOptions(header_row=3, skip_rows=1, skip_footer=1, delimiter=";")
    _headers, preview, _enc, _delim = parse_csv_preview(CSV_MESSY, options=opts)
    counted = count_csv_rows(CSV_MESSY, options=opts)
    streamed = list(iter_csv_dicts(CSV_MESSY, options=opts))
    buffered = FileParser.parse(CSV_MESSY, "messy.csv", read_options=opts)
    assert len(preview) == counted == len(streamed) == buffered.row_count == 2
    assert [r["id"] for r in streamed] == [r["id"] for r in buffered.data]


def test_csv_stream_peek_reports_the_declared_population():
    opts = ReadOptions(header_row=3, skip_footer=1, delimiter=";")
    columns, _schema, total, sample = peek_file_source(CSV_MESSY, "messy.csv", opts)
    assert columns == ["id", "name", "amount"]
    assert total == count_csv_rows(CSV_MESSY, options=opts) == 3
    assert sample[0]["id"] == "1"


def test_excel_preview_count_and_buffered_parse_agree():
    content = _workbook(MESSY)
    opts = ReadOptions(sheet="Data", header_row=3, skip_footer=1)
    _headers, _rows, preview_total = parse_excel_preview(content, options=opts)
    counted = count_excel_rows(content, opts)
    buffered = FileParser.parse(content, "messy.xlsx", read_options=opts)
    assert preview_total == counted == buffered.row_count == 3


def test_excel_stream_peek_reports_the_declared_population():
    content = _workbook(MESSY)
    opts = ReadOptions(sheet_index=1, header_row=3, skip_footer=1)
    columns, _schema, total, _sample = peek_file_source(content, "messy.xlsx", opts)
    assert columns == ["id", "name", "amount"]
    assert total == 3


# --- a window a reader cannot honour is refused, never ignored --------------


def test_sheet_selection_on_csv_is_refused():
    result = FileParser.parse(CSV_PLAIN, "plain.csv", read_options=ReadOptions(sheet="Data"))
    assert result.success is False
    assert "Excel" in (result.error or "")


def test_delimiter_on_json_is_refused():
    result = FileParser.parse(
        b'[{"id": 1}]', "rows.json", read_options=ReadOptions(delimiter=";")
    )
    assert result.success is False
    assert "delimited" in (result.error or "")


def test_row_window_on_jsonl_is_refused():
    result = FileParser.parse(
        b'{"id": 1}\n', "rows.jsonl", read_options=ReadOptions(skip_footer=1)
    )
    assert result.success is False
    assert "jsonl" in (result.error or "")


def test_read_options_survive_a_durable_transfer_request():
    from src.transfer.models import (
        EndpointConfig,
        TransferRequest,
        transfer_request_from_dict,
        transfer_request_to_dict,
    )

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sqlite", table="t"),
        source_filename="messy.csv",
        read_options=ReadOptions(header_row=3, skip_footer=1, delimiter=";").to_wire(),
    )
    revived = transfer_request_from_dict(transfer_request_to_dict(request))
    assert revived.read_options == request.read_options
