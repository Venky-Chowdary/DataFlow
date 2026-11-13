"""G7-F1: the delimited-row writer quotes what would split a cell, for any delimiter.

The TSV branch (tab/CR/LF/leading quote) and the transfer file_export / Query
console TSV round trips were fixed in the G7 PR (review H1,
``tests/test_g7_r1_tsv_quoting.py``). The remaining gap: ``format_csv_row``'s
non-tab branch called ``csv_quote_cell(cell)`` without the delimiter, so with
``;`` or ``|`` a cell holding that delimiter was written raw and split the row.
CSV (``,``) bytes are unchanged.
"""

from __future__ import annotations

import csv
import io

import pytest

CELLS = ["a;b", "c|d", "e,f", 'say "hi"', "g\nh", "i\rj", "k\tl", "plain", ""]


@pytest.mark.parametrize("delimiter", [",", ";", "|", "\t"])
def test_format_csv_row_round_trips_for_any_delimiter(delimiter):
    from services.csv_codec import csv_cell_encode, format_csv_row

    line = format_csv_row([csv_cell_encode(c) for c in CELLS], delimiter)
    parsed = next(csv.reader(io.StringIO(line, newline=""), delimiter=delimiter))
    assert parsed == CELLS, (delimiter, line)


@pytest.mark.parametrize("delimiter", [";", "|"])
def test_format_csv_row_quotes_its_own_delimiter(delimiter):
    from services.csv_codec import format_csv_row

    assert format_csv_row([f"a{delimiter}b", "c"], delimiter) == f'"a{delimiter}b"{delimiter}c'


def test_csv_bytes_unchanged_where_no_quoting_is_needed():
    from services.csv_codec import csv_cell_encode, format_csv_row

    cells = ["1", "alice", "-5", "=1+1", "a|b", "a;b", "a\tb", "\\N", None]
    assert format_csv_row([csv_cell_encode(c) for c in cells], ",") == "1,alice,-5,=1+1,a|b,a;b,a\tb,\\N,"
    assert format_csv_row([csv_cell_encode(c) for c in cells]) == format_csv_row(
        [csv_cell_encode(c) for c in cells], ","
    )
