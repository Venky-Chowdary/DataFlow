"""The dialect this product writes its own delimited exports in, for reading them back (review 3, H2).

A file export is written by :mod:`src.transfer.adapters_file_write`: UTF-8, a
header row, ``,`` (CSV) or a tab (TSV), ``"`` quoting, SQL NULL as an empty
unquoted cell and empty text as ``""`` (the C04 codec, :mod:`services.csv_codec`).
Reading such a file back must use exactly that, never a sniffer: a sniffer
reads text full of ``;`` or ``|`` as another delimiter, and a one-row file
with a quoted JSON cell as one column. Both the engine's own read-back of an
export it just wrote and the rehearsal's verification read through here.
"""

from __future__ import annotations

from typing import Any

#: Format -> the field delimiter the export writer uses.
DELIMITERS = {"csv": ",", "tsv": "\t"}


def written_dialect(fmt: str | None) -> Any | None:
    """The read options of an export written here in ``fmt`` (``None``: not a delimited format)."""
    delimiter = DELIMITERS.get(str(fmt or "").strip().lower().lstrip("."))
    if delimiter is None:
        return None
    from services.read_options import ReadOptions

    # Declared, not sniffed: the delimiter, the codec, and row 1 as the header (never "decide from the data").
    return ReadOptions(delimiter=delimiter, encoding="utf-8", header_row=1, sniff_header=False, empty_cell="null")


def format_written_row(cells: list[Any], delimiter: str) -> str:
    """One export row in the C04 codec: NULL an empty unquoted cell, ``''`` quoted ``""``.

    Anything else is quoted only when it holds the delimiter, a quote or a line
    break. Unlike :func:`services.csv_codec.format_csv_row`'s TSV branch (bulk
    COPY payloads, never quoted), a TSV export quotes too, so a tab or a line
    break inside a value stays one cell.
    """
    from services.csv_codec import csv_cell_encode, csv_quote_cell

    return delimiter.join(csv_quote_cell(csv_cell_encode(cell), delimiter) for cell in cells)
