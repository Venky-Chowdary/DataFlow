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


#: G7-F3: how a TSV destination spells SQL NULL. ``"empty"`` (default, every
#: existing job): an empty cell. ``"\\N"`` (opt-in, the MySQL/PostgreSQL text
#: format): NULL is ``\\N``, ``''`` an empty cell, a literal ``\\N`` text quoted.
TSV_NULL_MODES = ("empty", "\\N")
TSV_NULL_MARKER = "\\N"


def tsv_null_mode(value: object) -> str:
    """``"empty"`` or ``"\\N"``; unset means ``"empty"``. Anything else is refused."""
    text = "" if value is None else str(value).strip()
    if not text or text.lower() == "empty":
        return "empty"
    if text == TSV_NULL_MARKER:
        return text
    raise ValueError(f"tsv_null must be one of {', '.join(TSV_NULL_MODES)}; got {value!r}")


def tsv_null_from(cfg: Any) -> str:
    """The ``tsv_null`` mode a destination config (or its ``extra``) declares."""
    if not isinstance(cfg, dict):
        return "empty"
    nested = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}

    def _given(value: Any) -> Any:
        # Blank (None / "" / spaces) is "not set" — the writer's rule too
        # (connector_dispatch.file_writer_dest_extra).
        return value if value is not None and str(value).strip() else None

    return tsv_null_mode(_given(cfg.get("tsv_null")) or _given(nested.get("tsv_null")))


def tsv_null_marker(fmt: str | None, mode: str | None) -> str:
    """The NULL spelling a ``fmt`` file is written with: ``\\N`` or ``""`` (the default codec).

    ``\\N`` applies to TSV only; asking for it on any other format is refused,
    never silently ignored.
    """
    if tsv_null_mode(mode) == "empty":
        return ""
    if str(fmt or "").strip().lower().lstrip(".") != "tsv":
        raise ValueError(f"tsv_null {TSV_NULL_MARKER!r} applies to TSV files only, not {fmt or 'this format'!r}")
    return TSV_NULL_MARKER


def written_dialect(fmt: str | None, *, tsv_null: str | None = None) -> Any | None:
    """The read options of an export written here in ``fmt`` (``None``: not a delimited format).

    ``tsv_null="\\N"``: an unquoted empty cell is ``''`` and ``\\N`` is NULL.
    """
    delimiter = DELIMITERS.get(str(fmt or "").strip().lower().lstrip("."))
    if delimiter is None:
        return None
    from services.read_options import ReadOptions

    marker = tsv_null_marker(fmt, tsv_null)
    empty_cell = "keep" if marker else "null"
    # Declared, not sniffed: the delimiter, the codec, and row 1 as the header (never "decide from the data").
    return ReadOptions(delimiter=delimiter, encoding="utf-8", header_row=1, sniff_header=False,
                       empty_cell=empty_cell, null_marker=marker or "")


def format_written_row(cells: list[Any], delimiter: str, *, null_marker: str = "") -> str:
    """One export row in the C04 codec: NULL an empty unquoted cell, ``''`` quoted ``""``.

    Anything else is quoted only when it holds the delimiter, a quote or a line
    break. Unlike :func:`services.csv_codec.format_csv_row`'s TSV branch (bulk
    COPY payloads, quoted only when a cell would split: a tab, CR, LF or a
    leading quote), a TSV export quotes like CSV, so a tab or a line break
    inside a value stays one cell and can never start a new (formula) cell.
    ``null_marker="\\N"`` writes the text NULL convention instead
    (:func:`services.csv_codec.text_null_cell`).
    """
    from services.csv_codec import csv_cell_encode, csv_quote_cell, text_null_cell

    def quote(cell: str) -> str:
        return csv_quote_cell(cell, delimiter)

    if null_marker:
        return delimiter.join(text_null_cell(csv_cell_encode(cell), null_marker, quote) for cell in cells)
    return delimiter.join(quote(csv_cell_encode(cell)) for cell in cells)
