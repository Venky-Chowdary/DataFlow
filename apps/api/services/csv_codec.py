"""C04 — one CSV NULL codec for every engine and path.

The rule (owner spec):

- an **unquoted empty** field is SQL NULL;
- a **quoted** ``""`` field is the empty string ``''``;
- on write, NULL is an **empty unquoted** cell and ``''`` is quoted ``""``;
- a ``\\N`` cell is NULL only when the reader is told so (``null_marker``:
  the source's declared NULL spelling, or a file this product wrote that
  way); otherwise it is the two-character text (DATA-21).

Python's ``csv.reader`` deliberately erases the quoting distinction, so this
module reads each record's raw text alongside the reader and aligns the parsed
values back onto it — the mask is *verified*, not re-parsed, so a disagreement
degrades to "unknown" (treated as unquoted) instead of mislabeling a cell.

Cell semantics at the record edge: unquoted empty → ``None``; quoted ``""`` →
``''``; ``\\N`` → ``None``. Every downstream consumer (writers, Gate-8, the
empty-cell carrier bind) then sees real ``None`` vs ``''`` — no per-engine
interpretation.
"""
from __future__ import annotations

import csv
from collections.abc import Callable, Iterator
from typing import IO, Any

_QUOTE = '"'
LEGACY_CSV_NULL = "\\N"

# Internal marker the formatter turns into an empty unquoted cell while a real
# empty string stays quoted ``""``. It is never written to the file.
_CSV_NULL = "\x00"


def decode_cell(
    value: str, was_quoted: bool | None, empty_cell: str = "null", null_marker: str = ""
) -> str | None:
    """One parsed CSV field → its database value.

    ``was_quoted=None`` means the mask could not be determined — the cell is
    treated as unquoted, matching the historical reader's behaviour of
    landing an empty field as absent data.
    """
    if null_marker and value == null_marker and was_quoted is not True:
        # A *quoted* marker is the text (PostgreSQL CSV rule: a quoted NULL
        # string is not NULL); the G7-F3 TSV ``\\N`` convention writes a
        # literal ``\\N`` value that way. Without a declared marker a ``\\N``
        # cell is text: silently landing NULL lost it (DATA-21).
        return None
    if value != "":
        return value
    if was_quoted is True:
        return ""
    if empty_cell == "keep":
        return ""
    return None


def _quote_masks(raw: str, row: list[str], delimiter: str) -> list[bool | None]:
    """Align ``csv.reader``'s parsed row with its raw record text.

    Returns a per-cell ``was_quoted`` mask. On any mismatch the whole row is
    reported ``[None, ...]`` — the cells still parse, the codec just cannot
    prove quoting and treats them as unquoted.
    """
    masks: list[bool | None] = []
    pos = 0
    n = len(raw)
    for value in row:
        if pos < n and raw[pos] == _QUOTE:
            literal = _QUOTE + value.replace(_QUOTE, _QUOTE * 2) + _QUOTE
            if raw.startswith(literal, pos):
                masks.append(True)
                pos += len(literal)
            else:
                return [None] * len(row)
        else:
            if raw.startswith(value, pos):
                masks.append(False)
                pos += len(value)
            else:
                return [None] * len(row)
        if pos < n and raw[pos] == delimiter:
            pos += 1
        elif pos < n and raw[pos] == "\r":
            pos += 1
        elif pos < n and raw[pos] == "\n":
            pos += 1
        # Anything else left before the next delimiter is trailing junk the
        # reader tolerated; leave pos where it is and let the next cell check.
    return masks


def iter_csv_records(
    stream: IO[str],
    delimiter: str = ",",
    *,
    empty_cell: str = "null",
    nulled: list[int] | None = None,
    null_marker: str = "",
) -> Iterator[list[str | None]]:
    """Yield decoded rows: unquoted empty → ``None``, quoted ``""`` → ``''``.

    ``empty_cell='keep'`` keeps an *unquoted* empty field as ``''`` too
    (legacy operator opt-in) — quoted ``""`` is always ``''``. Each record is
    consumed lazily so multi-line quoted fields stay intact. ``nulled`` is a
    one-cell counter incremented for every field that landed ``None`` — the
    run summary reports it as ``empty_string_as_null_cells``.
    """
    parts: list[str] = []

    def _lines() -> Iterator[str]:
        for line in stream:
            parts.append(line)
            yield line

    reader = csv.reader(_lines(), delimiter=delimiter)
    for row in reader:
        raw = "".join(parts)
        parts.clear()
        masks = _quote_masks(raw, row, delimiter)
        out = [
            decode_cell(v, q, empty_cell=empty_cell, null_marker=null_marker)
            for v, q in zip(row, masks, strict=False)
        ]
        if nulled is not None:
            nulled[0] += sum(
                1 for v, d in zip(row, out, strict=False) if v == "" and d is None
            )
        yield out


def csv_cell_encode(value: Any) -> str:
    """Encode one database value for an operator CSV file.

    ``None`` → the ``\\x00`` NULL sentinel the formatter emits as an empty
    unquoted cell; ``''`` → ``''`` (the formatter quotes it as ``""``); a
    text ``\\N`` stays that text (the formatter quotes it, so it never reads
    back as the legacy NULL spelling).
    """
    if value is None:
        return _CSV_NULL
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError("BLOB values are not operator-CSV writable")
    if value == "":
        return ""
    return str(value)


def csv_quote_cell(cell: str, delimiter: str = ",") -> str:
    if cell == _CSV_NULL:
        return ""
    if cell == "":
        return '""'
    if cell == LEGACY_CSV_NULL:
        # A *text* value spelled ``\\N`` is quoted: every reader of this codec
        # takes a bare ``\\N`` as the legacy NULL spelling, so written bare it
        # read back as NULL (a 2-file combine, the S3 COPY writers).
        return _QUOTE + cell + _QUOTE
    if any(c in cell for c in (delimiter, '"', "\n", "\r")):
        return '"' + cell.replace('"', '""') + '"'
    return cell


def text_null_cell(cell: str, marker: str, quote: Callable[[str], str]) -> str:
    """One encoded cell in the MySQL/PostgreSQL text NULL convention (G7-F3, opt-in).

    NULL is ``marker`` (``\\N``) unquoted, ``''`` an empty unquoted cell, and a
    text value equal to the marker is quoted so it reads back as that text.
    Every other cell is quoted exactly as ``quote`` (the writer's own rule) does.
    """
    if cell == _CSV_NULL:
        return marker
    if cell == "":
        return ""
    if cell == marker:
        return _QUOTE + cell.replace(_QUOTE, _QUOTE * 2) + _QUOTE
    return quote(cell)


#: RFC 4180 record separator. CSV and TSV exports end every row with CRLF.
RECORD_TERMINATOR = "\r\n"


def format_csv_row(cells: list[str], delimiter: str = ",", *, null_marker: str = "") -> str:
    """One encoded row as delimited text (no record separator).

    CSV and TSV use one quoting rule (RFC 4180, migration rule 9): NULL is an
    empty unquoted field, ``''`` is ``""``, and a field that contains the
    delimiter, a quote, CR or LF is quoted with doubled quotes. ``null_marker``
    (``\\N``) opts into :func:`text_null_cell`.
    """

    def quote(cell: str) -> str:
        return csv_quote_cell(cell, delimiter)
    if null_marker:
        return delimiter.join(text_null_cell(c, null_marker, quote) for c in cells)
    return delimiter.join(quote(c) for c in cells)


def csv_null_sentinel() -> str:
    return _CSV_NULL
