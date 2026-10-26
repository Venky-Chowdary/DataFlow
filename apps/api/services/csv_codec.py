"""C04 — one CSV NULL codec for every engine and path.

The rule (owner spec):

- an **unquoted empty** field is SQL NULL;
- a **quoted** ``""`` field is the empty string ``''``;
- on write, NULL is an **empty unquoted** cell and ``''`` is quoted ``""``;
- the legacy ``\\N`` spelling still *reads* as NULL for files written by older
  builds (and for PostgreSQL COPY payloads, which carry their own NULL marker).

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
from collections.abc import Iterator
from typing import IO, Any

_QUOTE = '"'
LEGACY_CSV_NULL = "\\N"

# Internal marker the formatter turns into an empty unquoted cell while a real
# empty string stays quoted ``""``. It is never written to the file.
_CSV_NULL = "\x00"


def decode_cell(value: str, was_quoted: bool | None, empty_cell: str = "null") -> str | None:
    """One parsed CSV field → its database value.

    ``was_quoted=None`` means the mask could not be determined — the cell is
    treated as unquoted, matching the historical reader's behaviour of
    landing an empty field as absent data.
    """
    if value == LEGACY_CSV_NULL:
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
            decode_cell(v, q, empty_cell=empty_cell)
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
    unquoted cell; ``''`` → ``''`` (the formatter quotes it as ``""``);
    ``\\N`` is preserved verbatim for COPY-convention writers.
    """
    if value is None:
        return _CSV_NULL
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError("BLOB values are not operator-CSV writable")
    if value == "":
        return ""
    return str(value)


def csv_quote_cell(cell: str) -> str:
    if cell == _CSV_NULL:
        return ""
    if cell == "":
        return '""'
    if cell == LEGACY_CSV_NULL:
        return LEGACY_CSV_NULL
    if any(c in cell for c in ',"\n\r'):
        return '"' + cell.replace('"', '""') + '"'
    return cell


def format_csv_row(cells: list[str], delimiter: str = ",") -> str:
    """One encoded row as CSV text (no trailing newline)."""
    if delimiter == "\t":
        return "\t".join("" if c == _CSV_NULL else c for c in cells)
    return delimiter.join(csv_quote_cell(c) for c in cells)


def csv_null_sentinel() -> str:
    return _CSV_NULL
