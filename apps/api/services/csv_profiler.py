"""CSV profiling — delimiter and encoding detection per plan Part 2.

Dest-engine CSV/TSV COUNT (``count_csv_rows``) is RFC 4180 ``csv.reader``
records, not ``wc -l``. Quoted embedded newlines are one row. Header is
not a record. ``is_blank_row`` lines (empty / ``,,,,``) are not records.
Path inputs stream from disk; encoding and delimiter are sniffed from a
prefix, not from a slurp of the whole export.
"""

from __future__ import annotations

import csv
import io
from collections import Counter
from pathlib import Path

from services.tabular_rows import is_blank_row

_ENCODING_PREFIX = 65536
_DELIMITER_PREFIX = 8192


def detect_encoding(content: bytes) -> str:
    """Return encoding without loading the whole file into a decoded string."""
    if content.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    sample = content[:_ENCODING_PREFIX]
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def _csv_prefix_bytes(content: bytes | str | Path) -> bytes:
    """BOM / utf-8 / latin-1 sniff. Path reads a prefix, never the whole file."""
    if isinstance(content, Path):
        with content.open("rb") as handle:
            return handle.read(_ENCODING_PREFIX)
    if isinstance(content, bytes):
        return content[:_ENCODING_PREFIX]
    if isinstance(content, str):
        return content.encode("utf-8")[:_ENCODING_PREFIX]
    raise TypeError("CSV COUNT expects bytes, str, or Path")


def _csv_count_open(content: bytes | str | Path, encoding: str) -> io.TextIOBase:
    """Text stream for ``csv.reader``. ``newline=''`` keeps quoted newlines one row."""
    if isinstance(content, Path):
        binary = content.open("rb")
        return io.TextIOWrapper(binary, encoding=encoding, errors="replace", newline="")
    if isinstance(content, bytes):
        return io.TextIOWrapper(
            io.BytesIO(content), encoding=encoding, errors="replace", newline=""
        )
    if isinstance(content, str):
        return io.StringIO(content)
    raise TypeError("CSV COUNT expects bytes, str, or Path")


def detect_delimiter(sample: str) -> str:
    candidates = [",", ";", "\t", "|"]
    lines = sample.splitlines()[:10]
    if not lines:
        return ","
    scores: dict[str, float] = {}
    for delim in candidates:
        counts = [line.count(delim) for line in lines if line.strip()]
        if not counts:
            scores[delim] = 0
            continue
        mode = Counter(counts).most_common(1)[0][0]
        if mode == 0:
            scores[delim] = 0
            continue
        variance = sum(abs(c - mode) for c in counts) / len(counts)
        scores[delim] = mode - variance
    return max(scores, key=scores.get)


def _text_reader(content: bytes, encoding: str | None = None):
    """Return a streaming text reader for CSV content without a full decode."""
    enc = encoding or detect_encoding(content)
    return io.TextIOWrapper(io.BytesIO(content), encoding=enc, errors="replace", newline="")


def parse_csv_preview(content: bytes, encoding: str | None = None, preview_rows: int = 100) -> tuple[list[str], list[list[str]], str, str]:
    """Parse header + preview rows without loading the whole file into memory."""
    enc = encoding or detect_encoding(content)
    sample = content[:_DELIMITER_PREFIX].decode(enc, errors="replace")
    delim = detect_delimiter(sample)
    with _text_reader(content, enc) as reader_file:
        reader = csv.reader(reader_file, delimiter=delim)
        try:
            headers = next(reader)
        except StopIteration:
            return [], [], enc, delim
        preview: list[list[str]] = []
        for row in reader:
            # A blank line or a spreadsheet-exported ``,,,,`` line holds no
            # field value; counting it as a record lands an all-NULL row.
            if is_blank_row(row):
                continue
            if len(preview) >= preview_rows:
                break
            preview.append(row)
    return headers, preview, enc, delim


def count_csv_rows(content: bytes | str | Path, encoding: str | None = None) -> int:
    """Dest-engine record COUNT of CSV/TSV. Never ``wc -l``. Never ingest parse.

    Population is RFC 4180 ``csv.reader`` rows after the header.
    ``is_blank_row`` lines are not dest rows (spreadsheet ``,,,,`` export).
    A quoted field that contains a newline is one record, not N physical
    lines. Header-only / empty is 0. Encoding and delimiter are sniffed
    from a prefix (BOM → utf-8-sig, else utf-8 else latin-1) so a GB
    export is not decoded twice. Path inputs reopen from disk;
    bytes (object-store GET) stream from a buffer already in RAM.
    gzip still decompresses first at the artifact dispatcher.
    """
    prefix = _csv_prefix_bytes(content)
    enc = encoding or detect_encoding(prefix)
    sample = prefix[:_DELIMITER_PREFIX].decode(enc, errors="replace")
    delim = detect_delimiter(sample)
    reader_file = _csv_count_open(content, enc)
    try:
        reader = csv.reader(reader_file, delimiter=delim)
        count = 0
        for i, row in enumerate(reader):
            if i == 0:
                continue
            if is_blank_row(row):
                continue
            count += 1
        return count
    finally:
        reader_file.close()


def parse_csv_full(content: bytes, encoding: str | None = None) -> tuple[list[str], list[list[str]], str, str]:
    """Full parse for transfer execution."""
    enc = encoding or detect_encoding(content)
    sample = content[:_DELIMITER_PREFIX].decode(enc, errors="replace")
    delim = detect_delimiter(sample)
    with _text_reader(content, enc) as reader_file:
        reader = csv.reader(reader_file, delimiter=delim)
        rows = list(reader)
    if not rows:
        return [], [], enc, delim
    return rows[0], [r for r in rows[1:] if not is_blank_row(r)], enc, delim


def parse_csv(content: bytes, encoding: str | None = None) -> tuple[list[str], list[list[str]], str, str]:
    """Backward-compatible alias — preview only."""
    return parse_csv_preview(content, encoding)
