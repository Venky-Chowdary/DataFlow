"""CSV profiling — delimiter and encoding detection per plan Part 2.

Dest-engine CSV/TSV COUNT (``count_csv_rows``) is RFC 4180 ``csv.reader``
records, not ``wc -l``. Quoted embedded newlines are one row. Header is
not a record. ``is_blank_row`` lines (empty / ``,,,,``) are not records.
Path inputs stream from disk; encoding and delimiter are sniffed from a
prefix, not from a slurp of the whole export. A one-shot gzip / GET
stream is prefix-then-rest — COUNT does not ``seek(0)`` the HTTP body.
"""

from __future__ import annotations

import csv
import io
from collections import Counter
from pathlib import Path
from typing import Any

from services.tabular_rows import is_blank_row

_ENCODING_PREFIX = 65536
_DELIMITER_PREFIX = 8192


class _PrefixedRaw(io.RawIOBase):
    """Replay an already-read prefix, then the rest of a binary source.

    CSV encoding sniff consumes up to 64 KiB. Hadoop / Spark CSV readers
    keep that sample and continue the parse; they do not rewind a
    non-splittable gzip GET. ``seek(0)`` on ``GzipFile`` over ``BytesIO``
    works only because the GET already materialized. Prefix-then-rest is
    the same COUNT on a one-shot stream.
    """

    def __init__(self, prefix: bytes, rest: Any) -> None:
        super().__init__()
        self._prefix = prefix
        self._rest = rest

    def readable(self) -> bool:
        return True

    def readinto(self, b: Any) -> int:
        mv = memoryview(b)
        n = len(mv)
        if n == 0:
            return 0
        if self._prefix:
            take = min(n, len(self._prefix))
            mv[:take] = self._prefix[:take]
            self._prefix = self._prefix[take:]
            return take
        chunk = self._rest.read(n)
        if not chunk:
            return 0
        if not isinstance(chunk, (bytes, bytearray)):
            raise TypeError("CSV COUNT stream must yield bytes")
        mv[: len(chunk)] = chunk
        return len(chunk)


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


def _csv_prefix_bytes(content: bytes | str) -> bytes:
    """BOM / utf-8 / latin-1 sniff from in-RAM CSV. Path/stream sniff in COUNT."""
    if isinstance(content, bytes):
        return content[:_ENCODING_PREFIX]
    return content.encode("utf-8")[:_ENCODING_PREFIX]


def _csv_text_from_binary(source: Any, prefix: bytes, encoding: str) -> io.TextIOBase:
    """RFC 4180 reader over prefix + remainder. Never seeks ``source``."""
    chained = io.BufferedReader(_PrefixedRaw(prefix, source))
    return io.TextIOWrapper(
        chained, encoding=encoding, errors="replace", newline=""
    )


def _csv_count_open(content: bytes | str, encoding: str) -> io.TextIOBase:
    """Text stream for in-RAM CSV. ``newline=''`` keeps quoted newlines one row."""
    if isinstance(content, bytes):
        return io.TextIOWrapper(
            io.BytesIO(content), encoding=encoding, errors="replace", newline=""
        )
    return io.StringIO(content)


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


def _csv_count_rows_from_reader(reader_file: io.TextIOBase, delim: str) -> int:
    reader = csv.reader(reader_file, delimiter=delim)
    count = 0
    for i, row in enumerate(reader):
        if i == 0:
            continue
        if is_blank_row(row):
            continue
        count += 1
    return count


def count_csv_rows(content: bytes | str | Path, encoding: str | None = None) -> int:
    """Dest-engine record COUNT of CSV/TSV. Never ``wc -l``. Never ingest parse.

    Population is RFC 4180 ``csv.reader`` rows after the header.
    ``is_blank_row`` lines are not dest rows (spreadsheet ``,,,,`` export).
    A quoted field that contains a newline is one record, not N physical
    lines. Header-only / empty is 0. Encoding and delimiter are sniffed
    from a prefix (BOM → utf-8-sig, else utf-8 else latin-1) so a GB
    export is not decoded twice. In-RAM bytes/str keep a second view of
    the same buffer. Path gzip is one ``gzip.open``, not a prefix open
    plus a COUNT open. A one-shot stream (object-store GET gzip that is
    not rewindable) is prefix-then-rest — COUNT does not ``seek(0)``.
    """
    if isinstance(content, (bytes, str)):
        prefix = _csv_prefix_bytes(content)
        enc = encoding or detect_encoding(prefix)
        sample = prefix[:_DELIMITER_PREFIX].decode(enc, errors="replace")
        delim = detect_delimiter(sample)
        reader_file = _csv_count_open(content, enc)
        try:
            return _csv_count_rows_from_reader(reader_file, delim)
        finally:
            reader_file.close()

    closer = None
    reader_file: io.TextIOBase | None = None
    try:
        if isinstance(content, Path):
            from services.dest_precount import open_artifact_binary

            source, closer = open_artifact_binary(content)
        elif hasattr(content, "read"):
            source = content
        else:
            raise TypeError("CSV COUNT expects bytes, str, Path, or a readable stream")
        prefix = source.read(_ENCODING_PREFIX)
        if not isinstance(prefix, (bytes, bytearray)):
            raise TypeError("CSV COUNT stream must yield bytes")
        prefix_b = bytes(prefix)
        enc = encoding or detect_encoding(prefix_b)
        sample = prefix_b[:_DELIMITER_PREFIX].decode(enc, errors="replace")
        delim = detect_delimiter(sample)
        reader_file = _csv_text_from_binary(source, prefix_b, enc)
        return _csv_count_rows_from_reader(reader_file, delim)
    finally:
        if reader_file is not None:
            try:
                reader_file.close()
            except Exception:
                pass
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


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
