"""CSV profiling — delimiter and encoding detection per plan Part 2."""

from __future__ import annotations

import csv
import io
from collections import Counter


def detect_encoding(content: bytes) -> str:
    """Return encoding without loading the whole file into a decoded string."""
    if content.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    sample = content[:65536]
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


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
    sample = content[:8192].decode(enc, errors="replace")
    delim = detect_delimiter(sample)
    with _text_reader(content, enc) as reader_file:
        reader = csv.reader(reader_file, delimiter=delim)
        try:
            headers = next(reader)
        except StopIteration:
            return [], [], enc, delim
        preview: list[list[str]] = []
        for i, row in enumerate(reader):
            if i >= preview_rows:
                break
            preview.append(row)
    return headers, preview, enc, delim


def count_csv_rows(content: bytes, encoding: str | None = None) -> int:
    """Stream-count data rows without loading all cells into memory."""
    enc = encoding or detect_encoding(content)
    sample = content[:8192].decode(enc, errors="replace")
    delim = detect_delimiter(sample)
    with _text_reader(content, enc) as reader_file:
        reader = csv.reader(reader_file, delimiter=delim)
        count = 0
        for i, _row in enumerate(reader):
            if i == 0:
                continue
            count += 1
    return count


def parse_csv_full(content: bytes, encoding: str | None = None) -> tuple[list[str], list[list[str]], str, str]:
    """Full parse for transfer execution."""
    enc = encoding or detect_encoding(content)
    sample = content[:8192].decode(enc, errors="replace")
    delim = detect_delimiter(sample)
    with _text_reader(content, enc) as reader_file:
        reader = csv.reader(reader_file, delimiter=delim)
        rows = list(reader)
    if not rows:
        return [], [], enc, delim
    return rows[0], rows[1:], enc, delim


def parse_csv(content: bytes, encoding: str | None = None) -> tuple[list[str], list[list[str]], str, str]:
    """Backward-compatible alias — preview only."""
    return parse_csv_preview(content, encoding)
