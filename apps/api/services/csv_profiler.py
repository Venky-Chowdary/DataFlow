"""CSV profiling — delimiter and encoding detection per plan Part 2."""

from __future__ import annotations

import csv
import io
from collections import Counter


def detect_encoding(content: bytes) -> str:
    if content.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    try:
        content.decode("utf-8")
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


def _decode(content: bytes, encoding: str | None) -> tuple[str, str]:
    enc = encoding or detect_encoding(content)
    return content.decode(enc, errors="replace"), enc


def parse_csv_preview(content: bytes, encoding: str | None = None, preview_rows: int = 100) -> tuple[list[str], list[list[str]], str, str]:
    """Parse header + preview rows for upload UI and dry-run."""
    text, enc = _decode(content, encoding)
    delim = detect_delimiter(text[:8192])
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    rows = list(reader)
    if not rows:
        return [], [], enc, delim
    data = rows[1 : preview_rows + 1]
    return rows[0], data, enc, delim


def count_csv_rows(content: bytes, encoding: str | None = None) -> int:
    """Stream-count data rows without loading all cells into memory."""
    text, _enc = _decode(content, encoding)
    delim = detect_delimiter(text[:8192])
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    count = 0
    for i, _row in enumerate(reader):
        if i == 0:
            continue
        count += 1
    return count


def parse_csv_full(content: bytes, encoding: str | None = None) -> tuple[list[str], list[list[str]], str, str]:
    """Full parse for transfer execution."""
    text, enc = _decode(content, encoding)
    delim = detect_delimiter(text[:8192])
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    rows = list(reader)
    if not rows:
        return [], [], enc, delim
    return rows[0], rows[1:], enc, delim


def parse_csv(content: bytes, encoding: str | None = None) -> tuple[list[str], list[list[str]], str, str]:
    """Backward-compatible alias — preview only."""
    return parse_csv_preview(content, encoding)
