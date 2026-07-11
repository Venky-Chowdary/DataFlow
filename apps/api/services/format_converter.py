"""File format conversion — CSV, JSON, JSONL, TSV."""

from __future__ import annotations

import csv
import io
import json
from typing import Any


SUPPORTED_CONVERSIONS: dict[str, set[str]] = {
    "csv": {"json", "jsonl", "tsv"},
    "tsv": {"csv", "json", "jsonl"},
    "json": {"csv", "jsonl"},
    "jsonl": {"csv", "json"},
}


def can_convert(source_format: str, target_format: str) -> bool:
    src = (source_format or "").lower()
    tgt = (target_format or "").lower()
    if src == tgt:
        return True
    return tgt in SUPPORTED_CONVERSIONS.get(src, set())


def convert_rows(
    headers: list[str],
    rows: list[list[str]],
    *,
    source_format: str,
    target_format: str,
) -> tuple[bytes, str]:
    """Convert tabular data between supported file formats."""
    src = (source_format or "csv").lower()
    tgt = (target_format or "csv").lower()
    if not can_convert(src, tgt):
        raise ValueError(f"Conversion {src} → {tgt} not supported")

    if tgt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(headers)
        writer.writerows(rows)
        return buf.getvalue().encode("utf-8"), "text/csv"

    if tgt == "tsv":
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter="\t")
        writer.writerow(headers)
        writer.writerows(rows)
        return buf.getvalue().encode("utf-8"), "text/tab-separated-values"

    if tgt == "json":
        objects = [dict(zip(headers, row)) for row in rows]
        return json.dumps(objects, indent=2).encode("utf-8"), "application/json"

    if tgt == "jsonl":
        lines = []
        for row in rows:
            lines.append(json.dumps(dict(zip(headers, row)), ensure_ascii=False))
        return ("\n".join(lines) + "\n").encode("utf-8"), "application/x-ndjson"

    raise ValueError(f"Unsupported target format: {tgt}")


def conversion_matrix() -> dict[str, Any]:
    return {
        "formats": sorted({*SUPPORTED_CONVERSIONS.keys(), "jsonl"}),
        "matrix": {k: sorted(v) for k, v in SUPPORTED_CONVERSIONS.items()},
    }
