"""Schema fingerprinting — detect drift between mapping and execution."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def fingerprint_schema(
    columns: list[str],
    column_types: dict[str, str] | None = None,
) -> str:
    """Stable hash of column names and inferred types."""
    column_types = column_types or {}
    payload = [
        {"name": c, "type": (column_types.get(c) or "VARCHAR").upper()}
        for c in sorted(columns)
    ]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def fingerprint_mappings(mappings: list[dict[str, Any]]) -> str:
    """Hash of approved mapping contract."""
    payload = [
        {
            "source": m.get("source"),
            "target": m.get("target"),
            "transform": m.get("transform"),
            "confidence": round(float(m.get("confidence", 0)), 3),
        }
        for m in sorted(mappings, key=lambda x: str(x.get("source", "")))
    ]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def schemas_match(stored_fp: str, columns: list[str], column_types: dict[str, str] | None) -> bool:
    return stored_fp == fingerprint_schema(columns, column_types)
