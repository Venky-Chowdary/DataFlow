"""DATA-23: a list that JSON can spell exactly is not lossy into JSON/JSONB.

A Parquet ``list<int64>`` landed in a PostgreSQL JSONB column (the create-new
carrier; DataFlow does not invent ``BIGINT[]``) was graded ARRAY -> JSONB
lossy and refused, although every element round-trips. The typed-array
collapse stays for a PostgreSQL-family source: its ``INTEGER[]`` carries
lower bounds and dimensions JSON has no place for (pinned in
``test_mongo_mysql_array_json_fidelity``). For a list-shaped source the grade
follows the elements: integers, text, booleans and nested lists of them are
exact in JSON; a float list is exact when every sampled float is finite
(NaN and infinities have no JSON spelling).
"""

from __future__ import annotations

import json
import math
import re
from typing import Any

#: Sources whose arrays are plain 0-based lists (no bounds, no dimensions).
LIST_SHAPED_SOURCES = frozenset({
    "parquet", "avro", "orc", "arrow", "feather", "json", "jsonl", "ndjson",
    "mongodb", "mongo", "bigquery", "duckdb", "elasticsearch", "dynamodb",
})
_EXACT_ELEMENTS = frozenset({"integer", "string", "text", "boolean"})


def _element_type(source_type: str) -> str:
    text = " ".join(str(source_type or "").split())
    m = re.match(r"^(?:ARRAY|LIST)\s*<(.*)>$", text, re.I)
    if m:
        return m.group(1).strip()
    m = re.match(r"^(.*?)\s*\[\]$", text)
    return m.group(1).strip() if m else ""


def _finite(value: Any) -> bool:
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, (list, tuple)):
        return all(_finite(v) for v in value)
    if isinstance(value, dict):
        return all(_finite(v) for v in value.values())
    return True


def _population_floats_finite(population: Any) -> bool:
    if not isinstance(population, (list, tuple)) or not population:
        return False
    for cell in population:
        if cell is None or cell == "":
            continue
        if isinstance(cell, str):
            try:
                cell = json.loads(cell)
            except ValueError:
                return False
        if not _finite(cell):
            return False
    return True


def _elements_exact(element: str, population: Any) -> bool:
    from services.type_system import normalize_logical_type

    if not element:
        return False
    inner = _element_type(element)
    if inner:
        return _elements_exact(inner, population)
    logical = normalize_logical_type(element)
    if logical in _EXACT_ELEMENTS:
        return True
    if logical == "float":
        return _population_floats_finite(population)
    return False


def list_lands_exactly_as_json(
    source_type: str, target_type: str, *, dest_db: str, source_engine: str, population: Any = None
) -> bool:
    """True when this list column's values are spelled exactly by the JSON target."""
    from services.type_system import is_dialect_native_document_wire, normalize_logical_type

    if (source_engine or "").strip().lower() not in LIST_SHAPED_SOURCES:
        return False
    if normalize_logical_type(source_type) != "array":
        return False
    if normalize_logical_type(target_type) != "json":
        return False
    if not is_dialect_native_document_wire(target_type, dest_db=dest_db):
        return False
    return _elements_exact(_element_type(source_type), population)
