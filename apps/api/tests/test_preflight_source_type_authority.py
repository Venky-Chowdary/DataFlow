"""Validate must materialize the source's declared types, not re-infer them.

A SQLite ``id TEXT`` holding "0".."49" was re-inferred as INTEGER at Validate
while Execute kept TEXT, so the DDL identity gate refused a job whose Map never
changed — and an agreeing pair would have dropped leading zeros on write.
Schemaless sources (CSV / JSON / JSONL files) still get inference: their
declared types were themselves guessed. A self-describing source (Mongo,
Kafka, DynamoDB) declares each value's type, so its strings stay strings
(D190 / D194).
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.preflight_service import run_file_preflight  # noqa: E402

COLUMNS = ["id", "name"]
SAMPLE = [{"id": str(i), "name": f"Item {i}"} for i in range(5)]
MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0, "user_override": True},
    {"source": "name", "target": "name", "confidence": 1.0, "user_override": True},
]


def _stamped_types(source_kind: str, source_format: str) -> dict[str, str]:
    pf = run_file_preflight(
        columns=COLUMNS,
        column_types={"id": "TEXT", "name": "TEXT"},
        row_count=len(SAMPLE),
        mappings=MAPPINGS,
        destination_connected=True,
        sample_rows=SAMPLE,
        source_kind=source_kind,
        source_format=source_format,
        destination_db_type="sqlite",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        sync_mode="full_refresh_overwrite",
    )
    columns = ((pf.get("proof_bundle") or {}).get("ddl_identity") or {}).get(
        "columns"
    ) or []
    return {
        str(c.get("target")): str(c.get("materialized_ddl"))
        for c in columns
        if isinstance(c, dict)
    }


def test_relational_source_types_are_authoritative():
    assert _stamped_types("database", "sqlite") == {"id": "TEXT", "name": "TEXT"}


def test_self_describing_source_keeps_declared_strings():
    # D190 / D194: Mongo is not schemaless per value. A BSON string that looks
    # like digits ("0012", "20240101") is a string the source chose to send, so
    # samples never promote it to INTEGER (that dropped leading zeros).
    assert _stamped_types("database", "mongodb") == {"id": "TEXT", "name": "TEXT"}


def test_self_describing_source_still_types_real_numbers():
    # Inference still runs for Mongo: a field declared numeric keeps its type.
    pf = run_file_preflight(
        columns=COLUMNS,
        column_types={"id": "INTEGER", "name": "TEXT"},
        row_count=len(SAMPLE),
        mappings=MAPPINGS,
        destination_connected=True,
        sample_rows=[{"id": i, "name": f"Item {i}"} for i in range(5)],
        source_kind="database",
        source_format="mongodb",
        destination_db_type="sqlite",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        sync_mode="full_refresh_overwrite",
    )
    columns = ((pf.get("proof_bundle") or {}).get("ddl_identity") or {}).get(
        "columns"
    ) or []
    stamped = {str(c.get("target")): str(c.get("materialized_ddl")) for c in columns}
    assert stamped == {"id": "INTEGER", "name": "TEXT"}


def test_schemaless_json_file_source_still_infers_from_samples():
    # A JSON / JSONL file declares no types (its reader reports TEXT), so the
    # samples are the only evidence and digit-only text still promotes.
    assert _stamped_types("file", "json") == {"id": "INTEGER", "name": "TEXT"}
    assert _stamped_types("file", "jsonl") == {"id": "INTEGER", "name": "TEXT"}


def test_file_source_still_infers_from_samples():
    assert _stamped_types("file", "csv") == {"id": "INTEGER", "name": "TEXT"}
