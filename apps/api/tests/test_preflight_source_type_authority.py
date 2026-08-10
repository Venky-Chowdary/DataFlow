"""Validate must materialize the source's declared types, not re-infer them.

A SQLite ``id TEXT`` holding "0".."49" was re-inferred as INTEGER at Validate
while Execute kept TEXT, so the DDL identity gate refused a job whose Map never
changed — and an agreeing pair would have dropped leading zeros on write.
Schemaless sources (Mongo/S3 JSON) still get inference: their declared types
were themselves guessed.
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


def test_schemaless_source_still_infers_from_samples():
    # Mongo declares nothing binding, so digit-only samples still promote.
    assert _stamped_types("database", "mongodb") == {"id": "INTEGER", "name": "TEXT"}


def test_file_source_still_infers_from_samples():
    assert _stamped_types("file", "csv") == {"id": "INTEGER", "name": "TEXT"}
