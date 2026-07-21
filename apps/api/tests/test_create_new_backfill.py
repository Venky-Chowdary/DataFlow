"""create_new mappings must trigger ADD COLUMN (Snowflake id_text class failures)."""

from __future__ import annotations

from services.batch_progress import effective_backfill_new_fields, mappings_require_new_columns
from services.semantic_mapper import map_columns


def test_objectid_create_new_target_preserves_underscore_id():
    samples = [
        "693486a0f0d881be6f0c470e",
        "69349183a44dd21d08a19c2c",
        "6934a44da44dd21d08a1ac18",
        "6934b905a44dd21d08a1caca",
    ]
    out = map_columns(
        ["_id", "name"],
        ["id", "name"],
        source_schemas=[
            {"name": "_id", "inferred_type": "VARCHAR", "samples": samples},
            {"name": "name", "inferred_type": "VARCHAR", "samples": ["Ada"]},
        ],
        target_schemas=[
            {"name": "id", "inferred_type": "DECIMAL"},
            {"name": "name", "inferred_type": "VARCHAR"},
        ],
        destination_db_type="snowflake",
    )
    by = {m["source"]: m for m in out}
    assert by["_id"]["target"] == "_id"
    assert by["_id"].get("create_new") is True
    assert by["_id"]["target"] != "id_text"


def test_create_new_enables_effective_backfill_for_existing_table_writes():
    mappings = [
        {
            "source": "_id",
            "target": "_id",
            "create_new": True,
            "assignment_strategy": "create_compatible_new",
        },
        {"source": "name", "target": "name"},
    ]
    assert mappings_require_new_columns(mappings)
    assert effective_backfill_new_fields(
        backfill_new_fields=False,
        schema_policy="manual_review",
        mappings=mappings,
    )
