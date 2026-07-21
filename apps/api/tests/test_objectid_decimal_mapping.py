"""ObjectId / text identifiers must not glue onto DECIMAL destination `id`."""

from __future__ import annotations

from services.semantic_mapper import map_columns


def test_mongo_object_id_does_not_map_to_decimal_id():
    samples = [
        "693486a0f0d881be6f0c470e",
        "69349183a44dd21d08a19c2c",
        "6934a44da44dd21d08a1ac18",
        "6934b905a44dd21d08a1caca",
    ]
    out = map_columns(
        ["_id"],
        ["id", "column_2", "column_5"],
        source_schemas=[{"name": "_id", "inferred_type": "VARCHAR", "samples": samples}],
        target_schemas=[
            {"name": "id", "inferred_type": "DECIMAL"},
            {"name": "column_2", "inferred_type": "VARCHAR"},
            {"name": "column_5", "inferred_type": "VARCHAR"},
        ],
        threshold=0.75,
        destination_db_type="snowflake",
    )
    assert len(out) == 1
    assert out[0]["target"].lower() != "id"
    assert out[0].get("create_new") is True or out[0]["target"] in {"column_2", "column_5"}
