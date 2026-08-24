"""Names-only existing destination must not invent create_compatible_new."""

from __future__ import annotations


def test_names_only_existing_blocks_create_compatible_new():
    from services.semantic_mapper import map_columns

    # Existing table with column names but no typed schemas — invent cliff.
    mappings = map_columns(
        source_columns=["lat", "lon", "orphan_metric"],
        target_columns=["lat", "lon"],
        source_schemas=[
            {"name": "lat", "inferred_type": "DECIMAL(10,6)"},
            {"name": "lon", "inferred_type": "DECIMAL(10,6)"},
            {"name": "orphan_metric", "inferred_type": "DOUBLE"},
        ],
        target_schemas=None,
        destination_db_type="postgresql",
        destination_table_exists=True,
    )
    by_src = {m["source"]: m for m in mappings}
    assert by_src["orphan_metric"]["assignment_strategy"] == "pending_dest_schema"
    assert by_src["orphan_metric"].get("create_new") is False
    assert by_src["orphan_metric"].get("requires_review") is True
    # Pending dest must not invent target_type from source (Validate invent cliff).
    assert not str(by_src["orphan_metric"].get("target_type") or "").strip()
    # Matched names still map; invent path is what we refuse.
    assert by_src["lat"]["target"].lower() == "lat"
    assert by_src["lat"].get("create_new") is not True


def test_names_only_new_table_allows_create_compatible_new():
    from services.semantic_mapper import map_columns

    mappings = map_columns(
        source_columns=["amount"],
        target_columns=["other"],
        source_schemas=[{"name": "amount", "inferred_type": "DECIMAL(10,2)"}],
        target_schemas=None,
        destination_db_type="postgresql",
        destination_table_exists=False,
    )
    by_src = {m["source"]: m for m in mappings}
    assert by_src["amount"].get("create_new") is True
    assert by_src["amount"]["assignment_strategy"] == "create_compatible_new"
