"""Phase C7 — never claim optimal Hungarian after greedy / near-form patches."""

from __future__ import annotations

from services.semantic_mapper import map_columns


def test_pure_hungarian_keeps_optimal_label():
    mappings = map_columns(
        source_columns=["id", "name"],
        target_columns=["id", "name"],
        source_schemas=[
            {"name": "id", "inferred_type": "BIGINT"},
            {"name": "name", "inferred_type": "TEXT"},
        ],
        target_schemas=[
            {"name": "id", "inferred_type": "BIGINT"},
            {"name": "name", "inferred_type": "TEXT"},
        ],
        destination_db_type="postgresql",
    )
    strategies = {m["assignment_strategy"] for m in mappings}
    assert "hungarian_with_greedy_patch" not in strategies
    assert "optimal_bipartite_hungarian" in strategies


def test_create_new_patch_relabels_hungarian_rows():
    # Extra source column forces create_compatible_new after Hungarian fills exact pairs.
    mappings = map_columns(
        source_columns=["id", "name", "orphan_xyz_field"],
        target_columns=["id", "name"],
        source_schemas=[
            {"name": "id", "inferred_type": "BIGINT"},
            {"name": "name", "inferred_type": "TEXT"},
            {"name": "orphan_xyz_field", "inferred_type": "TEXT"},
        ],
        target_schemas=[
            {"name": "id", "inferred_type": "BIGINT"},
            {"name": "name", "inferred_type": "TEXT"},
        ],
        destination_db_type="postgresql",
    )
    by_src = {m["source"]: m for m in mappings}
    assert by_src["orphan_xyz_field"]["assignment_strategy"] == "create_compatible_new"
    assert by_src["id"]["assignment_strategy"] == "hungarian_with_greedy_patch"
    assert by_src["name"]["assignment_strategy"] == "hungarian_with_greedy_patch"
    assert all(
        m["assignment_strategy"] != "optimal_bipartite_hungarian" for m in mappings
    )
