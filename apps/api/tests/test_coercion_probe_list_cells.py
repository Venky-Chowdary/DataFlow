"""Coercion probe must not crash on Mongo/array list cells (unhashable)."""

from __future__ import annotations

from services.coercion_probe import analyze_coercion


def test_analyze_coercion_tolerates_list_cells() -> None:
    report = analyze_coercion(
        mappings=[
            {"source": "_id", "target": "_id", "confidence": 0.99, "transform": "none"},
            {
                "source": "skills",
                "target": "skills",
                "confidence": 0.95,
                "transform": "json",
                "target_type": "JSON",
            },
        ],
        sample_rows=[
            {"_id": "1", "skills": ["a", "b"]},
            {"_id": "2", "skills": ["c"]},
        ],
        source_types={"_id": "VARCHAR", "skills": "ARRAY"},
        dest_types={"_id": "VARCHAR", "skills": "JSON"},
        dest_db_type="postgresql",
    )
    assert isinstance(report, dict)
    # Must complete — prior bug: TypeError unhashable type: 'list'
    assert "columns" in report or "by_source" in report or "checked" in report


def test_analyze_coercion_tolerates_dict_cells() -> None:
    report = analyze_coercion(
        mappings=[
            {
                "source": "payload",
                "target": "payload",
                "confidence": 0.95,
                "transform": "json",
                "target_type": "JSON",
            },
        ],
        sample_rows=[{"payload": {"a": 1}}],
        source_types={"payload": "JSON"},
        dest_types={"payload": "JSON"},
        dest_db_type="mongodb",
    )
    assert isinstance(report, dict)
