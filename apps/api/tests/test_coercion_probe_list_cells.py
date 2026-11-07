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


def test_analyze_coercion_finds_folded_oracle_amount() -> None:
    """Oracle readers stamp AMOUNT; Map/introspect keep amount."""
    from services.transform_engine import dry_run_sample

    ok, errors = dry_run_sample(
        headers=["ID", "AMOUNT", "CODE"],
        sample_rows=[["1", "1000.00", "USD"], ["2", "2000.50", "EUR"]],
        mappings=[
            {"source": "id", "target": "id", "transform": "integer"},
            {"source": "amount", "target": "amount", "transform": "decimal"},
            {"source": "code", "target": "code", "transform": "none"},
        ],
        column_types={"id": "DECIMAL(38,0)", "amount": "DECIMAL(18,2)", "code": "VARCHAR"},
    )
    assert ok, errors


def test_lookup_row_value_and_header_index_fold() -> None:
    from services.column_case import header_index, lookup_row_value

    row = {"ID": "1", "AMOUNT": "1000.00"}
    assert lookup_row_value(row, "amount") == "1000.00"
    assert header_index(["ID", "AMOUNT", "CODE"], "amount") == 1
