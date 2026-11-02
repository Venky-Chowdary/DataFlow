"""A NULL is absence of evidence — never evidence that a column is text.

Regression: a PostgreSQL→MySQL Map turned 14 ``DECIMAL(n,3)`` columns into
``<col>_text`` LONGTEXT ("fidelity collapse") because the sampled cells carried
the ``__DF_SQL_NULL__`` wire sentinel, which the numeric-fit repair read as
non-numeric strings.
"""

from __future__ import annotations

from services.mapping_pipeline import run_mapping_pipeline
from services.schema_inference import infer_column, samples_fit_logical_type
from services.value_serializer import (
    SQL_NULL_SENTINEL,
    evidence_samples,
    is_null_evidence,
)

NUMERIC_COLS = {
    "total": "DECIMAL(7,3)",
    "e1_economy": "DECIMAL(5,3)",
    "c1_security_apparatus": "DECIMAL(6,3)",
}


def test_sentinel_is_not_evidence() -> None:
    assert is_null_evidence(SQL_NULL_SENTINEL)
    assert is_null_evidence(None)
    assert is_null_evidence("  ")
    assert not is_null_evidence("0")
    assert evidence_samples([SQL_NULL_SENTINEL, None, "1.5", ""]) == ["1.5"]


def test_null_samples_do_not_falsify_a_numeric_type() -> None:
    assert samples_fit_logical_type([SQL_NULL_SENTINEL] * 5, "DECIMAL(7,3)")
    assert infer_column([SQL_NULL_SENTINEL] * 5, field_name="total")["samples"] == []


def test_all_null_decimal_column_keeps_its_declared_numeric_type() -> None:
    cols = list(NUMERIC_COLS)
    result = run_mapping_pipeline(
        cols,
        [],
        source_schemas=[
            {"name": c, "inferred_type": t, "native_type": t, "samples": [SQL_NULL_SENTINEL] * 8}
            for c, t in NUMERIC_COLS.items()
        ],
        source_samples={c: [SQL_NULL_SENTINEL] * 8 for c in cols},
        use_llm=False,
        destination_db_type="mysql",
        destination_table_exists=False,
        source_types_authoritative=True,
    )
    by_source = {m["source"]: m for m in result["mappings"]}
    for col in cols:
        m = by_source[col]
        assert m["target"] == col, f"{col} was renamed to {m['target']}"
        target_type = str(m.get("target_type") or "").upper()
        assert "TEXT" not in target_type, f"{col} → {target_type} is a fidelity collapse"
        assert "CHAR" not in target_type, f"{col} → {target_type} is a fidelity collapse"
