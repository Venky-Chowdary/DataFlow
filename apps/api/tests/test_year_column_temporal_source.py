"""A column named "Year" that holds instants must not be cast to integer.

Spreadsheet exports (fsi-2019.xlsx) store a real ``2019-01-01T00:00:00`` in a
column called ``Year``. The name-only heuristic forced the ``integer``
transform, so Map showed ``TIMESTAMP → TIMESTAMP`` while Validate failed every
sampled row with ``Invalid integer: '2019-01-01T00:00:00'`` — a block no remap
could clear, because the declared pair was already identical.
"""

from services.mapping_proof import build_mapping_proof
from services.transform_engine import apply_transform, infer_transform_for_mapping
from services.transform_resolver import resolve_transform

ISO = "2019-01-01T00:00:00"


def test_year_with_timestamp_source_infers_datetime() -> None:
    assert infer_transform_for_mapping("Year", "year", "TIMESTAMP", "TIMESTAMP") == "datetime"


def test_year_with_temporal_samples_infers_datetime() -> None:
    assert (
        infer_transform_for_mapping("Year", "year", "VARCHAR", "TIMESTAMP", [ISO, ISO, ISO])
        == "datetime"
    )


def test_year_number_still_infers_integer() -> None:
    assert infer_transform_for_mapping("Year", "year", "INTEGER", "TIMESTAMP") == "integer"
    assert (
        infer_transform_for_mapping("Year", "year", "VARCHAR", "TIMESTAMP", ["2019", "2020"])
        == "integer"
    )
    assert infer_transform_for_mapping("fiscal_year", "fy", "BIGINT", "DATE") == "integer"


def test_year_number_into_text_sink_still_integer() -> None:
    assert infer_transform_for_mapping("Year", "year", "VARCHAR", "TEXT", ["2019", "2020"]) == "integer"


def test_resolver_gives_validate_the_declared_temporal_transform() -> None:
    mapping = {"source": "Year", "target": "year", "target_type": "TIMESTAMP"}
    transform = resolve_transform(
        mapping,
        column_types={"Year": "TIMESTAMP"},
        dest_types={"year": "TIMESTAMP"},
    )
    assert transform == "datetime"
    value, error = apply_transform(ISO, transform)
    assert error is None, error
    assert value is not None


def test_identical_timestamp_pair_is_not_a_fidelity_collapse() -> None:
    proof = build_mapping_proof(
        [
            {
                "source": "Year",
                "target": "year",
                "source_type": "TIMESTAMP",
                "target_type": "TIMESTAMP",
                "transform": resolve_transform(
                    {"source": "Year", "target": "year", "target_type": "TIMESTAMP"},
                    column_types={"Year": "TIMESTAMP"},
                    dest_types={"year": "TIMESTAMP"},
                ),
            }
        ],
        destination_db_type="postgresql",
    )
    pair = proof["mappings"][0]
    assert pair["fidelity"] == "preserve"
    assert pair["type_narrowing"] is False
