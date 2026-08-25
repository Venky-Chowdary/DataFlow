"""Sample-aware DECIMAL(p,s) / IEEE invent — enterprise Map create-new SSOT."""

from __future__ import annotations

from services.decimal_observe import (
    cell_int_digits_and_scale,
    create_new_decimal_carrier,
    ieee_float_create_new_risk,
    observe_numeric_samples,
)
from services.schema_inference import infer_column
from services.type_system import (
    assess_create_new_type_risk,
    create_new_mapping_target_type,
    parse_numeric_precision_scale,
)


def test_cell_normalize_collapses_trailing_zeros():
    assert cell_int_digits_and_scale("52.310500000000000") == (2, 4)
    assert cell_int_digits_and_scale("100") == (3, 0)


def test_fsi_scores_invent_fixed_decimal_not_platform_floor():
    # Excel FSI-like scores — fixed point, not DECIMAL(38,15).
    samples = ["85.5", "92.0", "78.25", "100", "66.75", "88.5"]
    obs = observe_numeric_samples(samples)
    assert obs["kind"] == "fixed_decimal"
    assert obs["carrier"].startswith("DECIMAL(")
    p, s = parse_numeric_precision_scale(obs["carrier"])
    assert p is not None and s is not None
    assert p <= 12
    assert s <= 6
    assert p < 38 or s != 15

    stamped = create_new_mapping_target_type("DECIMAL", "postgresql", samples=samples)
    assert stamped.upper().startswith("NUMERIC(") or stamped.upper().startswith("DECIMAL(")
    sp, ss = parse_numeric_precision_scale(stamped)
    assert sp is not None and ss is not None
    assert sp < 38 or ss != 15


def test_declared_decimal_typmod_wins_over_samples():
    samples = ["1.5", "2.25"]
    assert create_new_decimal_carrier(
        samples, source_type="DECIMAL(12,2)"
    ) == "DECIMAL(12,2)"
    stamped = create_new_mapping_target_type(
        "DECIMAL(12,2)", "mysql", samples=samples
    )
    assert "12" in stamped and "2" in stamped


def test_excel_ieee_residue_invents_float_with_risk():
    samples = [
        "111.89999999999999",
        "42.100000000000001",
        "7.199999999999999",
    ]
    obs = observe_numeric_samples(samples)
    assert obs["kind"] == "ieee_float"
    assert obs["carrier"] == "FLOAT"
    risk = ieee_float_create_new_risk(obs)
    assert risk and risk["kind"] == "ieee_float_artifact"

    stamped = create_new_mapping_target_type("FLOAT", "postgresql", samples=samples)
    assert "DOUBLE" in stamped.upper() or stamped.upper() == "FLOAT" or "REAL" in stamped.upper()

    risks = assess_create_new_type_risk(
        "FLOAT", stamped, destination_db_type="postgresql", samples=samples
    )
    assert any(r.get("kind") == "ieee_float_artifact" for r in risks)


def test_schema_inference_stamps_observed_decimal():
    samples = ["12.50", "99.99", "0.01", "1500.00"]
    col = infer_column(samples, field_name="amount")
    logical = str(col.get("logical_type") or "")
    assert logical.startswith("DECIMAL(")
    p, s = parse_numeric_precision_scale(logical)
    assert p is not None and s is not None
    assert s >= 2


def test_padded_trailing_zeros_do_not_invent_a_tighter_source_than_the_write():
    """CSV ``1.50000000`` is 1.5. The writer stores it in NUMBER(9,2).

    Observing it as DECIMAL(7,4) (create-new +2 scale) made Map demand a Risk
    Contract for a narrowing the values do not have. Source inference uses the
    cells; dest invent keeps the margin.
    """
    from services.decimal_observe import observe_source_numeric_samples
    from services.type_system import is_precision_collapse_coercion
    from connectors.writer_common import fits_decimal

    assert cell_int_digits_and_scale("1.50000000") == (1, 1)
    samples = ["1.50000000", "10.50"]
    assert all(fits_decimal(v, 9, 2, dest_db="snowflake") for v in samples)

    source = observe_source_numeric_samples(samples)
    invent = observe_numeric_samples(samples)
    assert source["kind"] == "fixed_decimal"
    assert source["scale"] == 2
    assert invent["scale"] == 4
    assert invent["carrier"] == "DECIMAL(7,4)"

    col = infer_column(samples, field_name="amount")
    logical = str(col.get("logical_type") or "")
    p, s = parse_numeric_precision_scale(logical)
    assert s == 2, logical
    assert p is not None and p < 7
    assert is_precision_collapse_coercion(
        logical, "NUMBER(9,2)", dest_db="snowflake"
    ) is False


def test_money_with_currency_symbols():
    samples = ["$1,234.56", "$99.00", "£10.50"]
    obs = observe_numeric_samples(samples)
    assert obs["kind"] == "fixed_decimal"
    assert obs["parse_rate"] >= 0.9


def test_money_locale_and_currency_codes_widen_for_population():
    """EU decimal comma + USD codes must not invent DECIMAL(11,6) from mis-strip."""
    samples = ["$1,000.00", "€2.000,50", "USD 1000000.89"]
    assert cell_int_digits_and_scale(samples[0]) == (4, 2)
    assert cell_int_digits_and_scale(samples[1]) == (4, 2)
    assert cell_int_digits_and_scale(samples[2]) == (7, 2)
    obs = observe_numeric_samples(samples)
    assert obs["kind"] == "fixed_decimal"
    assert obs["parse_rate"] == 1.0
    assert obs["max_int_digits"] == 7
    p, s = parse_numeric_precision_scale(obs["carrier"])
    assert p is not None and s is not None
    # 7 int digits + margin + scale≥2 must fit 1000000.89 (never 11,6 cliff).
    assert p - s >= 7
    assert s >= 2
    assert p >= 11


def test_empty_samples_no_fake_invent_via_create_new():
    # Without samples, bare DECIMAL still uses platform ddl path — not our invent.
    stamped = create_new_mapping_target_type("DECIMAL", "mysql", samples=None)
    # Platform floor is allowed when no evidence; sample path must not invent from [].
    empty = create_new_mapping_target_type("DECIMAL", "mysql", samples=[])
    assert empty == stamped or empty.upper().startswith("DECIMAL")


def test_map_columns_create_new_uses_sample_decimal():
    from services.semantic_mapper import map_columns

    mappings = map_columns(
        ["credit_score"],
        [],
        source_schemas=[
            {
                "name": "credit_score",
                "inferred_type": "DECIMAL",
                "samples": ["720.5", "680.0", "801.25", "655"],
            }
        ],
        destination_db_type="postgresql",
        destination_table_exists=False,
    )
    assert len(mappings) == 1
    tgt = str(mappings[0].get("target_type") or "")
    p, s = parse_numeric_precision_scale(tgt)
    assert p is not None and s is not None
    assert not (p == 38 and s in {10, 15})
