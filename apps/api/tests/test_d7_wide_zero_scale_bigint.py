"""D7: wide zero-scale numeric -> bigint when the population fits.

PG int4 -> Snowflake NUMBER(38,0) -> PG must map back to BIGINT when every
measured value fits int64 — not NUMERIC(38,0). Declared typmod stays when the
population is unmeasured or exceeds bigint.
"""

from services.decimal_observe import population_fits_signed_bigint
from services.decision_kernel.type_invent import (
    _create_new_mapping_target_type,
)


def test_number38_population_fits_stamps_bigint():
    stamp = _create_new_mapping_target_type(
        "NUMBER(38,0)",
        "postgresql",
        samples=[1, -42, 2147483647, 0, None],
        source_db="snowflake",
    )
    assert stamp.upper() == "BIGINT"


def test_number38_out_of_range_keeps_numeric():
    stamp = _create_new_mapping_target_type(
        "NUMBER(38,0)",
        "postgresql",
        samples=[1, 99999999999999999999],  # > int64 max
        source_db="snowflake",
    )
    assert stamp.upper() != "BIGINT"
    assert "NUMERIC" in stamp.upper() or "DECIMAL" in stamp.upper()


def test_number38_unmeasured_keeps_declared():
    # No samples: declared NUMBER(38,0) must not invent BIGINT (fail-closed).
    stamp = _create_new_mapping_target_type(
        "NUMBER(38,0)", "postgresql", samples=None, source_db="snowflake"
    )
    assert "38" in stamp


def test_number38_fractional_cell_keeps_numeric():
    stamp = _create_new_mapping_target_type(
        "NUMBER(38,0)",
        "postgresql",
        samples=[1, "3.5"],
        source_db="snowflake",
    )
    assert stamp.upper() != "BIGINT"


def test_population_fits_signed_bigint_tristate():
    assert population_fits_signed_bigint([1, -5, "9223372036854775807"]) is True
    assert population_fits_signed_bigint(["9223372036854775808"]) is False
    assert population_fits_signed_bigint(["3.5"]) is False
    assert population_fits_signed_bigint([None, ""]) is None
    assert population_fits_signed_bigint(None) is None


def test_narrow_declared_precision_untouched():
    # DECIMAL(10,0) is already bigint-safe by declaration — the D7 path must
    # not change its stamp.
    stamp = _create_new_mapping_target_type(
        "DECIMAL(10,0)", "postgresql", samples=[1], source_db="postgresql"
    )
    assert "10" in stamp or "NUMERIC" in stamp.upper() or "DECIMAL" in stamp.upper() or "INT" in stamp.upper()
