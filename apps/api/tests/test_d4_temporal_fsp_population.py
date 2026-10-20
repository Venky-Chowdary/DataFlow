"""D4: declared-FSP narrowing must check the population, like g3f.

Snowflake ``TIMESTAMP_LTZ(9)`` declares nanoseconds but a population of
microsecond-exact values fits ``TIMESTAMPTZ(6)`` whole — the g-fidelity block
must not demand CAST_AND_CONTINUE on data that fits. Unmeasured populations
stay fail-closed, and one sub-microsecond cell keeps the block.
"""

from services.type_coercion import is_lossy_coercion


def test_ltz9_to_timestamptz6_microsecond_population_not_lossy():
    pop = [
        "2024-01-01 12:00:00.123456+00",
        "2024-06-30 23:59:59.999999+00",
        None,
        "2024-12-31 00:00:00+00",
    ]
    assert not is_lossy_coercion(
        "TIMESTAMP_LTZ(9)", "TIMESTAMPTZ(6)", dest_db="postgresql", population=pop
    )


def test_ltz9_population_with_ns_cell_still_blocks():
    pop = [
        "2024-01-01 12:00:00.123456789+00",  # 9 fractional digits > 6
        "2024-06-30 23:59:59.1+00",
    ]
    assert is_lossy_coercion(
        "TIMESTAMP_LTZ(9)", "TIMESTAMPTZ(6)", dest_db="postgresql", population=pop
    )


def test_ltz9_unmeasured_population_still_blocks():
    # No population / all-NULL: declared ns ceiling stands (fail-closed).
    assert is_lossy_coercion(
        "TIMESTAMP_LTZ(9)", "TIMESTAMPTZ(6)", dest_db="postgresql"
    )
    assert is_lossy_coercion(
        "TIMESTAMP_LTZ(9)",
        "TIMESTAMPTZ(6)",
        dest_db="postgresql",
        population=[None, "  ", None],
    )


def test_ltz9_to_millisecond_carrier_blocks_even_on_us_population():
    # TIMESTAMP(3) drops real microseconds — the population fit must respect
    # the destination's own precision, not just "no ns digits".
    pop = ["2024-01-01 12:00:00.123456+00"]
    assert is_lossy_coercion(
        "TIMESTAMP_LTZ(9)", "TIMESTAMP(3)", dest_db="postgresql", population=pop
    )
