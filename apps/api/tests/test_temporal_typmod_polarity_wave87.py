"""Wave 87: temporal typmod + TZ polarity honesty across the DDL matrix.

Every failure below was a live defect found by auditing ``ddl_type`` against
vendor grammars and by executing the generated DDL on a real engine.

Research anchors
----------------
- ClickHouse: ``DateTime64(precision, [timezone])`` takes precision 0–9; plain
  ``DateTime([timezone])`` accepts *only* a timezone, so ``DateTime(6)`` is a
  syntax error. ``DateTime`` starts at 1970-01-01 while ``DateTime64`` reaches
  back to 1900 — mapping a generic datetime to ``DateTime`` overflows birthdates.
- PostgreSQL: ``timestamp``/``timestamptz``/``time``/``timetz`` accept ``(p)``
  0–6, and ``information_schema`` spells columns
  ``timestamp(6) without time zone``.
- Amazon Redshift: TIMESTAMP/TIMESTAMPTZ/TIME are always microsecond and take
  **no** precision parameter in the type definition.
- DuckDB 1.3.2 (executed here): ``TIMESTAMP(p)`` is accepted, while
  ``TIMESTAMPTZ(6)`` and ``TIME(6)`` raise
  "Type … does not support any modifiers!". ``TIMETZ`` is native.
- Oracle has no TIME type — DataFlow lands it as ``VARCHAR2(32)``, so a
  fractional-seconds precision must never be written as a character width.
"""

from __future__ import annotations

import pytest

from services.type_system import (
    DDL_TYPES,
    datetime_timezone_polarity,
    ddl_type,
    normalize_logical_type,
    parse_temporal_fractional_precision,
    time_timezone_polarity,
)

# Destinations with a real SQL DDL contract for temporal columns.
SQL_DESTS = [
    "postgresql",
    "mysql",
    "sqlserver",
    "oracle",
    "snowflake",
    "bigquery",
    "duckdb",
    "trino",
    "clickhouse",
    "databricks",
    "redshift",
]

# Engines that accept no temporal precision argument at all.
NO_TYPMOD_DESTS = ["redshift", "bigquery", "databricks"]


def test_clickhouse_never_emits_invalid_or_second_only_datetime():
    """DateTime(p) is a syntax error and bare DateTime truncates to seconds."""
    # Ambiguous/foreign datetime must land DateTime64 — never bare DateTime,
    # whose 1970 epoch floor would overflow any pre-1970 value.
    for src in ["datetime", "DATETIME", "DATETIME(6)", "DATETIME(3)", "TIMESTAMP(6)"]:
        out = ddl_type("clickhouse", src)
        assert out.startswith("DateTime64("), f"{src} -> {out}"
    # A numeric typmod becomes DateTime64(p), not the invalid DateTime(p).
    assert ddl_type("clickhouse", "DATETIME(6)") == "DateTime64(6)"
    assert ddl_type("clickhouse", "DATETIME(3)") == "DateTime64(3)"
    # ddl_type must agree with its own DDL_TYPES source of truth.
    assert ddl_type("clickhouse", "datetime") == DDL_TYPES["clickhouse"]["datetime"]
    # ClickHouse-native spellings still round-trip verbatim.
    assert ddl_type("clickhouse", "DateTime64(3)") == "DateTime64(3)"
    assert ddl_type("clickhouse", "DateTime64(6, 'UTC')") == "DateTime64(6, 'UTC')"
    assert ddl_type("clickhouse", "DateTime('UTC')") == "DateTime('UTC')"
    # Bare DateTime64 is not valid ClickHouse — precision is required.
    assert ddl_type("clickhouse", "DATETIME64") == "DateTime64(3)"


@pytest.mark.parametrize("fsp", [3, 6, 9])
def test_declared_precision_survives_ambiguous_polarity(fsp: int):
    """Bare ``TIMESTAMP(p)`` keeps its precision even though polarity defers.

    Polarity is intentionally ambiguous for bare TIMESTAMP (platform default),
    but dropping ``(p)`` silently narrowed microseconds to the table default of
    milliseconds on ClickHouse and Trino.
    """
    src = f"TIMESTAMP({fsp})"
    for dest in ("clickhouse", "trino", "snowflake", "oracle"):
        got = parse_temporal_fractional_precision(ddl_type(dest, src))
        assert got == fsp, f"{dest} narrowed {src} to {got}"


def test_naive_timestamp_with_typmod_never_flips_to_tz_aware():
    """``TIMESTAMP(6) WITHOUT TIME ZONE`` is PG's own introspect spelling.

    Missing the token left polarity ambiguous, so the column landed TZ-aware and
    every value was shifted by the session offset on write.
    """
    assert datetime_timezone_polarity("TIMESTAMP(6) WITHOUT TIME ZONE") == "ntz"
    assert datetime_timezone_polarity("TIMESTAMP(6) WITH TIME ZONE") == "tz"
    assert time_timezone_polarity("TIME(6) WITHOUT TIME ZONE") == "ntz"
    assert time_timezone_polarity("TIME(6) WITH TIME ZONE") == "tz"
    # Bare TIMESTAMP stays ambiguous on purpose (platform default, wave 65).
    assert datetime_timezone_polarity("TIMESTAMP(6)") is None

    naive = "TIMESTAMP(6) WITHOUT TIME ZONE"
    for dest in SQL_DESTS:
        out = ddl_type(dest, naive)
        # Re-reading the emitted DDL must never report an offset-bearing column.
        assert datetime_timezone_polarity(out) not in {"tz", "ltz"}, f"{dest} -> {out}"
        assert "WITH TIME ZONE" not in out.upper(), f"{dest} -> {out}"


def test_preflight_surfaces_offset_loss_into_parameterized_naive_column():
    """The same token gap also blinded preflight, not just create-new DDL.

    With polarity unknown, ``is_timezone_polarity_loss`` returned False, so a
    genuine offset→wall-clock write was reported as safe.
    """
    from services.type_system import (
        is_lossy_coercion,
        is_timezone_polarity_loss,
        time_timezone_polarity_loss,
    )

    for src in ["TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE"]:
        tgt = "TIMESTAMP(6) WITHOUT TIME ZONE"
        assert is_timezone_polarity_loss(src, tgt) is True, src
        assert is_lossy_coercion(src, tgt) is True, src
    assert time_timezone_polarity_loss("TIMETZ", "TIME(6) WITHOUT TIME ZONE") is True
    assert time_timezone_polarity_loss("TIME(6) WITH TIME ZONE", "TIME(6)") is True
    # Matching polarity must stay clean — no new false positives.
    naive = "TIMESTAMP(6) WITHOUT TIME ZONE"
    assert is_timezone_polarity_loss(naive, naive) is False
    assert is_lossy_coercion(naive, naive) is False


def test_time_precision_is_never_written_as_a_character_width():
    """Oracle carries TIME as VARCHAR2(32); ``TIME(6)`` must not shrink it.

    ``VARCHAR2(6)`` cannot hold ``12:34:56.123456`` — the seconds precision was
    being reused as a string width.
    """
    for src in ["TIME", "TIME(0)", "TIME(3)", "TIME(6)", "TIMETZ", "TIMETZ(6)"]:
        assert ddl_type("oracle", src) == "VARCHAR2(32)", src
    # Text carriers on other engines are equally protected.
    assert ddl_type("clickhouse", "TIME(6)") == "String"
    assert ddl_type("sqlite", "TIME(6)") == "TEXT"
    assert ddl_type("databricks", "TIME(6)") == "STRING"


def test_engines_without_typmod_support_stay_bare():
    """Appending ``(p)`` where the grammar forbids it is a syntax error."""
    for src in ["TIMESTAMP(6)", "TIMESTAMP(9)", "TIMESTAMP_NTZ(6)", "TIME(6)",
                "SMALLDATETIME"]:
        for dest in NO_TYPMOD_DESTS:
            out = ddl_type(dest, src)
            assert "(" not in out, f"{dest} {src} -> {out}"
    # DuckDB accepts a typmod on naive TIMESTAMP but rejects it on TIMESTAMPTZ
    # and on TIME (verified against DuckDB 1.3.2).
    assert ddl_type("duckdb", "TIMESTAMP(6)") == "TIMESTAMP(6)"
    assert ddl_type("duckdb", "TIMESTAMPTZ(6)") == "TIMESTAMPTZ"
    assert ddl_type("duckdb", "TIME(6)") == "TIME"
    # DuckDB has a native TIMETZ — do not silently drop the offset.
    assert ddl_type("duckdb", "TIMETZ") == "TIMETZ"
    assert ddl_type("duckdb", "TIME(6) WITH TIME ZONE") == "TIMETZ"


def test_smalldatetime_lands_valid_ddl_on_every_dest():
    """The catch-all emitted ``TIMESTAMP(0)`` onto engines that reject a typmod."""
    assert ddl_type("sqlserver", "SMALLDATETIME") == "SMALLDATETIME"
    # Pinned minute-accuracy sinks (wave 70).
    assert ddl_type("postgresql", "SMALLDATETIME") == "TIMESTAMP(0)"
    assert ddl_type("snowflake", "SMALLDATETIME") == "TIMESTAMP_NTZ(0)"
    assert ddl_type("oracle", "SMALLDATETIME") == "TIMESTAMP(0)"
    # Previously invalid: these engines take no precision argument.
    assert ddl_type("bigquery", "SMALLDATETIME") == "DATETIME"
    assert ddl_type("databricks", "SMALLDATETIME") == "TIMESTAMP_NTZ"
    assert ddl_type("redshift", "SMALLDATETIME") == "TIMESTAMP"
    assert ddl_type("clickhouse", "SMALLDATETIME") == "DateTime64(0)"
    # SMALLDATETIME starts at 1900 — ClickHouse DateTime (1970 floor) would clamp.
    assert not ddl_type("clickhouse", "SMALLDATETIME").startswith("DateTime(")


def test_generic_null_token_is_not_a_dynamodb_typed_null():
    """Avro/JSON-Schema ``null`` must honour CANONICAL_TYPES, not DynamoDB's code.

    ``normalize_logical_type`` consulted the DynamoDB AttributeValue map for
    *every* type string, so the generic ``null`` branch inherited DynamoDB's
    typed-null envelope and landed a JSON column.
    """
    assert normalize_logical_type("null") == "string"
    assert normalize_logical_type("NULL") == "string"
    # Consistent with the other typed-null spellings.
    assert normalize_logical_type("none") == "string"
    assert normalize_logical_type("void") == "string"
    # DynamoDB-only codes keep their document semantics (wave 78).
    assert normalize_logical_type("M") == "json"
    assert normalize_logical_type("L") == "array"
    assert normalize_logical_type("N") == "decimal"
    assert normalize_logical_type("BOOL") == "boolean"
    # A DynamoDB destination still round-trips the wire codes.
    assert ddl_type("dynamodb", "NULL") == "NULL"
    assert ddl_type("dynamodb", "M") == "M"


def test_generated_temporal_ddl_executes_on_real_duckdb():
    """Execute the emitted DDL — string assertions cannot catch a syntax error.

    This is the proof artifact that caught ``TIME(6)`` and ``TIMESTAMPTZ(6)``.
    """
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    sources = [
        "DATETIME", "DATETIME(0)", "DATETIME(3)", "DATETIME(6)",
        "DATETIME2", "DATETIME2(7)", "SMALLDATETIME",
        "TIMESTAMP", "TIMESTAMP(0)", "TIMESTAMP(3)", "TIMESTAMP(6)", "TIMESTAMP(9)",
        "TIMESTAMPTZ", "TIMESTAMPTZ(6)",
        "TIMESTAMP WITH TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE",
        "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP(6) WITHOUT TIME ZONE",
        "TIMESTAMP_NTZ", "TIMESTAMP_NTZ(6)", "TIMESTAMP_TZ", "TIMESTAMP_LTZ",
        "DATETIMEOFFSET", "DATETIMEOFFSET(7)",
        "DATETIME64(3)", "DATETIME64(6, 'UTC')",
        "DATE", "TIME", "TIME(0)", "TIME(3)", "TIME(6)",
        "TIMETZ", "TIMETZ(6)", "TIME(6) WITH TIME ZONE", "TIME WITHOUT TIME ZONE",
    ]
    rejected: list[tuple[str, str, str]] = []
    for src in sources:
        emitted = ddl_type("duckdb", src)
        try:
            con.execute(f"CREATE OR REPLACE TABLE wave87_probe (c {emitted})")
        except Exception as exc:  # pragma: no cover - only on a real regression
            rejected.append((src, emitted, str(exc).splitlines()[0]))
    assert not rejected, f"DuckDB rejected generated DDL: {rejected}"


def test_duckdb_preserves_offset_and_subsecond_through_roundtrip():
    """A TZ-aware, microsecond value must survive create-new + read-back."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    tz_ddl = ddl_type("duckdb", "TIMESTAMP(6) WITH TIME ZONE")
    naive_ddl = ddl_type("duckdb", "TIMESTAMP(6) WITHOUT TIME ZONE")
    con.execute(f"CREATE TABLE wave87_rt (tzc {tz_ddl}, ntzc {naive_ddl})")
    con.execute(
        "INSERT INTO wave87_rt VALUES "
        "('2024-03-10 12:34:56.123456+05:30', '2024-03-10 12:34:56.123456')"
    )
    tz_val, ntz_val = con.execute(
        "SELECT tzc, ntzc FROM wave87_rt"
    ).fetchone()
    # Microseconds are not rounded away.
    assert tz_val.microsecond == 123456
    assert ntz_val.microsecond == 123456
    # The offset is retained on the TZ column and absent on the naive one.
    assert tz_val.utcoffset() is not None
    assert ntz_val.utcoffset() is None
