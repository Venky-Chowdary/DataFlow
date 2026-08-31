"""``long`` means two different things, and only the source engine knows which.

Oracle spells its deprecated *text* LOB ``LONG``. Spark/Hive/Iceberg,
Avro/Parquet/ORC, Elasticsearch and BSON all spell a 64-bit *integer* ``long``.
Reading the token alone forces one of two wrong answers:

* treat every ``long`` as Oracle text — a Mongo ``long`` → ``BIGINT`` route, the
  most ordinary INT64 copy there is, gets flagged as an invented numeric domain
  and stops for an approval it never needed;
* treat every ``long`` as INT64 — an Oracle text LOB silently lands in a numeric
  column, which is exactly the invention the gate exists to catch.

So the decision is bound to the source engine, and an *unknown* source gets
neither guess: it keeps the conservative refusal and the pre-existing carrier.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.conversion_contract import classify_conversion  # noqa: E402
from services.decision_kernel.type_invent import (  # noqa: E402
    create_new_mapping_target_type,
    ddl_type,
)
from services.source_engine_scope import bind_source_engine  # noqa: E402
from services.type_system import (  # noqa: E402
    is_lossy_coercion,
    is_oracle_long_text_carrier,
    normalize_logical_type,
    oracle_long_numeric_invent,
    source_long_is_int64,
    source_long_is_text_lob,
)

#: Engines whose ``long`` is INT64. Every one of these can carry a 64-bit
#: integer into a BIGINT column with no loss and no approval.
INT64_SOURCES = (
    "spark",
    "hive",
    "iceberg",
    "delta",
    "parquet",
    "avro",
    "orc",
    "mongodb",
    "elasticsearch",
    "opensearch",
    "bigquery",
    "postgresql",
    "csv",
)
ORACLE_SOURCES = ("oracle", "oracledb", "oracle_db", "Oracle", " ORACLE ", "oracle-db")


@pytest.mark.parametrize("engine", INT64_SOURCES)
def test_a_known_non_oracle_source_spells_int64_long(engine: str) -> None:
    assert source_long_is_int64(engine) is True
    assert source_long_is_text_lob(engine) is False


@pytest.mark.parametrize("engine", ORACLE_SOURCES)
def test_oracle_long_is_a_text_lob(engine: str) -> None:
    assert source_long_is_int64(engine) is False
    assert source_long_is_text_lob(engine) is True


@pytest.mark.parametrize("engine", ["", "   ", None])
def test_an_unknown_source_gets_neither_guess(engine: str | None) -> None:
    """Both answers are False: unknown is not "not Oracle" and not "Oracle"."""
    assert source_long_is_int64(engine) is False
    assert source_long_is_text_lob(engine) is False


def test_the_token_itself_is_unchanged() -> None:
    """The carrier detector and the logical type keep their existing meaning."""
    assert is_oracle_long_text_carrier("LONG") is True
    assert is_oracle_long_text_carrier("long") is True
    assert is_oracle_long_text_carrier("LONGTEXT") is False
    assert is_oracle_long_text_carrier("BIGINT") is False
    # Lakehouse readers depend on this staying integer.
    assert normalize_logical_type("long") == normalize_logical_type("bigint")


# --------------------------------------------------------------------------
# The conversion decision (Map / Validate): approval or not.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ["mongodb", "spark", "iceberg", "elasticsearch", "parquet"])
def test_int64_long_into_bigint_is_not_an_invented_numeric(engine: str) -> None:
    assert oracle_long_numeric_invent("long", "BIGINT", source_db=engine) is False
    with bind_source_engine(engine):
        decision = classify_conversion("long", "BIGINT", dest_db="postgresql")
    assert decision["lossy"] is False
    assert decision["conversion_class"] == "equivalent"
    assert decision["requires_risk_contract"] is False


def test_oracle_long_into_bigint_still_needs_a_decision() -> None:
    assert oracle_long_numeric_invent("long", "BIGINT", source_db="oracle") is True
    with bind_source_engine("oracle"):
        decision = classify_conversion("long", "BIGINT", dest_db="postgresql")
    assert decision["lossy"] is True
    assert decision["conversion_class"] == "needs_user_approval"
    assert decision["requires_risk_contract"] is True


def test_an_unknown_source_keeps_the_conservative_refusal() -> None:
    """No source identity bound: the pre-existing gated answer must survive."""
    assert oracle_long_numeric_invent("long", "BIGINT") is True
    assert is_lossy_coercion("long", "BIGINT") is True
    with bind_source_engine(""):
        decision = classify_conversion("long", "BIGINT", dest_db="postgresql")
    assert decision["lossy"] is True
    assert decision["conversion_class"] == "needs_user_approval"


def test_a_text_target_is_never_an_invented_numeric() -> None:
    for target in ("CLOB", "TEXT", "VARCHAR(4000)"):
        assert oracle_long_numeric_invent("long", target, source_db="oracle") is False


# --------------------------------------------------------------------------
# The invention decision (create-new DDL): which carrier the column gets.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("dest", "expected"),
    [
        ("postgresql", "BIGINT"),
        ("mysql", "BIGINT"),
        ("snowflake", "BIGINT"),
        ("mssql", "BIGINT"),
        ("bigquery", "INT64"),
    ],
)
def test_int64_long_invents_an_integer_carrier(dest: str, expected: str) -> None:
    with bind_source_engine("mongodb"):
        assert create_new_mapping_target_type("long", dest, source_db="mongodb") == expected


def test_int64_long_does_not_land_in_an_oracle_clob() -> None:
    """A number must not acquire text polarity because Oracle is the destination."""
    with bind_source_engine("iceberg"):
        carrier = create_new_mapping_target_type("long", "oracle", source_db="iceberg")
    assert carrier == "NUMBER(38,0)"


@pytest.mark.parametrize(
    ("dest", "expected"),
    [
        ("oracle", "CLOB"),
        ("postgresql", "TEXT"),
        ("mysql", "LONGTEXT"),
        ("mssql", "NVARCHAR(MAX)"),
        ("bigquery", "STRING"),
    ],
)
def test_oracle_text_long_invents_the_destinations_text_carrier(dest: str, expected: str) -> None:
    """An Oracle text LOB stays text everywhere, not just on Oracle."""
    with bind_source_engine("oracle"):
        assert create_new_mapping_target_type("long", dest, source_db="oracle") == expected


def test_an_unknown_source_keeps_the_prior_carrier() -> None:
    """Unbound source: the historical answer, so no route changes on a guess."""
    with bind_source_engine(""):
        assert ddl_type("postgresql", "long") == "BIGINT"
        assert ddl_type("oracle", "long") == "CLOB"
