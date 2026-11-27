"""Accuracy Track A item 6: create-new type pairs are never graded safe while lossy.

The sweep below is an internal consistency check (the classifier against the
product's own lossy predicate), not an oracle. The independent oracle is the
hand-reviewed, cited table in tests/test_type_fidelity_truth_table.py.

The five pairs audit 12 deferred (C3 fixed the carriers and the Validate verdict)
are pinned here with the source engine bound, as Map and Validate bind it, and
the grading is swept over every engine-specific source token x destination: a
create-new cell graded a safe path must not lose values on its source engine,
and a declared tick the destination cannot hold is disclosed, not called a
round trip. Live writer proofs: tests/test_accuracy_type_pairs_live.py
(PostgreSQL 16 <-> MariaDB 10.11 here; SQL Server/Oracle in CI's live jobs).
"""

from __future__ import annotations

import pytest

from services.conversion_contract import ConversionClass, classify_conversion, create_new_mapping_reason
from services.source_engine_scope import bind_source_engine
from services.source_engine_semantics import engine_semantic_loss
from services.type_coercion import is_lossy_coercion
from services.type_system import create_new_mapping_target_type

SAFE = {
    ConversionClass.IDENTITY.value, ConversionClass.EQUIVALENT.value, ConversionClass.LOSSLESS.value,
    ConversionClass.WIDENING.value, ConversionClass.REPRESENTATION.value, ConversionClass.NORMALIZATION.value,
}
SEVENTH = ["2024-01-01 10:00:00.1234567"]


def _grade(engine, src, dest, tgt, population=None):
    with bind_source_engine(engine):
        conv = classify_conversion(src, tgt, dest_db=dest, population=population)
        lossy = is_lossy_coercion(src, tgt, dest_db=dest, source_db=engine, dest_table_exists=True,
                                  population=population)
        sem = engine_semantic_loss(src, tgt, dest_db=dest)
    return conv, bool(lossy), sem


# (engine, source type, destination, the carrier create-new used to emit)
FIVE = [
    ("sqlserver", "tinyint", "mysql", "TINYINT"),
    ("oracle", "DATE", "postgresql", "DATE"),
    ("mysql", "timestamp", "postgresql", "TIMESTAMP"),
    ("postgresql", "numeric", "mysql", "DECIMAL(38,15)"),
    ("sqlserver", "datetime2(7)", "postgresql", "TIMESTAMP(6)"),
]


@pytest.mark.parametrize("engine,src,dest,old", FIVE)
def test_the_old_carrier_is_not_graded_safe(engine, src, dest, old) -> None:
    population = SEVENTH if src.startswith("datetime2") else None
    conv, lossy, _sem = _grade(engine, src, dest, old, population)
    assert lossy is True
    assert conv["conversion_class"] not in SAFE, conv


@pytest.mark.parametrize("engine,src,dest,old", FIVE)
def test_create_new_carrier_keeps_values_or_says_what_it_cannot(engine, src, dest, old) -> None:
    with bind_source_engine(engine):
        target = create_new_mapping_target_type(src, dest, source_db=engine)
    conv, lossy, sem = _grade(engine, src, dest, target)
    if (engine, src) == ("postgresql", "numeric"):
        # No bounded carrier holds every unbounded numeric: approval until the
        # measured values fit, never a silent safe grade.
        assert lossy and conv["requires_risk_contract"] is True, (target, conv)
        assert conv["conversion_class"] not in SAFE
        _fit, fit_lossy, _s = _grade(engine, src, dest, target, ["12.5", "-3"])
        assert fit_lossy is False
        return
    # A carrier that keeps every value (the product's own second run is not refused).
    assert not lossy and not sem, (target, conv)
    if src.startswith("datetime2"):
        # No microsecond carrier holds 100 ns: graded with the disclosure.
        assert "7th fractional digit" in conv.get("unmeasured_loss", ""), conv
        assert "round-trips" not in conv["reason"]


def test_create_new_reason_for_a_7_digit_tick_is_not_lossless() -> None:
    for dest, carrier in (("postgresql", "TIMESTAMP(6)"), ("mysql", "DATETIME(6)")):
        text = create_new_mapping_reason("datetime2(7)", carrier, dest_db=dest)
        assert "lossless" not in text and "truncated" in text, text


def test_a_measured_seventh_digit_needs_a_risk_contract() -> None:
    conv, lossy, _ = _grade("sqlserver", "datetime2(7)", "postgresql", "TIMESTAMP(6)", SEVENTH)
    assert lossy and conv["requires_risk_contract"] is True
    clean, lossy2, _ = _grade("sqlserver", "datetime2(7)", "postgresql", "TIMESTAMP(6)",
                              ["2024-01-01 10:00:00.123456"])
    assert not lossy2


_ENGINE_TOKENS = {
    "sqlserver": ["tinyint", "smallint", "int", "bigint", "bit", "decimal(18,4)", "money", "smallmoney",
                  "real", "float", "date", "time(7)", "datetime", "datetime2(7)", "datetime2(3)",
                  "smalldatetime", "datetimeoffset(7)", "char(10)", "varchar(50)", "nvarchar(50)",
                  "nvarchar(max)", "uniqueidentifier", "varbinary(max)"],
    "oracle": ["DATE", "TIMESTAMP(6)", "TIMESTAMP(9)", "TIMESTAMP(6) WITH TIME ZONE", "NUMBER(10)",
               "NUMBER(19)", "NUMBER(12,2)", "NUMBER", "BINARY_DOUBLE", "BINARY_FLOAT", "FLOAT",
               "VARCHAR2(50)", "VARCHAR2(50 CHAR)", "NVARCHAR2(50)", "CLOB", "BLOB", "RAW(16)"],
    "mysql": ["tinyint", "tinyint unsigned", "tinyint(1)", "smallint unsigned", "int", "int unsigned",
              "bigint unsigned", "decimal(65,30)", "float", "double", "date", "datetime", "datetime(6)",
              "timestamp", "timestamp(3)", "time(6)", "year", "varchar(255)", "text", "longtext",
              "json", "blob", "bit(8)"],
    "postgresql": ["smallint", "integer", "bigint", "numeric", "numeric(12,2)", "real",
                   "double precision", "money", "date", "timestamp", "timestamp(3)", "timestamptz",
                   "time", "interval", "boolean", "text", "varchar(50)", "uuid", "jsonb", "bytea"],
}
_DESTS = ["postgresql", "mysql", "sqlserver", "oracle", "snowflake", "sqlite", "bigquery", "redshift", "duckdb"]


@pytest.mark.parametrize("engine", sorted(_ENGINE_TOKENS))
@pytest.mark.parametrize("dest", _DESTS)
def test_no_create_new_cell_is_graded_safe_while_it_loses_values(engine, dest) -> None:
    bad = []
    for src in _ENGINE_TOKENS[engine]:
        with bind_source_engine(engine):
            target = create_new_mapping_target_type(src, dest, source_db=engine)
        if not target:
            continue
        conv, lossy, sem = _grade(engine, src, dest, target)
        if conv["conversion_class"] in SAFE and (lossy or sem):
            bad.append((src, target, conv["conversion_class"], sem or "lossy"))
    assert bad == []
