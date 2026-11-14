"""C3 — the five lossy type pairs audit 12 deferred are flagged at Validate.

The static predicate graded these pairs lossless because the bare type token
does not say what the *source engine* stores (see
``services/source_engine_semantics.py``):

==========================================  ==================================
pair                                        what is lost
==========================================  ==================================
SQL Server ``tinyint`` → MySQL ``TINYINT``   128..255 (unsigned into signed)
Oracle ``DATE`` → PostgreSQL ``DATE``        the time of day
MySQL ``timestamp`` → PostgreSQL ``TIMESTAMP``  the instant's zone
PostgreSQL ``numeric`` → MySQL ``DECIMAL(38,15)``  digits past 23.15
SQL Server ``datetime2(7)`` → PG ``TIMESTAMP(6)``  the 100 ns digit
==========================================  ==================================

They were deferred because each target is what create-new emitted, so grading
it lossy would refuse the second run of every table Datawrap created. Where a
carrier keeps every value, create-new now emits it (``TINYINT UNSIGNED``,
``TIMESTAMP(0)``, ``TIMESTAMPTZ``) and the old target is flagged. Where none
exists the flag stands: bare ``numeric`` into a bounded decimal (already an
Execute-gate approval, now also the Validate verdict) and ``datetime2(7)`` when
the measured values use the 7th digit.

These are the conversion-contract proofs with the engine bound as Validate binds
it. The live proofs are ``test_c3_lossy_pairs_live.py`` (MySQL here; SQL
Server and Oracle in CI's ``cdc-sqlserver`` and ``cdc-oracle`` jobs).
"""

from __future__ import annotations

import pytest

from services.conversion_contract import ConversionClass, classify_conversion
from services.mapping_proof import mapping_fidelity
from services.source_engine_scope import bind_source_engine
from services.type_coercion import is_lossy_coercion
from services.type_system import create_new_mapping_target_type

SEVENTH_DIGIT = ["2024-01-01 10:00:00.1234567"]
MICROS_ONLY = ["2024-01-01 10:00:00.123456", "2024-01-01 10:00:00"]

#: (source engine, source type, destination, existing target, population)
PAIRS = [
    ("sqlserver", "tinyint", "mysql", "TINYINT", None),
    ("oracle", "DATE", "postgresql", "DATE", None),
    ("mysql", "timestamp", "postgresql", "TIMESTAMP", None),
    ("postgresql", "numeric", "mysql", "DECIMAL(38,15)", None),
    ("sqlserver", "datetime2(7)", "postgresql", "TIMESTAMP(6)", SEVENTH_DIGIT),
]


@pytest.mark.parametrize("engine,src,dest,tgt,population", PAIRS)
def test_pair_is_lossy_for_its_source_engine(engine, src, dest, tgt, population):
    assert is_lossy_coercion(
        src, tgt, dest_db=dest, source_db=engine, dest_table_exists=True, population=population
    ) is True


@pytest.mark.parametrize("engine,src,dest,tgt,population", PAIRS)
def test_validate_verdict_flags_the_pair(engine, src, dest, tgt, population):
    """The Map/Validate fidelity verdict (one SSOT) and the Execute contract class."""
    mapping = {"source": "c", "target": "c", "source_type": src, "target_type": tgt}
    with bind_source_engine(engine):
        verdict = mapping_fidelity(
            mapping, destination_db_type=dest, dest_table_exists=True, population=population
        )
        conv = classify_conversion(src, tgt, dest_db=dest)
    assert verdict["verdict"] == "lossy_cast", verdict
    if population is None:
        # Value-independent loss: the Execute contract needs a Risk Contract too.
        assert verdict["requires_risk_contract"] is True, verdict
        assert conv["conversion_class"] == ConversionClass.NEEDS_USER_APPROVAL.value, conv


@pytest.mark.parametrize(
    "engine,src,dest,expected",
    [
        ("sqlserver", "tinyint", "mysql", "TINYINT UNSIGNED"),
        ("oracle", "DATE", "postgresql", "TIMESTAMP(0)"),
        ("oracle", "DATE", "mysql", "DATETIME(0)"),
        ("oracle", "DATE", "sqlserver", "DATETIME2(0)"),
        ("mysql", "timestamp", "postgresql", "TIMESTAMPTZ"),
        ("mysql", "timestamp(3)", "postgresql", "TIMESTAMPTZ(3)"),
    ],
)
def test_create_new_emits_a_carrier_that_keeps_every_value(engine, src, dest, expected):
    target = create_new_mapping_target_type(src, dest, source_db=engine)
    assert target == expected
    # The product's own second run into that table is not refused.
    assert is_lossy_coercion(src, target, dest_db=dest, source_db=engine, dest_table_exists=True) is False


def test_datetime2_into_postgres_microseconds_is_judged_on_the_values():
    kw = {"dest_db": "postgresql", "source_db": "sqlserver", "dest_table_exists": True}
    assert is_lossy_coercion("datetime2(7)", "TIMESTAMP(6)", population=SEVENTH_DIGIT, **kw) is True
    assert is_lossy_coercion("datetime2(7)", "TIMESTAMP(6)", population=MICROS_ONLY, **kw) is False
    # Unmeasured: the dialect-default tick stays runnable (D-route exemption).
    assert is_lossy_coercion("datetime2(7)", "TIMESTAMP(6)", **kw) is False


def test_unbounded_numeric_is_cleared_by_measured_values_that_fit():
    kw = {"dest_db": "mysql", "source_db": "postgresql", "dest_table_exists": True}
    assert is_lossy_coercion("numeric", "DECIMAL(38,15)", population=["12.5", "-3"], **kw) is False
    assert is_lossy_coercion("numeric", "DECIMAL(38,15)", population=["1." + "1" * 16], **kw) is True
    assert is_lossy_coercion("numeric", "DECIMAL(38,15)", population=["9" * 24], **kw) is True
    # A declared precision is the existing decimal rule's business.
    assert is_lossy_coercion("numeric(12,2)", "DECIMAL(38,15)", **kw) is False


@pytest.mark.parametrize(
    "engine,src,dest,tgt",
    [
        ("mysql", "tinyint", "mysql", "TINYINT"),          # MySQL tinyint is signed
        ("sqlserver", "tinyint", "sqlserver", "TINYINT"),  # same engine, same range
        ("sqlserver", "tinyint", "postgresql", "SMALLINT"),
        ("postgresql", "date", "postgresql", "DATE"),      # a PostgreSQL date is a day
        ("oracle", "DATE", "oracle", "DATE"),
        ("postgresql", "timestamp", "postgresql", "TIMESTAMP"),
        ("mysql", "datetime", "postgresql", "TIMESTAMP"),  # MySQL DATETIME is a wall clock
        ("", "tinyint", "mysql", "TINYINT"),              # no engine: generic meaning
        ("", "DATE", "postgresql", "DATE"),
    ],
)
def test_value_preserving_neighbours_stay_clean(engine, src, dest, tgt):
    assert is_lossy_coercion(src, tgt, dest_db=dest, source_db=engine, dest_table_exists=True) is False


def test_tinyint_into_an_existing_wider_or_unsigned_column_is_clean():
    kw = {"source_db": "sqlserver", "dest_table_exists": True}
    assert is_lossy_coercion("tinyint", "TINYINT UNSIGNED", dest_db="mysql", **kw) is False
    assert is_lossy_coercion("tinyint", "NUMBER(3)", dest_db="oracle", **kw) is False


def test_engine_semantic_loss_names_what_is_lost():
    from services.source_engine_semantics import engine_semantic_loss

    with bind_source_engine("oracle"):
        assert "time of day" in engine_semantic_loss("DATE", "DATE", dest_db="postgresql")
    with bind_source_engine("sqlserver"):
        assert "0..255" in engine_semantic_loss("tinyint", "TINYINT", dest_db="mysql")
    with bind_source_engine("mysql"):
        assert "instant" in engine_semantic_loss("timestamp", "TIMESTAMP", dest_db="postgresql")
