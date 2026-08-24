"""PROPERTY 1 — type identity is referentially transparent.

Case variants of the same logical spelling must invent identical destination
DDL. Ambiguous INTEGER/INT/FLOAT never case-select 32-bit. Unambiguous INT4 /
FLOAT32 keep width. Monotonicity: ddl_type never narrower than DDL_TYPES.
"""

from __future__ import annotations

import itertools

import pytest

from services.decision_kernel import LogicalType, ddl_invent_never_narrower_than_table, ddl_type
from services.type_system import (
    DDL_TYPES,
    LOGICAL_FLOAT,
    LOGICAL_INTEGER,
    float_mantissa_bits,
    integer_bit_width,
    normalize_logical_type,
)


def _case_variants(token: str) -> list[str]:
    """lower / UPPER / Title / mIxEd spellings of a single-token type name."""
    lower = token.lower()
    upper = token.upper()
    title = token[:1].upper() + token[1:].lower() if token else token
    mixed = "".join(
        c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(token.lower())
    )
    out: list[str] = []
    for v in (token, lower, upper, title, mixed):
        if v not in out:
            out.append(v)
    return out


# Ambiguous + logical family tokens — must invent identical 64-bit across cases.
_AMBIGUOUS_INTEGER = ("integer", "INTEGER", "INT", "Int", "iNt")
_AMBIGUOUS_FLOAT = ("float", "FLOAT", "Float", "fLoAt")

# Representative logical family spellings for full dest × case matrix.
_LOGICAL_SPELLINGS = (
    "integer",
    "float",
    "string",
    "boolean",
    "date",
    "datetime",
    "decimal",
    "json",
    "binary",
    "uuid",
)


@pytest.mark.parametrize("dest", sorted(DDL_TYPES))
@pytest.mark.parametrize("spelling", _AMBIGUOUS_INTEGER)
def test_ambiguous_integer_invents_64_all_cases(dest: str, spelling: str):
    assert normalize_logical_type(spelling) == LOGICAL_INTEGER
    assert integer_bit_width(spelling) is None
    assert ddl_type(dest, spelling) == ddl_type(dest, LOGICAL_INTEGER)
    assert ddl_type(dest, spelling) == DDL_TYPES[dest][LOGICAL_INTEGER]


@pytest.mark.parametrize("dest", sorted(DDL_TYPES))
@pytest.mark.parametrize("spelling", _AMBIGUOUS_FLOAT)
def test_ambiguous_float_invents_64_all_cases(dest: str, spelling: str):
    assert normalize_logical_type(spelling) == LOGICAL_FLOAT
    assert float_mantissa_bits(spelling) is None
    assert ddl_type(dest, spelling) == ddl_type(dest, LOGICAL_FLOAT)


@pytest.mark.parametrize("dest", sorted(DDL_TYPES))
@pytest.mark.parametrize("logical", _LOGICAL_SPELLINGS)
def test_case_variant_matrix_identical_invent(dest: str, logical: str):
    variants = _case_variants(logical)
    invented = [ddl_type(dest, v) for v in variants]
    assert len(set(invented)) == 1, (dest, logical, list(zip(variants, invented)))


@pytest.mark.parametrize("dest", sorted(DDL_TYPES))
@pytest.mark.parametrize("logical", (LOGICAL_INTEGER, LOGICAL_FLOAT))
def test_monotonicity_vs_ddl_types(dest: str, logical: str):
    assert ddl_invent_never_narrower_than_table(dest, logical), (
        f"{dest}/{logical}: ddl_type={ddl_type(dest, logical)!r} "
        f"DDL_TYPES={DDL_TYPES[dest].get(logical)!r}"
    )


def test_unambiguous_int4_stays_32():
    assert integer_bit_width("INT4") == 32
    assert ddl_type("postgresql", "INT4") == "INTEGER"
    assert ddl_type("clickhouse", "INT4") == "Int32"
    assert ddl_type("iceberg", "INT4") == "int"
    assert ddl_type("mysql", "INT4") == "INT"


def test_unambiguous_float32_stays_24():
    assert float_mantissa_bits("FLOAT32") == 24
    assert float_mantissa_bits("REAL") == 24
    assert ddl_type("clickhouse", "FLOAT32") == "Float32"
    assert ddl_type("iceberg", "FLOAT32") == "float"
    assert ddl_type("mysql", "FLOAT32") == "FLOAT"


def test_logical_type_width_bearing_api():
    assert ddl_type("clickhouse", LogicalType(kind="integer")) == "Int64"
    assert ddl_type("clickhouse", LogicalType(kind="integer", width=32)) == "Int32"
    assert ddl_type("iceberg", LogicalType(kind="float")) == "double"
    assert ddl_type("iceberg", LogicalType(kind="float", width=24)) == "float"


def test_full_case_matrix_dump_for_proof_artifact():
    """Executable proof artifact: every dest × logical × case → one DDL cell."""
    rows: list[tuple[str, str, str, str]] = []
    for dest, logical in itertools.product(sorted(DDL_TYPES), _LOGICAL_SPELLINGS):
        cells = {v: ddl_type(dest, v) for v in _case_variants(logical)}
        assert len(set(cells.values())) == 1, (dest, logical, cells)
        rows.append((dest, logical, next(iter(cells.values())), ",".join(cells)))
    # Sanity: integer cells match DDL_TYPES never-narrower default.
    # SQLite's affinity token is ``INTEGER`` but holds 64-bit values — allowed
    # only when it is the table default, never as a silent INT32 invent.
    for dest, logical, invented, _ in rows:
        if logical != "integer":
            continue
        table = DDL_TYPES[dest][LOGICAL_INTEGER]
        assert invented == table, (dest, invented, table)
        if invented.strip().upper() in {"INTEGER", "INT", "INT32", "SIGNED"}:
            assert dest == "sqlite", (dest, invented)
    assert len(rows) == len(DDL_TYPES) * len(_LOGICAL_SPELLINGS)
