"""A column type is a join over what was observed, never a vote.

Every schemaless source resolved this independently and all of them voted, so a
column of 999 integers and one ``2000.50`` resolved to INTEGER and that row then
failed the write. The defect scaled the wrong way: the more rows a table had,
the more certain it was that the minority value would be mistyped.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.type_lattice import (  # noqa: E402
    canonical_logical,
    join_logical_types,
    resolve_observed_types,
)

_CARRIERS = [
    "BOOLEAN",
    "INTEGER",
    "DECIMAL",
    "FLOAT",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "BINARY",
    "ARRAY",
    "OBJECT",
    "JSON",
    "VARCHAR",
    "TEXT",
    "UUID",
]


# ── the algebra ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("left", _CARRIERS)
@pytest.mark.parametrize("right", _CARRIERS)
def test_join_is_commutative(left: str, right: str) -> None:
    """The order values happen to arrive in cannot change a column's type."""
    assert join_logical_types(left, right) == join_logical_types(right, left)


@pytest.mark.parametrize("carrier", _CARRIERS)
def test_join_is_idempotent(carrier: str) -> None:
    assert join_logical_types(carrier, carrier) == carrier


@pytest.mark.parametrize("left", _CARRIERS)
@pytest.mark.parametrize("right", _CARRIERS)
def test_join_is_associative(left: str, right: str) -> None:
    """Paging must not change the answer: fold order is irrelevant."""
    for third in ("INTEGER", "TEXT", "JSON", "TIMESTAMP"):
        assert join_logical_types(join_logical_types(left, right), third) == (
            join_logical_types(left, join_logical_types(right, third))
        )


@pytest.mark.parametrize("carrier", _CARRIERS)
def test_text_is_the_top(carrier: str) -> None:
    """Text holds every serialization, so nothing widens past it."""
    assert join_logical_types(carrier, "TEXT") == "TEXT"


# ── the ordering itself ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        # The headline defect: one decimal among a thousand integers.
        ({"INTEGER": 999, "DECIMAL": 1}, "DECIMAL"),
        ({"INTEGER": 999, "FLOAT": 1}, "FLOAT"),
        ({"DECIMAL": 50, "FLOAT": 1}, "FLOAT"),
        ({"BOOLEAN": 9, "INTEGER": 1}, "INTEGER"),
        # A date is a timestamp at midnight; an aware value keeps its offset.
        ({"DATE": 9, "TIMESTAMP": 1}, "TIMESTAMP"),
        ({"TIMESTAMP": 9, "TIMESTAMPTZ": 1}, "TIMESTAMPTZ"),
        # ARRAY and OBJECT are siblings: neither holds the other.
        ({"ARRAY": 3, "OBJECT": 1}, "JSON"),
        ({"ARRAY": 3, "JSON": 1}, "JSON"),
        # Different families have no common carrier but text. JSON is not
        # treated as an upper bound of scalars: landing a numeric field in a
        # JSON column changes how it is queried, while text does not pretend
        # to be structured.
        ({"OBJECT": 3, "INTEGER": 1}, "TEXT"),
        ({"INTEGER": 3, "TIMESTAMP": 1}, "TEXT"),
        ({"BINARY": 3, "INTEGER": 1}, "TEXT"),
        ({"UUID": 3, "INTEGER": 1}, "TEXT"),
        # Single observations stay exactly what they were.
        ({"DECIMAL": 1}, "DECIMAL"),
        ({"VARCHAR": 3}, "VARCHAR"),
        ({"VARCHAR": 3, "TEXT": 1}, "TEXT"),
    ],
)
def test_observed_types_resolve_to_the_widest(observed, expected) -> None:
    assert resolve_observed_types(observed) == expected


def test_counts_do_not_influence_the_result() -> None:
    """How often a value appeared says nothing about whether it must fit."""
    assert resolve_observed_types({"INTEGER": 1, "DECIMAL": 1}) == resolve_observed_types(
        {"INTEGER": 10_000_000, "DECIMAL": 1}
    )


def test_nothing_observed_stays_unknown() -> None:
    """A null-only column must not invent a carrier."""
    assert resolve_observed_types({}) == ""
    assert resolve_observed_types({"INTEGER": 0}) == ""
    assert resolve_observed_types([]) == ""


def test_accepts_a_bare_iterable_of_carriers() -> None:
    assert resolve_observed_types(["INTEGER", "DECIMAL"]) == "DECIMAL"


def test_dialect_spellings_normalize() -> None:
    """A connector reports its own dialect's word without every caller mapping it."""
    assert canonical_logical("bigint") == "INTEGER"
    assert canonical_logical("Double") == "FLOAT"
    assert canonical_logical("timestamp_ltz") == "TIMESTAMPTZ"
    assert resolve_observed_types({"NUMERIC": 1, "INT": 2}) == "DECIMAL"


def test_unknown_carriers_are_not_assumed_compatible() -> None:
    assert join_logical_types("SOMETHING_NEW", "SOMETHING_NEW") == "SOMETHING_NEW"
    assert join_logical_types("SOMETHING_NEW", "INTEGER") == "TEXT"


# ── the sources that share it ────────────────────────────────────────────────


def test_dynamodb_reader_uses_the_lattice() -> None:
    from connectors.dynamodb_reader import widen_logical_votes

    assert widen_logical_votes({"INTEGER": 999, "FLOAT": 1}) == "FLOAT"
    # A null-only Dynamo attribute has no observations to join.
    assert widen_logical_votes({}) == "VARCHAR"


def test_mongodb_resolution_keeps_its_sentinel_policy() -> None:
    """Sentinel handling is data quality; the join is type resolution.

    ``"N/A"`` among 49 integers should quarantine the outlier rather than widen
    the column to text, and that policy is Mongo's own. What the typed values
    resolve to is not a vote.
    """
    from services.schema_introspect import (
        _finalize_mongodb_type,
        _finalize_mongodb_type_with_note,
    )

    assert _finalize_mongodb_type({"INTEGER": 49, "TEXT": 1}) == "INTEGER"
    assert _finalize_mongodb_type({"INTEGER": 5, "TEXT": 5}) == "TEXT"
    # …but a real float among integers widens, where it used to lose the vote.
    assert _finalize_mongodb_type({"INTEGER": 999, "FLOAT": 1}) == "FLOAT"
    _chosen, note = _finalize_mongodb_type_with_note({"INTEGER": 49, "TEXT": 1})
    assert note and "sentinel" in note


def test_pairwise_widen_no_longer_ranks_by_specificity() -> None:
    """BINARY outranked INTEGER, so a field with both resolved to BINARY."""
    from services.schema_introspect import _widen_mongodb_type

    assert _widen_mongodb_type("INTEGER", "BINARY") == "TEXT"
    assert _widen_mongodb_type("INTEGER", "DECIMAL") == "DECIMAL"
    assert _widen_mongodb_type("", "INTEGER") == "INTEGER"
    assert _widen_mongodb_type("INTEGER", "") == "INTEGER"
