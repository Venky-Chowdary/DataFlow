"""Unit tests for the row filter engine."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.row_filter import (  # noqa: E402
    apply_row_filter,
    apply_row_filter_to_matrix,
)

RECORDS = [
    {"id": "1", "status": "active", "amount": "100"},
    {"id": "2", "status": "inactive", "amount": "250"},
    {"id": "3", "status": "active", "amount": "75"},
    {"id": "4", "status": "pending", "amount": "300"},
]


def test_empty_spec_is_noop() -> None:
    assert apply_row_filter(RECORDS, {}) == RECORDS
    assert apply_row_filter(RECORDS, None) == RECORDS


def test_eq_string_filter() -> None:
    result = apply_row_filter(RECORDS, {"column": "status", "operator": "eq", "value": "active"})
    assert {r["id"] for r in result} == {"1", "3"}


def test_ne_filter() -> None:
    result = apply_row_filter(RECORDS, {"column": "status", "operator": "ne", "value": "active"})
    assert {r["id"] for r in result} == {"2", "4"}


def test_numeric_gt_gte_lt_lte() -> None:
    assert {r["id"] for r in apply_row_filter(RECORDS, {"column": "amount", "operator": "gt", "value": "100"})} == {"2", "4"}
    assert {r["id"] for r in apply_row_filter(RECORDS, {"column": "amount", "operator": "gte", "value": "100"})} == {"1", "2", "4"}
    assert {r["id"] for r in apply_row_filter(RECORDS, {"column": "amount", "operator": "lt", "value": "250"})} == {"1", "3"}
    assert {r["id"] for r in apply_row_filter(RECORDS, {"column": "amount", "operator": "lte", "value": "100"})} == {"1", "3"}


def test_in_filter() -> None:
    result = apply_row_filter(RECORDS, {"column": "status", "operator": "in", "value": ["active", "pending"]})
    assert {r["id"] for r in result} == {"1", "3", "4"}


def test_contains_startswith_endswith() -> None:
    records = [
        {"id": "1", "email": "alice@example.com"},
        {"id": "2", "email": "bob@acme.org"},
        {"id": "3", "email": "charlie@example.com"},
    ]
    assert {r["id"] for r in apply_row_filter(records, {"column": "email", "operator": "contains", "value": "example"})} == {"1", "3"}
    assert {r["id"] for r in apply_row_filter(records, {"column": "email", "operator": "startswith", "value": "a"})} == {"1"}
    assert {r["id"] for r in apply_row_filter(records, {"column": "email", "operator": "endswith", "value": ".org"})} == {"2"}


def test_regex_filter() -> None:
    result = apply_row_filter(RECORDS, {"column": "amount", "operator": "regex", "value": r"^1|^3"})
    assert {r["id"] for r in result} == {"1", "4"}


def test_null_filters() -> None:
    records = [
        {"id": "1", "note": ""},
        {"id": "2", "note": "hello"},
        {"id": "3", "note": None},
    ]
    assert {r["id"] for r in apply_row_filter(records, {"column": "note", "operator": "is_null"})} == {"1", "3"}
    assert {r["id"] for r in apply_row_filter(records, {"column": "note", "operator": "is_not_null"})} == {"2"}


def test_and_or_composition() -> None:
    spec = {
        "and": [
            {"column": "status", "operator": "eq", "value": "active"},
            {"column": "amount", "operator": "gte", "value": "75"},
        ]
    }
    assert {r["id"] for r in apply_row_filter(RECORDS, spec)} == {"1", "3"}

    spec_or = {
        "or": [
            {"column": "status", "operator": "eq", "value": "inactive"},
            {"column": "amount", "operator": "gt", "value": "200"},
        ]
    }
    assert {r["id"] for r in apply_row_filter(RECORDS, spec_or)} == {"2", "4"}


def test_matrix_filter() -> None:
    headers = ["id", "status", "amount"]
    rows = [["1", "active", "100"], ["2", "inactive", "250"], ["3", "active", "75"]]
    result = apply_row_filter_to_matrix(headers, rows, {"column": "status", "operator": "eq", "value": "active"})
    assert result == [["1", "active", "100"], ["3", "active", "75"]]
