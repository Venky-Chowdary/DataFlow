"""UX-33: a column whose type is spelled the same on both sides was described
as "widens VARCHAR(200) to VARCHAR(200)" when the engines differ. Equal types
say "same type", never "widens X to X"."""
from __future__ import annotations

import re

import pytest

from services.reversibility import classify_mapping

PAIRS = [("mysql", "postgresql"), ("postgresql", "mysql"), ("mysql", "sqlserver"), ("sqlserver", "postgresql")]
TYPES = ["VARCHAR(200)", "INTEGER", "BIGINT", "TEXT", "DATE"]


@pytest.mark.parametrize("source_db,dest_db", PAIRS)
@pytest.mark.parametrize("col_type", TYPES)
def test_equal_types_never_read_as_widened(source_db: str, dest_db: str, col_type: str) -> None:
    out = classify_mapping(
        {"source": "name", "target": "name", "source_type": col_type, "target_type": col_type},
        dest_db=dest_db, source_db=source_db,
    )
    reason = out["reason"]
    assert not re.search(r"(\S+) to \1\b", reason), reason
    if out["reason_code"] == "widen":
        assert "same type" in reason, reason


def test_same_spelling_says_same_type_and_keeps_its_verdict() -> None:
    out = classify_mapping(
        {"source": "name", "target": "name", "source_type": "VARCHAR(200)", "target_type": "varchar(200)"},
        dest_db="postgresql", source_db="mysql",
    )
    assert out["invertibility"] == "invertible"
    assert out["reason"] == "name keeps the same type (varchar(200)); every value fits."


def test_a_real_widen_still_names_both_types() -> None:
    out = classify_mapping(
        {"source": "cust", "target": "cust", "source_type": "VARCHAR(50)", "target_type": "VARCHAR(200)"},
        dest_db="postgresql", source_db="mysql",
    )
    assert out["reason"] == "cust widens VARCHAR(50) to VARCHAR(200); every value fits."
