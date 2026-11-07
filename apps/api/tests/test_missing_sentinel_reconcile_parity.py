"""An absent source field must reconcile against a destination NULL.

Schemaless sources (Mongo, DynamoDB, Redis, object stores, sparse CDC images)
hand the engine a ``Missing`` sentinel for fields a document simply does not
have. A SQL destination has no way to store absence, so the writer stores NULL
and the read-back returns NULL. Gate-8 has to agree, or every sparse document
fails reconciliation on a transfer that lost nothing.

The dangerous direction is the other one: absence must not be confused with an
empty string, or a destination that really stored ``''`` would reconcile clean
against a source that never had the field.
"""

from __future__ import annotations

import pytest

from services.reconciliation import checksum_rows, fingerprint_for_reconcile
from services.value_serializer import (
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)

ENGINES = ["postgresql", "mysql", "snowflake", "sqlite", "bigquery", ""]


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("ddl", ["TEXT", "VARCHAR(50)", "NUMBER(38,0)", ""])
def test_every_absent_spelling_fingerprints_as_null(engine: str, ddl: str):
    prints = {
        fingerprint_for_reconcile(value, ddl_type=ddl, engine=engine)
        for value in (Missing, DF_MISSING_SENTINEL, SQL_NULL_SENTINEL, None)
    }
    assert len(prints) == 1, f"absent spellings disagree on {engine}/{ddl}: {prints}"
    assert prints.pop() == "\x00NULL\x00"


@pytest.mark.parametrize("engine", ["postgresql", "mysql", "snowflake", "sqlite"])
def test_absent_is_not_empty_string(engine: str):
    absent = fingerprint_for_reconcile(Missing, ddl_type="TEXT", engine=engine)
    empty = fingerprint_for_reconcile("", ddl_type="TEXT", engine=engine)
    assert absent != empty, (
        f"{engine}: an absent field fingerprints the same as a stored empty "
        "string, so a destination that wrote '' would reconcile clean"
    )


def test_sparse_source_row_matches_null_readback():
    """The Mongo→Snowflake shape that failed: one doc missing a flattened field."""
    cols = ["id", "profile_age"]
    dest_types = {"id": "NUMBER(38,0)", "profile_age": "TEXT"}
    source = [(1, "30"), (2, Missing)]
    readback = [{"id": 1, "profile_age": "30"}, {"id": 2, "profile_age": None}]

    assert checksum_rows(
        source, cols, dest_db_type="snowflake", dest_types=dest_types
    ) == checksum_rows(
        readback, cols, dest_db_type="snowflake", dest_types=dest_types
    )


def test_sparse_source_does_not_match_empty_string_readback():
    cols = ["id", "profile_age"]
    dest_types = {"id": "NUMBER(38,0)", "profile_age": "TEXT"}
    source = [(1, "30"), (2, Missing)]
    readback = [{"id": 1, "profile_age": "30"}, {"id": 2, "profile_age": ""}]

    assert checksum_rows(
        source, cols, dest_db_type="snowflake", dest_types=dest_types
    ) != checksum_rows(
        readback, cols, dest_db_type="snowflake", dest_types=dest_types
    )
