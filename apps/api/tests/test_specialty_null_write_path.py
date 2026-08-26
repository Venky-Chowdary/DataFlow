"""GEOGRAPHY / INTERVAL bind treats reader-null as SQL NULL, not garbage text.

After extract emits SQL_NULL_SENTINEL, is_geography_wire / is_interval_wire
used to return False, so coerce raised and a valid NULL cell was refused.
Empty / whitespace stay unfit — they are not extract NULL. Missing stays
Missing (sparse omit). 0 and False stay present-and-unfit.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sql_bind import (  # noqa: E402
    coerce_geography_wire,
    coerce_interval_wire,
)
from services.schema_inference import (  # noqa: E402
    geography_wire_srid,
    interval_wire_family,
    is_geography_wire,
    is_interval_wire,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
    is_reader_null_cell,
)


def test_reader_null_cell_is_not_empty_string():
    assert is_reader_null_cell(None) is True
    assert is_reader_null_cell(SQL_NULL_SENTINEL) is True
    assert is_reader_null_cell(DF_MISSING_SENTINEL) is True
    assert is_reader_null_cell(Missing) is True
    assert is_reader_null_cell("") is False
    assert is_reader_null_cell("   ") is False
    assert is_reader_null_cell(0) is False
    assert is_reader_null_cell(False) is False


def test_geography_wire_allows_reader_null():
    assert is_geography_wire(SQL_NULL_SENTINEL) is True
    assert is_geography_wire(Missing) is True
    assert is_geography_wire(None) is True
    assert is_geography_wire("") is False
    assert is_geography_wire("   ") is False
    assert geography_wire_srid(SQL_NULL_SENTINEL) is None


def test_interval_wire_allows_reader_null():
    assert is_interval_wire(SQL_NULL_SENTINEL) is True
    assert is_interval_wire(Missing) is True
    assert is_interval_wire(None) is True
    assert is_interval_wire("") is False
    assert interval_wire_family(SQL_NULL_SENTINEL) is None


def test_coerce_geography_reader_null_is_sql_null():
    assert coerce_geography_wire(SQL_NULL_SENTINEL, ddl_type="GEOGRAPHY") is None
    assert coerce_geography_wire(None, ddl_type="GEOGRAPHY") is None
    assert coerce_geography_wire(Missing, ddl_type="GEOGRAPHY") is Missing
    with pytest.raises(ValueError, match="not WKT"):
        coerce_geography_wire("", ddl_type="GEOGRAPHY")
    with pytest.raises(ValueError, match="not WKT"):
        coerce_geography_wire(0, ddl_type="GEOGRAPHY")


def test_coerce_interval_reader_null_is_sql_null():
    assert coerce_interval_wire(SQL_NULL_SENTINEL, ddl_type="INTERVAL") is None
    assert coerce_interval_wire(None, ddl_type="INTERVAL") is None
    assert coerce_interval_wire(Missing, ddl_type="INTERVAL") is Missing
    with pytest.raises(ValueError, match="not ISO-8601"):
        coerce_interval_wire("", ddl_type="INTERVAL")
    with pytest.raises(ValueError, match="not ISO-8601"):
        coerce_interval_wire(False, ddl_type="INTERVAL")
