"""Wave 51: HSTORE + RANGE bind; JSONB PG native polarity; SF Datetime→TIMESTAMPTZ."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_hstore_dict_and_literal():
    from connectors.sql_bind import coerce_hstore_wire, normalize_sql_bind_value

    assert json.loads(coerce_hstore_wire({"color": "blue", "size": "large"})) == {
        "color": "blue",
        "size": "large",
    }
    lit = '"color"=>"blue","size"=>NULL'
    assert json.loads(coerce_hstore_wire(lit)) == {"color": "blue", "size": None}
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_hstore_wire(42)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_hstore_wire([1, 2, 3])
    assert isinstance(normalize_sql_bind_value({"a": 1}, "HSTORE"), str)


def test_coerce_range_literal_and_dict():
    from connectors.sql_bind import coerce_range_wire, normalize_sql_bind_value

    assert coerce_range_wire("[1,10)") == "[1,10)"
    assert coerce_range_wire("empty") == "empty"
    assert coerce_range_wire({"lower": 1, "upper": 10, "bounds": "[)"}) == "[1,10)"
    assert coerce_range_wire(["[1,2)", "[5,6)"], multi=True) == "{[1,2),[5,6)}"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_range_wire(7)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_range_wire("not-a-range")
    assert normalize_sql_bind_value("[3,7)", "INT4RANGE") == "[3,7)"
    assert normalize_sql_bind_value("{[3,7)}", "INT4MULTIRANGE") == "{[3,7)}"


def test_jsonb_binds_as_text_on_every_engine():
    """Wave 88: Postgres binds JSON text, not a native dict.

    This asserted a native dict for Postgres on the theory that the driver
    adapts it. psycopg2 does not ("can't adapt type 'dict'" — only psycopg3
    does), so JSON-source transfers into JSONB aborted with 0 rows written.
    Postgres casts an unknown-typed text parameter into jsonb, and psycopg2
    still parses jsonb back into a native dict on read.
    """
    from connectors.sql_bind import normalize_sql_bind_value

    for engine in ("postgresql", "mysql", "redshift", "cockroachdb"):
        bound = normalize_sql_bind_value({"k": 1}, "JSONB", engine=engine)
        assert bound == '{"k":1}', engine
        assert not isinstance(bound, dict), f"{engine} would raise can't adapt type 'dict'"
    assert normalize_sql_bind_value({"k": 1}, "VARIANT", engine="snowflake") == '{"k":1}'


def test_salesforce_datetime_is_timestamptz():
    from connectors.salesforce_writer import salesforce_field_to_carrier

    assert salesforce_field_to_carrier({"type": "datetime"}) == "TIMESTAMPTZ"
    assert salesforce_field_to_carrier({"type": "date"}) == "DATE"
    assert salesforce_field_to_carrier(
        {"type": "string", "length": 80}
    ) == "VARCHAR(80)"
