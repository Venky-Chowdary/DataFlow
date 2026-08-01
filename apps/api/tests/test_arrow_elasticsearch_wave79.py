"""Wave 79: Apache Arrow dtype strings + Elasticsearch specialty field types.

Research anchors
----------------
- Arrow ``timestamp[us, tz=UTC]`` ↔ Iceberg timestamptz; bare unit ↔ NTZ.
- ``decimal128(p,s)`` / ``fixed_size_binary[n]`` / ``date32`` / ``time64[us]``.
- Elasticsearch keyword / geo_point / dense_vector / ip / scaled_float /
  flattened — never invent opaque ``text`` (Elasticsearch mapping docs).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_arrow_timestamp_decimal_binary():
    from services.schema_introspect import _arrow_to_logical
    from services.type_system import (
        arrow_dtype_to_carrier,
        datetime_timezone_polarity,
        ddl_type,
        normalize_logical_type,
        temporal_precision_would_narrow,
    )

    assert arrow_dtype_to_carrier("timestamp[us, tz=UTC]") == "TIMESTAMPTZ(6)"
    assert arrow_dtype_to_carrier("timestamp[ns]") == "TIMESTAMP_NTZ(9)"
    assert arrow_dtype_to_carrier("timestamp[ms, tz=America/New_York]") == (
        "TIMESTAMPTZ(3)"
    )
    assert datetime_timezone_polarity("timestamp[us, tz=UTC]") == "ltz"
    assert datetime_timezone_polarity("timestamp[us]") == "ntz"

    assert _arrow_to_logical("decimal128(38,10)") == "DECIMAL(38,10)"
    assert normalize_logical_type("decimal128(38,10)") == "decimal"
    assert ddl_type("postgresql", "decimal128(38,10)") == "NUMERIC(38,10)"
    assert ddl_type("iceberg", "decimal128(38,10)") == "decimal(38,10)"

    assert _arrow_to_logical("fixed_size_binary[16]") == "BINARY(16)"
    assert ddl_type("postgresql", "fixed_size_binary[16]") == "BYTEA"
    assert ddl_type("iceberg", "fixed_size_binary[16]") == "fixed(16)"

    assert _arrow_to_logical("date32") == "DATE"
    assert _arrow_to_logical("date64") == "DATE"
    assert _arrow_to_logical("time64[us]") == "TIME(6)"
    assert _arrow_to_logical("duration[us]") == "INTERVAL DAY TO SECOND"
    assert _arrow_to_logical("large_string") == "TEXT"
    assert _arrow_to_logical("dictionary<values=string, indices=int32>") == "TEXT"

    # Must not invent TEXT for Arrow timestamps.
    assert ddl_type("postgresql", "timestamp[us, tz=UTC]") == "TIMESTAMPTZ(6)"
    # PostgreSQL caps fractional seconds at 6, so TIMESTAMP(9) is DDL it would
    # reject. Clamp to the engine maximum and surface the nanosecond loss.
    assert ddl_type("postgresql", "timestamp[ns]") == "TIMESTAMP(6)"
    assert temporal_precision_would_narrow("timestamp[ns]", "TIMESTAMP(6)") is True
    # Engines that really hold nanoseconds keep them.
    assert ddl_type("snowflake", "timestamp[ns]") == "TIMESTAMP_NTZ(9)"
    assert ddl_type("clickhouse", "timestamp[ns]") == "DateTime64(9)"


def test_elasticsearch_specialty_not_text():
    from services.type_system import (
        ddl_type,
        normalize_logical_type,
        specialty_carrier_would_collapse,
    )

    assert normalize_logical_type("keyword") == "string"
    assert normalize_logical_type("scaled_float") == "float"
    assert normalize_logical_type("geo_point") == "geography"
    assert normalize_logical_type("dense_vector") == "vector"
    assert normalize_logical_type("flattened") == "json"
    assert normalize_logical_type("ip") == "string"

    assert ddl_type("elasticsearch", "keyword") == "keyword"
    assert ddl_type("elasticsearch", "geo_point") == "geo_point"
    assert ddl_type("elasticsearch", "dense_vector") == "dense_vector"
    assert ddl_type("elasticsearch", "ip") == "ip"
    assert ddl_type("elasticsearch", "scaled_float") == "scaled_float"
    assert ddl_type("elasticsearch", "flattened") == "flattened"

    assert ddl_type("postgresql", "geo_point") == "GEOGRAPHY"
    assert ddl_type("postgresql", "ip") == "INET"
    assert specialty_carrier_would_collapse("IP", "TEXT") is True
    assert specialty_carrier_would_collapse("IP", "INET") is False

    # Must not invent ES text mapping for specialty sources.
    assert ddl_type("elasticsearch", "geo_point") != "text"
    assert ddl_type("elasticsearch", "dense_vector") != "text"
