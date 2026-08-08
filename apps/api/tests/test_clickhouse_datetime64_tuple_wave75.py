"""Wave 75: ClickHouse DateTime64 / Tuple / IPv4–IPv6 fidelity SSOT.

Research anchors
----------------
- ClickHouse DateTime64(p[, timezone]) stores UTC ticks; TZ is column metadata
  (LTZ-class), not per-row offset.
- Tuple/Map/Array are native nested (PG JSONB = honest document collapse).
- IPv4/IPv6 → PostgreSQL INET (specialty); never invent opaque String.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_ch_datetime64_polarity_and_ddl():
    from services.schema_introspect import _ch_to_logical
    from services.type_system import (
        datetime_timezone_polarity,
        ddl_type,
        normalize_logical_type,
        parse_temporal_fractional_precision,
        specialty_carrier_would_collapse,
    )

    assert _ch_to_logical("DateTime64(3)") == "DateTime64(3)"
    assert _ch_to_logical("DateTime64(3, 'UTC')") == "DateTime64(3, 'UTC')"
    assert _ch_to_logical("DateTime") == "DateTime"
    assert _ch_to_logical("DateTime('UTC')") == "DateTime('UTC')"

    assert normalize_logical_type("DateTime64(3)") == "datetime"
    assert normalize_logical_type("DateTime64(3, 'UTC')") == "datetime"
    assert parse_temporal_fractional_precision("DateTime64(3, 'UTC')") == 3
    assert parse_temporal_fractional_precision("DateTime64(6)") == 6

    assert datetime_timezone_polarity("DateTime64(3)") == "ntz"
    assert datetime_timezone_polarity("DateTime64(3, 'UTC')") == "ltz"
    assert datetime_timezone_polarity("DateTime('Europe/Moscow')") == "ltz"

    assert ddl_type("clickhouse", "DateTime64(3)") == "DateTime64(3)"
    assert ddl_type("clickhouse", "DateTime64(3, 'UTC')") == "DateTime64(3, 'UTC')"
    assert ddl_type("postgresql", "DateTime64(3)") == "TIMESTAMP(3)"
    assert ddl_type("postgresql", "DateTime64(3, 'UTC')") == "TIMESTAMPTZ(3)"
    # Must never collapse to TEXT/String.
    assert ddl_type("postgresql", "DateTime64(3)").upper() != "TEXT"
    assert ddl_type("clickhouse", "DateTime64(3)") != "String"


def test_ch_tuple_map_array_nested():
    from services.schema_introspect import _ch_to_logical
    from services.type_system import ddl_type, is_nested_document_collapse

    assert _ch_to_logical("Array(String)") == "ARRAY<TEXT>"
    # Width-preserving nested carriers (audit §2.1) — Int64 must not collapse to INTEGER.
    assert _ch_to_logical("Map(String, Int64)") == "MAP<TEXT,Int64>"
    assert _ch_to_logical("Tuple(Int64, String)") == "STRUCT<_0:Int64, _1:TEXT>"

    assert ddl_type("clickhouse", "Array(String)") == "Array(String)"
    assert ddl_type("clickhouse", "Map(String, Int64)") == "Map(String, Int64)"
    assert ddl_type("clickhouse", "Tuple(Int64, String)").startswith("Tuple(")
    assert ddl_type("postgresql", "Tuple(Int64, String)") == "JSONB"
    assert is_nested_document_collapse("STRUCT<_0:Int64, _1:TEXT>", "JSON") is True


def test_ch_ipv4_ipv6_to_inet():
    from services.schema_introspect import _ch_to_logical
    from services.type_system import ddl_type, specialty_carrier_would_collapse

    assert _ch_to_logical("IPv4") == "IPv4"
    assert _ch_to_logical("IPv6") == "IPv6"
    assert ddl_type("clickhouse", "IPv4") == "IPv4"
    assert ddl_type("postgresql", "IPv4") == "INET"
    assert ddl_type("postgresql", "IPv6") == "INET"
    assert specialty_carrier_would_collapse("IPv4", "TEXT") is True
    assert specialty_carrier_would_collapse("IPv4", "INET") is False
