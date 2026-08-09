"""Wave 80: CDC LSN family isolation + Mongo BSON + Avro logical fidelity.

Research anchors
----------------
- Debezium / HVR: never invent ``newer`` across dialect stamp families
  (PG WAL vs MySQL binlog/GTID vs numeric CT versions).
- Iceberg CoW upsert: strict-newer LSN (``>`` only; equal = keep existing).
- MongoDB BSON ObjectId / Decimal128 / Binary / UTCDateTime polarity.
- Avro logicalType timestamp-millis / local-timestamp / enum symbols /
  fixed(n) — Confluent + Iceberg schema contracts.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_lsn_family_isolation_refuses_cross_dialect_invent():
    from connectors.writer_common import (
        compare_lsn,
        lsn_family,
        lsn_is_newer,
    )

    assert lsn_family("0/100") == "pg_wal"
    assert lsn_family("mysql-bin.000003:154") == "mysql_binlog"
    assert lsn_family("gtid:source-uuid:1-100") == "mysql_gtid"
    assert lsn_family("00000000000000000042") == "numeric_version"
    assert lsn_family("") == "empty"

    # Cross-family must not invent newer (kind-order trap).
    assert compare_lsn("0/100", "mysql-bin.000003:154") == 0
    assert not lsn_is_newer("0/100", "mysql-bin.000003:154")
    assert not lsn_is_newer("mysql-bin.000003:999", "0/1")
    assert compare_lsn("0/100", "00000000000000000099") == 0
    assert compare_lsn("gtid:a:1", "mysql-bin.000001:1") == 0

    # Empty is older than any concrete stamp.
    assert compare_lsn("", "0/100") == -1
    assert lsn_is_newer("0/100", "")
    assert not lsn_is_newer("", "0/100")

    # Same-family still orders correctly.
    assert compare_lsn("0/100", "0/20") == 1
    assert compare_lsn("mysql-bin.000003:200", "mysql-bin.000003:100") == 1
    assert compare_lsn("00000000000000000042", "00000000000000000010") == 1


def test_sql_lsn_guards_refuse_cross_family_text_fallback():
    from connectors.writer_common import (
        bigquery_lsn_match_predicate,
        mysql_lsn_values_newer_sql,
        postgres_lsn_update_guard_sql,
        snowflake_lsn_match_predicate,
        sqlite_lsn_update_guard_sql,
    )

    pg = postgres_lsn_update_guard_sql("orders")
    assert "::bigint" in pg and "pg_lsn" in pg
    # Numeric + opaque branches — not sole bare text > after NOT filepos.
    assert "NOT (" not in pg or "~ '^[0-9]+$'" in pg

    mysql = mysql_lsn_values_newer_sql()
    assert "REGEXP" in mysql and "CAST(" in mysql

    sqlite = sqlite_lsn_update_guard_sql("orders")
    assert "GLOB" in sqlite and "CAST(" in sqlite

    sf = snowflake_lsn_match_predicate()
    assert "REGEXP_LIKE" in sf and "TRY_TO_NUMBER" in sf
    # Must not invent with NOT both_filepos AND NOT both_pg AND text > alone.
    assert "NOT ({both_filepos}) AND NOT ({both_pg}) AND" not in sf

    bq = bigquery_lsn_match_predicate()
    assert "REGEXP_CONTAINS" in bq and "SAFE_CAST" in bq
    assert "NOT ({both_filepos}) AND NOT ({both_pg}) AND" not in bq


def test_iceberg_equal_lsn_keeps_existing():
    from connectors.iceberg_writer import _merge_upsert_rows

    existing = [{"id": "1", "v": "keep", "_df_lsn": "0/200"}]
    incoming = [{"id": "1", "v": "redelivery", "_df_lsn": "0/200"}]
    merged = _merge_upsert_rows(existing, incoming, pk_cols=["id"])
    assert merged[0]["v"] == "keep"

    # Cross-family refuse invent overwrite.
    incoming2 = [{"id": "1", "v": "invent", "_df_lsn": "mysql-bin.000001:999"}]
    merged2 = _merge_upsert_rows(existing, incoming2, pk_cols=["id"])
    assert merged2[0]["v"] == "keep"


def test_mongo_bson_objectid_binary_timestamptz():
    __import__("pytest").importorskip("bson")
    from bson import ObjectId
    from bson.binary import Binary
    from bson.decimal128 import Decimal128

    from services.schema_introspect import _sample_logical_type
    from services.type_system import (
        ddl_type,
        specialty_carrier_would_collapse,
    )

    oid = ObjectId()
    assert _sample_logical_type(oid) == "OBJECTID"
    assert ddl_type("mongodb", "OBJECTID") == "objectId"
    assert ddl_type("postgresql", "OBJECTID") == "VARCHAR(24)"
    assert specialty_carrier_would_collapse("OBJECTID", "TEXT") is False
    assert specialty_carrier_would_collapse("OBJECTID", "VARCHAR(12)") is True

    assert _sample_logical_type(Binary(b"\x01\x02")) == "BINARY"
    assert _sample_logical_type(Decimal128("12.34")) == "DECIMAL"
    aware = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert _sample_logical_type(aware) == "TIMESTAMPTZ"
    naive = datetime(2024, 1, 1, 12, 0)
    assert _sample_logical_type(naive) == "TIMESTAMP_NTZ"


def test_avro_logical_tokens_enum_fixed_polarity():
    from services.avro_schema import avro_type_to_logical
    from services.type_system import (
        avro_logical_token_to_carrier,
        datetime_timezone_polarity,
        ddl_type,
        normalize_logical_type,
    )

    assert avro_logical_token_to_carrier("timestamp-millis") == "TIMESTAMPTZ"
    assert avro_logical_token_to_carrier("local-timestamp-micros") == "TIMESTAMP_NTZ"
    assert normalize_logical_type("timestamp-millis") == "datetime"
    assert datetime_timezone_polarity("timestamp-millis") == "ltz"
    assert datetime_timezone_polarity("local-timestamp-millis") == "ntz"

    assert avro_type_to_logical(
        {"type": "long", "logicalType": "timestamp-millis"}
    ) == "TIMESTAMPTZ"
    assert avro_type_to_logical(
        {"type": "long", "logicalType": "local-timestamp-micros"}
    ) == "TIMESTAMP_NTZ"
    assert avro_type_to_logical(
        {"type": "long", "logicalType": "time-micros"}
    ) == "TIME(6)"

    enum_c = avro_type_to_logical(
        {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]}
    )
    assert enum_c == "ENUM('RED', 'GREEN')"
    assert "RED" in ddl_type("mysql", enum_c).upper()
    assert "GREEN" in ddl_type("mysql", enum_c).upper()

    assert avro_type_to_logical(
        {"type": "fixed", "name": "MD5", "size": 16}
    ) == "BINARY(16)"
    assert ddl_type("iceberg", "BINARY(16)") == "fixed(16)"
