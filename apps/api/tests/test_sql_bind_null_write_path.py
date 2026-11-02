"""Every SQL bind coerce treats reader-null as SQL NULL via _absent_sql_bind.

Direct coerce_*_wire callers used to only check None / Missing. After extract
emits SQL_NULL_SENTINEL, INET / JSON / BOOLEAN tried to parse the sentinel
spelling. Missing stays Missing. Empty string still refuses on specialty
types — that is not extract NULL. 0 and False stay present.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sql_bind import (  # noqa: E402
    coerce_array_wire,
    coerce_binary_wire,
    coerce_bitstring_wire,
    coerce_boolean_wire,
    coerce_box_wire,
    coerce_cid_wire,
    coerce_cidr_wire,
    coerce_circle_wire,
    coerce_citext_wire,
    coerce_decimal_wire,
    coerce_enum_wire,
    coerce_float_wire,
    coerce_geography_wire,
    coerce_hierarchyid_wire,
    coerce_hstore_wire,
    coerce_inet_wire,
    coerce_integer_wire,
    coerce_interval_wire,
    coerce_json_wire,
    coerce_jsonpath_wire,
    coerce_line_wire,
    coerce_lseg_wire,
    coerce_ltree_wire,
    coerce_macaddr_wire,
    coerce_map_wire,
    coerce_oid_wire,
    coerce_path_wire,
    coerce_pg_lsn_wire,
    coerce_point_wire,
    coerce_polygon_wire,
    coerce_range_wire,
    coerce_rowid_wire,
    coerce_rowversion_wire,
    coerce_set_wire,
    coerce_sql_variant_wire,
    coerce_struct_wire,
    coerce_tid_wire,
    coerce_tsvector_wire,
    coerce_txid_snapshot_wire,
    coerce_uuid_wire,
    coerce_xid_wire,
    coerce_xml_wire,
    coerce_year_wire,
    normalize_sql_bind_value,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def _enum(value):
    return coerce_enum_wire(value, ddl_type="ENUM('a','b')")


def _set(value):
    return coerce_set_wire(value, ddl_type="SET('a','b')")


_COERCES = (
    coerce_inet_wire,
    coerce_cidr_wire,
    coerce_macaddr_wire,
    coerce_hstore_wire,
    coerce_range_wire,
    coerce_jsonpath_wire,
    coerce_xml_wire,
    coerce_citext_wire,
    coerce_ltree_wire,
    coerce_hierarchyid_wire,
    coerce_tsvector_wire,
    coerce_point_wire,
    coerce_box_wire,
    coerce_circle_wire,
    coerce_lseg_wire,
    coerce_line_wire,
    coerce_path_wire,
    coerce_polygon_wire,
    coerce_pg_lsn_wire,
    coerce_oid_wire,
    coerce_tid_wire,
    coerce_xid_wire,
    coerce_cid_wire,
    coerce_txid_snapshot_wire,
    _enum,
    _set,
    coerce_year_wire,
    coerce_boolean_wire,
    coerce_integer_wire,
    coerce_sql_variant_wire,
    coerce_rowid_wire,
    coerce_float_wire,
    coerce_json_wire,
    coerce_binary_wire,
    coerce_uuid_wire,
    coerce_rowversion_wire,
    coerce_bitstring_wire,
    coerce_array_wire,
    coerce_struct_wire,
    coerce_map_wire,
    coerce_decimal_wire,
    coerce_interval_wire,
    coerce_geography_wire,
)

_NULL_WIRES = (None, SQL_NULL_SENTINEL, "__df_ddb_null__")


def test_coerce_reader_null_is_sql_null():
    for fn in _COERCES:
        for wire in _NULL_WIRES:
            assert fn(wire) is None, (fn.__name__ if hasattr(fn, "__name__") else fn, wire)
        assert fn(Missing) is Missing, fn
        assert fn(DF_MISSING_SENTINEL) is Missing or fn(DF_MISSING_SENTINEL) == DF_MISSING_SENTINEL, fn


def test_normalize_reader_null_is_sql_null():
    for ddl in (
        "INET",
        "CIDR",
        "MACADDR",
        "HSTORE",
        "INT4RANGE",
        "JSONPATH",
        "XML",
        "CITEXT",
        "LTREE",
        "TSVECTOR",
        "POINT",
        "BOOLEAN",
        "INTEGER",
        "JSONB",
        "UUID",
        "FLOAT",
        "DECIMAL",
        "INTERVAL",
        "GEOGRAPHY",
        "YEAR",
        "OID",
        "PG_LSN",
        "BYTEA",
        "BIT(4)",
        "ENUM('a','b')",
        "SET('a','b')",
    ):
        for wire in _NULL_WIRES:
            assert normalize_sql_bind_value(wire, ddl, engine="postgresql") is None
        assert normalize_sql_bind_value(Missing, ddl, engine="postgresql") is Missing


def test_empty_string_still_refuses_specialty_null_invent():
    with pytest.raises(ValueError, match="empty string"):
        coerce_inet_wire("")
    with pytest.raises(ValueError, match="empty string"):
        coerce_json_wire("")
    with pytest.raises(ValueError, match="empty string"):
        coerce_boolean_wire("")
    with pytest.raises(ValueError, match="not WKT"):
        coerce_geography_wire("")
    with pytest.raises(ValueError, match="not ISO-8601"):
        coerce_interval_wire("")


def test_zero_and_false_stay_present():
    assert coerce_integer_wire(0) == 0
    assert coerce_boolean_wire(False) is False
    assert coerce_boolean_wire("false") is False
    assert normalize_sql_bind_value(0, "INTEGER", engine="postgresql") == 0
    assert normalize_sql_bind_value(False, "BOOLEAN", engine="postgresql") is False
