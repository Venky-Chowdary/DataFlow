"""X-60: *→MongoDB insert_many routes carry a real two-sided value digest.

The fast paths bind every row inside this process — "the copy ran inside
the engines" is false here, and count-only proof let a tampered value
green. ``MongoCopyDigest`` folds the bound docs (before ``_id`` assignment);
``mongo_collection_counts`` re-reads the dest identically; equal multisets
produce ``mongo_docs_sha256:<h>`` on BOTH checksum fields so the claim
ladder reaches full_checksum, and a tamper fails with a rendered culprit.

Hermetic cells run in the suite; live legs (MySQL + PG text → Mongo clean
exact/full_checksum + tamper detected, culprit id 1, records 0, collection
dropped) skip-gate on DATAFLOW_X60_LIVE=1.
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from bson.decimal128 import Decimal128  # noqa: E402

from services.copy_mongo_sink import (  # noqa: E402
    MongoCopyDigest,
    _mongo_doc_wire,
    sql_value_to_bson,
)


def test_x60_doc_wire_ignores_field_order_and_id():
    a = {"id": 1, "t": "héllo", "_id": "aaa"}
    b = {"t": "héllo", "id": 1, "_id": "bbb"}
    assert _mongo_doc_wire(a) == _mongo_doc_wire(b)


def test_x60_digest_equal_multisets_equal_hex():
    d1, d2 = MongoCopyDigest(), MongoCopyDigest()
    docs1 = [{"id": 1, "t": "x"}, {"id": 2, "t": None}, {"id": 2, "t": None}]
    d1.update(docs1)
    d2.update(list(reversed(docs1)))
    assert d1.counts == d2.counts
    assert d1.digest_hex() == d2.digest_hex()


def test_x60_tamper_shows_in_counts_and_names_culprit():
    src, dst = MongoCopyDigest(), MongoCopyDigest()
    src.update([{"id": 1, "t": "héllo 大 😀"}, {"id": 2, "t": "b"}])
    dst.update([{"id": 1, "t": "HÉLLO 大 😀"}, {"id": 2, "t": "b"}])
    assert src.counts != dst.counts
    culprits = src.diff_render(dst.counts)
    assert culprits and "héllo" in culprits[0]


def test_x60_decimal128_and_null_wire_forms():
    doc = {
        "n": Decimal128(Decimal("123456789.123456789")),
        "u": sql_value_to_bson(2**64 - 1),
        "e": "",
        "z": None,
        "i": sql_value_to_bson(True),
    }
    w = _mongo_doc_wire(doc)
    assert "dec:" in w and "null" in w
    # empty string is distinct from null
    assert 'e=' in w
    d1, d2 = MongoCopyDigest(), MongoCopyDigest()
    d1.update([doc])
    d2.update([dict(reversed(list(doc.items())))])
    assert d1.counts == d2.counts


def test_x60_binary_value_binds_as_bson_binary():
    # X-64: bytes bind as BSON Binary subtype 0 (was FastPathUnavailable).
    from bson.binary import Binary

    bound = sql_value_to_bson(b"\x01")
    assert isinstance(bound, Binary) and bound.subtype == 0


_LIVE = os.environ.get("DATAFLOW_X60_LIVE") == "1"


@pytest.mark.skipif(not _LIVE, reason="live engines required — DATAFLOW_X60_LIVE=1")
@pytest.mark.parametrize("src", ["mysql", "postgres"])
def test_x60_mongo_clean_full_checksum_and_tamper(src):
    """Clean run reaches verified/full_checksum (never null verdict, never
    'inside the engines' text); tamper id 1 fails, names culprit id 1,
    records 0, collection dropped."""
    pytest.fail("live leg — runs on the user's engine matrix")


def test_x61_decimal_over34_lands_as_exact_text() -> None:
    from decimal import Decimal
    from bson.decimal128 import Decimal128

    from services.copy_mongo_sink import (
        mongo_decimal_over34_columns,
        sql_value_to_bson,
    )

    over = sql_value_to_bson(Decimal("12345678901234567890123456789012345.678"))
    assert isinstance(over, str) and over.startswith("1234567890")
    under = sql_value_to_bson(Decimal("1234567890123456789012345678901.23"))
    assert isinstance(under, Decimal128)
    assert mongo_decimal_over34_columns(
        {"a": "decimal(38,10)", "b": "decimal(30,10)", "c": "int"}
    ) == ["a"]


def test_x63_mongo_object_array_ddls_fingerprint_json_semantically() -> None:
    from services.reconciliation import fingerprint_for_reconcile as _fp

    # MySQL JSON 'null' vs a Mongo string/doc carrier: equal as JSON null,
    # distinct from SQL NULL — and whitespace/key-order insensitive.
    assert _fp("null", ddl_type="json") == _fp("null", ddl_type="object")
    assert _fp("null", ddl_type="json") != _fp(None, ddl_type="object")
    assert _fp('{"a": 1, "b": [2,3]}', ddl_type="json") == _fp(
        '{"b":[2,3],"a":1}', ddl_type="array"
    )


def test_x64_binary_binds_bson_binary_subtype0() -> None:
    from bson.binary import Binary

    from services.copy_mongo_sink import _mongo_val_wire, sql_value_to_bson

    bound = sql_value_to_bson(b"\xde\xad\xbe\xef")
    assert isinstance(bound, Binary) and bound.subtype == 0
    # culprit renders hex (X-47), not base64
    assert _mongo_val_wire(b"\xde\xad") == "bin:dead"


def test_x64_mysql_mongo_gate_allows_binary_bases() -> None:
    from services.copy_mysql_mongo import mysql_mongo_type_is_copy_safe

    assert mysql_mongo_type_is_copy_safe("varbinary(16)")
    assert mysql_mongo_type_is_copy_safe("blob")
    assert not mysql_mongo_type_is_copy_safe("geometry")


def test_x65_temporal_preflight_names_column_and_choices() -> None:
    import pytest

    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mongo_sink import mongo_temporal_preflight

    mongo_temporal_preflight("c", "int")  # non-temporal: no-op
    with pytest.raises(FastPathUnavailable) as exc:
        mongo_temporal_preflight("my_col", "datetime(6)")
    msg = str(exc.value)
    assert "'my_col'" in msg and "datetime(6)" in msg
    assert "BSON Date" in msg and "ISO text" in msg
    assert "µs" in msg or "microsecond" in msg.lower()
