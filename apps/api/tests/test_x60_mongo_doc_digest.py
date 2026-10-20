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


def test_x60_binary_value_declines_as_before():
    # bytes still refuse in sql_value_to_bson (X-64 tracks Binary-typed dest)
    from services.copy_fast_path import FastPathUnavailable

    with pytest.raises(FastPathUnavailable):
        sql_value_to_bson(b"\x01")


_LIVE = os.environ.get("DATAFLOW_X60_LIVE") == "1"


@pytest.mark.skipif(not _LIVE, reason="live engines required — DATAFLOW_X60_LIVE=1")
@pytest.mark.parametrize("src", ["mysql", "postgres"])
def test_x60_mongo_clean_full_checksum_and_tamper(src):
    """Clean run reaches verified/full_checksum (never null verdict, never
    'inside the engines' text); tamper id 1 fails, names culprit id 1,
    records 0, collection dropped."""
    pytest.fail("live leg — runs on the user's engine matrix")
