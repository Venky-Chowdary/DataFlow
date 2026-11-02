"""Mongo insert refuses reader-null ``_id``, not only Python None.

insert_many only raised when ``_id is None``. After extract emits
SQL_NULL_SENTINEL, that token stored as document identity.
Empty string stays a present key. 0 stays present.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.mongodb_writer import (  # noqa: E402
    _idempotent_insert_many,
    _mongo_insert_id_is_null,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_mongo_insert_id_treats_reader_null_as_null():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL):
        assert _mongo_insert_id_is_null({"_id": wire}) is True, wire
    assert _mongo_insert_id_is_null({"_id": ""}) is False
    assert _mongo_insert_id_is_null({"_id": 0}) is False
    assert _mongo_insert_id_is_null({"name": "x"}) is False


def test_idempotent_insert_many_refuses_reader_null_id():
    with pytest.raises(ValueError, match="null `_id`"):
        _idempotent_insert_many(None, [{"_id": SQL_NULL_SENTINEL, "v": 1}])
    with pytest.raises(ValueError, match="null `_id`"):
        _idempotent_insert_many(None, [{"_id": None, "v": 1}])
