"""Elasticsearch document ids use present_cell_text, not str(value).

SQL_NULL_SENTINEL and DuckDB null used to become ``_id``. True became
``True`` so dest ``true`` missed upsert identity. Composite parts still
fail closed when any part is absent. 0 stays present.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.elasticsearch_writer import _resolve_doc_id  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_es_id_reader_null_is_absent():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", "", "   "):
        assert (
            _resolve_doc_id(
                {"_id": wire},
                conflict_columns=[],
                target_cols=["_id"],
            )
            is None
        )


def test_es_id_true_shares_dest_true():
    assert (
        _resolve_doc_id({"_id": True}, conflict_columns=[], target_cols=["_id"])
        == "true"
    )
    assert (
        _resolve_doc_id({"id": True}, conflict_columns=[], target_cols=["id"])
        == "true"
    )
    assert (
        _resolve_doc_id(
            {"tenant_id": True, "order_id": "o9"},
            conflict_columns=["tenant_id", "order_id"],
            target_cols=["tenant_id", "order_id"],
        )
        == "true|o9"
    )


def test_es_id_zero_stays_present():
    assert (
        _resolve_doc_id({"_id": 0}, conflict_columns=[], target_cols=["_id"]) == "0"
    )


def test_es_composite_sentinel_part_still_fail_closed():
    assert (
        _resolve_doc_id(
            {"tenant_id": "t1", "order_id": SQL_NULL_SENTINEL},
            conflict_columns=["tenant_id", "order_id"],
            target_cols=["tenant_id", "order_id"],
        )
        is None
    )
