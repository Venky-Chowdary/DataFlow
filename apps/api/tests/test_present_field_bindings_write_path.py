"""Shared present_field_bindings: omit Missing, bind reader-null as None.

Missing-only filters used to copy SQL_NULL_SENTINEL into dest payloads
(Iceberg leftover, Redis JSON, Mongo $set, sparse CDC SET, BQ JSON).
One owner now binds the extract token as None and keeps 0 / False / ''.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.iceberg_writer import _iceberg_present_fields  # noqa: E402
from connectors.redis_writer import _redis_row_to_doc  # noqa: E402
from connectors.warehouse_temporal import records_for_bigquery  # noqa: E402
from connectors.writer_common import (  # noqa: E402
    present_field_bindings,
    sparse_present_bindings,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_present_field_bindings_null_vs_missing():
    got = present_field_bindings(
        {
            "id": "1",
            "note": SQL_NULL_SENTINEL,
            "ddb": "__df_ddb_null__",
            "extra": Missing,
            "gone": DF_MISSING_SENTINEL,
            "zero": 0,
            "flag": False,
            "blank": "",
        }
    )
    assert got == {
        "id": "1",
        "note": None,
        "ddb": None,
        "zero": 0,
        "flag": False,
        "blank": "",
    }
    assert SQL_NULL_SENTINEL not in got.values()
    assert DF_MISSING_SENTINEL not in got.values()


def test_sparse_present_bindings_binds_reader_null():
    present = sparse_present_bindings(
        ("1", SQL_NULL_SENTINEL, DF_MISSING_SENTINEL, 0),
        ["id", "note", "extra", "amt"],
    )
    assert present == {"id": "1", "note": None, "amt": 0}
    assert "extra" not in present


def test_iceberg_and_redis_aliases_share_owner():
    row = {
        "id": "1",
        "note": SQL_NULL_SENTINEL,
        "extra": Missing,
        "zero": 0,
    }
    assert _iceberg_present_fields(row) == present_field_bindings(row)
    assert _redis_row_to_doc(["id", "note", "extra", "zero"], ("1", SQL_NULL_SENTINEL, Missing, 0)) == {
        "id": "1",
        "note": None,
        "zero": 0,
    }


def test_bq_records_bind_reader_null_omit_missing():
    recs = records_for_bigquery(
        [("1", SQL_NULL_SENTINEL, DF_MISSING_SENTINEL, 0)],
        ["id", "note", "extra", "amt"],
        ["STRING", "STRING", "STRING", "INT64"],
    )
    assert recs == [{"id": "1", "note": None, "amt": 0}]
    assert "extra" not in recs[0]
    assert SQL_NULL_SENTINEL not in recs[0].values()


def test_mysql_sparse_update_binds_reader_null_not_token():
    from connectors.mysql_writer import _mysql_apply_sparse_upsert

    cur = MagicMock()
    cur.fetchone.return_value = ("1", "old", "keep-extra")
    cur.rowcount = 1
    written, skipped, checksum_rows = _mysql_apply_sparse_upsert(
        cur,
        table_q="`t`",
        target_cols=["id", "note", "extra"],
        conflict_columns=["id"],
        sparse_rows=[("1", SQL_NULL_SENTINEL, DF_MISSING_SENTINEL)],
    )
    assert written == 1
    assert skipped == 0
    bound = cur.execute.call_args_list[-1].args[1]
    assert None in bound
    assert SQL_NULL_SENTINEL not in bound
    assert DF_MISSING_SENTINEL not in bound
    assert checksum_rows == [("1", None, "keep-extra")]


def test_mongo_set_binds_reader_null_not_token():
    from connectors.mongodb_writer import write_mapped_rows

    captured: list = []

    class _Coll:
        def find(self, *a, **k):
            return []

        def bulk_write(self, ops, ordered=False):
            captured.extend(ops)

    class _Db:
        def __getitem__(self, name):
            return _Coll()

        def list_collection_names(self, filter=None):  # noqa: A002
            return []

    class _Client:
        def __getitem__(self, name):
            return _Db()

        def close(self):
            return None

    with patch("connectors.mongodb_common._mongo_client", return_value=_Client()):
        result = write_mapped_rows(
            host="localhost",
            port=27017,
            database="testdb",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "note", "extra"],
            data_rows=[["1", SQL_NULL_SENTINEL, DF_MISSING_SENTINEL]],
            mappings=[
                {"source": "id", "target": "id", "confidence": 1},
                {"source": "note", "target": "note", "confidence": 1},
                {"source": "extra", "target": "extra", "confidence": 1},
            ],
            column_types={"id": "string", "note": "string", "extra": "string"},
            create_table=True,
            write_mode="upsert",
            conflict_columns=["id"],
        )
    assert result.ok, result.error
    assert len(captured) == 1
    update = captured[0]._doc
    assert update["$set"].get("note") is None
    assert "extra" not in update["$set"]
    assert SQL_NULL_SENTINEL not in update["$set"].values()
    assert DF_MISSING_SENTINEL not in update["$set"].values()
