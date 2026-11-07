"""Exception handlers must not look up pymongo.errors while matching.

``import pymongo`` does not bind the errors submodule. An except clause that
reads ``pymongo.errors`` at match time replaces the real failure with
``AttributeError: module 'pymongo' has no attribute 'errors'``.
"""

from __future__ import annotations

import sys
from unittest.mock import patch

from services.value_serializer import DF_MISSING_SENTINEL


def test_unbound_pymongo_errors_does_not_mask_the_original_error():
    import pymongo

    from connectors.mongodb_writer import write_mapped_rows

    saved_module = sys.modules.pop("pymongo.errors", None)
    had_attr = hasattr(pymongo, "errors")
    saved_attr = getattr(pymongo, "errors", None)
    if had_attr:
        delattr(pymongo, "errors")

    captured: list = []

    class _Coll:
        def find(self, *args, **kwargs):
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

    try:
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
                data_rows=[["1", "keep-note", DF_MISSING_SENTINEL]],
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
    finally:
        if saved_module is not None:
            sys.modules["pymongo.errors"] = saved_module
        if had_attr:
            pymongo.errors = saved_attr

    assert result.ok, result.error
    assert "has no attribute 'errors'" not in (result.error or "")
    assert captured
    assert type(captured[0]).__name__ == "UpdateOne"
