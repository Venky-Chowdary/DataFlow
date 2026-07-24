"""Integration: older CDC LSN must not overwrite a newer MongoDB document."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mongodb_writer import write_mapped_rows  # noqa: E402
from connectors.writer_common import DF_LSN_COL  # noqa: E402


def _mapping(source: str, target: str) -> dict:
    return {"source": source, "target": target, "confidence": 0.95}


def test_mongodb_upsert_rejects_older_lsn():
    pymongo = pytest.importorskip("pymongo")
    client = pymongo.MongoClient("localhost", 27017, serverSelectionTimeoutMS=2000)
    client.server_info()

    db_name = "dataflow_cdc_lsn_test"
    collection_name = f"cdc_lsn_{uuid.uuid4().hex[:8]}"
    coll = client[db_name][collection_name]
    coll.drop()

    common = {
        "host": "localhost",
        "port": 27017,
        "database": db_name,
        "username": "",
        "password": "",
        "schema": "",
        "connection_string": "",
        "ssl": False,
        "table_name": collection_name,
        "headers": ["id", "amount", DF_LSN_COL],
        "mappings": [
            _mapping("id", "id"),
            _mapping("amount", "amount"),
            _mapping(DF_LSN_COL, DF_LSN_COL),
        ],
        "column_types": {"id": "INTEGER", "amount": "TEXT", DF_LSN_COL: "TEXT"},
    }

    r1 = write_mapped_rows(
        **common,
        data_rows=[["1", "new", "0/16B3748"]],
        create_table=True,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r1.ok, r1.error

    r2 = write_mapped_rows(
        **common,
        data_rows=[["1", "stale", "0/16B3700"]],
        create_table=False,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r2.ok, r2.error

    doc = coll.find_one({"id": 1})
    coll.drop()
    client.close()

    assert doc is not None
    assert str(doc["amount"]) == "new"
    assert str(doc[DF_LSN_COL]) == "0/16B3748"
