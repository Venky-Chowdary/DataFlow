"""DATA-24: U+0000 inside text lands in SQLite and MongoDB, which store it.

Before the fix strict Validate refused "Invisible / undecodable characters in
source text (U+0000)" for SQLite and Mongo, although both keep a NUL inside a
string byte for byte. PostgreSQL text cannot hold it and still refuses.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from tests import w2dat_live as live

ROWS = [{"id": 1, "t": "a\u0000b"}, {"id": 2, "t": "plain"}]


def _run(dest):
    from src.transfer.models import EndpointConfig

    return live.run(EndpointConfig(kind="file", format="json"), dest, "full_refresh_overwrite", key="id",
                    extra_req={"source_content": json.dumps(ROWS).encode(), "source_filename": "n.json"})


def test_nul_text_lands_in_sqlite(tmp_path):
    res = _run(live.ep_sqlite(tmp_path / "n.db", "n24"))
    assert res.success, res.error
    with sqlite3.connect(tmp_path / "n.db") as conn:
        assert conn.execute('SELECT t FROM "n24" WHERE id = 1').fetchone() == ("a\u0000b",)


def test_nul_text_lands_in_mongo():
    if not live.reachable("127.0.0.1", 27017):
        pytest.skip("MongoDB is not reachable")
    import pymongo

    coll = f"w2dat_d24_{live.uid()}"
    client = pymongo.MongoClient("127.0.0.1", 27017, directConnection=True, serverSelectionTimeoutMS=3000)
    try:
        res = _run(live.ep_mongo(coll))
        assert res.success, res.error
        assert client["dataflow"][coll].find_one({"id": 1})["t"] == "a\u0000b"
    finally:
        client["dataflow"].drop_collection(coll)
        client.close()


def test_postgres_still_refuses_nul():
    from services.data_integrity import _check_encoding_anomalies

    assert _check_encoding_anomalies([ROWS[0]], dest_kind="postgresql")["blocks_transfer"]
    assert not _check_encoding_anomalies([ROWS[0]], dest_kind="sqlite")["blocks_transfer"]
