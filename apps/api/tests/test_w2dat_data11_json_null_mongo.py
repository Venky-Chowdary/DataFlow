"""DATA-11: a jsonb null into Mongo passes Gate-8 (BSON null is JSON null).

Before the fix the run failed "Read-back sample verification failed: row 1
column j->j: source='null' dest='null'" although the document held null.
"""

from __future__ import annotations

import pytest

from tests import w2dat_live as live


def test_document_json_null_rule():
    from services.reconciliation import _document_json_null

    assert _document_json_null("mongodb", "null", None, "json")
    assert not _document_json_null("mongodb", "null", None, "none")
    assert not _document_json_null("postgresql", "null", None, "json")
    assert not _document_json_null("mongodb", '"null"', None, "json")


def test_pg_jsonb_null_to_mongo_verifies():
    live.need_pg()
    if not live.reachable("127.0.0.1", 27017):
        pytest.skip("MongoDB is not reachable")
    import pymongo

    u = live.uid()
    src, coll = f"w2dat_d11_{u}", f"w2dat_d11m_{u}"
    client = pymongo.MongoClient("127.0.0.1", 27017, directConnection=True, serverSelectionTimeoutMS=3000)
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, j jsonb)')
        live.pg(f"INSERT INTO public.\"{src}\" VALUES (1, 'null'), (2, NULL), (3, '{{\"k\": 1}}'), (4, '0')")
        res = live.run(live.ep_pg(src), live.ep_mongo(coll), "full_refresh_overwrite", key="id",
                       cols=["id", "j"])
        assert res.success, res.error
        docs = {d["id"]: d.get("j") for d in client["dataflow"][coll].find({})}
        assert docs[1] is None and docs[2] is None and docs[3] == {"k": 1}
    finally:
        client["dataflow"].drop_collection(coll)
        client.close()
        live.drop_pg(src)
