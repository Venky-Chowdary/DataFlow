"""DATA-7: the duplicate-key check compares exact key text.

Before the fix Mongo overwrite and append refused keys 'a' and 'a ' as
duplicates (the check stripped them), although upsert landed both.
"""

from __future__ import annotations

import pytest

from tests import w2dat_live as live


def test_source_duplicate_check_keeps_trailing_space_keys_apart():
    from services.data_quality import run_integrity_audit

    for kwargs in ({"primary_key": "k"}, {"primary_key_columns": ["k", "r"]}):
        report = run_integrity_audit(
            ["k", "r", "v"], [["a", "x", "1"], ["a ", "x", "2"], ["b", "x", "3"]],
            sync_mode="full_refresh_overwrite", dest_kind="mongodb", **kwargs,
        )
        assert not any("Duplicate" in str(i) for i in report.issues), report.issues
    dup = run_integrity_audit(["k", "v"], [["a", "1"], ["a", "2"]], primary_key="k",
                              sync_mode="full_refresh_overwrite", dest_kind="mongodb")
    assert any("Duplicate" in str(i) for i in dup.issues + dup.warnings)


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "full_refresh_append"])
def test_mongo_keeps_a_and_a_space_as_two_keys(mode):
    live.need_pg()
    if not live.reachable("127.0.0.1", 27017):
        pytest.skip("MongoDB is not reachable")
    import pymongo

    u = live.uid()
    src, coll = f"w2dat_d7_{u}", f"w2dat_d7m_{u}"
    client = pymongo.MongoClient("127.0.0.1", 27017, directConnection=True, serverSelectionTimeoutMS=3000)
    try:
        live.pg(f'CREATE TABLE public."{src}" (k text primary key, v int)')
        live.pg(f"INSERT INTO public.\"{src}\" VALUES ('a', 1), ('a ', 2)")
        res = live.run(live.ep_pg(src), live.ep_mongo(coll), mode, key="k", cols=["k", "v"])
        assert res.success, res.error
        keys = sorted(d["k"] for d in client["dataflow"][coll].find({}))
        assert keys == ["a", "a "]
    finally:
        client["dataflow"].drop_collection(coll)
        client.close()
        live.drop_pg(src)
