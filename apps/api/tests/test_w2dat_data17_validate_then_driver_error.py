"""DATA-17: a run that Validate passed does not die on a raw driver error.

Before the fix MySQL BIGINT UNSIGNED -> SQLite died with "Python int too
large to convert to SQLite INTEGER", and a 38-digit decimal into MongoDB
died with decimal.Inexact (Decimal128 holds 34 digits).
"""

from __future__ import annotations

import sqlite3

import pytest

from tests import w2dat_live as live


def test_mysql_unsigned_bigint_into_sqlite_lands_exactly(tmp_path):
    live.need_mysql()
    u = live.uid()
    src = f"w2dat_d17_{u}"
    try:
        live.my(f"CREATE TABLE `{src}` (id INT PRIMARY KEY, ub BIGINT UNSIGNED)")
        live.my(f"INSERT INTO `{src}` VALUES (1, 18446744073709551615), (2, 0), (3, NULL)")
        res = live.run(live.ep_my(src), live.ep_sqlite(tmp_path / "u.db", "ub"), "full_refresh_overwrite",
                       key="id", cols=["id", "ub"])
        assert res.success, res.error
        with sqlite3.connect(tmp_path / "u.db") as conn:
            got = conn.execute('SELECT id, CAST(ub AS TEXT) FROM "ub" ORDER BY id').fetchall()
        assert got == [(1, "18446744073709551615"), (2, "0"), (3, None)]
    finally:
        live.drop_my(src)


def test_38_digit_decimal_into_mongo_is_refused_at_validate_not_by_the_driver():
    live.need_pg()
    if not live.reachable("127.0.0.1", 27017):
        pytest.skip("MongoDB is not reachable")
    import pymongo

    u = live.uid()
    src, coll = f"w2dat_d17m_{u}", f"w2dat_d17mc_{u}"
    client = pymongo.MongoClient("127.0.0.1", 27017, directConnection=True, serverSelectionTimeoutMS=3000)
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, d numeric(38,18))')
        live.pg(f"INSERT INTO public.\"{src}\" VALUES (1, 12345678901234567890.123456789012345678), (2, 1.5)")
        res = live.run(live.ep_pg(src), live.ep_mongo(coll), "full_refresh_overwrite", key="id", cols=["id", "d"])
        err = res.error or ""
        assert "Inexact" not in err and "decimal." not in err, err
        if res.success:
            from decimal import Decimal

            doc = client["dataflow"][coll].find_one({"id": 1})
            assert Decimal(str(doc["d"])) == Decimal("12345678901234567890.123456789012345678")
        else:
            assert client["dataflow"][coll].count_documents({}) == 0
            assert "34" in err or "digit" in err.lower(), err
    finally:
        client["dataflow"].drop_collection(coll)
        client["dataflow"].drop_collection(f"{coll}_df_quarantine")
        client.close()
        live.drop_pg(src)
