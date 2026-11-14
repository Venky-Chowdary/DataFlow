"""DATA-10: MySQL BIT(8) + DATETIME(6) -> SQLite verifies on identical data.

The ladder's MySQL population read selected BIT raw, so the source side was
base64 of the driver bytes ('gQ==') while the destination held the bit string
the transfer reader wrote ('10000001'): L4/L5 reported a false red. Both
sides now go through the reader's own projection (BIT as MSB-first digits,
JSON as text), and DATETIME(6) keeps its microseconds.
"""

from __future__ import annotations

import sqlite3

from tests import w2dat_live as live


def test_ladder_mysql_read_uses_the_bitstring_wire():
    live.need_mysql()
    from services.verification_ladder import _cell_wire, read_mysql_rows

    t = f"w2dat_d10r_{live.uid()}"
    try:
        live.my(f"CREATE TABLE `{t}` (id INT PRIMARY KEY, b8 BIT(8), dt6 DATETIME(6))")
        live.my(f"INSERT INTO `{t}` VALUES (1, b'10000001', '9999-12-31 23:59:59.999999'), (2, NULL, NULL)")
        rows = read_mysql_rows(table=t, columns=["id", "b8", "dt6"], **live.MY)
        by_id = {str(r["id"]): r for r in rows}
        assert by_id["1"]["b8"] == "10000001"
        assert by_id["1"]["dt6"].endswith("23:59:59.999999")
        assert by_id["2"]["b8"] == _cell_wire(None)
    finally:
        live.drop_my(t)


def test_bit8_and_datetime6_into_sqlite_verifies(tmp_path):
    live.need_mysql()
    u = live.uid()
    src = f"w2dat_d10_{u}"
    db = tmp_path / f"w2dat_d10_{u}.db"
    try:
        live.my(f"CREATE TABLE `{src}` (id INT PRIMARY KEY, b8 BIT(8), dt6 DATETIME(6))")
        live.my(
            f"INSERT INTO `{src}` VALUES (1, b'10000001', '9999-12-31 23:59:59.999999'), "
            "(2, b'00000000', '1000-01-01 00:00:00'), (3, NULL, NULL)"
        )
        res = live.run(live.ep_my(src), live.ep_sqlite(db, "d"), key="id", cols=["id", "b8", "dt6"])
        assert res.success, res.error
        rec = res.reconciliation or {}
        assert rec.get("passed") is True, rec.get("message")
        with sqlite3.connect(db) as c:
            got = c.execute("SELECT id, b8, dt6 FROM d ORDER BY id").fetchall()
        assert got[0][1] == "10000001" and got[0][2].endswith("23:59:59.999999")
        assert got[2][1] is None and got[2][2] is None
    finally:
        live.drop_my(src)
