"""D257 review F4 + nits: a MySQL key budget counts the bytes each part really takes.

* A VARBINARY part costs one byte per byte and is budgeted with the other
  growing parts (it was counted as 8 bytes and never sized: the key overran
  3072 bytes and the ALTER failed with 1071).
* A latin1 column costs one byte per character, not four.
* A COMPACT / REDUNDANT table caps a key at 767 bytes, not 3072.

Live MySQL -> MySQL and PostgreSQL -> MySQL through ``execute_tracked``.
"""

from __future__ import annotations

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


@pytest.fixture()
def live():
    if not h._port_up(3306) or not h._port_up(5432):
        pytest.skip("live PostgreSQL / MySQL not reachable")


def _contract(table: str, key: str) -> list[dict]:
    return [{"name": table, "sync_mode": "incremental_upsert", "primary_key": key,
             "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
             "selected": True}]


def _run(fake, src_kind, table, out, key, **kw):
    return h.execute(
        fake, h.endpoint(src_kind, table), h.endpoint("mysql", out),
        sync_mode="incremental_upsert", stream_contracts=_contract(table, key), **kw,
    )


def _types(out: str) -> dict[str, str]:
    rows = h.run_sql(
        "mysql",
        "SELECT column_name, column_type FROM information_schema.columns "
        f"WHERE table_schema = DATABASE() AND table_name = '{out}'",  # nosec B608
    )
    return {str(c).lower(): str(t).upper() for c, t in rows}


def test_a_varbinary_key_part_is_budgeted_in_bytes_with_the_other_part(isolated, live):  # noqa: F811
    table = f"kb_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("mysql", f"CREATE TABLE {table} (vb VARBINARY(1000) NOT NULL, b VARCHAR(2000) NOT NULL, "
                  "v INT, updated_at DATETIME(6), PRIMARY KEY (vb(100), b(100))) DEFAULT CHARSET=utf8mb4")
        h.run_sql("mysql", f"CREATE TABLE {out} (vb VARBINARY(100) NOT NULL, b VARCHAR(100) NOT NULL, "
                  "v INT, updated_at DATETIME(6), PRIMARY KEY (vb, b)) DEFAULT CHARSET=utf8mb4")
        rows = [(b"\x00\x01", "", 1), (b"x" * 1000, "東京 🚀", 2), (b"k", "b" * 518, 3)]
        h.run_sql("mysql", f"INSERT INTO {table} VALUES (%s, %s, %s, '2024-01-01')",  # nosec B608
                  params=rows)
        approved = _run(isolated, "mysql", table, out, "vb,b", schema_drift_acknowledged=True)
        assert approved.success, approved.error
        types = _types(out)
        # 1000 bytes + 518 utf8mb4 characters (2072 bytes) = 3072. Counted as
        # 8 bytes, the binary part left ``b`` 766 characters: 4064 bytes, 1071.
        assert types["vb"] == "VARBINARY(1000)" and types["b"].startswith("VARCHAR(518)"), types
        got = h.run_sql("mysql", f"SELECT vb, b, v FROM {out} ORDER BY v")  # nosec B608
        assert [tuple(r) for r in got] == rows
    finally:
        h.drop("mysql", table)
        h.drop("mysql", out)


def test_a_latin1_key_costs_one_byte_per_character(isolated, live):  # noqa: F811
    table = f"kl_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (c VARCHAR(5000) PRIMARY KEY, v INT, "
                  "updated_at TIMESTAMP)")
        h.run_sql("mysql", f"CREATE TABLE {out} (c VARCHAR(100) CHARACTER SET latin1 NOT NULL "
                  "PRIMARY KEY, v INT, updated_at DATETIME(6))")
        rows = [("abc", 1), ("", 2), ("z" * 2500, 3)]
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s, %s, '2024-01-01')",  # nosec B608
                  params=rows)
        approved = _run(isolated, "postgresql", table, out, "c", schema_drift_acknowledged=True)
        assert approved.success, approved.error
        assert _types(out)["c"].startswith("VARCHAR(3072)"), _types(out)
        got = h.run_sql("mysql", f"SELECT c, v FROM {out} ORDER BY v")  # nosec B608
        assert [tuple(r) for r in got] == rows
    finally:
        h.drop("postgresql", table)
        h.drop("mysql", out)


def test_a_compact_row_format_key_is_capped_at_767_bytes(isolated, live):  # noqa: F811
    table = f"kc_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (c VARCHAR(2000) PRIMARY KEY, v INT, "
                  "updated_at TIMESTAMP)")
        h.run_sql("mysql", f"CREATE TABLE {out} (c VARCHAR(100) NOT NULL PRIMARY KEY, v INT, "
                  "updated_at DATETIME(6)) ROW_FORMAT=COMPACT DEFAULT CHARSET=utf8mb4")
        rows = [("abc", 1), ("東京 ✓", 2), ("q" * 191, 3)]
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s, %s, '2024-01-01')",  # nosec B608
                  params=rows)
        refused = _run(isolated, "postgresql", table, out, "c")
        assert "VARCHAR(191)" in (refused.error or ""), refused.error
        approved = _run(isolated, "postgresql", table, out, "c", schema_drift_acknowledged=True)
        assert approved.success, approved.error
        assert _types(out)["c"].startswith("VARCHAR(191)"), _types(out)
        got = h.run_sql("mysql", f"SELECT c, v FROM {out} ORDER BY v")  # nosec B608
        assert [tuple(r) for r in got] == rows
    finally:
        h.drop("postgresql", table)
        h.drop("mysql", out)


def test_fixed_key_parts_cost_their_declared_storage():
    """DECIMAL by precision, ENUM / SET by value count, temporal by fsp (not a flat 8)."""
    from services.schema_fidelity import mysql_key_part_bytes, mysql_key_unit_bytes, mysql_key_part_limit

    assert mysql_key_part_bytes("DECIMAL(18,4)") == 9
    assert mysql_key_part_bytes("DECIMAL(10,2)") == 5
    assert mysql_key_part_bytes("ENUM('a','b')") == 1
    assert mysql_key_part_bytes("SET('a','b','c')") == 1
    assert mysql_key_part_bytes("DATETIME(6)") == 8 and mysql_key_part_bytes("INT") == 4
    assert mysql_key_part_bytes("VARBINARY(100)") == 100
    assert mysql_key_part_bytes("VARCHAR(50) CHARACTER SET utf8mb3") == 150
    assert mysql_key_unit_bytes("VARCHAR(10) COLLATE latin1_swedish_ci") == 1
    assert mysql_key_part_limit("Compact") == 767 and mysql_key_part_limit("DYNAMIC") == 3072



def test_compact_767_is_a_per_part_limit_and_the_key_total_stays_3072():
    """S2: two growing parts on COMPACT get 191 characters each (764 bytes per part), not 95."""
    from connectors.schema_drift import mysql_keyed_widen_carrier

    live = {"a": "VARCHAR(100)", "b": "VARCHAR(100)", "c": "VARCHAR(100)", "d": "VARCHAR(100)",
            "e": "VARCHAR(100)"}
    grow = {k: "VARCHAR(2000)" for k in live}
    two = [["a", "b"]]
    assert mysql_keyed_widen_carrier(two, "a", "VARCHAR(2000)", live, growing={"a": "VARCHAR(2000)",
                                     "b": "VARCHAR(2000)"}, max_part_bytes=767) == ("VARCHAR(191)", False)
    # Five parts: the 3072-byte total binds before the 767-byte part limit.
    five = [list(live)]
    assert mysql_keyed_widen_carrier(five, "a", "VARCHAR(2000)", live, growing=grow,
                                     max_part_bytes=767) == ("VARCHAR(153)", False)


def test_a_compact_two_part_key_gets_767_bytes_per_part(isolated, live):  # noqa: F811
    """S2 live: COMPACT caps each key part at 767 bytes; the key total stays 3072."""
    table = f"kp_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (a VARCHAR(2000), b VARCHAR(2000), v INT, "
                  "updated_at TIMESTAMP, PRIMARY KEY (a, b))")
        h.run_sql("mysql", f"CREATE TABLE {out} (a VARCHAR(100) NOT NULL, b VARCHAR(100) NOT NULL, "
                  "v INT, updated_at DATETIME(6), PRIMARY KEY (a, b)) ROW_FORMAT=COMPACT "
                  "DEFAULT CHARSET=utf8mb4")
        rows = [("x", "", 1), ("a" * 191, "東京 🚀", 2)]
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s, %s, %s, '2024-01-01')",  # nosec B608
                  params=rows)
        approved = _run(isolated, "postgresql", table, out, "a,b", schema_drift_acknowledged=True)
        assert approved.success, approved.error
        types = _types(out)
        assert types["a"].startswith("VARCHAR(191)") and types["b"].startswith("VARCHAR(191)"), types
        got = h.run_sql("mysql", f"SELECT a, b, v FROM {out} ORDER BY v")  # nosec B608
        assert [tuple(r) for r in got] == rows
    finally:
        h.drop("postgresql", table)
        h.drop("mysql", out)


def test_an_inherited_latin1_charset_costs_one_byte_per_character(isolated, live):  # noqa: F811
    """S5: the column inherits latin1 from a latin1 database, so nothing spells its charset.

    The default collation is not declared on the column (D275), so the only
    evidence is information_schema.COLUMNS.CHARACTER_SET_NAME.
    """
    db = "df_r14_latin1"
    table = f"ki_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("mysql", f"CREATE DATABASE IF NOT EXISTS {db} DEFAULT CHARACTER SET latin1")
    except Exception as exc:  # noqa: BLE001 — needs CREATE on that database
        pytest.skip(f"cannot create a latin1 scratch database here: {exc}")
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (c VARCHAR(5000) PRIMARY KEY, v INT, "
                  "updated_at TIMESTAMP)")
        h.run_sql("mysql", f"CREATE TABLE {db}.{out} (c VARCHAR(100) NOT NULL PRIMARY KEY, v INT, "
                  "updated_at DATETIME(6))")
        rows = [("abc", 1), ("", 2), ("z" * 2500, 3)]
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s, %s, '2024-01-01')",  # nosec B608
                  params=rows)

        def run(**kw):
            return h.execute(
                isolated, h.endpoint("postgresql", table), h.endpoint("mysql", out, database=db),
                sync_mode="incremental_upsert", stream_contracts=_contract(table, "c"), **kw,
            )

        refused = run()
        assert "VARCHAR(3072)" in (refused.error or ""), refused.error
        approved = run(schema_drift_acknowledged=True)
        assert approved.success, approved.error
        width = h.run_sql("mysql", "SELECT character_maximum_length FROM information_schema.columns "
                          f"WHERE table_schema = '{db}' AND table_name = '{out}' "  # nosec B608
                          "AND column_name = 'c'")
        assert int(width[0][0]) == 3072
        got = h.run_sql("mysql", f"SELECT c, v FROM {db}.{out} ORDER BY v")  # nosec B608
        assert [tuple(r) for r in got] == rows
    finally:
        h.drop("postgresql", table)
        h.run_sql("mysql", f"DROP DATABASE IF EXISTS {db}")


def test_the_writer_prices_an_inherited_charset_from_the_catalog(live):
    """S5, writer path: the live column spells no charset; CHARACTER_SET_NAME says latin1."""
    from connectors.schema_drift import widen_existing_columns_native

    db = "df_r14_latin1"
    out = f"kw_{h.token()}"
    try:
        h.run_sql("mysql", f"CREATE DATABASE IF NOT EXISTS {db} DEFAULT CHARACTER SET latin1")
    except Exception as exc:  # noqa: BLE001 — needs CREATE on that database
        pytest.skip(f"cannot create a latin1 scratch database here: {exc}")
    conn = h.connect("mysql")
    try:
        with conn.cursor() as cur:
            cur.execute(f"USE {db}")  # the writer's session is on its own database
            cur.execute(f"CREATE TABLE {out} (c VARCHAR(100) NOT NULL PRIMARY KEY, v INT)")
            ddl = widen_existing_columns_native(
                cur, "mysql", db, out, ["c"], ["VARCHAR(5000)"], backfill=True,
                source_types={"c": "VARCHAR(5000)"}, key_columns=["c"],
            )
            assert ddl and "VARCHAR(3072)" in ddl[0], ddl
            cur.execute("SELECT character_maximum_length, character_set_name FROM "
                        "information_schema.columns WHERE table_schema = %s AND "
                        "table_name = %s AND column_name = 'c'", (db, out))
            assert cur.fetchone() == (3072, "latin1")
    finally:
        conn.close()
        h.run_sql("mysql", f"DROP DATABASE IF EXISTS {db}")
