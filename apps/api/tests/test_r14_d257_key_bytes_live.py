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
    from services.schema_fidelity import mysql_key_part_bytes, mysql_key_unit_bytes, mysql_max_key_bytes

    assert mysql_key_part_bytes("DECIMAL(18,4)") == 9
    assert mysql_key_part_bytes("DECIMAL(10,2)") == 5
    assert mysql_key_part_bytes("ENUM('a','b')") == 1
    assert mysql_key_part_bytes("SET('a','b','c')") == 1
    assert mysql_key_part_bytes("DATETIME(6)") == 8 and mysql_key_part_bytes("INT") == 4
    assert mysql_key_part_bytes("VARBINARY(100)") == 100
    assert mysql_key_part_bytes("VARCHAR(50) CHARACTER SET utf8mb3") == 150
    assert mysql_key_unit_bytes("VARCHAR(10) COLLATE latin1_swedish_ci") == 1
    assert mysql_max_key_bytes("Compact") == 767 and mysql_max_key_bytes("DYNAMIC") == 3072
