"""QA batch 1 open item: array child tables on a server that forces a PRIMARY KEY.

A normalize / hybrid array policy writes a child table (parent key columns +
ordinal + element columns) keyed ``UNIQUE (parent keys, ordinal)``. A
MySQL-family server that forces a PRIMARY KEY (Aiven ``sql_require_primary_key``,
MariaDB ``innodb_force_primary_key``) refuses that keyless CREATE (3750 / 1173),
so on such a server the same columns are the PRIMARY KEY, NOT NULL, and the
parent key columns are a VARCHAR that fits InnoDB's key (a PRIMARY KEY cannot
be TEXT).

Live half: ``DATAFLOW_TEST_MYSQL_REQUIRE_PK_*`` (locally a private MariaDB with
``innodb_force_primary_key=ON``).
"""

from __future__ import annotations

import json
import uuid

from services.structural_array import ARRAY_POLICY_HYBRID, child_table_key_ddl
from tests.test_d396_require_primary_key_surrogate_e2e import _pk, _q, _require_pk_server


def test_unforced_server_keeps_the_unique_child_key():
    body, width = child_table_key_ddl(
        ["_id", "_df_ord", "sku"], ["TEXT", "BIGINT", "TEXT"], 1, forced_primary_key=False
    )
    assert body == "`_id` TEXT, `_df_ord` BIGINT, `sku` TEXT, UNIQUE (`_id`, `_df_ord`)"
    assert width == 0


def test_forced_server_child_key_is_a_not_null_primary_key_that_fits_innodb():
    body, width = child_table_key_ddl(
        ["_id", "_df_ord", "sku"], ["TEXT", "BIGINT", "TEXT"], 1, forced_primary_key=True
    )
    assert body == (
        "`_id` VARCHAR(766) NOT NULL, `_df_ord` BIGINT NOT NULL, `sku` TEXT, "
        "PRIMARY KEY (`_id`, `_df_ord`)"
    )
    assert width == 766
    # Two parent key columns share the 3072-byte key.
    body2, width2 = child_table_key_ddl(
        ["a", "b", "_df_ord", "v"], ["TEXT", "TEXT", "BIGINT", "TEXT"], 2, forced_primary_key=True
    )
    assert width2 == 383 and "PRIMARY KEY (`a`, `b`, `_df_ord`)" in body2
    assert 4 * 2 * width2 + 8 <= 3072


def _mapping() -> list[dict]:
    return [{
        "source": "items",
        "target": "items",
        "struct_policy": ARRAY_POLICY_HYBRID,
        "child_table_spec": {
            "child_table": "",  # set per test
            "parent_key_columns": ["_id"],
            "ordinal_column": "_df_ord",
            "columns": [{"name": "sku", "type": "VARCHAR"}, {"name": "qty", "type": "INTEGER"}],
            "keep_parent_json": True,
        },
    }]


def _flush(cfg: dict, child: str, rows: list[list]) -> dict:
    import pymysql

    from connectors.writer_common import flush_normalized_child_batches

    maps = _mapping()
    maps[0]["child_table_spec"]["child_table"] = child
    conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
                           password=cfg["password"], database=cfg["database"], charset="utf8mb4")
    try:
        with conn.cursor() as cur:
            out = flush_normalized_child_batches(
                headers=["_id", "items"], data_rows=rows, mappings=maps, dest_db="mysql",
                create_table=True, cursor=cur, quote="`", placeholder="%s",
            )
        conn.commit()
        return out
    finally:
        conn.close()


def test_live_forced_key_child_table_is_created_with_a_primary_key():
    cfg = _require_pk_server()
    child = f"qab1_child_{uuid.uuid4().hex[:8]}"
    rows = [
        ["p1", json.dumps([{"sku": "A", "qty": 1}, {"sku": "B", "qty": 2}])],
        ["p2", json.dumps([{"sku": "東京", "qty": 3}])],
    ]
    try:
        out = _flush(cfg, child, rows)
        assert out["ok"], out
        assert out["rows_written"] == 3
        assert _pk(cfg, child) == ["_id", "_df_ord"]
        nullable = dict(_q(cfg, "SELECT column_name, is_nullable FROM information_schema.columns "
                                f"WHERE table_schema = DATABASE() AND table_name = '{child}'"))
        assert nullable["_id"] == "NO" and nullable["_df_ord"] == "NO"
        assert _q(cfg, f"SELECT `_id`, `_df_ord`, sku, qty FROM `{child}` ORDER BY `_id`, `_df_ord`") == [
            ("p1", 0, "A", 1), ("p1", 1, "B", 2), ("p2", 0, "東京", 3),
        ]
        # At-least-once replay of the same batch adds nothing.
        again = _flush(cfg, child, rows)
        assert again["ok"], again
        assert _q(cfg, f"SELECT COUNT(*) FROM `{child}`") == [(3,)]
    finally:
        _q(cfg, f"DROP TABLE IF EXISTS `{child}`")


def test_live_forced_key_refuses_a_parent_key_wider_than_the_key_allows():
    cfg = _require_pk_server()
    child = f"qab1_child_{uuid.uuid4().hex[:8]}"
    try:
        out = _flush(cfg, child, [["k" * 800, json.dumps([{"sku": "A", "qty": 1}])]])
        assert not out["ok"]
        assert "longer than 766 characters" in "; ".join(out["errors"])
        assert not _q(cfg, f"SHOW TABLES LIKE '{child}'")
    finally:
        _q(cfg, f"DROP TABLE IF EXISTS `{child}`")
