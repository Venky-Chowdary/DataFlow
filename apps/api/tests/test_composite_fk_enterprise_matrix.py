"""Enterprise matrix: composite FK MATCH SIMPLE on real engines.

Principal / migration-architect scenarios — every case is a named fixture.
``100%`` here means every row in this matrix, not a marketing score.

Skipped engines are reported, never invented green.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Any, Callable

import pytest

from services.population_orphan_probe import probe_population_fk_orphans
from services.preflight_service import run_file_preflight
from services.sample_orphan_probe import _fk_parts, probe_sample_fk_orphans


def _sid(prefix: str = "df_cfk") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def _sqlite_cfg(tmp_path: Path) -> dict[str, Any]:
    path = str(tmp_path / f"{_sid()}.db")
    return {"type": "sqlite", "database": path, "_path": path}


def _connect_sqlite(cfg: dict[str, Any]):
    return sqlite3.connect(cfg["_path"])


def _seed_sqlite(cfg: dict[str, Any], statements: list[str]) -> None:
    with _connect_sqlite(cfg) as conn:
        for stmt in statements:
            conn.execute(stmt)
        conn.commit()


def _pg_cfg() -> dict[str, Any] | None:
    from tests.helpers.live_env import pg_creds, pg_up

    if pg_up():
        c = pg_creds()
        return {
            "type": "postgresql",
            "host": c["host"],
            "port": c["port"],
            "database": c["database"],
            "username": c["username"],
            "password": c["password"],
            "schema": "public",
        }
    try:
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=3,
        )
        conn.close()
        return {
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "public",
        }
    except Exception:
        return None


def _mysql_cfg() -> dict[str, Any] | None:
    from tests.helpers.live_env import mysql_creds, mysql_up

    if mysql_up():
        c = mysql_creds()
        return {
            "type": "mysql",
            "host": c["host"],
            "port": c["port"],
            "database": c["database"],
            "username": c["username"],
            "password": c["password"],
        }
    try:
        import pymysql

        conn = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            connect_timeout=3,
        )
        conn.close()
        return {
            "type": "mysql",
            "host": "127.0.0.1",
            "port": 3306,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
        }
    except Exception:
        return None


def _pg_exec(cfg: dict[str, Any], statements: list[str]) -> None:
    import psycopg2

    conn = psycopg2.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        dbname=cfg["database"],
        user=cfg["username"],
        password=cfg["password"],
        connect_timeout=5,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
    finally:
        conn.close()


def _mysql_exec(cfg: dict[str, Any], statements: list[str]) -> None:
    import pymysql

    conn = pymysql.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        autocommit=True,
        connect_timeout=5,
    )
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
    finally:
        conn.close()


COMPOSITE = [
    {
        "columns": ["tenant_id", "order_no"],
        "referenced_table": "orders",
        "referenced_columns": ["tenant_id", "order_no"],
    }
]


def _pop(cfg: dict[str, Any], child: str, fks: list[dict[str, Any]], mappings=None):
    return probe_population_fk_orphans(
        child_table=child,
        mappings=mappings or [],
        foreign_keys=fks,
        source_config=cfg,
        validation_mode="strict",
    )


def _sample(cfg: dict[str, Any], rows: list[dict[str, Any]], fks, mappings=None):
    return probe_sample_fk_orphans(
        sample_rows=rows,
        mappings=mappings or [],
        foreign_keys=fks,
        source_config=cfg,
        validation_mode="strict",
    )


# ── SQLite: always-on architect scenarios ───────────────────────────────────


def test_sqlite_dest_shaped_payload_is_parsed():
    cols, table, ref = _fk_parts(
        {
            "constrained_columns": ["tenant_id", "order_no"],
            "referred_table": "orders",
            "referred_columns": ["tenant_id", "order_no"],
            "referred_schema": "sales",
        }
    )
    assert cols == ["tenant_id", "order_no"]
    assert table == "sales.orders"
    assert ref == ["tenant_id", "order_no"]


def test_sqlite_three_column_orphan_is_the_whole_tuple(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            "CREATE TABLE parent (a INTEGER, b INTEGER, c INTEGER, PRIMARY KEY (a,b,c))",
            "INSERT INTO parent VALUES (1, 2, 3), (1, 2, 4)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)",
            "INSERT INTO child VALUES (1, 1, 2, 3)",
            "INSERT INTO child VALUES (2, 1, 2, 9)",
        ],
    )
    fks = [
        {
            "columns": ["a", "b", "c"],
            "referenced_table": "parent",
            "referenced_columns": ["a", "b", "c"],
        }
    ]
    report = _pop(cfg, "child", fks)
    assert report["complete"] is True
    assert report["orphan_count"] == 1
    assert report["population_proof"] is False
    assert "1+2+9" in report["findings"][0]["message"]


def test_sqlite_mixed_single_and_composite_both_must_pass(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            "CREATE TABLE customers (id INTEGER PRIMARY KEY)",
            "INSERT INTO customers VALUES (10)",
            "CREATE TABLE orders (tenant_id INTEGER, order_no INTEGER, PRIMARY KEY (tenant_id, order_no))",
            "INSERT INTO orders VALUES (1, 100)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, customer_id INTEGER, tenant_id INTEGER, order_no INTEGER)",
            "INSERT INTO child VALUES (1, 10, 1, 100)",
        ],
    )
    fks = [
        {"columns": ["customer_id"], "referenced_table": "customers", "referenced_columns": ["id"]},
        {
            "columns": ["tenant_id", "order_no"],
            "referenced_table": "orders",
            "referenced_columns": ["tenant_id", "order_no"],
        },
    ]
    clean = _pop(cfg, "child", fks)
    assert clean["complete"] is True
    assert clean["population_proof"] is True

    with _connect_sqlite(cfg) as conn:
        conn.execute("INSERT INTO child VALUES (2, 10, 9, 9)")
        conn.commit()
    dirty = _pop(cfg, "child", fks)
    assert dirty["population_proof"] is False
    assert dirty["orphan_count"] == 1


def test_sqlite_remapped_dest_names_read_source_columns(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            "CREATE TABLE orders (tenant_id INTEGER, order_no INTEGER, PRIMARY KEY (tenant_id, order_no))",
            "INSERT INTO orders VALUES (1, 100)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, src_tenant INTEGER, src_order INTEGER)",
            "INSERT INTO child VALUES (1, 2, 101)",
        ],
    )
    fks = [
        {
            "columns": ["tenant_id", "order_no"],
            "referenced_table": "orders",
            "referenced_columns": ["tenant_id", "order_no"],
        }
    ]
    mappings = [
        {"source": "src_tenant", "target": "tenant_id"},
        {"source": "src_order", "target": "order_no"},
    ]
    report = _pop(cfg, "child", fks, mappings=mappings)
    assert report["orphan_count"] == 1
    assert report["checks"][0]["columns"] == ["src_tenant", "src_order"]


def test_sqlite_different_parent_column_names(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            "CREATE TABLE org_order (org INTEGER, num INTEGER, PRIMARY KEY (org, num))",
            "INSERT INTO org_order VALUES (1, 100)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, tenant_id INTEGER, order_no INTEGER)",
            "INSERT INTO child VALUES (1, 1, 999)",
        ],
    )
    fks = [
        {
            "columns": ["tenant_id", "order_no"],
            "referenced_table": "org_order",
            "referenced_columns": ["org", "num"],
        }
    ]
    report = _pop(cfg, "child", fks)
    assert report["orphan_count"] == 1
    sample = _sample(cfg, [{"tenant_id": 1, "order_no": 999}, {"tenant_id": 1, "order_no": 100}], fks)
    assert sample["population_proof"] is False
    assert sample["orphan_count"] == 1
    assert sample["checked_values"] == 2


def test_sqlite_parent_missing_fail_closed(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            "CREATE TABLE child (id INTEGER PRIMARY KEY, tenant_id INTEGER, order_no INTEGER)",
            "INSERT INTO child VALUES (1, 1, 100)",
        ],
    )
    report = _pop(cfg, "child", COMPOSITE)
    assert report["complete"] is False
    assert report["population_proof"] is False
    assert report["findings"][0]["code"] == "population_orphan_probe_unavailable"


def test_sqlite_empty_child_is_proven(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            "CREATE TABLE orders (tenant_id INTEGER, order_no INTEGER, PRIMARY KEY (tenant_id, order_no))",
            "INSERT INTO orders VALUES (1, 100)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, tenant_id INTEGER, order_no INTEGER)",
        ],
    )
    report = _pop(cfg, "child", COMPOSITE)
    assert report["complete"] is True
    assert report["orphan_count"] == 0
    assert report["population_proof"] is True


def test_sqlite_reserved_word_tables_are_quoted(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            'CREATE TABLE "order" (tenant_id INTEGER, "user" INTEGER, '
            "PRIMARY KEY (tenant_id, \"user\"))",
            'INSERT INTO "order" VALUES (1, 100)',
            'CREATE TABLE "user" (id INTEGER PRIMARY KEY, tenant_id INTEGER, "user" INTEGER)',
            'INSERT INTO "user" VALUES (1, 1, 100)',
            'INSERT INTO "user" VALUES (2, 2, 101)',
        ],
    )
    fks = [
        {
            "columns": ["tenant_id", "user"],
            "referenced_table": "order",
            "referenced_columns": ["tenant_id", "user"],
        }
    ]
    report = _pop(cfg, "user", fks)
    assert report["complete"] is True, report
    assert report["orphan_count"] == 1
    assert "2+101" in report["findings"][0]["message"]


def test_sqlite_self_referential_composite(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            "CREATE TABLE emp (org INTEGER, emp_id INTEGER, mgr_org INTEGER, mgr_id INTEGER, "
            "PRIMARY KEY (org, emp_id))",
            "INSERT INTO emp VALUES (1, 1, NULL, NULL)",
            "INSERT INTO emp VALUES (1, 2, 1, 1)",
            "INSERT INTO emp VALUES (1, 3, 1, 99)",
        ],
    )
    fks = [
        {
            "columns": ["mgr_org", "mgr_id"],
            "referenced_table": "emp",
            "referenced_columns": ["org", "emp_id"],
        }
    ]
    report = _pop(cfg, "emp", fks)
    assert report["complete"] is True
    assert report["orphan_count"] == 1
    assert "1+99" in report["findings"][0]["message"]


def test_sqlite_duplicate_orphan_rows_are_counted(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            "CREATE TABLE orders (tenant_id INTEGER, order_no INTEGER, PRIMARY KEY (tenant_id, order_no))",
            "INSERT INTO orders VALUES (1, 100)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, tenant_id INTEGER, order_no INTEGER)",
            "INSERT INTO child VALUES (1, 9, 9)",
            "INSERT INTO child VALUES (2, 9, 9)",
        ],
    )
    report = _pop(cfg, "child", COMPOSITE)
    assert report["orphan_count"] == 2


def test_sqlite_preflight_population_scan_proves_and_blocks(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            "CREATE TABLE orders (tenant_id INTEGER, order_no INTEGER, PRIMARY KEY (tenant_id, order_no))",
            "INSERT INTO orders VALUES (1, 100), (1, 101)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, tenant_id INTEGER, order_no INTEGER, "
            "FOREIGN KEY (tenant_id, order_no) REFERENCES orders(tenant_id, order_no))",
            "INSERT INTO child VALUES (1, 1, 100)",
        ],
    )
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "tenant_id", "target": "tenant_id", "confidence": 1.0},
        {"source": "order_no", "target": "order_no", "confidence": 1.0},
    ]
    common = dict(
        columns=["id", "tenant_id", "order_no"],
        column_types={"id": "INTEGER", "tenant_id": "INTEGER", "order_no": "INTEGER"},
        row_count=1,
        mappings=mappings,
        sample_rows=[{"id": 1, "tenant_id": 1, "order_no": 100}],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        destination_table_exists=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_column_types={"id": "INTEGER", "tenant_id": "INTEGER", "order_no": "INTEGER"},
        destination_foreign_keys=COMPOSITE,
        validation_mode="strict",
        destination_db_type="sqlite",
        source_config=cfg,
        source_table="child",
        run_population_orphan_scan=True,
    )
    clean = run_file_preflight(**common)
    ri = clean.get("referential_integrity") or {}
    assert ri.get("proven") is True, ri
    assert ri.get("coverage") == "population_orphan_probe"
    assert clean.get("population_orphan_probe", {}).get("complete") is True

    with _connect_sqlite(cfg) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("INSERT INTO child VALUES (2, 2, 101)")
        conn.commit()
    dirty = run_file_preflight(
        **{
            **common,
            "row_count": 2,
            "sample_rows": [
                {"id": 1, "tenant_id": 1, "order_no": 100},
                {"id": 2, "tenant_id": 2, "order_no": 101},
            ],
        }
    )
    dri = dirty.get("referential_integrity") or {}
    assert dri.get("proven") is False
    assert dirty.get("population_orphan_probe", {}).get("orphan_count") == 1
    codes = [f.get("code") for f in (dirty.get("constraint_findings") or [])]
    assert "fk_orphan_in_population" in codes
    assert "composite_fk_not_probed" not in codes


def test_sqlite_dest_shaped_fk_scans(tmp_path: Path):
    cfg = _sqlite_cfg(tmp_path)
    _seed_sqlite(
        cfg,
        [
            "CREATE TABLE orders (tenant_id INTEGER, order_no INTEGER, PRIMARY KEY (tenant_id, order_no))",
            "INSERT INTO orders VALUES (1, 100)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, tenant_id INTEGER, order_no INTEGER)",
            "INSERT INTO child VALUES (1, 1, 100)",
        ],
    )
    report = _pop(
        cfg,
        "child",
        [
            {
                "constrained_columns": ["tenant_id", "order_no"],
                "referred_table": "orders",
                "referred_columns": ["tenant_id", "order_no"],
            }
        ],
    )
    assert report["population_proof"] is True


# ── Live Postgres / MySQL ────────────────────────────────────────────────────


def _live_engines() -> list[tuple[str, dict[str, Any], Callable, Callable]]:
    out: list[tuple[str, dict[str, Any], Callable, Callable]] = []
    pg = _pg_cfg()
    if pg:
        out.append(("postgresql", pg, _pg_exec, lambda ident: f'"{ident}"'))
    mysql = _mysql_cfg()
    if mysql:
        out.append(("mysql", mysql, _mysql_exec, lambda ident: f"`{ident}`"))
    return out


def _live_int(dialect: str) -> str:
    return "INT" if dialect == "mysql" else "INTEGER"


def test_live_engines_composite_matrix():
    engines = _live_engines()
    if not engines:
        pytest.skip("Neither Postgres nor MySQL authenticated — live composite matrix skipped")

    results: list[dict[str, Any]] = []
    for dialect, cfg, exec_sql, q in engines:
        parent = _sid("p")
        child = _sid("c")
        emp = _sid("e")
        reserved_parent = _sid("order")  # prefix + uuid; still exercise quoting
        reserved_child = _sid("user")
        schema_name = _sid("sch")
        drop = [
            f"DROP TABLE IF EXISTS {q(child)}",
            f"DROP TABLE IF EXISTS {q(parent)}",
            f"DROP TABLE IF EXISTS {q(emp)}",
            f"DROP TABLE IF EXISTS {q(reserved_child)}",
            f"DROP TABLE IF EXISTS {q(reserved_parent)}",
        ]
        if dialect == "postgresql":
            drop.extend(
                [
                    f"DROP TABLE IF EXISTS {q(schema_name)}.{q(parent)}",
                    f"DROP SCHEMA IF EXISTS {q(schema_name)} CASCADE",
                ]
            )
        itype = _live_int(dialect)
        try:
            exec_sql(
                cfg,
                drop
                + [
                    f"CREATE TABLE {q(parent)} (tenant_id {itype} NOT NULL, order_no {itype} NOT NULL, "
                    f"PRIMARY KEY (tenant_id, order_no))",
                    f"INSERT INTO {q(parent)} (tenant_id, order_no) VALUES (1,100),(1,101),(2,100)",
                    f"CREATE TABLE {q(child)} (id {itype} PRIMARY KEY, tenant_id {itype} NULL, order_no {itype} NULL)",
                    f"INSERT INTO {q(child)} (id, tenant_id, order_no) VALUES (1,1,100),(2,2,101),(3,NULL,999)",
                    f"CREATE TABLE {q(emp)} (org {itype} NOT NULL, emp_id {itype} NOT NULL, "
                    f"mgr_org {itype} NULL, mgr_id {itype} NULL, PRIMARY KEY (org, emp_id))",
                    f"INSERT INTO {q(emp)} (org, emp_id, mgr_org, mgr_id) VALUES "
                    f"(1,1,NULL,NULL),(1,2,1,1),(1,3,1,99)",
                    f"CREATE TABLE {q(reserved_parent)} (tenant_id {itype} NOT NULL, "
                    f"{q('user')} {itype} NOT NULL, PRIMARY KEY (tenant_id, {q('user')}))",
                    f"INSERT INTO {q(reserved_parent)} (tenant_id, {q('user')}) VALUES (1,100)",
                    f"CREATE TABLE {q(reserved_child)} (id {itype} PRIMARY KEY, tenant_id {itype} NULL, "
                    f"{q('user')} {itype} NULL)",
                    f"INSERT INTO {q(reserved_child)} (id, tenant_id, {q('user')}) VALUES (1,1,100),(2,2,101)",
                ],
            )
            if dialect == "postgresql":
                exec_sql(
                    cfg,
                    [
                        f"CREATE SCHEMA {q(schema_name)}",
                        f"CREATE TABLE {q(schema_name)}.{q(parent)} "
                        f"(tenant_id INTEGER NOT NULL, order_no INTEGER NOT NULL, "
                        f"PRIMARY KEY (tenant_id, order_no))",
                        f"INSERT INTO {q(schema_name)}.{q(parent)} VALUES (1,100)",
                    ],
                )

            fks = [
                {
                    "columns": ["tenant_id", "order_no"],
                    "referenced_table": parent,
                    "referenced_columns": ["tenant_id", "order_no"],
                }
            ]
            pop = _pop(cfg, child, fks)
            samp = _sample(
                cfg,
                [
                    {"tenant_id": 1, "order_no": 100},
                    {"tenant_id": 2, "order_no": 101},
                    {"tenant_id": None, "order_no": 999},
                ],
                fks,
            )
            self_ref = _pop(
                cfg,
                emp,
                [
                    {
                        "columns": ["mgr_org", "mgr_id"],
                        "referenced_table": emp,
                        "referenced_columns": ["org", "emp_id"],
                    }
                ],
            )
            reserved = _pop(
                cfg,
                reserved_child,
                [
                    {
                        "columns": ["tenant_id", "user"],
                        "referenced_table": reserved_parent,
                        "referenced_columns": ["tenant_id", "user"],
                    }
                ],
            )
            row: dict[str, Any] = {
                "dialect": dialect,
                "pop_orphans": pop.get("orphan_count"),
                "pop_complete": pop.get("complete"),
                "pop_proof": pop.get("population_proof"),
                "sample_orphans": samp.get("orphan_count"),
                "sample_checked": samp.get("checked_values"),
                "sample_proof": samp.get("population_proof"),
                "self_ref_orphans": self_ref.get("orphan_count"),
                "self_ref_complete": self_ref.get("complete"),
                "reserved_orphans": reserved.get("orphan_count"),
                "reserved_complete": reserved.get("complete"),
                "example": (pop.get("findings") or [{}])[0].get("message", ""),
            }
            assert pop["complete"] is True, pop
            assert pop["orphan_count"] == 1, pop
            assert pop["population_proof"] is False
            assert "2+101" in pop["findings"][0]["message"]
            assert samp["population_proof"] is False
            assert samp["orphan_count"] == 1
            assert samp["checked_values"] == 2
            assert self_ref["complete"] is True, self_ref
            assert self_ref["orphan_count"] == 1, self_ref
            assert "1+99" in self_ref["findings"][0]["message"]
            assert reserved["complete"] is True, reserved
            assert reserved["orphan_count"] == 1, reserved

            if dialect == "postgresql":
                cross = _pop(
                    cfg,
                    child,
                    [
                        {
                            "columns": ["tenant_id", "order_no"],
                            "referenced_table": f"{schema_name}.{parent}",
                            "referenced_columns": ["tenant_id", "order_no"],
                        }
                    ],
                )
                row["cross_schema_orphans"] = cross.get("orphan_count")
                row["cross_schema_complete"] = cross.get("complete")
                assert cross["complete"] is True, cross
                # child has (1,100) matching parent in other schema; (2,101) orphan; NULL unconstrained
                assert cross["orphan_count"] == 1, cross

            results.append(row)
        finally:
            try:
                exec_sql(cfg, drop)
            except Exception:
                pass

    assert results, "live matrix produced no engine rows"
    proven_engines = {r["dialect"] for r in results}
    assert proven_engines <= {"postgresql", "mysql"}
