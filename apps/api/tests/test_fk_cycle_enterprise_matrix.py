"""Enterprise matrix: FK cycles are recreated by post-load ALTER.

A cycle is not a reason to drop keys. Create-new lands tables without FKs;
``ALTER TABLE ADD CONSTRAINT`` after the load is the portable deferred
strategy. PostgreSQL/Oracle also emit DEFERRABLE INITIALLY DEFERRED.

``100%`` means every row on this named fixture — not every engine on earth.
Skipped engines are reported, never invented green.
"""

from __future__ import annotations

import uuid
from typing import Any, Callable

import pytest

from services.foreign_key_metadata import ForeignKey, ForeignKeys
from services.foreign_key_orchestration import carry_foreign_keys, summarize
from services.migration_certificate import build_migration_certificate


def _sid(prefix: str = "df_cyc") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


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


def _fk(name: str, cols: list[str], ref_table: str, ref_cols: list[str]) -> ForeignKey:
    return ForeignKey(
        name=name,
        columns=cols,
        referenced_schema="",
        referenced_table=ref_table,
        referenced_columns=ref_cols,
    )


def _measured(dialect: str, table: str, *keys: ForeignKey) -> ForeignKeys:
    return ForeignKeys(dialect=dialect, status="measured", table=table, items=list(keys))


def _cert(fk_summary: dict[str, Any]) -> dict[str, Any]:
    job = {
        "id": "a" * 24,
        "status": "completed",
        "records_processed": 2,
        "sync_mode": "overwrite",
        "source": {"format": "postgresql"},
        "destination": {"format": "mysql"},
        "reconciliation": {
            "passed": True,
            "phase": "post_write_verified",
            "assurance_level": "full_checksum",
            "checksum_match": True,
            "source_rows": 2,
            "target_rows": 2,
            "rejected_rows": 0,
            "rows_skipped": 0,
            "source_checksum": "abc",
            "target_checksum": "abc",
            "message": "Verified",
            "population_proof": True,
        },
        "destination_summary": {"rejected_details": [], "foreign_keys": fk_summary},
    }
    return build_migration_certificate(job)["verdict"]


def _live_engines() -> list[tuple[str, dict[str, Any], Callable, Callable]]:
    out: list[tuple[str, dict[str, Any], Callable, Callable]] = []
    pg = _pg_cfg()
    if pg:
        out.append(("postgresql", pg, _pg_exec, lambda ident: f'"{ident}"'))
    mysql = _mysql_cfg()
    if mysql:
        out.append(("mysql", mysql, _mysql_exec, lambda ident: f"`{ident}`"))
    return out


def _pg_cycle_is_deferrable(cfg: dict[str, Any], table: str) -> bool:
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
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bool_and(c.condeferrable AND c.condeferred)
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.contype = 'f' AND t.relname = %s AND n.nspname = 'public'
                """,
                (table,),
            )
            row = cur.fetchone()
            return bool(row and row[0])
    finally:
        conn.close()


def test_live_engines_recreate_mutual_and_self_ref_cycles():
    engines = _live_engines()
    if not engines:
        pytest.skip("Neither Postgres nor MySQL authenticated — live FK cycle matrix skipped")

    itype = lambda d: "INT" if d == "mysql" else "INTEGER"
    results: list[dict[str, Any]] = []
    for dialect, cfg, exec_sql, q in engines:
        customers = _sid("cust")
        orders = _sid("ord")
        emp = _sid("emp")
        dirty_c = _sid("dc")
        dirty_o = _sid("do")
        drop = [
            f"DROP TABLE IF EXISTS {q(orders)}",
            f"DROP TABLE IF EXISTS {q(customers)}",
            f"DROP TABLE IF EXISTS {q(emp)}",
            f"DROP TABLE IF EXISTS {q(dirty_o)}",
            f"DROP TABLE IF EXISTS {q(dirty_c)}",
        ]
        try:
            exec_sql(
                cfg,
                drop
                + [
                    f"CREATE TABLE {q(customers)} (id {itype(dialect)} PRIMARY KEY, "
                    f"last_order_id {itype(dialect)} NULL)",
                    f"CREATE TABLE {q(orders)} (id {itype(dialect)} PRIMARY KEY, "
                    f"customer_id {itype(dialect)} NULL)",
                    f"INSERT INTO {q(customers)} (id, last_order_id) VALUES (1, 10)",
                    f"INSERT INTO {q(orders)} (id, customer_id) VALUES (10, 1)",
                    f"CREATE TABLE {q(emp)} (id {itype(dialect)} PRIMARY KEY, "
                    f"mgr_id {itype(dialect)} NULL)",
                    f"INSERT INTO {q(emp)} (id, mgr_id) VALUES (1, NULL), (2, 1)",
                    f"CREATE TABLE {q(dirty_c)} (id {itype(dialect)} PRIMARY KEY, "
                    f"last_order_id {itype(dialect)} NULL)",
                    f"CREATE TABLE {q(dirty_o)} (id {itype(dialect)} PRIMARY KEY, "
                    f"customer_id {itype(dialect)} NULL)",
                    f"INSERT INTO {q(dirty_c)} (id, last_order_id) VALUES (1, 10)",
                    f"INSERT INTO {q(dirty_o)} (id, customer_id) VALUES (10, 99)",
                ],
            )
            source_keys = {
                customers: _measured(
                    dialect,
                    customers,
                    _fk("cust_last", ["last_order_id"], orders, ["id"]),
                ),
                orders: _measured(
                    dialect,
                    orders,
                    _fk("ord_cust", ["customer_id"], customers, ["id"]),
                ),
            }
            identity = {customers: customers, orders: orders}
            cols = {
                customers: ["id", "last_order_id"],
                orders: ["id", "customer_id"],
            }
            cmap = {
                customers: {"id": "id", "last_order_id": "last_order_id"},
                orders: {"id": "id", "customer_id": "customer_id"},
            }
            decisions = carry_foreign_keys(
                dest_dialect=dialect,
                dest_cfg=cfg,
                dest_schema=str(cfg.get("schema") or ""),
                source_keys=source_keys,
                table_map=identity,
                column_maps=cmap,
                dest_columns=cols,
                cycle_tables=[customers, orders],
            )
            summary = summarize(decisions, cycle=[customers, orders])
            verdict = _cert(summary)
            assert summary["cycle_resolved"] is True, summary
            assert summary["carried"] == 2, summary
            assert summary["integrity_violations"] == 0
            assert not any("cycle" in b.lower() for b in verdict["blockers"]), verdict
            assert not any("foreign key" in b.lower() for b in verdict["blockers"]), verdict

            self_dec = carry_foreign_keys(
                dest_dialect=dialect,
                dest_cfg=cfg,
                dest_schema=str(cfg.get("schema") or ""),
                source_keys={
                    emp: _measured(dialect, emp, _fk("emp_mgr", ["mgr_id"], emp, ["id"]))
                },
                table_map={emp: emp},
                column_maps={emp: {"id": "id", "mgr_id": "mgr_id"}},
                dest_columns={emp: ["id", "mgr_id"]},
                cycle_tables=[emp],
            )
            self_sum = summarize(self_dec, cycle=[emp])
            assert self_sum["cycle_resolved"] is True, self_sum
            assert self_sum["carried"] == 1, self_sum

            dirty = carry_foreign_keys(
                dest_dialect=dialect,
                dest_cfg=cfg,
                dest_schema=str(cfg.get("schema") or ""),
                source_keys={
                    dirty_c: _measured(
                        dialect,
                        dirty_c,
                        _fk("dc_last", ["last_order_id"], dirty_o, ["id"]),
                    ),
                    dirty_o: _measured(
                        dialect,
                        dirty_o,
                        _fk("do_cust", ["customer_id"], dirty_c, ["id"]),
                    ),
                },
                table_map={dirty_c: dirty_c, dirty_o: dirty_o},
                column_maps={
                    dirty_c: {"id": "id", "last_order_id": "last_order_id"},
                    dirty_o: {"id": "id", "customer_id": "customer_id"},
                },
                dest_columns={
                    dirty_c: ["id", "last_order_id"],
                    dirty_o: ["id", "customer_id"],
                },
                cycle_tables=[dirty_c, dirty_o],
            )
            dirty_sum = summarize(dirty, cycle=[dirty_c, dirty_o])
            dirty_verdict = _cert(dirty_sum)
            assert dirty_sum["cycle_resolved"] is False, dirty_sum
            assert dirty_sum["integrity_violations"] >= 1
            assert any("cycle" in b.lower() for b in dirty_verdict["blockers"]), dirty_verdict

            row: dict[str, Any] = {
                "dialect": dialect,
                "mutual_carried": summary["carried"],
                "mutual_resolved": summary["cycle_resolved"],
                "self_resolved": self_sum["cycle_resolved"],
                "orphan_resolved": dirty_sum["cycle_resolved"],
                "orphan_violations": dirty_sum["integrity_violations"],
            }
            if dialect == "postgresql":
                row["pg_deferrable"] = _pg_cycle_is_deferrable(cfg, orders)
                assert row["pg_deferrable"] is True
            results.append(row)
        finally:
            try:
                exec_sql(cfg, drop)
            except Exception:
                pass

    assert results, "live matrix produced no engine rows"
    assert {r["dialect"] for r in results} <= {"postgresql", "mysql"}
