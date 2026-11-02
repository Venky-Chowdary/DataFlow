"""Enterprise matrix: name dependent procedures/functions for cutover.

Table transfer does not emit routine SQL. The certificate must still name
procedures and functions that depend on the moved table. Advisory never
vetoes ``migration_proven``. SQLite has no stored routines — empty is
measured, not unreadable.

``100%`` means every row on this named fixture — not every engine on earth.
"""

from __future__ import annotations

import uuid
from typing import Any, Callable

import pytest

from services.physical_state_diff import verify_physical_state


def _sid(prefix: str = "df_rt") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


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


def _live_engines() -> list[tuple[str, dict[str, Any], Callable, Callable]]:
    out: list[tuple[str, dict[str, Any], Callable, Callable]] = []
    pg = _pg_cfg()
    if pg:
        out.append(("postgresql", pg, _pg_exec, lambda ident: f'"{ident}"'))
    mysql = _mysql_cfg()
    if mysql:
        out.append(("mysql", mysql, _mysql_exec, lambda ident: f"`{ident}`"))
    return out


def test_live_engines_name_missing_dependent_routines():
    engines = _live_engines()
    if not engines:
        pytest.skip("Neither Postgres nor MySQL authenticated — routine cutover matrix skipped")

    results: list[dict[str, Any]] = []
    for dialect, cfg, exec_sql, q in engines:
        src = _sid("s")
        dst = _sid("d")
        other = _sid("o")
        fn = _sid("fn")
        other_fn = _sid("ofn")
        pl_fn = _sid("pl")
        trg_fn = _sid("trgfn")
        trg = _sid("trg")
        itype = "INT" if dialect == "mysql" else "INTEGER"
        schema = str(cfg.get("schema") or "")
        drop = [
            f"DROP TABLE IF EXISTS {q(dst)}",
            f"DROP TABLE IF EXISTS {q(src)}",
            f"DROP TABLE IF EXISTS {q(other)}",
        ]
        try:
            if dialect == "postgresql":
                drop = [
                    f"DROP TRIGGER IF EXISTS {q(trg)} ON {q(src)}",
                    f"DROP FUNCTION IF EXISTS {q(fn)}() CASCADE",
                    f"DROP FUNCTION IF EXISTS {q(other_fn)}() CASCADE",
                    f"DROP FUNCTION IF EXISTS {q(pl_fn)}() CASCADE",
                    f"DROP FUNCTION IF EXISTS {q(trg_fn)}() CASCADE",
                    f"DROP TABLE IF EXISTS {q(dst)}",
                    f"DROP TABLE IF EXISTS {q(src)}",
                    f"DROP TABLE IF EXISTS {q(other)}",
                ]
                exec_sql(
                    cfg,
                    drop
                    + [
                        f"CREATE TABLE {q(src)} (id {itype} PRIMARY KEY, qty {itype})",
                        f"CREATE TABLE {q(dst)} (id {itype} PRIMARY KEY, qty {itype})",
                        f"CREATE TABLE {q(other)} (id {itype} PRIMARY KEY, qty {itype})",
                        f"INSERT INTO {q(src)} VALUES (1, 10)",
                        f"INSERT INTO {q(other)} VALUES (1, 10)",
                        f"CREATE FUNCTION {q(fn)}() RETURNS {itype} LANGUAGE sql STABLE AS "
                        f"$$ SELECT qty FROM {q(src)} LIMIT 1 $$",
                        f"CREATE FUNCTION {q(other_fn)}() RETURNS {itype} LANGUAGE sql STABLE AS "
                        f"$$ SELECT qty FROM {q(other)} LIMIT 1 $$",
                        # PL/pgSQL does not record pg_depend — body scan must still name it.
                        f"CREATE FUNCTION {q(pl_fn)}() RETURNS {itype} LANGUAGE plpgsql AS $$"
                        f"BEGIN RETURN (SELECT qty FROM {q(src)} LIMIT 1); END; $$",
                        f"CREATE FUNCTION {q(trg_fn)}() RETURNS trigger LANGUAGE plpgsql AS $$"
                        f"BEGIN NEW.qty := NEW.qty; RETURN NEW; END; $$",
                        f"CREATE TRIGGER {q(trg)} BEFORE INSERT ON {q(src)} "
                        f"FOR EACH ROW EXECUTE FUNCTION {q(trg_fn)}()",
                    ],
                )
            else:
                drop = [
                    f"DROP PROCEDURE IF EXISTS {q(fn)}",
                    f"DROP PROCEDURE IF EXISTS {q(other_fn)}",
                    f"DROP TABLE IF EXISTS {q(dst)}",
                    f"DROP TABLE IF EXISTS {q(src)}",
                    f"DROP TABLE IF EXISTS {q(other)}",
                ]
                exec_sql(
                    cfg,
                    drop
                    + [
                        f"CREATE TABLE {q(src)} (id {itype} PRIMARY KEY, qty {itype})",
                        f"CREATE TABLE {q(dst)} (id {itype} PRIMARY KEY, qty {itype})",
                        f"CREATE TABLE {q(other)} (id {itype} PRIMARY KEY, qty {itype})",
                        f"CREATE PROCEDURE {q(fn)}() BEGIN SELECT id FROM {q(src)}; END",
                        f"CREATE PROCEDURE {q(other_fn)}() BEGIN SELECT id FROM {q(other)}; END",
                    ],
                )
            result = verify_physical_state(
                source_db_type=dialect,
                source_cfg=cfg,
                source_schema=schema,
                source_table=src,
                dest_db_type=dialect,
                dest_cfg=cfg,
                dest_schema=schema,
                dest_table=dst,
            )
            routines = result["aspects"]["routines"]
            recreate = result.get("cutover_recreate") or []
            routine_items = [i for i in recreate if i.get("kind") == "routine"]
            names = " ".join(i["name"] for i in routine_items)
            assert result["verified"] is True, result
            assert "routines" not in result["absent"]
            assert routines["advisory"] is True
            assert routines["status"] == "absent", routines
            assert fn.lower() in routines["missing"], routines
            assert other_fn.lower() not in routines["missing"], routines
            if dialect == "postgresql":
                assert pl_fn.lower() in routines["missing"], routines
                assert trg_fn.lower() not in routines["missing"], routines
            assert fn.lower() in names, recreate
            assert other_fn.lower() not in names
            results.append(
                {
                    "dialect": dialect,
                    "routine": fn,
                    "missing": routines["missing"],
                    "recreate": routine_items,
                    "verified": result["verified"],
                    "absent": result["absent"],
                }
            )
        finally:
            try:
                exec_sql(cfg, drop)
            except Exception:
                pass

    assert results, "live matrix produced no engine rows"
    assert {r["dialect"] for r in results} <= {"postgresql", "mysql"}
