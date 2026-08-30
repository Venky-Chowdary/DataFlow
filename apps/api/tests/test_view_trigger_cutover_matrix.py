"""Enterprise matrix: name dependent views and triggers for cutover.

Table transfer does not emit view SQL or trigger bodies. The certificate
must still name the objects so cutover recreates them. Advisory never
vetoes ``migration_proven``.

``100%`` means every row on this named fixture — not every engine on earth.
"""

from __future__ import annotations

import uuid
from typing import Any, Callable

import pytest

from services.physical_state_diff import verify_physical_state


def _sid(prefix: str = "df_vt") -> str:
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


def test_live_engines_name_missing_views_and_triggers():
    engines = _live_engines()
    if not engines:
        pytest.skip("Neither Postgres nor MySQL authenticated — view/trigger matrix skipped")

    results: list[dict[str, Any]] = []
    for dialect, cfg, exec_sql, q in engines:
        src = _sid("s")
        dst = _sid("d")
        view = _sid("v")
        trg = _sid("trg")
        fn = _sid("fn")
        itype = "INT" if dialect == "mysql" else "INTEGER"
        schema = str(cfg.get("schema") or "")
        drop = [
            f"DROP VIEW IF EXISTS {q(view)}",
            f"DROP TABLE IF EXISTS {q(dst)}",
            f"DROP TABLE IF EXISTS {q(src)}",
        ]
        try:
            if dialect == "postgresql":
                exec_sql(
                    cfg,
                    drop
                    + [
                        f"CREATE TABLE {q(src)} (id {itype} PRIMARY KEY, qty {itype})",
                        f"CREATE TABLE {q(dst)} (id {itype} PRIMARY KEY, qty {itype})",
                        f"CREATE VIEW {q(view)} AS SELECT id, qty FROM {q(src)} WHERE qty > 0",
                        f"CREATE FUNCTION {q(fn)}() RETURNS trigger AS $$ "
                        f"BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
                        f"CREATE TRIGGER {q(trg)} AFTER INSERT ON {q(src)} "
                        f"FOR EACH ROW EXECUTE PROCEDURE {q(fn)}()",
                    ],
                )
                drop = [
                    f"DROP VIEW IF EXISTS {q(view)}",
                    f"DROP TABLE IF EXISTS {q(dst)}",
                    f"DROP TABLE IF EXISTS {q(src)}",
                    f"DROP FUNCTION IF EXISTS {q(fn)}() CASCADE",
                ]
            else:
                exec_sql(
                    cfg,
                    drop
                    + [
                        f"CREATE TABLE {q(src)} (id {itype} PRIMARY KEY, qty {itype})",
                        f"CREATE TABLE {q(dst)} (id {itype} PRIMARY KEY, qty {itype})",
                        f"CREATE VIEW {q(view)} AS SELECT id, qty FROM {q(src)} WHERE qty > 0",
                        f"CREATE TRIGGER {q(trg)} AFTER INSERT ON {q(src)} "
                        f"FOR EACH ROW BEGIN SET @df_vt := NEW.id; END",
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
            views = result["aspects"]["views"]
            triggers = result["aspects"]["triggers"]
            recreate = result.get("cutover_recreate") or []
            kinds = {i["kind"] for i in recreate}
            names = " ".join(i["name"] for i in recreate)
            assert result["verified"] is True, result
            assert "views" not in result["absent"]
            assert "triggers" not in result["absent"]
            assert views["advisory"] is True
            assert views["status"] == "absent", views
            assert view.lower() in views["missing"], views
            assert triggers["advisory"] is True
            assert triggers["status"] == "absent", triggers
            assert trg.lower() in names, (triggers, recreate)
            assert "view" in kinds and "trigger" in kinds
            results.append(
                {
                    "dialect": dialect,
                    "view": view,
                    "trigger": trg,
                    "view_missing": views["missing"],
                    "trigger_missing": triggers["missing"],
                    "recreate": recreate,
                }
            )
        finally:
            try:
                exec_sql(cfg, drop)
            except Exception:
                pass

    assert results, "live matrix produced no engine rows"
    assert {r["dialect"] for r in results} <= {"postgresql", "mysql"}
