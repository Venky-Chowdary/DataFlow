"""PROPERTY 6 — schema fidelity is more than column types.

Create-new must CARRY PK / NOT NULL / simple DEFAULT / UNIQUE when the source
catalog exposes them, and emit an explicit unsupported/skipped certificate for
CHECK, FK, views, triggers, etc. Silence is a bug.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.schema_fidelity import (
    REQUIRED_ASPECTS,
    SourceSchemaCatalog,
    build_catalog_from_introspect,
    is_safe_default_expr,
    plan_create_new_fidelity,
    render_create_column_defs,
)
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _run(req: TransferRequest):
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


def _pg_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=0.4):
            return True
    except OSError:
        return False


def _pg_creds() -> dict:
    return {
        "host": os.environ.get("P6_PG_HOST", os.environ.get("P2_PG_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P6_PG_PORT", os.environ.get("P2_PG_PORT", "5432"))),
        "database": os.environ.get("P6_PG_DB", os.environ.get("P2_PG_DB", "postgres")),
        "username": os.environ.get("P6_PG_USER", os.environ.get("P2_PG_USER", "postgres")),
        "password": os.environ.get("P6_PG_PASSWORD", os.environ.get("P2_PG_PASSWORD", "admin")),
    }


def _aspect_status(report: dict, aspect: str) -> set[str]:
    return {i["status"] for i in report["items"] if i["aspect"] == aspect}


def test_plan_carries_pk_not_null_default_unique_and_certifies_check_fk():
    catalog = SourceSchemaCatalog(
        dialect="sqlite",
        columns=["id", "email", "status", "note"],
        column_types={
            "id": "BIGINT",
            "email": "TEXT",
            "status": "TEXT",
            "note": "TEXT",
        },
        nullable={"id": False, "email": False, "status": False, "note": True},
        defaults={"status": "'active'"},
        primary_key=["id"],
        unique_keys=[["email"]],
        check_constraints=["status IN ('active','inactive')"],
        foreign_keys=[{"name": "fk_note", "columns": ["note"]}],
        views=["v_people"],
        triggers=["trg_people"],
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="sqlite",
        target_columns=["id", "email", "status", "note"],
        target_types=["INTEGER", "TEXT", "TEXT", "TEXT"],
        source_to_target={c: c for c in catalog.columns},
    )
    report = plan.report.to_dict()
    assert "carried" in _aspect_status(report, "primary_key")
    assert "carried" in _aspect_status(report, "not_null")
    assert "carried" in _aspect_status(report, "default")
    assert "carried" in _aspect_status(report, "unique")
    assert "unsupported" in _aspect_status(report, "check")
    assert "unsupported" in _aspect_status(report, "foreign_key")
    assert "unsupported" in _aspect_status(report, "view")
    assert "unsupported" in _aspect_status(report, "trigger")
    present = {i["aspect"] for i in report["items"]}
    assert set(REQUIRED_ASPECTS).issubset(present)

    ddl = render_create_column_defs(
        columns=plan.dest_columns,
        types=["INTEGER", "TEXT", "TEXT", "TEXT"],
        plan=plan,
        dialect="sqlite",
    )
    assert "PRIMARY KEY" in ddl.upper()
    assert "NOT NULL" in ddl.upper()
    assert "DEFAULT" in ddl.upper()
    assert "UNIQUE" in ddl.upper()
    assert "CHECK" not in ddl.upper()
    assert "FOREIGN KEY" not in ddl.upper()


def test_unsafe_default_is_unsupported_not_silently_emitted():
    assert is_safe_default_expr("1 + 1") is False
    assert is_safe_default_expr("'ok'") is True
    assert is_safe_default_expr("'active'::text") is True
    assert is_safe_default_expr("('active'::text)") is True
    catalog = SourceSchemaCatalog(
        dialect="postgresql",
        columns=["id", "x"],
        nullable={"id": False, "x": True},
        defaults={"x": "(SELECT max(id) FROM t)"},
        primary_key=["id"],
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="postgresql",
        target_columns=["id", "x"],
        target_types=["BIGINT", "TEXT"],
        source_to_target={"id": "id", "x": "x"},
    )
    assert "unsupported" in _aspect_status(plan.report.to_dict(), "default")
    ddl = render_create_column_defs(
        columns=["id", "x"],
        types=["BIGINT", "TEXT"],
        plan=plan,
        dialect="postgresql",
    )
    assert "SELECT" not in ddl.upper()


def test_sqlite_create_new_carries_constraints_end_to_end(tmp_path: Path):
    src_path = tmp_path / "p6_src.db"
    dst_path = tmp_path / "p6_dst.db"
    src = sqlite3.connect(str(src_path))
    try:
        src.execute(
            """
            CREATE TABLE people (
              id INTEGER NOT NULL,
              email TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'active',
              note TEXT,
              PRIMARY KEY (id),
              UNIQUE (email)
            )
            """
        )
        src.execute(
            "INSERT INTO people (id, email, status, note) VALUES (1, 'a@x.com', 'active', NULL)"
        )
        src.execute(
            "INSERT INTO people (id, email, note) VALUES (2, 'b@x.com', 'hello')"
        )
        src.commit()
    finally:
        src.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(src_path),
            table="people",
        ),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(dst_path),
            table="people_out",
        ),
        mappings=[
            {
                "source": "id",
                "target": "id",
                "source_type": "BIGINT",
                "target_type": "BIGINT",
                "approved": True,
                "confidence": 0.99,
            },
            {
                "source": "email",
                "target": "email",
                "source_type": "TEXT",
                "target_type": "TEXT",
                "approved": True,
                "confidence": 0.99,
            },
            {
                "source": "status",
                "target": "status",
                "source_type": "TEXT",
                "target_type": "TEXT",
                "approved": True,
                "confidence": 0.99,
            },
            {
                "source": "note",
                "target": "note",
                "source_type": "TEXT",
                "target_type": "TEXT",
                "approved": True,
                "confidence": 0.99,
            },
        ],
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    result = _run(req)
    assert result.success, result.error
    summary = result.destination_summary or {}
    fid = summary.get("schema_fidelity") or {}
    assert fid, "schema_fidelity certificate missing from destination_summary"
    assert fid.get("carried_count", 0) >= 3
    assert "carried" in _aspect_status(fid, "primary_key")
    assert "carried" in _aspect_status(fid, "not_null")
    assert "carried" in _aspect_status(fid, "default")
    assert "carried" in _aspect_status(fid, "unique")

    dst = sqlite3.connect(str(dst_path))
    try:
        cols = list(dst.execute('PRAGMA table_info("people_out")'))
        by_name = {r[1]: r for r in cols}
        assert by_name["id"][3] == 1  # notnull
        assert by_name["email"][3] == 1
        assert by_name["status"][3] == 1
        assert str(by_name["status"][4]).lower().replace('"', "'") in {
            "'active'",
            "active",
        }
        # PK flag
        assert by_name["id"][5] >= 1
        indexes = list(dst.execute('PRAGMA index_list("people_out")'))
        uniq = [i for i in indexes if int(i[2] or 0) == 1]
        assert uniq, "UNIQUE(email) missing on destination"
        # Row values survived
        rows = list(dst.execute('SELECT id, email, status FROM "people_out" ORDER BY id'))
        assert rows == [(1, "a@x.com", "active"), (2, "b@x.com", "active")]
        notes = list(dst.execute('SELECT note FROM "people_out" ORDER BY id'))
        assert notes == [(None,), ("hello",)]
    finally:
        dst.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not listening on 5432")
def test_pg_create_new_carries_pk_not_null_default_live():
    import psycopg2

    creds = _pg_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p6_src_{suffix}"
    dst_table = f"p6_dst_{suffix}"
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(
                f'''
                CREATE TABLE public."{src_table}" (
                  id bigint NOT NULL,
                  email text NOT NULL,
                  status text NOT NULL DEFAULT 'active',
                  note text,
                  PRIMARY KEY (id),
                  UNIQUE (email)
                )
                '''
            )
            cur.execute(
                f'''INSERT INTO public."{src_table}" (id, email, status, note)
                    VALUES (1, 'a@x.com', 'active', NULL),
                           (2, 'b@x.com', DEFAULT, 'hello')'''
            )
    finally:
        conn.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="postgresql",
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
            username=creds["username"],
            password=creds["password"],
            schema="public",
            table=src_table,
            ssl=False,
        ),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
            username=creds["username"],
            password=creds["password"],
            schema="public",
            table=dst_table,
            ssl=False,
        ),
        mappings=[
            {
                "source": "id",
                "target": "id",
                "source_type": "BIGINT",
                "target_type": "BIGINT",
                "approved": True,
                "confidence": 0.99,
            },
            {
                "source": "email",
                "target": "email",
                "source_type": "TEXT",
                "target_type": "TEXT",
                "approved": True,
                "confidence": 0.99,
            },
            {
                "source": "status",
                "target": "status",
                "source_type": "TEXT",
                "target_type": "TEXT",
                "approved": True,
                "confidence": 0.99,
            },
            {
                "source": "note",
                "target": "note",
                "source_type": "TEXT",
                "target_type": "TEXT",
                "approved": True,
                "confidence": 0.99,
            },
        ],
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        result = _run(req)
        assert result.success, result.error
        fid = (result.destination_summary or {}).get("schema_fidelity") or {}
        assert fid, "schema_fidelity missing on PG destination_summary"
        assert "carried" in _aspect_status(fid, "primary_key")
        assert "carried" in _aspect_status(fid, "not_null")
        assert "carried" in _aspect_status(fid, "default")

        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=%s
                    ORDER BY ordinal_position
                    """,
                    (dst_table,),
                )
                cols = {r[0]: r for r in cur.fetchall()}
                assert cols["id"][1] == "NO"
                assert cols["email"][1] == "NO"
                assert cols["status"][1] == "NO"
                assert cols["status"][2] and "active" in str(cols["status"][2])
                cur.execute(
                    """
                    SELECT constraint_type
                    FROM information_schema.table_constraints
                    WHERE table_schema='public' AND table_name=%s
                    """,
                    (dst_table,),
                )
                ctypes = {r[0] for r in cur.fetchall()}
                assert "PRIMARY KEY" in ctypes
                assert "UNIQUE" in ctypes
        finally:
            conn.close()
    finally:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        finally:
            conn.close()


def test_build_catalog_filters_primary_unique_entry():
    cat = build_catalog_from_introspect(
        dialect="sqlite",
        columns=["id", "email"],
        nullable={"id": False, "email": False},
        keys={
            "primary_key_columns": ["id"],
            "unique_keys": [
                {"name": "PRIMARY", "columns": ["id"], "primary": True},
                {"name": "uq_email", "columns": ["email"], "primary": False},
            ],
            "defaults": {"email": "'x'"},
        },
    )
    assert cat.primary_key == ["id"]
    assert cat.unique_keys == [["email"]]
    assert cat.defaults["email"] == "'x'"
