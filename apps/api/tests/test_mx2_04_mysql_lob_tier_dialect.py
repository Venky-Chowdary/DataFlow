"""MX2-04 — MySQL LOB tiers are a MySQL fact, not a fidelity rule for every TEXT.

QA: MySQL ``description LONGTEXT COLLATE utf8mb4_0900_ai_ci`` → SQLite create-new
blocked with "Lossy / fidelity collapse across type path … (description LONGTEXT
COLLATE UTF8MB4_0900_AI_CI → TEXT)". The collation was a red herring: the
comparator ranked the SQLite/Postgres ``TEXT`` as MySQL's 64 KiB TEXT tier.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.type_system import (  # noqa: E402
    is_lossy_coercion,
    is_precision_collapse_coercion,
    string_width_would_narrow,
)

_LONGTEXT = "LONGTEXT COLLATE utf8mb4_0900_ai_ci"


@pytest.mark.parametrize(
    "dest_db,target",
    [
        ("sqlite", "TEXT"),
        ("postgresql", "TEXT"),
        ("postgres", "TEXT"),
        ("duckdb", "TEXT"),
        ("mssql", "NVARCHAR(MAX)"),
        ("oracle", "CLOB"),
        ("mysql", "LONGTEXT"),
    ],
)
@pytest.mark.parametrize("src", [_LONGTEXT, "LONGTEXT", "MEDIUMTEXT CHARACTER SET utf8mb4"])
@pytest.mark.parametrize("exists", [False, True])
def test_mysql_lob_into_unbounded_text_is_not_a_collapse(src, dest_db, target, exists):
    assert not is_precision_collapse_coercion(
        src, target, dest_db=dest_db, dest_table_exists=exists
    )
    assert not is_lossy_coercion(src, target, dest_db=dest_db, dest_table_exists=exists)
    assert not string_width_would_narrow(src, target, dest_db=dest_db)


@pytest.mark.parametrize("db", ["mysql", "mariadb", ""])
@pytest.mark.parametrize(
    "src,target",
    [
        ("LONGTEXT", "TEXT"),
        ("LONGTEXT", "MEDIUMTEXT"),
        (_LONGTEXT, "TINYTEXT"),
        ("MEDIUMTEXT", "TEXT"),
    ],
)
def test_real_mysql_tier_narrowing_still_blocks(db, src, target):
    # On MySQL (and when the dialect is unknown) TEXT is the 64 KiB tier.
    assert string_width_would_narrow(src, target, dest_db=db)
    assert is_precision_collapse_coercion(src, target, dest_db=db, dest_table_exists=True)


def test_bounded_sinks_still_narrow_on_every_engine():
    for db in ("sqlite", "postgresql", "mssql", "mysql"):
        assert string_width_would_narrow("LONGTEXT", "VARCHAR(255)", dest_db=db), db
        assert is_precision_collapse_coercion(
            "LONGTEXT", "VARCHAR(255)", dest_db=db, dest_table_exists=True
        ), db


# ---------------------------------------------------------------------------
# Live: MySQL → SQLite through the engine with preflight (the QA route).
# ---------------------------------------------------------------------------

_MYSQL_PORT = int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT") or 3307)


def _mysql():
    pymysql = pytest.importorskip("pymysql")
    try:
        socket.create_connection(("127.0.0.1", _MYSQL_PORT), timeout=1).close()
        return pymysql.connect(
            host="127.0.0.1",
            port=_MYSQL_PORT,
            user="root",
            password="dataflow",
            database="dataflow",
            autocommit=True,
        )
    except Exception as exc:  # noqa: BLE001 — skip reason, not swallowed
        pytest.skip(f"MySQL not reachable on 127.0.0.1:{_MYSQL_PORT}: {exc}")


@pytest.fixture
def mysql_products():
    conn = _mysql()
    table = f"mx204_{uuid.uuid4().hex[:8]}"
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(80) NOT NULL, "
        "description LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci) "
        "DEFAULT CHARSET=utf8mb4"
    )
    rows = [(i, f"p{i}", f"Désc 日本 {i} 🚀" + "x" * (70_000 if i == 1 else 0)) for i in range(1, 21)]
    cur.executemany(f"INSERT INTO {table} VALUES (%s,%s,%s)", rows)
    try:
        yield table, rows
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def test_live_mysql_longtext_to_sqlite_create_new(mysql_products, tmp_path, monkeypatch):
    table, rows = mysql_products
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Jobs(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            return True

    jobs = _Jobs()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: jobs)
    dest = tmp_path / "dest.sqlite"
    req = TransferRequest(
        mappings=[
            {"source": n, "target": n, "confidence": 1.0} for n in ("id", "name", "description")
        ],
        source=EndpointConfig(
            kind="database",
            format="mysql",
            host="127.0.0.1",
            port=_MYSQL_PORT,
            database="dataflow",
            username="root",
            password="dataflow",
            table=table,
            ssl=False,
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dest), table="QA_E2E_RT_my_products"
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
    )
    job_id = f"mx204-{uuid.uuid4().hex[:8]}"
    jobs.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert "Lossy / fidelity collapse" not in str(result.error or "")
    assert result.success, result.error
    with sqlite3.connect(dest) as c:
        got = c.execute(
            'SELECT id, name, description FROM "QA_E2E_RT_my_products" ORDER BY id'
        ).fetchall()
    assert got == rows  # 70 KB utf8mb4 value (> MySQL TEXT tier) intact
