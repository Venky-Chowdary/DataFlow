"""Live source duplicate-key probe for MySQL and PostgreSQL.

Runs against the docker-compose services on localhost; skips cleanly when the
ports are not reachable.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest


@pytest.fixture
def unique_table() -> str:
    return f"dupes_{uuid.uuid4().hex[:8]}"


def _seed_mysql(table: str) -> None:
    pymysql = pytest.importorskip("pymysql")
    conn = pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="dataflow",
        database="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"CREATE TABLE {table} (id VARCHAR(50), name VARCHAR(50))")
            cur.executemany(
                f"INSERT INTO {table} VALUES (%s, %s)",
                [("a", "A"), ("b", "B"), ("a", "A2"), ("c", "C"), ("b", "B2")],
            )
            conn.commit()
    finally:
        conn.close()


def _seed_postgresql(table: str) -> None:
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="dataflow",
        password="dataflow",
        dbname="dataflow",
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE TABLE {table} (id TEXT, name TEXT)")
        cur.executemany(
            f"INSERT INTO {table} VALUES (%s, %s)",
            [("a", "A"), ("b", "B"), ("a", "A2"), ("c", "C"), ("b", "B2")],
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


@pytest.mark.parametrize(
    "db_type,seed_fn,config",
    [
        (
            "mysql",
            _seed_mysql,
            {
                "name": "MySQL Dupes Live",
                "type": "mysql",
                "role": "source",
                "host": "localhost",
                "port": 3306,
                "database": "dataflow",
                "username": "root",
                "password": "dataflow",
                "ssl": False,
            },
        ),
        (
            "postgresql",
            _seed_postgresql,
            {
                "name": "PG Dupes Live",
                "type": "postgresql",
                "role": "source",
                "host": "localhost",
                "port": 5432,
                "database": "dataflow",
                "username": "dataflow",
                "password": "dataflow",
                "ssl": False,
            },
        ),
    ],
)
def test_live_duplicate_key_probe(
    db_type: str,
    seed_fn: Any,
    config: dict[str, Any],
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    unique_table: str,
) -> None:
    """Duplicate keys in a live SQL source are detected before the transfer runs."""
    from tests.typed_fidelity_helpers import require_ports

    port = 3306 if db_type == "mysql" else 5432
    require_ports(port)

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    store_path = tmp_path / "connectors.json"
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store_path))

    seed_fn(unique_table)

    from services.connector_store import create_connector
    from services.source_duplicate_probe import probe_source_duplicate_keys

    saved = create_connector(config)
    result = probe_source_duplicate_keys(
        source_connector_id=saved.id,
        source_table=unique_table,
        primary_key="id",
    )
    assert len(result) == 2
    values = {r["value"]: r["count"] for r in result}
    assert values.get("a") == 2
    assert values.get("b") == 2
