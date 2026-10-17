"""A dotted Postgres destination must create the missing schema before the ledger.

``qa_schema.orders`` used to be probed as ``public."qa_schema.orders"``. The
writable check passed, then ``CREATE TABLE "qa_schema"."_dataflow_write_..."``
failed because the schema did not exist.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.1,127.0.0.0/8,::1/128")
os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_ALLOW_STUB_WRITES", "1")
os.environ.setdefault("DATAFLOW_PII_HASH_KEY", "test-pii-key")


def _postgres_or_fail():
    try:
        import psycopg2
    except ImportError:
        pytest.fail("psycopg2 is required for the dotted-schema regression")
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            user="dataflow",
            password="dataflow",
            dbname="dataflow",
            connect_timeout=3,
        )
    except Exception as exc:
        pytest.fail(f"Postgres is required for the dotted-schema regression: {exc}")
    conn.close()
    return psycopg2


def test_dotted_destination_creates_missing_schema_before_the_write_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The row writer creates the ledger. COPY would create the table first and
    # hide that proof. Scope the switch to this test so later COPY tests still run.
    monkeypatch.setenv("DATAFLOW_SQLITE_PG_COPY", "0")
    psycopg2 = _postgres_or_fail()
    from services.destination_privilege_probe import probe_destination_privileges
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    token = uuid.uuid4().hex[:8]
    schema = f"qa_r16_schema_{token}"
    table = "qa_r16_orders"
    dotted = f"{schema}.{table}"
    src = tmp_path / "src.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO orders (id, name) VALUES (1, 'alpha'), (2, 'beta')")
    conn.commit()
    conn.close()

    pg = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        user="dataflow",
        password="dataflow",
        dbname="dataflow",
    )
    pg.autocommit = True
    with pg.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    pg.close()

    probe = probe_destination_privileges(
        "postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        schema="",
        table=dotted,
        username="dataflow",
        password="dataflow",
        table_exists=False,
    )
    assert probe.status == "ok", probe.detail
    assert probe.can_create_table is True
    assert probe.signals.get("schema_exists") is False
    assert "cannot create it" not in (probe.detail or "")

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(
        TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(src),
                table="orders",
            ),
            destination=EndpointConfig(
                kind="database",
                format="postgresql",
                host="127.0.0.1",
                port=5432,
                database="dataflow",
                username="dataflow",
                password="dataflow",
                table=dotted,
            ),
            sync_mode="full_refresh_overwrite",
            mappings=[
                {"source": "id", "target": "id", "confidence": 1},
                {"source": "name", "target": "name", "confidence": 1},
            ],
            stream_contracts=[{
                "name": "orders",
                "sync_mode": "full_refresh_overwrite",
                "primary_key": "id",
                "selected": True,
            }],
        ),
        uuid.uuid4().hex[:24],
    )
    assert result.success, result.error

    pg = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        user="dataflow",
        password="dataflow",
        dbname="dataflow",
    )
    pg.autocommit = True
    try:
        with pg.cursor() as cur:
            cur.execute(f'SELECT id, name FROM "{schema}"."{table}" ORDER BY id')
            assert cur.fetchall() == [(1, "alpha"), (2, "beta")]
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, "_dataflow_write_ledger"),
            )
            assert cur.fetchone() is not None
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        pg.close()
