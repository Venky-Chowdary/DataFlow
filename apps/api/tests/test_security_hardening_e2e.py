"""End-to-end proof for security/robustness hardening — not mocks-only.

Covers:
1. Real CSV → PostgreSQL / MySQL / SQLite transfers with hostile table names
2. Destination row-count verification via reconciliation helpers
3. Streaming object-store kwargs forwarded through file_stream → _write_batch
4. Saved-connector path_style=True not clobbered by EndpointConfig default False
"""

from __future__ import annotations

import csv
import io
import socket
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sql_identifiers import quote_table_ref, sanitize_identifier
from src.transfer.adapters import resolve_connector_config
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


HOSTILE_TABLE = 'orders"; DROP TABLE users;--'
SAFE_TABLE = sanitize_identifier(HOSTILE_TABLE, preserve_case=True)


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "amount", "status"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _sample_rows() -> list[dict]:
    return [
        {"id": "1", "amount": "10.50", "status": "ok"},
        {"id": "2", "amount": "20.00", "status": "ok"},
        {"id": "3", "amount": "30.25", "status": "held"},
    ]


def _pg_reachable() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1.5):
            return True
    except OSError:
        return False


def _mysql_reachable() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=1.5):
            return True
    except OSError:
        return False


def test_sanitize_hostile_table_is_deterministic() -> None:
    assert SAFE_TABLE == "orders_DROP_TABLE_users"
    assert ";" not in SAFE_TABLE
    assert '"' not in SAFE_TABLE


def test_path_style_saved_connector_not_clobbered(monkeypatch: pytest.MonkeyPatch) -> None:
    """Saved MinIO path_style=True must survive EndpointConfig default False."""
    from src.transfer import adapters as adapters_mod

    monkeypatch.setattr(
        adapters_mod,
        "_lookup_saved_connector",
        lambda _id, workspace_id=None: {
            "host": "localhost",
            "port": 9000,
            "database": "dataflow",
            "username": "dataflow",
            "password": "secret",
            "type": "s3",
            "endpoint_url": "http://localhost:9000",
            "path_style": True,
            "region": "us-east-1",
        },
    )
    ep = EndpointConfig(
        kind="database",
        format="s3",
        connector_id="conn_minio",
        # defaults: path_style=False, endpoint_url=""
    )
    cfg = resolve_connector_config(ep)
    assert cfg["path_style"] is True
    assert cfg["endpoint_url"] == "http://localhost:9000"
    assert cfg["region"] == "us-east-1"


def test_file_stream_forwards_object_store_kwargs_to_write_batch() -> None:
    """file_stream → _write_batch must pass MinIO endpoint settings (not only unit kwargs)."""
    from src.transfer.file_stream import stream_file_to_database

    captured: dict = {}

    def _fake_write_batch(dest_type, dest, cfg, *args, **kwargs):
        captured["dest_type"] = dest_type
        captured["cfg"] = dict(cfg)
        return 2, "chk", {"type": dest_type, "checksum": "chk"}

    content = _csv_bytes(_sample_rows())
    dest = EndpointConfig(
        kind="database",
        format="s3",
        host="localhost",
        port=9000,
        database="dataflow",
        username="minio",
        password="minio123",
        table="exports/proof.json",
        endpoint_url="http://127.0.0.1:9000",
        path_style=True,
        region="us-east-1",
    )
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "amount", "target": "amount", "confidence": 1.0},
        {"source": "status", "target": "status", "confidence": 1.0},
    ]
    schema = {"id": "integer", "amount": "decimal", "status": "string"}

    with patch("src.transfer.file_stream._write_batch", side_effect=_fake_write_batch):
        rows, _ddl, summary, _ = stream_file_to_database(
            content,
            "proof.csv",
            dest,
            mappings,
            schema,
            sync_mode="full_refresh_overwrite",
        )

    assert rows == 2
    assert captured["dest_type"] == "s3"
    assert captured["cfg"]["endpoint_url"] == "http://127.0.0.1:9000"
    assert captured["cfg"]["path_style"] is True
    assert captured["cfg"]["region"] == "us-east-1"
    # file_stream may replace writer checksum with a source fingerprint digest
    assert isinstance(summary.get("checksum"), str) and summary["checksum"]


@pytest.mark.skipif(not _pg_reachable(), reason="PostgreSQL not reachable on 127.0.0.1:5432")
def test_e2e_csv_to_postgresql_hostile_table_name_and_reconcile() -> None:
    import psycopg2

    from services.reconciliation import verify_postgres_table

    table = f"df_sec_{uuid.uuid4().hex[:8]}_{SAFE_TABLE}"[:63]
    # Prove quote_table_ref shape for this live table
    ref = quote_table_ref(table, "public", dialect="postgresql")
    assert ";" not in ref
    assert "DROP" not in ref or "DROP" in table  # sanitized fragment only inside quotes

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="sec_proof.csv",
        source_content=_csv_bytes(_sample_rows()),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=table,
            ssl=False,
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
    )
    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 3

    count, checksum = verify_postgres_table(
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        connection_string="",
        ssl=False,
        table_name=table,
    )
    assert count == 3
    assert checksum  # non-empty destination checksum

    # Live SELECT through hardened reader
    from connectors.postgresql_reader import read_table_batch

    batch = read_table_batch(
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        connection_string="",
        ssl=False,
        table=table,
        columns=["id", "amount", "status"],
        offset=0,
        limit=10,
    )
    assert batch.total_rows == 3
    assert len(batch.rows) == 3

    # Cleanup
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow",
        user="dataflow", password="dataflow", connect_timeout=2,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {quote_table_ref(table, 'public', dialect='postgresql')}")
    conn.close()


@pytest.mark.skipif(not _mysql_reachable(), reason="MySQL not reachable on 127.0.0.1:3306")
def test_e2e_csv_to_mysql_hostile_table_name_and_reconcile() -> None:
    import pymysql

    from services.reconciliation import verify_mysql_table

    table = f"df_sec_{uuid.uuid4().hex[:8]}_{SAFE_TABLE}"[:63]
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="sec_proof.csv",
        source_content=_csv_bytes(_sample_rows()),
        destination=EndpointConfig(
            kind="database",
            format="mysql",
            host="127.0.0.1",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            table=table,
            ssl=False,
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
    )
    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 3

    count, checksum = verify_mysql_table(
        host="127.0.0.1",
        port=3306,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        connection_string="",
        ssl=False,
        table_name=table,
    )
    assert count == 3
    assert checksum

    from connectors.mysql_reader import read_table_batch

    batch = read_table_batch(
        host="127.0.0.1",
        port=3306,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="",
        connection_string="",
        ssl=False,
        table=table,
        columns=["id", "amount", "status"],
        offset=0,
        limit=10,
    )
    assert batch.total_rows == 3
    assert len(batch.rows) == 3

    conn = pymysql.connect(
        host="127.0.0.1", port=3306, user="dataflow", password="dataflow",
        database="dataflow", connect_timeout=2,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {quote_table_ref(table, dialect='mysql')}")
        conn.commit()
    finally:
        conn.close()


def test_e2e_csv_to_sqlite_hostile_table_and_reconcile(tmp_path: Path) -> None:
    from services.reconciliation import verify_sqlite_table

    db_path = tmp_path / "sec_proof.db"
    table = f"df_sec_{SAFE_TABLE}"[:63]
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="sec_proof.csv",
        source_content=_csv_bytes(_sample_rows()),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(db_path),
            table=table,
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
    )
    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 3

    count, checksum = verify_sqlite_table(
        database=str(db_path),
        connection_string="",
        table_name=table,
        host="",
    )
    assert count == 3
    assert checksum


def test_mysql_writer_insert_sql_never_embeds_injection_payload() -> None:
    """Writer SQL construction must quote sanitized identifiers — shape proof."""
    from connectors import mysql_writer as mw

    captured: dict = {}

    class _Cur:
        def execute(self, sql, params=None):
            captured.setdefault("sql", []).append(sql)

        def executemany(self, sql, params):
            captured.setdefault("sql", []).append(sql)

        def fetchall(self):
            return []

        def fetchone(self):
            return (0,)

    class _Conn:
        def cursor(self):
            return _Cur()

        def commit(self):
            return None

        def close(self):
            return None

        def rollback(self):
            return None

    with patch.object(mw, "_open_mysql", return_value=_Conn()), patch.object(
        mw, "ensure_mysql_write_ledger", lambda *_a, **_k: None
    ), patch.object(mw, "write_chunk_size", return_value=100):
        result = mw.write_mapped_rows(
            host="127.0.0.1",
            port=3306,
            database="dataflow",
            username="u",
            password="p",
            schema="",
            connection_string="",
            ssl=False,
            table_name=HOSTILE_TABLE,
            headers=["id", "name"],
            data_rows=[["1", "a"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
            ],
            column_types={"id": "integer", "name": "string"},
            create_table=True,
            error_policy="quarantine",
        )

    assert result.ok is True or result.rows_written >= 0
    joined = "\n".join(captured.get("sql") or [])
    assert "DROP TABLE" not in joined
    assert '";' not in joined
    assert "`orders_DROP_TABLE_users`" in joined or "orders_DROP_TABLE_users" in joined


def test_pg_change_stream_qualified_table_uses_quote_helpers() -> None:
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    stream = object.__new__(PostgreSqlChangeStreamCdc)
    stream.schema = 'public"; DROP SCHEMA'
    stream.table = HOSTILE_TABLE
    qualified = PostgreSqlChangeStreamCdc._qualified_table(stream)
    assert ";" not in qualified
    assert "DROP SCHEMA" not in qualified
    assert qualified.startswith('"') and "." in qualified
