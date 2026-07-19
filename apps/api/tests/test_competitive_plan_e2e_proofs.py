"""Competitive-plan E2E proofs — real HTTP + real local DBs, not catalog theater.

Covers:
- unique_drivers honesty contract
- schedule detail mappings for Pipeline Detail
- JSON POST /transfer/execute db→db with fidelity
- rest_api → sqlite via a live local HTTP fixture
- ops attention signals (DLQ + freshness endpoints)
"""

from __future__ import annotations

import json
import sqlite3
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

pg_ok = False
if psycopg2 is not None:
    try:
        _c = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=2,
        )
        _c.close()
        pg_ok = True
    except Exception:
        pg_ok = False


def _client() -> TestClient:
    from src.main import app

    return TestClient(app)


# --------------------------------------------------------------------------- #
# 1. Honesty — unique drivers primary, tiles secondary
# --------------------------------------------------------------------------- #


def test_catalog_summary_unique_drivers_below_tile_inflation():
    from services.catalog_service import catalog_summary
    from src.transfer.connector_capabilities import transfer_live_driver_types

    data = catalog_summary()
    unique = int(data.get("unique_drivers") or 0)
    tiles = int(data.get("catalog_tiles") or data.get("transfer_live_tiles") or 0)
    drivers = transfer_live_driver_types()

    assert unique == len(drivers)
    assert unique >= 8, f"expected a real driver surface, got {unique}: {drivers}"
    assert unique < 80, f"unique drivers inflated to {unique} — likely counting aliases"
    # Catalog tiles may be higher (aliases), but must not redefine unique_drivers.
    assert data.get("transfer_live") == unique
    if tiles:
        assert tiles >= unique


def test_platform_status_exposes_live_driver_list():
    client = _client()
    res = client.get("/api/v1/transfer/platform")
    assert res.status_code == 200
    data = res.json()
    drivers = data.get("live_drivers") or []
    assert isinstance(drivers, list)
    assert "postgresql" in drivers or "sqlite" in drivers
    assert len(drivers) == len(set(drivers))
    assert data["transfer_ready"] == len(drivers)


# --------------------------------------------------------------------------- #
# 2. Pipeline Detail — mappings on GET /schedules/{id}
# --------------------------------------------------------------------------- #


def test_schedule_detail_returns_schema_mappings(tmp_path: Path, monkeypatch):
    import services.schedule_store as store

    store_path = tmp_path / "schedules_proof.json"
    monkeypatch.setattr(store, "STORE_PATH", store_path)
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)

    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "email", "target": "email_addr", "confidence": 0.95},
        {"source": "amount", "target": "amount", "confidence": 1.0},
    ]
    sched = store.create_schedule(
        {
            "name": "proof-pipeline",
            "source_connector_id": "src-pg",
            "source_table": "customers",
            "dest_connector_id": "dst-sqlite",
            "dest_table": "customers_out",
            "interval": "daily",
            "mappings": mappings,
            "primary_key": "id",
            "enabled": True,
        }
    )

    client = _client()
    detail = client.get(f"/api/v1/schedules/{sched.id}")
    assert detail.status_code == 200, detail.text
    body = detail.json()
    assert body["mapping_count"] == 3
    assert len(body["mappings"]) == 3
    assert body["mappings"][1]["target"] == "email_addr"
    assert body["primary_key"] == "id"

    listing = client.get("/api/v1/schedules/")
    assert listing.status_code == 200
    row = next(s for s in listing.json() if s["id"] == sched.id)
    assert row.get("mapping_count") == 3
    assert "mappings" not in row


# --------------------------------------------------------------------------- #
# 3. JSON execute — real Postgres → SQLite over HTTP
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(not pg_ok, reason="Local Postgres (dataflow/dataflow) not reachable")
def test_json_transfer_execute_postgres_to_sqlite_with_fidelity(tmp_path: Path):
    assert psycopg2 is not None
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
        connect_timeout=2,
    )
    conn.autocommit = True
    table = f"json_exec_{uuid.uuid4().hex[:8]}"
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE {table} (
                  id INT PRIMARY KEY,
                  email TEXT NOT NULL,
                  amount NUMERIC(12,2) NOT NULL
                );
                INSERT INTO {table} (id, email, amount) VALUES
                  (1, 'alice@example.com', 10.50),
                  (2, 'bob@example.com', 22.00),
                  (3, 'carol@example.com', 7.25);
                """
            )
    finally:
        conn.close()

    dest = tmp_path / "json_exec_out.db"
    client = _client()
    payload = {
        "source": {
            "kind": "database",
            "format": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "public",
            "table": table,
        },
        "destination": {
            "kind": "database",
            "format": "sqlite",
            "database": str(dest),
            "table": "customers_out",
        },
        "mappings": [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "email", "target": "email", "confidence": 1.0},
            {"source": "amount", "target": "amount", "confidence": 1.0},
        ],
        "sync_mode": "full_refresh_overwrite",
        "skip_preflight": True,
        "async_mode": False,
        "validation_mode": "strict",
        "enforce_contract": False,
    }
    res = client.post("/api/v1/transfer/execute", json=payload)
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["success"] is True
    assert body["async"] is False
    assert body["records_transferred"] == 3
    assert body.get("job_id")
    recon = body.get("reconciliation") or {}
    assert recon.get("passed") is True, recon

    with sqlite3.connect(dest) as db:
        rows = db.execute(
            "SELECT id, email, amount FROM customers_out ORDER BY id"
        ).fetchall()
    assert len(rows) == 3
    assert rows[0][1] == "alice@example.com"

    # Job trust fields persist for the Jobs UI strip.
    job = client.get(f"/api/v1/connectors/jobs/{body['job_id']}")
    assert job.status_code == 200
    detail = job.json()
    processed = int(
        detail.get("records_processed")
        or detail.get("records_written")
        or detail.get("records_transferred")
        or detail.get("total_rows")
        or 0
    )
    assert processed >= 3, detail
    assert (detail.get("status") or "").lower() in {"completed", "success", "succeeded"}
    # Fidelity / reconcile artifacts used by Job Theater trust strip
    assert detail.get("reconciliation") or body.get("reconciliation")
    # Cleanup source table
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
        connect_timeout=2,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 4. rest_api unlock — live HTTP fixture → sqlite
# --------------------------------------------------------------------------- #


def test_rest_api_to_sqlite_transfer_via_local_http_fixture(tmp_path: Path):
    rows_payload = [
        {"id": "1", "amount": "1000.00"},
        {"id": "2", "amount": "2000.50"},
    ]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = json.dumps({"data": rows_payload}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):  # silence
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        from src.transfer.engine import UniversalTransferEngine
        from src.transfer.models import EndpointConfig, TransferRequest

        dest = tmp_path / "rest_out.db"
        engine = UniversalTransferEngine()
        result = engine.execute_tracked(
            TransferRequest(
                source=EndpointConfig(
                    kind="database",
                    format="rest_api",
                    host=f"http://127.0.0.1:{port}",
                    port=port,
                    database="",
                    table="records",
                    connection_string=json.dumps(
                        {"data_path": "data", "pagination_type": "none"}
                    ),
                ),
                destination=EndpointConfig(
                    kind="database",
                    format="sqlite",
                    database=str(dest),
                    table="from_rest",
                ),
                mappings=[
                    {"source": "id", "target": "id"},
                    {"source": "amount", "target": "amount"},
                ],
                skip_preflight=True,
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success, result.error
        assert result.records_transferred == 2
        with sqlite3.connect(dest) as db:
            got = db.execute("SELECT id, amount FROM from_rest ORDER BY id").fetchall()
        normalized = [(str(a), str(b)) for a, b in got]
        assert normalized == [("1", "1000.00"), ("2", "2000.50")]
    finally:
        server.shutdown()


# --------------------------------------------------------------------------- #
# 5. Alerts / ops attention APIs (Overview strip inputs)
# --------------------------------------------------------------------------- #


def test_ops_freshness_and_dlq_endpoints_shape():
    client = _client()
    fresh = client.get("/api/v1/ops/freshness")
    assert fresh.status_code == 200, fresh.text
    dlq = client.get("/api/v1/ops/dlq")
    assert dlq.status_code == 200, dlq.text
    # Endpoints must return JSON objects/arrays — Overview strip depends on this.
    assert isinstance(fresh.json(), (dict, list))
    assert isinstance(dlq.json(), (dict, list))
