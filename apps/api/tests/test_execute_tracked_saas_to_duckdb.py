"""E2E proof that the five priority SaaS sources can drive a real transfer.

Each test mocks the brand's HTTP API, runs the engine source -> DuckDB, and
asserts the transfer reconciles.  This is the TRANSFER_READY evidence for
Stripe, Shopify, Zendesk, Notion, and Airtable.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _mock_response(json_data: Any, status: int = 200, headers: dict | None = None) -> mock.MagicMock:
    m = mock.MagicMock()
    m.status_code = status
    m.headers = headers or {}
    m.json.return_value = json_data
    return m


def _run_transfer(
    brand: str,
    source_cfg: dict[str, Any],
    response_payload: Any,
    table_name: str,
    tmp_path: Path,
) -> tuple[Any, str]:
    """Run source->DuckDB transfer with a mocked requests.request."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    calls: list[tuple[str, dict]] = []

    def _respond(method: str, url: str, **kwargs: Any) -> mock.MagicMock:
        calls.append((url, kwargs.get("params") or {}))
        return _mock_response(response_payload)

    db_path = str(tmp_path / f"{table_name}.duck")
    request = TransferRequest(
        source=EndpointConfig(kind="database", format=brand, **source_cfg),
        destination=EndpointConfig(
            kind="database",
            format="duckdb",
            database=db_path,
            table=table_name,
        ),
        sync_mode="upsert",
        stream_contracts=[{
            "name": table_name,
            "sync_mode": "upsert",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )

    with mock.patch("requests.request", side_effect=_respond):
        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    return result, db_path


def _cleanup(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Brand tests
# ---------------------------------------------------------------------------
def test_stripe_customers_to_duckdb(tmp_path: Path):
    pytest.importorskip("duckdb")
    table = "stripe_customers_" + uuid.uuid4().hex[:8]
    payload = {
        "data": [
            {"id": "cus_1", "name": "Alice", "email": "alice@example.com"},
            {"id": "cus_2", "name": "Bob", "email": "bob@example.com"},
        ],
        "has_more": False,
    }
    result, path = _run_transfer(
        "stripe",
        {"host": "api.stripe.com", "api_key": "sk_test_xxx", "table": "customers"},
        payload,
        table,
        tmp_path,
    )
    try:
        assert result.success, result.error
        assert result.records_transferred == 2
        assert result.reconciliation.get("passed") is True
        import duckdb
        conn = duckdb.connect(path)
        rows = conn.execute(f'SELECT * FROM "{table}" ORDER BY id').fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0][0] == "cus_1"
        assert rows[1][0] == "cus_2"
    finally:
        _cleanup(path)


def test_shopify_products_to_duckdb(tmp_path: Path):
    pytest.importorskip("duckdb")
    table = "shopify_products_" + uuid.uuid4().hex[:8]
    payload = {
        "products": [
            {"id": 1, "title": "Shirt", "price": 25.0},
            {"id": 2, "title": "Hat", "price": 15.0},
        ]
    }
    result, path = _run_transfer(
        "shopify",
        {
            "host": "",
            "database": "demo",
            "api_key": "shpat_xxx",
            "table": "products",
            "extra": {"shop": "demo"},
        },
        payload,
        table,
        tmp_path,
    )
    try:
        assert result.success, result.error
        assert result.records_transferred == 2
        assert result.reconciliation.get("passed") is True
        import duckdb
        conn = duckdb.connect(path)
        rows = conn.execute(f'SELECT * FROM "{table}" ORDER BY id').fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2
    finally:
        _cleanup(path)


def test_zendesk_tickets_to_duckdb(tmp_path: Path):
    pytest.importorskip("duckdb")
    from src.transfer.connector_capabilities import _DRIVER_CAPS, transfer_ready

    if not transfer_ready(_DRIVER_CAPS["zendesk"]):
        pytest.skip("zendesk is Planned until live SKU proof")
    table = "zendesk_tickets_" + uuid.uuid4().hex[:8]
    payload = {
        "tickets": [
            {"id": 101, "subject": "Login issue", "status": "open"},
            {"id": 102, "subject": "Refund", "status": "closed"},
        ]
    }
    result, path = _run_transfer(
        "zendesk",
        {
            "host": "",
            "database": "demo",
            "api_key": "token_xxx",
            "table": "tickets",
        },
        payload,
        table,
        tmp_path,
    )
    try:
        assert result.success, result.error
        assert result.records_transferred == 2
        assert result.reconciliation.get("passed") is True
        import duckdb
        conn = duckdb.connect(path)
        rows = conn.execute(f'SELECT * FROM "{table}" ORDER BY id').fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0][0] == 101
        assert rows[1][0] == 102
    finally:
        _cleanup(path)


def test_notion_databases_to_duckdb(tmp_path: Path):
    pytest.importorskip("duckdb")
    from src.transfer.connector_capabilities import _DRIVER_CAPS, transfer_ready

    if not transfer_ready(_DRIVER_CAPS["notion"]):
        pytest.skip("notion is Planned until live SKU proof")
    table = "notion_pages_" + uuid.uuid4().hex[:8]
    payload = {
        "results": [
            {"id": "p1", "title": "Page one"},
            {"id": "p2", "title": "Page two"},
        ],
        "has_more": False,
        "next_cursor": None,
    }
    result, path = _run_transfer(
        "notion",
        {"host": "", "api_key": "secret_xxx", "table": "databases"},
        payload,
        table,
        tmp_path,
    )
    try:
        assert result.success, result.error
        assert result.records_transferred == 2
        assert result.reconciliation.get("passed") is True
        import duckdb
        conn = duckdb.connect(path)
        rows = conn.execute(f'SELECT * FROM "{table}" ORDER BY id').fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0][0] == "p1"
        assert rows[1][0] == "p2"
    finally:
        _cleanup(path)


def test_airtable_records_to_duckdb(tmp_path: Path):
    pytest.importorskip("duckdb")
    table = "airtable_records_" + uuid.uuid4().hex[:8]
    payload = {
        "records": [
            {"id": "r1", "fields": {"Name": "Alice", "Status": "Active"}},
            {"id": "r2", "fields": {"Name": "Bob", "Status": "Paused"}},
        ]
    }
    result, path = _run_transfer(
        "airtable",
        {"host": "", "api_key": "pat_xxx", "table": "appXXX/tblYYY"},
        payload,
        table,
        tmp_path,
    )
    try:
        assert result.success, result.error
        assert result.records_transferred == 2
        assert result.reconciliation.get("passed") is True
        import duckdb
        conn = duckdb.connect(path)
        rows = conn.execute(f'SELECT * FROM "{table}" ORDER BY id').fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0][0] == "r1"
        assert rows[1][0] == "r2"
    finally:
        _cleanup(path)
