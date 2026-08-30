"""Named Stripe SKU — incremental ``created`` cursor + dest COUNT.

Not more catalog tiles. Shopify stays Planned. ``100%`` is not claimed here.
A local Stripe-shaped list API is the named fixture, not a Stripe tenant.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.stripe import (
    stripe_created_watermark,
    stripe_list_params,
    stripe_row_after_watermark,
)
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def test_stripe_created_cursor_helpers() -> None:
    assert stripe_list_params(limit=50, created_gte=1000)["created[gte]"] == 1000
    assert "starting_after" not in stripe_list_params(limit=10)
    created, last = stripe_created_watermark("created", "2000")
    assert created == 2000
    assert last == ""
    rec = {"id": "cus_2", "created": 2000}
    assert stripe_row_after_watermark(rec, created_gte=2000, last_id="cus_2") is False
    assert stripe_row_after_watermark(
        {"id": "cus_3", "created": 3000}, created_gte=2000, last_id="cus_2"
    )
    assert stripe_created_watermark("id", "cus_9")[0] is None


def test_stripe_id_uniqueness_is_platform_assigned() -> None:
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    result = probe_source_duplicate_keys_result(
        source_config={"format": "stripe", "host": "http://127.0.0.1:1", "api_key": "sk_test_fixture"},
        source_table="customers",
        primary_key="id",
    )
    assert result.ran is True
    assert result.status == "ran"
    assert result.findings == []


class _StripeListHandler(BaseHTTPRequestHandler):
    customers: list[dict[str, Any]] = []

    def log_message(self, *_args: Any) -> None:  # noqa: ARG002
        return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.endswith("/v1/account"):
            self._json({"id": "acct_fixture", "object": "account"})
            return
        if "/v1/" not in parsed.path:
            self.send_error(404)
            return
        qs = parse_qs(parsed.query)
        created_raw = (qs.get("created[gte]") or [None])[0]
        created_gte = int(created_raw) if created_raw not in (None, "") else None
        starting = (qs.get("starting_after") or [""])[0]
        rows = list(self.customers)
        if starting:
            ids = [str(r.get("id") or "") for r in rows]
            if starting in ids:
                rows = rows[ids.index(starting) + 1 :]
        if created_gte is not None:
            rows = [
                r
                for r in rows
                if stripe_row_after_watermark(
                    r, created_gte=created_gte, last_id=""
                )
                or int(r.get("created") or 0) >= created_gte
            ]
        self._json({"object": "list", "data": rows, "has_more": False})

    def _json(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _serve(customers: list[dict[str, Any]]) -> tuple[ThreadingHTTPServer, str]:
    _StripeListHandler.customers = customers
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _StripeListHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    return httpd, f"http://{host}:{port}"


STRIPE_MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 0.99, "source_type": "TEXT", "target_type": "TEXT"},
    {"source": "email", "target": "email", "confidence": 0.99, "source_type": "TEXT", "target_type": "TEXT"},
    {
        "source": "created",
        "target": "created",
        "confidence": 0.99,
        "source_type": "INTEGER",
        "target_type": "INTEGER",
    },
]


def test_stripe_incremental_sku_execute_dest_count(tmp_path: Path) -> None:
    """Named SKU: Stripe created cursor → sqlite dest COUNT. Not a Stripe tenant."""
    customers = [
        {"id": "cus_1", "created": 1000, "email": "a@example.com"},
        {"id": "cus_2", "created": 2000, "email": "b@example.com"},
    ]
    httpd, base = _serve(customers)
    dst = tmp_path / "stripe_sku.db"
    table = "customers_" + uuid.uuid4().hex[:8]
    try:
        source = EndpointConfig(
            kind="database",
            format="stripe",
            host=base,
            api_key="sk_test_fixture",
            table="customers",
            ssl=False,
        )
        dest = EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(dst),
            table=table,
            connection_string=f"sqlite:///{dst}",
            ssl=False,
        )
        contracts = [
            {
                "name": "customers",
                "sync_mode": "incremental_deduped",
                "cursor_field": "created",
                "cursor_semantics": "insert_only",
                "primary_key": "id",
                "selected": True,
            }
        ]
        engine = UniversalTransferEngine()
        first = engine.execute_tracked(
            TransferRequest(
                source=source,
                destination=dest,
                mappings=list(STRIPE_MAPPINGS),
                sync_mode="incremental_deduped",
                stream_contracts=contracts,
                skip_preflight=False,
                validation_mode="balanced",
            ),
            uuid.uuid4().hex[:24],
        )
        assert first.success, first.error
        con = sqlite3.connect(dst)
        try:
            count1 = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        finally:
            con.close()
        assert count1 == 2, f"first dest COUNT={count1}"

        customers.append(
            {"id": "cus_3", "created": 3000, "email": "c@example.com"}
        )
        _StripeListHandler.customers = customers
        second = engine.execute_tracked(
            TransferRequest(
                source=source,
                destination=dest,
                mappings=list(STRIPE_MAPPINGS),
                sync_mode="incremental_deduped",
                stream_contracts=contracts,
                skip_preflight=False,
                validation_mode="balanced",
            ),
            uuid.uuid4().hex[:24],
        )
        assert second.success, second.error
        con = sqlite3.connect(dst)
        try:
            count2 = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            emails = sorted(
                r[0] for r in con.execute(f'SELECT email FROM "{table}"').fetchall()
            )
        finally:
            con.close()
        assert count2 == 3, f"incremental dest COUNT={count2} (writer ack is not dest proof)"
        assert emails == ["a@example.com", "b@example.com", "c@example.com"]
        assert int(second.records_transferred or 0) <= 2
    finally:
        httpd.shutdown()
        httpd.server_close()
