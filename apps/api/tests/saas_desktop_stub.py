"""Local HTTP stub for Salesforce / HubSpot / Stripe reverse-ETL on this desktop.

Not a customer org. Proves the writer + Gate-8 read-back path against a
named fixture so the matrix is not invented green against live SaaS.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


class _Store:
    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {
            "Account": [],
            "contacts": [],
            "customers": [],
        }


STORE = _Store()

_SAAS_FIELDS = (
    "id", "email", "name", "Name", "description",
    "amount", "code", "updated_at",
)


def _field_describe(col: str) -> dict[str, Any]:
    """Typed Describe for the tabular SKU fixture — not a customer org.

    ``amount`` is currency(18,2) so G19 does not treat live DECIMAL as TEXT.
    Business ``id`` is int; Salesforce ``Id`` stays the 18-char Id carrier.
    """
    name = col
    lower = col.lower()
    if lower == "amount":
        ftype, precision, scale, length = "currency", 18, 2, None
    elif lower == "id":
        # Tabular SKU identity — long so Validate sample BIGINT and Describe agree.
        ftype, precision, scale, length = "long", None, None, None
    else:
        ftype, precision, scale, length = "string", None, None, 255
    return {
        "name": name,
        "type": ftype,
        "nillable": lower not in {"id"},
        "length": length,
        "precision": precision,
        "scale": scale,
        "updateable": True,
        "createable": True,
        "externalId": lower == "id",
        "idLookup": lower == "id",
    }


def _hubspot_property(col: str) -> dict[str, Any]:
    lower = col.lower()
    if lower == "amount":
        return {
            "name": col,
            "type": "number",
            "fieldType": "number",
            "numberDisplayHint": "currency",
        }
    if lower == "id":
        return {"name": col, "type": "string", "fieldType": "text"}
    return {"name": col, "type": "string", "fieldType": "text"}


def _row_identity(rec: dict[str, Any]) -> str:
    return str(rec.get("id") or rec.get("Id") or rec.get("hs_object_id") or "").strip()


def _upsert_row(store_key: str, rec: dict[str, Any]) -> dict[str, Any]:
    """Identity replace — overwrite must not keep leftover extra dest keys."""
    rows = STORE.rows.setdefault(store_key, [])
    rid = _row_identity(rec)
    if rid:
        for i, existing in enumerate(rows):
            if _row_identity(existing) == rid:
                rows[i] = rec
                return rec
    rows.append(rec)
    return rec


class SaasStubHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        return

    def _json(self, code: int, payload: Any) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> Any:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        ctype = (self.headers.get("Content-Type") or "").lower()
        text = raw.decode("utf-8")
        if "json" in ctype or text[:1] in "{[":
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                pass
        parsed = parse_qs(text, keep_blank_values=True)
        return {key: (vals[0] if len(vals) == 1 else vals) for key, vals in parsed.items()}

    def _describe_fields(self, name: str) -> dict[str, Any]:
        fields = [_field_describe(col) for col in _SAAS_FIELDS]
        return {"name": name, "fields": fields}

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path.endswith("/describe") and "/sobjects/" in path:
            name = path.split("/sobjects/")[1].split("/")[0]
            self._json(200, self._describe_fields(name))
            return
        if path.endswith("/sobjects"):
            self._json(200, {"sobjects": [{"name": "Account", "queryable": True}]})
            return
        if path.endswith("/limits"):
            self._json(200, {"DailyApiRequests": {"Remaining": 10000, "Max": 10000}})
            return
        if "/query" in path:
            recs = STORE.rows.get("Account") or []
            self._json(200, {"totalSize": len(recs), "done": True, "records": recs})
            return
        if path.startswith("/crm/v3/properties/"):
            self._json(200, {"results": [_hubspot_property(col) for col in _SAAS_FIELDS]})
            return
        if path.startswith("/crm/v3/objects/"):
            wrapped = [
                {"id": _row_identity(r), "properties": dict(r)}
                for r in (STORE.rows.get("contacts") or [])
            ]
            self._json(200, {"results": wrapped})
            return
        if path in {"/v1/account", "/v1/accounts"}:
            self._json(200, {"id": "acct_stub", "object": "account"})
            return
        if path.startswith("/v1/customers"):
            self._json(200, {"object": "list", "data": STORE.rows.get("customers") or [], "has_more": False})
            return
        if path.rstrip("/").endswith("/records") or path == "/records":
            self._json(200, STORE.rows.get("records") or [])
            return
        self._json(200, {"ok": True})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        payload = self._read_body()
        if "/composite/sobjects" in path:
            records = payload.get("records") or []
            results = []
            for i, rec in enumerate(records):
                rec = dict(rec)
                rec.pop("attributes", None)
                if not rec.get("Id") and not rec.get("id"):
                    rec["Id"] = f"001STUB{i:03d}AAA"
                    rec["id"] = rec["Id"]
                stored = _upsert_row("Account", rec)
                results.append({"id": stored.get("Id") or stored.get("id"), "success": True, "errors": []})
            self._json(200, results)
            return
        if path.endswith("/batch/upsert") or path.endswith("/batch/create"):
            inputs = payload.get("inputs") or []
            results = []
            for i, item in enumerate(inputs):
                props = dict(item.get("properties") or {})
                rid = str(item.get("id") or props.get("id") or f"hs_{i}")
                props["id"] = rid
                stored = _upsert_row("contacts", props)
                results.append({"id": stored["id"], "properties": stored})
            self._json(200, {"results": results, "errors": [], "status": "COMPLETE"})
            return
        if path.startswith("/v1/customers"):
            rec = dict(payload) if isinstance(payload, dict) else {}
            rec["id"] = rec.get("id") or f"cus_stub_{len(STORE.rows['customers'])}"
            stored = _upsert_row("customers", rec)
            self._json(200, stored)
            return
        self._json(200, {"ok": True})

    def do_PATCH(self) -> None:  # noqa: N802
        self.do_POST()


def start_saas_stub(port: int = 0) -> tuple[HTTPServer, str]:
    STORE.rows = {"Account": [], "contacts": [], "customers": [], "records": []}
    server = HTTPServer(("127.0.0.1", port), SaasStubHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    assigned = server.server_address[1]
    return server, f"http://127.0.0.1:{assigned}"


def seed_tabular_fixture() -> None:
    """Two id/amount rows for PRODUCTION_SKU source reads against this stub."""
    rows = [
        {"id": "1", "amount": "1000.00", "Name": "A", "email": "a@example.com"},
        {"id": "2", "amount": "2000.50", "Name": "B", "email": "b@example.com"},
    ]
    STORE.rows["Account"] = [dict(r) for r in rows]
    STORE.rows["contacts"] = [dict(r) for r in rows]
    STORE.rows["customers"] = [
        {"id": "cus_1", "object": "customer", "amount": 100000, "email": "a@example.com"},
        {"id": "cus_2", "object": "customer", "amount": 200050, "email": "b@example.com"},
    ]
    STORE.rows["records"] = [
        {"id": "1", "amount": "1000.00"},
        {"id": "2", "amount": "2000.50"},
    ]
