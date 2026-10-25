"""Sales-lead store (P4-5) — inbound "contact sales" submissions.

Same backend discipline as the signup stores: Mongo when available, the
JSON file fallback otherwise, isolated via ``_leads_path`` in tests.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.platform_config import data_dir

LEADS_COLLECTION = "sales_leads"
_EMPTY_FILE = {"leads": []}
_LEAD_FIELDS = (
    "first_name", "last_name", "email", "company", "role", "volume",
    "region", "timeframe", "phone", "message", "sources", "destinations",
)


def _database():
    return mongo_database()


def _leads_path() -> Path:
    return data_dir() / "sales_leads.json"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def create_lead(fields: dict[str, Any], *, ip: str = "",
                user_agent: str = "") -> dict[str, Any]:
    doc = {
        "_id": f"lead_{uuid.uuid4().hex[:16]}",
        **{k: fields.get(k) for k in _LEAD_FIELDS if fields.get(k) not in (None, "", [])},
        "ip": ip,
        "user_agent": (user_agent or "")[:512],
        "status": "new",
        "created_at": _now(),
    }
    db = _database()
    if db is not None:
        db[LEADS_COLLECTION].replace_one({"_id": doc["_id"]}, doc, upsert=True)
    else:
        with json_doc_transaction(_leads_path(), _EMPTY_FILE) as data:
            data["leads"] = [*data.get("leads", []), doc]
    return {k: v for k, v in doc.items() if k != "_id"}


def list_leads(*, status: str | None = None) -> list[dict[str, Any]]:
    db = _database()
    if db is not None:
        rows = [dict(r) for r in db[LEADS_COLLECTION].find({}, {"_id": False})]
    else:
        rows = list(load_json_doc(_leads_path(), _EMPTY_FILE).get("leads", []))
    if status is not None:
        rows = [r for r in rows if r.get("status") == status]
    return sorted(rows, key=lambda r: r.get("created_at", ""), reverse=True)


def _reset_for_tests() -> None:
    pass  # isolation is via _leads_path monkeypatching
