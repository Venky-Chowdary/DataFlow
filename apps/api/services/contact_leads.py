"""Contact-sales lead persistence (D103).

Persist-first contract: the lead is written before any mail is attempted, so a
mail outage never loses a submission. MongoDB ``contact_sales_leads`` is the
primary store; when Mongo is unreachable the lead is appended to
``data/contact_sales_leads.jsonl`` — a degraded copy that is still never
silently dropped.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from services.platform_config import data_dir

LOG = logging.getLogger(__name__)

_STORE_PATH = data_dir() / "contact_sales_leads.jsonl"


def persist_lead(lead: dict[str, Any]) -> dict[str, Any]:
    """Write the lead and return the stored record (with ``lead_id``).

    Never raises: a lead that survives nowhere is worse than a degraded copy.
    """
    record = dict(lead)
    record.setdefault("lead_id", uuid.uuid4().hex)
    record.setdefault(
        "created_at", datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    try:
        from services.mongodb_service import get_mongodb_service

        db = get_mongodb_service().get_database()
        db["contact_sales_leads"].insert_one(dict(record))
        record["store"] = "mongo"
        return record
    except Exception as exc:  # mongo down — fall back to the append-only file
        LOG.warning("contact lead: Mongo persist failed, writing file copy: %s", exc)
    _STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _STORE_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, default=str) + "\n")
    record["store"] = "jsonl"
    return record
