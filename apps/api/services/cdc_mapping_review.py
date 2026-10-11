"""CDC schema drift → Map review signals.

When native CDC detects DDL / catalog drift, operators must re-open Map — never
silently remap. Signals are append-only and acknowledgeable.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.brand_env import getenv_brand
from services.platform_config import data_dir
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "cdc_mapping_reviews.jsonl"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _store_path() -> Path:
    env = (getenv_brand("CDC_MAPPING_REVIEW_STORE", "") or "").strip()
    return Path(env) if env else STORE_PATH


def flag_mapping_review(
    *,
    source_key: str,
    table: str,
    reason: str,
    schema_version: int | None = None,
    ddl: str = "",
    column_names: list[str] | None = None,
) -> dict[str, Any]:
    """Record that CDC schema drifted and Map review is required."""
    signal = {
        "id": str(uuid.uuid4()),
        "source_key": str(source_key or "").strip(),
        "table": str(table or "").strip(),
        "reason": reason or "schema_drift",
        "schema_version": schema_version,
        "ddl": (ddl or "")[:500],
        "column_names": list(column_names or [])[:200],
        "status": "open",
        "created_at": _now(),
        "acknowledged_at": None,
        "honesty": (
            "CDC delivery remains at-least-once. Schema drift does not auto-remap — "
            "open Map, review fidelity, and re-Validate before continuing."
        ),
    }
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(signal, default=json_default) + "\n")
    logger.warning(
        "CDC mapping review required: %s.%s (%s)",
        signal["source_key"],
        signal["table"],
        reason,
    )
    return signal


def list_reviews(
    *,
    source_key: str | None = None,
    status: str = "open",
    limit: int = 50,
) -> list[dict[str, Any]]:
    path = _store_path()
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception as exc:
        logger.error("Failed reading CDC mapping review store: %s", exc)
        return []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if status and status != "all" and row.get("status") != status:
            continue
        if source_key and row.get("source_key") != source_key:
            continue
        out.append(row)
        if len(out) >= limit:
            break
    return out


def acknowledge_review(review_id: str) -> dict[str, Any] | None:
    path = _store_path()
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8").splitlines()
    updated: dict[str, Any] | None = None
    rewritten: list[str] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            rewritten.append(line)
            continue
        if row.get("id") == review_id and row.get("status") == "open":
            row["status"] = "acknowledged"
            row["acknowledged_at"] = _now()
            updated = row
        rewritten.append(json.dumps(row, default=json_default))
    if updated is None:
        return None
    path.write_text("\n".join(rewritten) + "\n", encoding="utf-8")
    return updated


def open_review_for_source(source_key: str, table: str = "") -> dict[str, Any] | None:
    """Latest open review for a CDC source (optional table filter)."""
    for row in list_reviews(source_key=source_key, status="open", limit=20):
        if table and row.get("table") != table:
            continue
        return row
    return None
