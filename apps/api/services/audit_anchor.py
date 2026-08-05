"""External audit tip anchoring — WORM / Object-Lock style receipts.

Engineering diligence: seals the current HMAC chain tip into an append-only
anchor store. The default provider is a local stub (not S3 Object Lock / TSA).
Real object-lock backends can plug in behind ``DATAFLOW_AUDIT_ANCHOR_PROVIDER``.
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

ANCHOR_STORE = data_dir() / "audit_tip_anchors.jsonl"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _store_path() -> Path:
    env = (getenv_brand("AUDIT_ANCHOR_STORE", "") or "").strip()
    return Path(env) if env else ANCHOR_STORE


def anchor_provider() -> str:
    return (getenv_brand("AUDIT_ANCHOR_PROVIDER", "stub") or "stub").strip().lower()


def anchor_tip(tip_hash: str, *, event_time: str = "") -> dict[str, Any] | None:
    """Seal ``tip_hash`` into the anchor store. Returns the receipt or None if empty tip."""
    tip = (tip_hash or "").strip()
    if not tip:
        return None
    provider = anchor_provider()
    receipt = {
        "id": str(uuid.uuid4()),
        "tip_hash": tip,
        "anchored_at": _now(),
        "event_time": event_time or None,
        "provider": provider,
        "honesty": (
            "Local stub anchor — not S3 Object Lock / RFC-3161 TSA. "
            "Configure DATAFLOW_AUDIT_ANCHOR_PROVIDER=s3 when infra is ready."
            if provider in {"stub", "local", ""}
            else f"Anchor provider={provider} (verify ops runbook separately)."
        ),
    }
    # Future: provider == "s3" → PutObject with Object Lock; keep stub fail-safe.
    path = _store_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(receipt, default=json_default) + "\n")
    except Exception as exc:
        logger.warning("Audit tip anchor write failed (chain still intact): %s", exc)
        return None
    return receipt


def latest_anchor() -> dict[str, Any] | None:
    path = _store_path()
    if not path.exists():
        return None
    try:
        lines = path.read_text(encoding="utf-8").strip().splitlines()
    except Exception:
        return None
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


def list_anchors(*, limit: int = 20) -> list[dict[str, Any]]:
    path = _store_path()
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").strip().splitlines()
    except Exception:
        return []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
        if len(out) >= limit:
            break
    return out
