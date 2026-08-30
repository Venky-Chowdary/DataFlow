"""CDC resume-token classification for load-safe watermark / ack handling.

Log CDC watermarks (slot/LSN, binlog file:pos/GTID, Mongo resume tokens) must
never be overwritten by side-channel tokens from incremental snapshots or
mid-transaction holds. Doing so under load causes silent gaps or slot storms.
"""

from __future__ import annotations

import json
import re
from typing import Any

_PG_LSN_RE = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")


def unwrap_resume_token(token: Any) -> Any:
    """Undo ``json.dumps`` wrapping of an already-serialized JSON token.

    Oracle LogMiner and SQL Server native encode a JSON **string**. Persist
    used to ``json.dumps`` that string again, so the stored watermark became
    ``"{\\"kind\\":...}"``. Decode then saw a Python ``str``, reset SCN/LSN
    to 0, and re-snapshotted the live table — CDC deletes that had already
    committed never applied (silent destination leftover).
    """
    if token is None:
        return None
    cur: Any = token
    for _ in range(4):
        if isinstance(cur, (bytes, bytearray)):
            cur = cur.decode("utf-8", errors="replace")
        if not isinstance(cur, str):
            return cur
        text = cur.strip()
        if not text:
            return cur
        if not (text.startswith("{") or text.startswith("[") or text.startswith('"')):
            return cur
        try:
            nxt = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            return cur
        if nxt == cur:
            return cur
        cur = nxt
    return cur


def serialize_resume_token(token: Any, *, default: Any = None) -> str:
    """Persist a resume token once — never wrap an already-serialized JSON object."""
    unwrapped = unwrap_resume_token(token)
    if unwrapped is None:
        return ""
    if isinstance(unwrapped, (dict, list)):
        return json.dumps(
            unwrapped,
            default=default or str,
            separators=(",", ":"),
        )
    return str(unwrapped)


def looks_like_pg_lsn(value: Any) -> bool:
    """True when ``value`` is a PostgreSQL ``hi/lo`` LSN string."""
    if value is None:
        return False
    return bool(_PG_LSN_RE.match(str(value).strip()))


def is_txn_held_token(token: Any) -> bool:
    return isinstance(token, dict) and bool(token.get("txn_held"))


def is_incremental_snapshot_token(token: Any) -> bool:
    if isinstance(token, dict):
        return bool(token.get("incremental_snapshot"))
    text = str(token or "")
    return "incremental_snapshot" in text


def is_side_channel_resume_token(token: Any) -> bool:
    """Tokens that must not become the durable log cursor or drive source ack."""
    if token is None:
        return False
    if is_txn_held_token(token) or is_incremental_snapshot_token(token):
        return True
    if isinstance(token, dict):
        nested = token.get("token")
        if nested is not None and (
            is_txn_held_token(nested) or is_incremental_snapshot_token(nested)
        ):
            return True
    return False


def is_durable_log_resume_token(token: Any) -> bool:
    """True when token is safe to persist as the stream watermark."""
    if token is None or is_side_channel_resume_token(token):
        return False
    if isinstance(token, dict):
        if token.get("file") and token.get("pos") is not None:
            return True
        if token.get("gtid") or token.get("gtid_set"):
            return True
        if token.get("lsn") or token.get("scn") or token.get("_data"):
            return True
        if token.get("phase") in {"streaming", "snapshot"}:
            return True
        nested = token.get("token")
        if nested is not None:
            return is_durable_log_resume_token(nested)
        # Opaque Mongo-style resume documents.
        return bool(token)
    text = str(token).strip()
    if not text:
        return False
    if text.startswith("slot=") or "lsn=" in text or "phase=" in text:
        return True
    # Opaque string watermarks (SQL Server / Oracle encodings, Mongo JSON).
    return True
