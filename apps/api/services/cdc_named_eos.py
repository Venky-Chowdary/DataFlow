"""Named dest-owned exactly-once route — MySQL CDC → PostgreSQL.

Honesty
-------
``PLATFORM_EXACTLY_ONCE_CLAIMED`` stays False. This module names **one**
route whose dest-owned watermark protocol is eligible and whose crash-replay
artifact is the proof, not a platform-wide exactly-once claim.

Debezium remains at-least-once by design. Estuary materializes with a dest
fence. We do dest-owned apply + watermark in one PostgreSQL transaction and
prove crash-before-commit rolls back, crash-after-commit replay is a no-op.
"""

from __future__ import annotations

from typing import Any

from services.cdc_exactly_once import (
    ALGORITHM,
    PLATFORM_EXACTLY_ONCE_CLAIMED,
    PROTOCOL,
    classify_exactly_once_route,
)

# One named route. Not every MySQL destination. Not every Postgres source.
NAMED_EOS_ROUTE_ID = "mysql_cdc_postgresql_dest_owned"
NAMED_EOS_SOURCE = "mysql"
NAMED_EOS_DEST = "postgresql"
NAMED_EOS_SYNC = "cdc"
NAMED_EOS_LSN_FAMILY = "mysql_binlog"


def _norm(value: str) -> str:
    raw = (value or "").strip().lower().replace("-", "_")
    if raw in {"postgres", "pg"}:
        return "postgresql"
    if raw in {"mariadb"}:
        return "mysql"
    return raw


def is_named_dest_owned_eos_route(
    *,
    source_type: str,
    dest_type: str,
    sync_mode: str = "cdc",
) -> bool:
    """True only for MySQL CDC → PostgreSQL (the named dest-owned route)."""
    src = _norm(source_type)
    dest = _norm(dest_type)
    mode = _norm(sync_mode) or NAMED_EOS_SYNC
    if mode in {"cdc_incremental"}:
        mode = "cdc"
    return src == NAMED_EOS_SOURCE and dest == NAMED_EOS_DEST and mode == NAMED_EOS_SYNC


def named_eos_eligibility(
    *,
    source_type: str = NAMED_EOS_SOURCE,
    dest_type: str = NAMED_EOS_DEST,
    sync_mode: str = NAMED_EOS_SYNC,
    has_primary_key: bool = True,
) -> dict[str, Any]:
    """Classify the named route. Platform claim stays False even when eligible."""
    named = is_named_dest_owned_eos_route(
        source_type=source_type, dest_type=dest_type, sync_mode=sync_mode
    )
    eligibility = classify_exactly_once_route(
        dest_type=dest_type,
        sync_mode=sync_mode,
        has_primary_key=has_primary_key,
        source_type=source_type,
    )
    blob = eligibility.to_dict()
    blob.update(
        {
            "route_id": NAMED_EOS_ROUTE_ID if named else "",
            "named_route": named,
            "source_type": _norm(source_type),
            "lsn_family": NAMED_EOS_LSN_FAMILY if named else "",
            "algorithm": ALGORITHM,
            "protocol": PROTOCOL,
            "platform_exactly_once_claimed": PLATFORM_EXACTLY_ONCE_CLAIMED,
            "exactly_once_active": bool(named and eligibility.eligible),
        }
    )
    return blob


def crash_replay_artifact_template() -> dict[str, Any]:
    """Empty measured artifact. Never pre-fills green."""
    return {
        "route_id": NAMED_EOS_ROUTE_ID,
        "source": NAMED_EOS_SOURCE,
        "destination": NAMED_EOS_DEST,
        "sync_mode": NAMED_EOS_SYNC,
        "algorithm": ALGORITHM,
        "protocol": PROTOCOL,
        "lsn_family": NAMED_EOS_LSN_FAMILY,
        "platform_exactly_once_claimed": PLATFORM_EXACTLY_ONCE_CLAIMED,
        "exactly_once_active": True,
        "honesty": (
            "Named dest-owned route only. PLATFORM_EXACTLY_ONCE_CLAIMED stays "
            "False. Dest COUNT is the destination's COUNT(*), not a writer ack."
        ),
        "scenarios": [],
        "measured": False,
        "skip_reason": "",
    }
