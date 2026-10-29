"""One audit event for "a rule that is switched off by default would have applied".

Behaviour that was not in production is gated behind a flag. While the flag is
off the call is allowed, and this records ``<area>.would_<verb>`` in the audit
log (and the operational log) so an owner can see what turning the flag on would
change before they do it. One event per ``dedupe_key`` per hour, so a long
transfer does not flood the hash-chained audit log; the write never raises into
the caller.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Hashable
from typing import Any

logger = logging.getLogger(__name__)

_TTL_SEC = 3600.0
_MAX_KEYS = 1024
_SEEN: dict[Hashable, float] = {}
_LOCK = threading.Lock()


def audit_would_apply(
    action: str,
    *,
    resource: str,
    dedupe_key: Hashable,
    details: dict[str, Any],
    flag: str,
    workspace_id: str = "",
) -> None:
    """Write ``action`` (e.g. ``egress.would_block``) once per ``dedupe_key`` per hour."""
    key = (action, dedupe_key)
    now = time.monotonic()
    with _LOCK:
        last = _SEEN.get(key)
        if last is not None and now - last < _TTL_SEC:
            return
        if len(_SEEN) >= _MAX_KEYS:
            _SEEN.clear()
        _SEEN[key] = now
    logger.warning("%s %s (allowed: %s is off)", action, details, flag)
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action=action,
            resource=resource,
            actor="system",
            level="warn",
            workspace_id=workspace_id or None,
            details={**details, "enforced": False, "flag": flag},
        )
    except Exception as exc:
        logger.debug("%s audit write failed: %s", action, type(exc).__name__)


def reset_dedupe() -> None:
    """Forget what was already reported (tests, and an admin who flipped a flag)."""
    with _LOCK:
        _SEEN.clear()
