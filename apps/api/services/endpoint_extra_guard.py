"""A connector's ``extra`` adds driver options; it never picks the engine (security follow-up 6).

``adapters.resolve_connector_config`` flattens an endpoint's ``extra`` into the
connector config after setting ``cfg["type"]`` from the endpoint, and every
safety check downstream (the pre-dial egress check, the local-disk and
residency rules, the reader and writer dispatch) keys on ``cfg["type"]``. A
``type`` (or ``db_type``) in ``extra`` therefore chose another engine's rule:
``{"format": "postgresql", "extra": {"type": "s3"}}`` let the egress check skip
an AWS region label in ``host`` that the PostgreSQL driver then dialled
(review round 2, L-E3, on the transfer path; the probe route already refused it).

The endpoint's own type wins. An ``extra`` type naming another engine is
dropped and recorded as a ``connector.extra_type_ignored`` security event,
once per endpoint and spelling per hour (the resolution runs several times per
transfer).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Hashable
from typing import Any

#: ``cfg`` keys that name the engine; only the endpoint (or its saved connector) sets them.
ENGINE_TYPE_KEYS = frozenset({"type", "db_type"})

_TTL_SEC = 3600.0
_MAX_KEYS = 1024
_SEEN: dict[Hashable, float] = {}
_LOCK = threading.Lock()


def _driver(value: Any) -> str:
    from src.transfer.connector_capabilities import resolve_driver_type

    return str(resolve_driver_type(str(value or "").strip()) or "").strip().lower()


def _first_report(key: Hashable) -> bool:
    now = time.monotonic()
    with _LOCK:
        last = _SEEN.get(key)
        if last is not None and now - last < _TTL_SEC:
            return False
        if len(_SEEN) >= _MAX_KEYS:
            _SEEN.clear()
        _SEEN[key] = now
    return True


def engine_extra(endpoint: Any, workspace_id: str | None = None) -> dict[str, Any]:
    """``endpoint.extra`` without the keys that name an engine; a mismatch is audited."""
    extra = dict(getattr(endpoint, "extra", None) or {})
    named = {key: extra.pop(key) for key in ENGINE_TYPE_KEYS if key in extra}
    endpoint_type = str(getattr(endpoint, "format", "") or "").strip()
    real = _driver(endpoint_type)
    spoofed = {
        key: str(value) for key, value in named.items()
        if str(value or "").strip() and _driver(value) != real
    }
    if spoofed:
        _record(endpoint, endpoint_type, spoofed, workspace_id)
    return extra


def _record(endpoint: Any, endpoint_type: str, spoofed: dict[str, str], workspace_id: str | None) -> None:
    connector_id = str(getattr(endpoint, "connector_id", "") or "")
    host = str(getattr(endpoint, "host", "") or "")
    if not _first_report((endpoint_type, tuple(sorted(spoofed.items())), connector_id, host)):
        return
    from services.audit_coverage import record_security_event

    record_security_event(
        "connector.extra_type_ignored",
        outcome="skipped",
        level="warn",
        resource=connector_id or endpoint_type,
        reason_code="extra_type_ignored",
        workspace_id=str(workspace_id or "") or None,
        details={
            "endpoint_type": endpoint_type,
            "extra_type": next(iter(spoofed.values())),
            "keys": sorted(spoofed),
            "connector_id": connector_id,
        },
    )


def reset_dedupe() -> None:
    """Forget what was already reported (tests)."""
    with _LOCK:
        _SEEN.clear()
