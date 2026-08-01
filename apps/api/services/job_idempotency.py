"""Stop an equivalent transfer from running twice concurrently.

A double-clicked Run button, a retried HTTP request, or two schedule ticks that
overlap all produce the same failure: two workers reading the same source and
writing the same destination table at the same time. In insert mode that doubles
the rows; in upsert mode the writers race on the same keys and the destination
ends up with an interleaving neither run intended. Both jobs report success, so
nothing surfaces the problem until someone counts rows.

The guard is the standard idempotency-key pattern:

* A **fingerprint** is derived from the parts of a transfer request that decide
  what it will write — endpoints, table, sync mode, mappings, filter, limit.
  Cosmetic fields (labels, correlation ids, timestamps) are excluded so a resubmit
  of the same intent produces the same fingerprint.
* A **claim** on that fingerprint is inserted with the fingerprint as the primary
  key. The insert either succeeds (this caller owns the run) or hits a duplicate
  key (someone else already owns it), decided by the storage engine rather than
  by a read-then-write that has a race window between the two steps.
* The claim is **released** when the job reaches a terminal state, so running the
  same transfer again later — which is entirely legitimate — is never blocked.
* Claims carry a **TTL** so a worker that dies without releasing does not wedge
  the pipeline forever.

A client may also supply its own key (``Idempotency-Key`` header), which is what
API consumers use to make their own retries safe. An explicit key always wins
over the derived fingerprint.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_CLAIM_TTL_SECONDS",
    "JobClaim",
    "claim_key",
    "normalize_client_key",
    "request_fingerprint",
]

#: How long a claim survives without an explicit release. Long enough that a
#: slow-starting worker keeps its slot, short enough that a hard crash frees the
#: pipeline within one operator coffee break rather than needing manual cleanup.
DEFAULT_CLAIM_TTL_SECONDS = 21_600  # 6 hours

#: Fields on an endpoint that change what gets written. Credentials are hashed
#: rather than included verbatim so a claim key never carries a secret, but they
#: still participate: the same table on two different hosts is not the same run.
_ENDPOINT_IDENTITY_FIELDS: tuple[str, ...] = (
    "kind",
    "format",
    "connector_id",
    "host",
    "port",
    "database",
    "schema",
    "table",
    "collection",
    "warehouse",
    "path",
    "bucket",
    "index",
    "topic",
)


@dataclass(frozen=True)
class JobClaim:
    """Outcome of trying to claim the right to run a transfer."""

    key: str
    job_id: str
    acquired: bool
    existing_job_id: str = ""
    existing_status: str = ""

    @property
    def duplicate(self) -> bool:
        """Whether an equivalent transfer was already in flight."""
        return not self.acquired

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "job_id": self.job_id,
            "acquired": self.acquired,
            "duplicate": self.duplicate,
            "existing_job_id": self.existing_job_id,
            "existing_status": self.existing_status,
        }


def normalize_client_key(value: Any) -> str:
    """Clean a client-supplied idempotency key.

    Bounded to 200 characters because the key becomes a document id, and empty
    or whitespace-only values are treated as absent rather than as a key that
    every caller would collide on.
    """
    text = str(value or "").strip()
    return text[:200]


def request_fingerprint(request: Any) -> str:
    """Derive a stable fingerprint from what a transfer request will write.

    Two submissions produce the same fingerprint exactly when they would write
    the same rows to the same place. Fields that do not affect the outcome are
    deliberately excluded, so retrying a request whose only difference is a new
    correlation id is correctly recognised as the same run.

    File uploads fold in a digest of their content: the same filename with
    different bytes is a different transfer, and re-uploading identical bytes is
    the duplicate case we want to catch.
    """
    payload: dict[str, Any] = {
        "source": _endpoint_identity(getattr(request, "source", None)),
        "destination": _endpoint_identity(getattr(request, "destination", None)),
        "sync_mode": _text(getattr(request, "sync_mode", "")),
        "operation": _text(getattr(request, "operation", "")),
        "workspace_id": _text(getattr(request, "workspace_id", "")),
        "limit": _int(getattr(request, "limit", 0)),
        "priority_column": _text(getattr(request, "priority_column", "")),
        "priority_direction": _text(getattr(request, "priority_direction", "")),
        "source_filter": _canonical(getattr(request, "source_filter", None)),
        "mappings": _mapping_identity(getattr(request, "mappings", None)),
        "stream_contracts": _canonical(getattr(request, "stream_contracts", None)),
    }

    content = getattr(request, "source_content", None)
    if content:
        payload["source_content"] = hashlib.sha256(
            content if isinstance(content, bytes) else str(content).encode("utf-8")
        ).hexdigest()
        payload["source_filename"] = _text(getattr(request, "source_filename", ""))

    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def claim_key(*, workspace_id: str, key: str) -> str:
    """Namespace a key to its workspace.

    Without this, two tenants that happen to run structurally identical
    transfers — same table names against their own databases — would collide and
    one would be told its job was a duplicate of the other's.
    """
    return f"{_text(workspace_id) or 'default'}:{key}"


def claim_expiry(ttl_seconds: int | None = None) -> datetime:
    """When an unreleased claim becomes reclaimable."""
    ttl = ttl_seconds if ttl_seconds and ttl_seconds > 0 else DEFAULT_CLAIM_TTL_SECONDS
    return datetime.now(timezone.utc) + timedelta(seconds=ttl)


def _endpoint_identity(endpoint: Any) -> dict[str, Any]:
    if endpoint is None:
        return {}
    out: dict[str, Any] = {}
    for field in _ENDPOINT_IDENTITY_FIELDS:
        value = getattr(endpoint, field, None)
        if value in (None, "", 0):
            continue
        out[field] = _text(value)
    # Credentials are part of the identity but must never appear in a key that
    # gets logged or returned to a client, so only their digest participates.
    secret_material = "|".join(
        str(getattr(endpoint, f, "") or "")
        for f in ("username", "password", "connection_string")
    )
    if secret_material.strip("|"):
        out["credential_digest"] = hashlib.sha256(
            secret_material.encode("utf-8")
        ).hexdigest()[:16]
    return out


def _mapping_identity(mappings: Any) -> list[dict[str, Any]]:
    """Reduce mappings to the fields that change the written result.

    Sorted by target column so a reordered but otherwise identical mapping list
    is recognised as the same transfer.
    """
    if not mappings:
        return []
    rows: list[dict[str, Any]] = []
    try:
        for m in mappings:
            if not isinstance(m, dict):
                continue
            rows.append(
                {
                    "source": _text(m.get("source")),
                    "target": _text(m.get("target")),
                    "target_type": _text(m.get("target_type")),
                    "transform": _text(m.get("transform")),
                }
            )
    except TypeError:
        return []
    rows.sort(key=lambda r: (r["target"], r["source"]))
    return rows


def _canonical(value: Any) -> Any:
    """Make a nested structure order-stable for hashing."""
    if value is None:
        return None
    try:
        return json.loads(json.dumps(value, sort_keys=True, default=str))
    except (TypeError, ValueError):
        return str(value)


def _text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
