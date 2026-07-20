"""Effectively-once row state for PK sinks under at-least-once CDC delivery.

Honesty
-------
Log delivery remains **at-least-once**. When destinations upsert on a primary
key and stamp ``_df_lsn`` from the resume token, redelivery of an *older*
token must not regress row state. That is **effectively once for PK sink
state** — not exactly-once end-to-end delivery, and not claimed for append-
only sinks or destinations without the LSN guard.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from connectors.writer_common import DF_LSN_COL, compare_lsn

# Public posture for Theater / mapping proof / docs.
DELIVERY_DEFAULT = "at-least-once"
EFFECTIVELY_ONCE_PK_SINKS = True  # only when _df_lsn guard is applied
APPEND_ONLY_SINKS_EFFECTIVELY_ONCE = False
EXACTLY_ONCE_CLAIMED = False

SINK_EFFECTIVELY_ONCE_ELIGIBLE = "effectively_once_eligible"
SINK_APPEND_ONLY = "append_only_at_least_once"


class CdcAppendOnlySinkError(ValueError):
    """CDC destination cannot provide PK+_df_lsn effectively-once semantics."""


@dataclass
class EffectivelyOnceResult:
    applied: bool
    reason: str
    prior_lsn: str | None = None
    incoming_lsn: str | None = None


def should_apply_pk_row(
    *,
    existing_lsn: Any,
    incoming_lsn: Any,
) -> EffectivelyOnceResult:
    """Return whether an upsert may overwrite an existing PK row.

    Rules
    -----
    - Missing incoming LSN → apply (legacy / non-CDC paths; at-least-once).
    - Missing existing LSN → apply (first write).
    - Incoming newer or equal → apply (equal = idempotent redelivery).
    - Incoming older → skip (prevents silent regression under redelivery).
    """
    if incoming_lsn is None or str(incoming_lsn).strip() == "":
        return EffectivelyOnceResult(
            applied=True,
            reason="no_incoming_lsn",
            prior_lsn=str(existing_lsn) if existing_lsn is not None else None,
            incoming_lsn=None,
        )
    if existing_lsn is None or str(existing_lsn).strip() == "":
        return EffectivelyOnceResult(
            applied=True,
            reason="no_existing_lsn",
            prior_lsn=None,
            incoming_lsn=str(incoming_lsn),
        )
    cmp = compare_lsn(incoming_lsn, existing_lsn)
    if cmp >= 0:
        return EffectivelyOnceResult(
            applied=True,
            reason="newer_or_equal",
            prior_lsn=str(existing_lsn),
            incoming_lsn=str(incoming_lsn),
        )
    return EffectivelyOnceResult(
        applied=False,
        reason="stale_lsn_rejected",
        prior_lsn=str(existing_lsn),
        incoming_lsn=str(incoming_lsn),
    )


@dataclass
class PkSinkState:
    """In-memory PK sink used for chaos proofs (mirrors upsert+_df_lsn guard)."""

    rows: dict[str, dict[str, Any]] = field(default_factory=dict)
    rejected_stale: int = 0
    applied_count: int = 0

    def upsert(self, pk: str, row: dict[str, Any]) -> EffectivelyOnceResult:
        existing = self.rows.get(pk)
        prior = existing.get(DF_LSN_COL) if existing else None
        incoming = row.get(DF_LSN_COL)
        decision = should_apply_pk_row(existing_lsn=prior, incoming_lsn=incoming)
        if decision.applied:
            self.rows[pk] = dict(row)
            self.applied_count += 1
        else:
            self.rejected_stale += 1
        return decision


def chaos_redeliver_older_then_newer(pk: str = "1") -> PkSinkState:
    """Canonical chaos: apply new LSN, redeliver older, then equal — state holds."""
    sink = PkSinkState()
    sink.upsert(pk, {"id": pk, "v": "first", DF_LSN_COL: "0/100"})
    sink.upsert(pk, {"id": pk, "v": "new", DF_LSN_COL: "0/200"})
    # Redeliver stale (peek without ack / at-least-once).
    sink.upsert(pk, {"id": pk, "v": "stale", DF_LSN_COL: "0/100"})
    # Idempotent equal LSN redelivery.
    sink.upsert(pk, {"id": pk, "v": "new-again", DF_LSN_COL: "0/200"})
    return sink


def classify_sink_delivery(
    *,
    dest_type: str,
    has_primary_key: bool,
    write_mode: str = "upsert",
) -> dict[str, Any]:
    """Classify CDC sink delivery guard posture (not platform exactly-once)."""
    from services.connector_capability_registry import get_connector_capability

    caps = get_connector_capability(dest_type or "")
    mode = (write_mode or "insert").strip().lower()
    upsert_capable = bool(caps.get("supports_upsert"))
    upsert_mode = mode in {"upsert", "merge"}
    eligible = bool(has_primary_key and upsert_capable and upsert_mode)
    if eligible:
        return {
            "class": SINK_EFFECTIVELY_ONCE_ELIGIBLE,
            "exactly_once": False,
            "effectively_once_pk_sink": True,
            "duplicates_on_redelivery": False,
            "dest_type": (dest_type or "").lower(),
            "supports_upsert": upsert_capable,
            "notes": [
                "PK upsert + _df_lsn can reject stale redelivery (row state).",
                "Log capture remains at-least-once; not exactly-once delivery.",
            ],
        }
    return {
        "class": SINK_APPEND_ONLY,
        "exactly_once": False,
        "effectively_once_pk_sink": False,
        "duplicates_on_redelivery": True,
        "dest_type": (dest_type or "").lower(),
        "supports_upsert": upsert_capable,
        "notes": [
            "Append-only / non-upsert sinks duplicate rows under at-least-once CDC.",
            "Refuse exactly-once / effectively-once claims for this route.",
        ],
    }


def gate_cdc_destination(
    *,
    dest_type: str,
    has_primary_key: bool,
    write_mode: str = "upsert",
    allow_append_only: bool = False,
    require_effectively_once: bool = False,
) -> dict[str, Any]:
    """Fail-fast when CDC would write append-only without an explicit allow.

    Default: block CDC → non-upsert sinks so operators do not silently get
    duplicate rows on redelivery while thinking they have effectively-once.
    Pass ``allow_append_only=True`` to opt into honest at-least-once append.
    """
    posture = classify_sink_delivery(
        dest_type=dest_type,
        has_primary_key=has_primary_key,
        write_mode=write_mode,
    )
    if posture["class"] == SINK_EFFECTIVELY_ONCE_ELIGIBLE:
        return posture
    if require_effectively_once or not allow_append_only:
        raise CdcAppendOnlySinkError(
            "CDC destination "
            f"'{dest_type or 'unknown'}' is append-only (or missing PK/upsert). "
            "At-least-once redelivery will duplicate rows — not effectively-once. "
            "Use a PK upsert sink, or set allow_append_only=true to acknowledge "
            "duplicate risk."
        )
    return posture


def honesty_dict() -> dict[str, Any]:
    return {
        "delivery_default": DELIVERY_DEFAULT,
        "exactly_once_claimed": EXACTLY_ONCE_CLAIMED,
        "effectively_once_pk_sinks": EFFECTIVELY_ONCE_PK_SINKS,
        "append_only_sinks_effectively_once": APPEND_ONLY_SINKS_EFFECTIVELY_ONCE,
        "requires": ["primary_key", DF_LSN_COL, "upsert_destination"],
        "notes": [
            "Log capture remains at-least-once (peek→apply→ack).",
            "PK sinks with _df_lsn reject older tokens so row state does not regress.",
            "Append-only sinks are gated unless allow_append_only is set.",
            "Do not claim exactly-once pipeline delivery.",
        ],
    }
