"""PK-sink LSN-guarded idempotent upsert under at-least-once CDC delivery.

Honesty
-------
Log delivery remains **at-least-once**. When destinations upsert on a primary
key and stamp ``_df_lsn`` from the resume token, redelivery of an *older*
token must not regress row state. That is **LSN-guarded idempotency for PK
sink state** — not exactly-once end-to-end delivery, and not claimed for
append-only sinks or destinations without the LSN guard.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from connectors.writer_common import DF_LSN_COL, compare_lsn, lsn_family

# Public posture for Theater / mapping proof / docs.
DELIVERY_DEFAULT = "at-least-once"
EFFECTIVELY_ONCE_PK_SINKS = True  # only when _df_lsn guard is applied
APPEND_ONLY_SINKS_EFFECTIVELY_ONCE = False
EXACTLY_ONCE_CLAIMED = False

SINK_EFFECTIVELY_ONCE_ELIGIBLE = "effectively_once_eligible"
SINK_APPEND_ONLY = "append_only_at_least_once"


class CdcAppendOnlySinkError(ValueError):
    """CDC destination cannot provide PK+_df_lsn idempotent upsert semantics."""


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

    Aligned with ``filter_stale_lsn_rows`` / SQL ``>`` guards (strict-newer):

    - Missing incoming LSN → apply (legacy / non-CDC paths; at-least-once).
    - Missing existing LSN → apply (first write).
    - Incoming strictly newer → apply.
    - Incoming equal → skip (idempotent redelivery; do not rewrite payload).
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
    if cmp > 0:
        return EffectivelyOnceResult(
            applied=True,
            reason="newer_lsn",
            prior_lsn=str(existing_lsn),
            incoming_lsn=str(incoming_lsn),
        )
    if cmp == 0:
        # Cross-family incomparable stamps also return 0 — refuse invent overwrite.
        if lsn_family(incoming_lsn) != lsn_family(existing_lsn):
            return EffectivelyOnceResult(
                applied=False,
                reason="incomparable_lsn_family",
                prior_lsn=str(existing_lsn),
                incoming_lsn=str(incoming_lsn),
            )
        return EffectivelyOnceResult(
            applied=False,
            reason="equal_lsn_skipped",
            prior_lsn=str(existing_lsn),
            incoming_lsn=str(incoming_lsn),
        )
    return EffectivelyOnceResult(
        applied=False,
        reason="stale_lsn_rejected",
        prior_lsn=str(existing_lsn),
        incoming_lsn=str(incoming_lsn),
    )


def should_apply_pk_delete(
    *,
    existing_lsn: Any,
    incoming_lsn: Any,
) -> EffectivelyOnceResult:
    """Return whether a CDC DELETE may remove an existing PK row.

    Under at-least-once redelivery a stale DELETE must not wipe a row that was
    recreated/updated at a newer ``_df_lsn``. Apply when:

    - No incoming LSN (legacy path) → apply
    - No existing LSN → apply
    - Incoming >= existing → apply (equal = idempotent redelivery of same delete)
    - Incoming older → skip
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
    if cmp > 0:
        return EffectivelyOnceResult(
            applied=True,
            reason="delete_lsn_ok",
            prior_lsn=str(existing_lsn),
            incoming_lsn=str(incoming_lsn),
        )
    if cmp == 0:
        if lsn_family(incoming_lsn) != lsn_family(existing_lsn):
            return EffectivelyOnceResult(
                applied=False,
                reason="incomparable_lsn_family",
                prior_lsn=str(existing_lsn),
                incoming_lsn=str(incoming_lsn),
            )
        return EffectivelyOnceResult(
            applied=True,
            reason="equal_lsn_delete",
            prior_lsn=str(existing_lsn),
            incoming_lsn=str(incoming_lsn),
        )
    return EffectivelyOnceResult(
        applied=False,
        reason="stale_delete_rejected",
        prior_lsn=str(existing_lsn),
        incoming_lsn=str(incoming_lsn),
    )


@dataclass
class PkSinkState:
    """In-memory PK sink used for chaos proofs (mirrors upsert+_df_lsn guard)."""

    rows: dict[str, dict[str, Any]] = field(default_factory=dict)
    rejected_stale: int = 0
    applied_count: int = 0
    deleted_count: int = 0
    rejected_stale_deletes: int = 0

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

    def delete(self, pk: str, *, incoming_lsn: Any = None) -> EffectivelyOnceResult:
        existing = self.rows.get(pk)
        prior = existing.get(DF_LSN_COL) if existing else None
        decision = should_apply_pk_delete(existing_lsn=prior, incoming_lsn=incoming_lsn)
        if not decision.applied:
            self.rejected_stale_deletes += 1
            return decision
        if existing is not None:
            del self.rows[pk]
            self.deleted_count += 1
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


def filter_keys_for_lsn_delete(
    keys: list[str],
    existing_lsn_by_pk: dict[str, Any],
    incoming_lsn: Any,
) -> list[str]:
    """Keep only PK keys whose DELETE is safe under ``incoming_lsn``."""
    out: list[str] = []
    for key in keys:
        prior = existing_lsn_by_pk.get(key)
        if should_apply_pk_delete(existing_lsn=prior, incoming_lsn=incoming_lsn).applied:
            out.append(key)
    return out


def chaos_stale_delete_after_recreate(pk: str = "1") -> PkSinkState:
    """DELETE@100 must not wipe a row recreated at LSN 200."""
    sink = PkSinkState()
    sink.upsert(pk, {"id": pk, "v": "v1", DF_LSN_COL: "0/50"})
    sink.delete(pk, incoming_lsn="0/100")
    sink.upsert(pk, {"id": pk, "v": "recreated", DF_LSN_COL: "0/200"})
    # Stale delete redelivery (at-least-once).
    sink.delete(pk, incoming_lsn="0/100")
    return sink


# Engines where writers apply filter_stale_lsn_rows / MERGE `_df_lsn` guards.
_LSN_GUARD_ENGINES = frozenset({
    "postgresql",
    "postgres",
    "redshift",  # delete+insert path honors compare_lsn when `_df_lsn` present
    "mysql",
    "mariadb",
    "snowflake",
    "bigquery",
    "sqlserver",
    "mssql",
    "azure_sql",
    "oracle",
    "oracle_db",
    "sqlite",
    "mongodb",
    "mongo",
    "iceberg",
    # SQLAlchemy path — sparse CDC upsert + `_df_lsn` via writer_common.
    "generic_sql",
})


def classify_sink_delivery(
    *,
    dest_type: str,
    has_primary_key: bool,
    write_mode: str = "upsert",
    has_lsn_column: bool | None = None,
) -> dict[str, Any]:
    """Classify CDC sink delivery guard posture (not platform exactly-once).

    Eligibility requires PK upsert **and** a live ``_df_lsn`` guard path.
    Upsert alone is not enough (Redshift delete+insert without LSN, etc.).
    """
    from services.connector_capability_registry import get_connector_capability

    dest = (dest_type or "").strip().lower()
    caps = get_connector_capability(dest)
    mode = (write_mode or "insert").strip().lower()
    # SQL Server/Oracle MERGE (NULL-safe ON) is the upsert path when wired;
    # treat supports_merge as upsert-capable. Still at-least-once until proven.
    upsert_capable = bool(caps.get("supports_upsert") or caps.get("supports_merge"))
    upsert_mode = mode in {"upsert", "merge"}
    caps_lsn = caps.get("supports_lsn_guard")
    if caps_lsn is None:
        caps_lsn = dest in _LSN_GUARD_ENGINES
    # Explicit False from caller means LSN was not stamped on this route.
    lsn_ok = True if has_lsn_column is None else bool(has_lsn_column)
    eligible = bool(
        has_primary_key and upsert_capable and upsert_mode and caps_lsn and lsn_ok
    )
    if eligible:
        return {
            "class": SINK_EFFECTIVELY_ONCE_ELIGIBLE,
            "exactly_once": False,
            "effectively_once_pk_sink": True,
            "duplicates_on_redelivery": False,
            "dest_type": dest,
            "supports_upsert": upsert_capable,
            "has_lsn_guard": True,
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
        "dest_type": dest,
        "supports_upsert": upsert_capable,
        "has_lsn_guard": bool(caps_lsn and lsn_ok),
        "notes": [
            "Append-only / non-upsert / missing _df_lsn sinks duplicate rows under at-least-once CDC.",
            "Refuse exactly-once / LSN-guarded idempotency claims for this route.",
        ],
    }


def gate_cdc_destination(
    *,
    dest_type: str,
    has_primary_key: bool,
    write_mode: str = "upsert",
    allow_append_only: bool = False,
    require_effectively_once: bool = False,
    has_lsn_column: bool | None = None,
) -> dict[str, Any]:
    """Fail-fast when CDC would write append-only without an explicit allow.

    Default: block CDC → non-upsert sinks so operators do not silently get
    duplicate rows on redelivery while thinking they have LSN-guarded idempotency.
    Pass ``allow_append_only=True`` to opt into honest at-least-once append.
    """
    posture = classify_sink_delivery(
        dest_type=dest_type,
        has_primary_key=has_primary_key,
        write_mode=write_mode,
        has_lsn_column=has_lsn_column,
    )
    if posture["class"] == SINK_EFFECTIVELY_ONCE_ELIGIBLE:
        return posture
    if require_effectively_once or not allow_append_only:
        raise CdcAppendOnlySinkError(
            "CDC destination "
            f"'{dest_type or 'unknown'}' is append-only (or missing PK/upsert). "
            "At-least-once redelivery will duplicate rows — not LSN-guarded idempotent upsert. "
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
