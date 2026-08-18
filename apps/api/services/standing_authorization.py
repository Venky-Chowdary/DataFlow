"""Delegated authority for unattended runs — Autopilot's kernel.

A scheduled run has no operator standing at the keyboard, so every gate that
needs a human signature refuses it. The honest fix is not to weaken the gate and
not to widen the retry budget: it is to let a named human sign *in advance*, and
to make that signature worthless the moment the thing they signed changes.

A **standing authorization** is that signature, persisted with the exact artifact
it covers:

* ``binding`` — a hash over the mapping contract, the source shape, and the
  execution policies the human was looking at when they signed.
* ``scopes`` — a closed set naming exactly what may proceed unattended.
* ``acknowledgments`` — the attestations they actually made (never more).
* ``expires_at`` — authority is always time-boxed.

At run time the runner recomputes the binding from live inputs. Equal hashes
replay the signature; anything else replays nothing and raises a finding for a
human. Unknown never becomes granted.

What a grant can never cover is deliberately not a matter of configuration:
``schema_drift.HARD_BREAKING_KINDS`` (narrowing, type change, NOT NULL
tightening, primary-key change, cursor removal) is rejected at grant time, so a
caller cannot ask for authority the product refuses to delegate.

This module is pure: it decides, it does not persist and it does not run.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from services.acknowledgment_contract import (
    MIN_ACTOR_LEN,
    MIN_REASON_LEN,
    Acknowledgments,
)
from services.schema_drift import HARD_BREAKING_KINDS, SOFT_NET_ADDITIVE_KINDS
from services.schema_fingerprint import fingerprint_mappings

# --- scopes -----------------------------------------------------------------

SCOPE_COMPLIANCE = "replay_compliance_ack"
SCOPE_SCHEMA_DRIFT = "replay_schema_drift_ack"
SCOPE_FK_RISK = "replay_fk_risk_ack"
SCOPE_NET_ADDITIVE_DRIFT = "net_additive_drift"

GRANTABLE_SCOPES: frozenset[str] = frozenset({
    SCOPE_COMPLIANCE,
    SCOPE_SCHEMA_DRIFT,
    SCOPE_FK_RISK,
    SCOPE_NET_ADDITIVE_DRIFT,
})

# A scope that replays an attestation is meaningless unless the human made it.
_SCOPE_REQUIRES_ACK: dict[str, str] = {
    SCOPE_COMPLIANCE: "compliance",
    SCOPE_SCHEMA_DRIFT: "schema_drift",
    SCOPE_FK_RISK: "fk_risk",
}

# Authority is time-boxed; an unbounded delegation is not a delegation.
MAX_GRANT_DAYS = 90
DEFAULT_GRANT_DAYS = 30

# Refusal codes carried on an approval request so the UI, the audit trail and
# Pilot all name the same finding.
CODE_BINDING_CHANGED = "AUTH_BINDING_CHANGED"
CODE_EXPIRED = "AUTH_EXPIRED"
CODE_REVOKED = "AUTH_REVOKED"
CODE_NO_AUTHORIZATION = "AUTH_ABSENT"
CODE_OUT_OF_SCOPE = "AUTH_OUT_OF_SCOPE"
CODE_NOT_DELEGABLE = "AUTH_NOT_DELEGABLE"
CODE_DETERMINISTIC_REFUSAL = "RUN_REFUSED_DETERMINISTIC"
CODE_EXHAUSTED = "AUTH_EXHAUSTED"

# Everything a signature is bound to. A grant is for one plan on one route in one
# workspace: change any of it and the signature stops applying, because the human
# signed for what they were shown and nothing else.
#
# The destination schema and its DDL identity are deliberately absent: a schedule
# does not persist them, and inventing a fingerprint the product cannot read would
# be a hash over a guess. The destination stays governed where it is actually
# observed — execution preflight, the reread and Gate-8 reconciliation — none of
# which any scope here can widen.
_BINDING_FIELDS = (
    "schedule_id",
    "workspace_id",
    "source_connector_id",
    "source_table",
    "dest_connector_id",
    "dest_table",
    "mapping_fingerprint",
    "source_schema_fingerprint",
    "source_selector",
    "stream_contract_fingerprint",
    "primary_key",
    "cursor_column",
    "sync_mode",
    "schema_policy",
    "validation_mode",
    "delivery_guarantee",
    "contract_id",
    "require_signed_contract",
    "backfill_new_fields",
)

_BINDING_LABELS = {
    "schedule_id": "the schedule",
    "workspace_id": "the workspace",
    "source_connector_id": "the source connector",
    "source_table": "the source table",
    "dest_connector_id": "the destination connector",
    "dest_table": "the destination table",
    "mapping_fingerprint": "the column mapping",
    "source_schema_fingerprint": "the source shape",
    "source_selector": "what the run reads (query, procedure or read mode)",
    "stream_contract_fingerprint": "the stream contracts",
    "primary_key": "the primary key",
    "cursor_column": "the cursor column",
    "sync_mode": "the sync mode",
    "schema_policy": "the schema-change policy",
    "validation_mode": "the validation mode",
    "delivery_guarantee": "the delivery guarantee",
    "contract_id": "the bound contract",
    "require_signed_contract": "the signed-contract requirement",
    "backfill_new_fields": "the backfill setting",
}


class AuthorizationRefused(ValueError):
    """Raised when a grant is requested that the product will not delegate."""


# Findings an operator attestation can actually clear, and the scope each needs.
# Everything absent from this table is resolved by changing the plan, not by
# signing for it: a mapping below the confidence floor, a lossy type path, an
# unsupported conversion, a missing primary key, a NOT NULL column with no
# source, rejected credentials. Offering to "approve" those would be a lie, so
# the default is deliberately no scope at all.
_DELEGABLE_FINDINGS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        r"source (schema|shape) (changed|drift)|schema drift|drift .*(refus|block)"
        r"|column .*(added|renamed|dropped)",
        (SCOPE_SCHEMA_DRIFT,),
    ),
    (
        r"\bpii\b|personally identifiable|compliance (gate|blocker|acknowledg)"
        r"|sensitive (column|data) ",
        (SCOPE_COMPLIANCE,),
    ),
    (
        r"foreign key|\bfk\b .*(risk|unvalidated)|orphan",
        (SCOPE_FK_RISK,),
    ),
)

# Never delegable, whatever the text also says. Mirrors
# ``schema_drift.HARD_BREAKING_KINDS`` — "always pause, never acknowledge-away".
_NEVER_DELEGABLE = (
    r"narrow(ing|ed)? type|type change|changed type|retyped|precision loss|truncat"
    r"|not null|nullab|primary key|cursor (removed|dropped)"
    r"|unsupported (type|semantic|sync mode|conversion)"
    r"|confidence below floor|below the map confidence floor"
    r"|fidelity collapse|lossy"
)


def scopes_for_drift_kinds(kinds: list[str] | tuple[str, ...] | set[str]) -> tuple[str, ...]:
    """Scopes that could cover a set of structured drift kinds — or nothing.

    Structured beats textual: when the drift evaluator already named the kinds it
    found, the decision is made on those and not on a phrase in a summary. One
    hard-breaking kind in the set disqualifies the whole finding, because the run
    would still be refused for it after any approval.
    """
    found = {str(k).strip().lower() for k in (kinds or []) if str(k).strip()}
    if not found or found & HARD_BREAKING_KINDS:
        return ()
    if not found <= SOFT_NET_ADDITIVE_KINDS:
        # A kind the drift model does not classify is treated as hard, exactly as
        # ``schema_drift`` treats it: unknown is not the same as safe.
        return ()
    return (SCOPE_NET_ADDITIVE_DRIFT, SCOPE_SCHEMA_DRIFT)


def delegable_scopes_for(text: str) -> tuple[str, ...]:
    """Which scopes, if any, would let an unattended run past this finding.

    Empty means an approval is the wrong instrument — the finding names a
    configuration change, and the inbox has to say so rather than offer a
    signature that would be refused again on the next beat.
    """
    haystack = str(text or "").strip().lower()
    if not haystack:
        return ()
    if re.search(_NEVER_DELEGABLE, haystack):
        return ()
    scopes: set[str] = set()
    for pattern, granted in _DELEGABLE_FINDINGS:
        if re.search(pattern, haystack):
            scopes.update(granted)
    return tuple(sorted(scopes))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse(ts: object) -> datetime | None:
    raw = str(ts or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


# --- binding ----------------------------------------------------------------


def compute_binding(
    *,
    mappings: list[dict[str, Any]] | None,
    schedule_id: str = "",
    workspace_id: str = "",
    source_connector_id: str = "",
    source_table: str = "",
    dest_connector_id: str = "",
    dest_table: str = "",
    source_schema_fingerprint: str = "",
    source_read_mode: str = "",
    source_query: str = "",
    procedure_call: str = "",
    procedure_params: dict[str, Any] | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    primary_key: str = "",
    cursor_column: str = "",
    sync_mode: str = "",
    schema_policy: str = "",
    validation_mode: str = "",
    delivery_guarantee: str = "",
    contract_id: str = "",
    require_signed_contract: bool = False,
    backfill_new_fields: bool = False,
) -> dict[str, Any]:
    """Describe the artifact an authorization is signed against.

    The mapping contract is hashed with the same function the plan store uses for
    a mapping revision, so a grant and a plan revision move together instead of
    disagreeing about what "the mapping" is.
    """
    binding: dict[str, Any] = {
        "schedule_id": str(schedule_id or "").strip(),
        "workspace_id": str(workspace_id or "").strip(),
        "source_connector_id": str(source_connector_id or "").strip(),
        "source_table": str(source_table or "").strip(),
        "dest_connector_id": str(dest_connector_id or "").strip(),
        "dest_table": str(dest_table or "").strip(),
        "mapping_fingerprint": fingerprint_mappings(list(mappings or [])),
        "source_schema_fingerprint": str(source_schema_fingerprint or "").strip(),
        # What the run actually reads. A rewritten query or a different procedure
        # is a different migration wearing the same schedule's name.
        "source_selector": _hash({
            "read_mode": str(source_read_mode or "").strip().lower(),
            "query": str(source_query or "").strip(),
            "procedure": str(procedure_call or "").strip(),
            "params": dict(procedure_params or {}),
        }),
        "stream_contract_fingerprint": _hash({"contracts": list(stream_contracts or [])}),
        "primary_key": str(primary_key or "").strip(),
        "cursor_column": str(cursor_column or "").strip(),
        "sync_mode": str(sync_mode or "").strip().lower(),
        "schema_policy": str(schema_policy or "").strip().lower(),
        "validation_mode": str(validation_mode or "").strip().lower(),
        "delivery_guarantee": str(delivery_guarantee or "").strip().lower(),
        "contract_id": str(contract_id or "").strip(),
        "require_signed_contract": bool(require_signed_contract),
        "backfill_new_fields": bool(backfill_new_fields),
    }
    binding["binding_hash"] = _hash({k: binding[k] for k in _BINDING_FIELDS})
    return binding


def binding_from_schedule(sched: Any) -> dict[str, Any]:
    """Binding for a ``PipelineSchedule`` as it stands right now."""
    return compute_binding(
        mappings=list(getattr(sched, "mappings", None) or []),
        schedule_id=getattr(sched, "id", "") or "",
        workspace_id=getattr(sched, "workspace_id", "") or "",
        source_connector_id=getattr(sched, "source_connector_id", "") or "",
        source_table=getattr(sched, "source_table", "") or "",
        dest_connector_id=getattr(sched, "dest_connector_id", "") or "",
        dest_table=getattr(sched, "dest_table", "") or "",
        source_schema_fingerprint=getattr(sched, "source_schema_fingerprint", "") or "",
        source_read_mode=getattr(sched, "source_read_mode", "") or "",
        source_query=getattr(sched, "source_query", "") or "",
        procedure_call=getattr(sched, "procedure_call", "") or "",
        procedure_params=dict(getattr(sched, "procedure_params", None) or {}),
        stream_contracts=list(getattr(sched, "stream_contracts", None) or []),
        primary_key=getattr(sched, "primary_key", "") or "",
        cursor_column=getattr(sched, "cursor_column", "") or "",
        sync_mode=getattr(sched, "sync_mode", "") or "",
        schema_policy=getattr(sched, "schema_policy", "") or "",
        validation_mode=getattr(sched, "validation_mode", "") or "",
        delivery_guarantee=getattr(sched, "delivery_guarantee", "") or "",
        contract_id=getattr(sched, "contract_id", "") or "",
        require_signed_contract=bool(getattr(sched, "require_signed_contract", False)),
        backfill_new_fields=bool(getattr(sched, "backfill_new_fields", False)),
    )


def binding_differences(
    approved: dict[str, Any] | None,
    current: dict[str, Any] | None,
) -> list[dict[str, str]]:
    """Field-level diff between a signed binding and the live one."""
    was = approved or {}
    now = current or {}
    out: list[dict[str, str]] = []
    for key in _BINDING_FIELDS:
        before = was.get(key)
        after = now.get(key)
        if before == after:
            continue
        out.append({
            "field": key,
            "label": _BINDING_LABELS.get(key, key),
            "approved": "" if before is None else str(before),
            "current": "" if after is None else str(after),
        })
    return out


# --- grant ------------------------------------------------------------------


@dataclass(frozen=True)
class StandingAuthorization:
    """A named human's advance signature for one schedule."""

    id: str = ""
    actor: str = ""
    reason: str = ""
    granted_at: str = ""
    expires_at: str = ""
    scopes: tuple[str, ...] = ()
    acknowledgments: dict[str, bool] = field(default_factory=dict)
    binding: dict[str, Any] = field(default_factory=dict)
    uses: int = 0
    #: 0 means unlimited within the expiry. 1 is an approve-once decision: the
    #: operator signed for this run, not for every run after it.
    max_uses: int = 0
    last_used_at: str = ""
    #: Source-shape advances this grant carried itself across, newest last. Only
    #: ``REBINDABLE_FIELDS`` can move this way; anything else needs a human.
    rebinds: tuple[dict[str, str], ...] = ()
    revoked_at: str = ""
    revoked_by: str = ""
    revoked_reason: str = ""

    @property
    def revoked(self) -> bool:
        return bool(self.revoked_at)

    def expired(self, *, now: datetime | None = None) -> bool:
        deadline = _parse(self.expires_at)
        if deadline is None:
            # Fail closed: an authorization with no readable expiry is not usable.
            return True
        return (now or _now()) >= deadline

    def has_scope(self, scope: str) -> bool:
        return scope in self.scopes

    @property
    def exhausted(self) -> bool:
        return self.max_uses > 0 and self.uses >= self.max_uses

    def signed(self) -> Acknowledgments:
        """The attestations this grant may replay, and nothing more."""
        acks = self.acknowledgments or {}
        return Acknowledgments(
            compliance=bool(acks.get("compliance")) and self.has_scope(SCOPE_COMPLIANCE),
            schema_drift=(
                bool(acks.get("schema_drift")) and self.has_scope(SCOPE_SCHEMA_DRIFT)
            ),
            fk_risk=bool(acks.get("fk_risk")) and self.has_scope(SCOPE_FK_RISK),
            actor=self.actor,
            reason=self.reason,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "actor": self.actor,
            "reason": self.reason,
            "granted_at": self.granted_at,
            "expires_at": self.expires_at,
            "scopes": list(self.scopes),
            "acknowledgments": dict(self.acknowledgments),
            "binding": dict(self.binding),
            "uses": int(self.uses),
            "max_uses": int(self.max_uses),
            "last_used_at": self.last_used_at,
            "rebinds": [dict(r) for r in self.rebinds],
            "revoked_at": self.revoked_at,
            "revoked_by": self.revoked_by,
            "revoked_reason": self.revoked_reason,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> StandingAuthorization | None:
        if not data or not isinstance(data, dict):
            return None
        if not str(data.get("actor") or "").strip():
            # No named actor means no authority, whatever else the record holds.
            return None
        acks = data.get("acknowledgments") or {}
        scopes = tuple(
            s for s in (str(x) for x in (data.get("scopes") or [])) if s in GRANTABLE_SCOPES
        )
        return cls(
            id=str(data.get("id") or ""),
            actor=str(data.get("actor") or "").strip(),
            reason=str(data.get("reason") or "").strip(),
            granted_at=str(data.get("granted_at") or ""),
            expires_at=str(data.get("expires_at") or ""),
            scopes=scopes,
            acknowledgments={
                "compliance": bool(acks.get("compliance")),
                "schema_drift": bool(acks.get("schema_drift")),
                "fk_risk": bool(acks.get("fk_risk")),
            },
            binding=dict(data.get("binding") or {}),
            uses=int(data.get("uses") or 0),
            max_uses=int(data.get("max_uses") or 0),
            last_used_at=str(data.get("last_used_at") or ""),
            rebinds=tuple(
                {str(k): str(v) for k, v in dict(r).items()}
                for r in (data.get("rebinds") or [])
                if isinstance(r, dict)
            ),
            revoked_at=str(data.get("revoked_at") or ""),
            revoked_by=str(data.get("revoked_by") or ""),
            revoked_reason=str(data.get("revoked_reason") or ""),
        )


def grant_authorization(
    *,
    actor: str,
    reason: str,
    scopes: list[str] | tuple[str, ...],
    binding: dict[str, Any],
    acknowledgments: dict[str, bool] | None = None,
    expires_in_days: int = DEFAULT_GRANT_DAYS,
    max_uses: int = 0,
    now: datetime | None = None,
) -> StandingAuthorization:
    """Mint a grant, or refuse to.

    ``max_uses=1`` expresses approve-once: the same machinery carries the
    operator's signature into exactly one unattended run, so a one-time decision
    needs no second, weaker code path.

    Refuses an unnamed actor, an unexplained delegation, an unknown or
    non-delegable scope, an attestation replay with nothing signed behind it, and
    an unbounded or absurd expiry.
    """
    actor = str(actor or "").strip()
    reason = str(reason or "").strip()
    if len(actor) < MIN_ACTOR_LEN:
        raise AuthorizationRefused(
            "A standing authorization needs the name of the person granting it."
        )
    if len(reason) < MIN_REASON_LEN:
        raise AuthorizationRefused(
            "A standing authorization needs a reason of at least "
            f"{MIN_REASON_LEN} characters — it is an audit record, not a checkbox."
        )

    requested = [str(s).strip() for s in (scopes or []) if str(s).strip()]
    if not requested:
        raise AuthorizationRefused(
            "Name at least one scope — a grant that authorizes nothing is not a grant."
        )
    unknown = [s for s in requested if s not in GRANTABLE_SCOPES]
    if unknown:
        raise AuthorizationRefused(
            f"{CODE_NOT_DELEGABLE}: {', '.join(sorted(set(unknown)))} cannot be "
            "delegated to an unattended run. Narrowing, type changes, NOT NULL "
            "tightening, primary-key and cursor changes always pause for a human."
        )

    acks = {
        "compliance": bool((acknowledgments or {}).get("compliance")),
        "schema_drift": bool((acknowledgments or {}).get("schema_drift")),
        "fk_risk": bool((acknowledgments or {}).get("fk_risk")),
    }
    for scope, ack_key in _SCOPE_REQUIRES_ACK.items():
        if scope in requested and not acks[ack_key]:
            raise AuthorizationRefused(
                f"{scope} replays an attestation that was never made — accept the "
                f"{ack_key.replace('_', ' ')} risk explicitly, or drop the scope."
            )

    days = int(expires_in_days or 0)
    if days <= 0 or days > MAX_GRANT_DAYS:
        raise AuthorizationRefused(
            f"Expiry must be between 1 and {MAX_GRANT_DAYS} days — unattended "
            "authority is always time-boxed."
        )

    if not str((binding or {}).get("binding_hash") or "").strip():
        raise AuthorizationRefused(
            "A grant must be bound to a mapping and source shape — refusing to "
            "authorize an unidentified plan."
        )

    issued = now or _now()
    bound = dict(binding)
    return StandingAuthorization(
        id=_hash({
            "actor": actor,
            "binding": bound.get("binding_hash"),
            "granted_at": _iso(issued),
            "scopes": sorted(set(requested)),
        })[:16],
        actor=actor,
        reason=reason,
        granted_at=_iso(issued),
        expires_at=_iso(issued + timedelta(days=days)),
        scopes=tuple(sorted(set(requested))),
        acknowledgments=acks,
        binding=bound,
        max_uses=max(0, int(max_uses or 0)),
    )


def revoke_authorization(
    grant: StandingAuthorization,
    *,
    actor: str,
    reason: str = "",
    now: datetime | None = None,
) -> StandingAuthorization:
    """Return the same grant, permanently unusable, with the revocation recorded."""
    return replace(
        grant,
        revoked_at=_iso(now or _now()),
        revoked_by=str(actor or "").strip(),
        revoked_reason=str(reason or "").strip(),
    )


# --- evaluation -------------------------------------------------------------


@dataclass(frozen=True)
class AuthorizationDecision:
    """What a grant permits for one specific run, and why."""

    applies: bool
    acknowledgments: Acknowledgments = field(default_factory=Acknowledgments)
    scopes: tuple[str, ...] = ()
    code: str = ""
    reason: str = ""
    corrective_action: str = ""
    differences: tuple[dict[str, str], ...] = ()
    grant_id: str = ""

    @property
    def has_grant(self) -> bool:
        return self.code != CODE_NO_AUTHORIZATION

    def allows(self, scope: str) -> bool:
        return self.applies and scope in self.scopes

    def to_dict(self) -> dict[str, Any]:
        return {
            "applies": self.applies,
            "scopes": list(self.scopes),
            "code": self.code,
            "reason": self.reason,
            "corrective_action": self.corrective_action,
            "differences": [dict(d) for d in self.differences],
            "grant_id": self.grant_id,
            "actor": self.acknowledgments.actor,
            "compliance_acknowledged": self.acknowledgments.compliance,
            "schema_drift_acknowledged": self.acknowledgments.schema_drift,
            "fk_risk_acknowledged": self.acknowledgments.fk_risk,
        }


def evaluate_authorization(
    grant: StandingAuthorization | dict[str, Any] | None,
    *,
    binding_now: dict[str, Any],
    now: datetime | None = None,
) -> AuthorizationDecision:
    """Decide whether a grant covers the run that is about to happen.

    Fails closed on every path: absent, revoked, expired, unreadable, or bound to
    an artifact that has since moved.
    """
    resolved = (
        grant
        if isinstance(grant, StandingAuthorization)
        else StandingAuthorization.from_dict(grant)
    )
    if resolved is None:
        return AuthorizationDecision(
            applies=False,
            code=CODE_NO_AUTHORIZATION,
            reason="No standing authorization for this schedule.",
            corrective_action=(
                "Approve the finding once, or grant a scoped standing "
                "authorization so future runs can proceed unattended."
            ),
        )
    if resolved.revoked:
        return AuthorizationDecision(
            applies=False,
            code=CODE_REVOKED,
            grant_id=resolved.id,
            reason=(
                f"The standing authorization was revoked by "
                f"{resolved.revoked_by or 'an administrator'}."
            ),
            corrective_action="Grant a new authorization, or approve each run.",
        )
    if resolved.exhausted:
        return AuthorizationDecision(
            applies=False,
            code=CODE_EXHAUSTED,
            grant_id=resolved.id,
            reason=(
                f"{resolved.actor} approved {resolved.max_uses} run(s), and that "
                "authority has been used."
            ),
            corrective_action=(
                "Approve this run too, or grant a scoped standing authorization."
            ),
        )
    if resolved.expired(now=now):
        return AuthorizationDecision(
            applies=False,
            code=CODE_EXPIRED,
            grant_id=resolved.id,
            reason=(
                f"The standing authorization expired on {resolved.expires_at}."
                if resolved.expires_at
                else "The standing authorization has no readable expiry."
            ),
            corrective_action="Re-grant the authorization with a new expiry.",
        )

    differences = binding_differences(resolved.binding, binding_now)
    if differences:
        moved = ", ".join(d["label"] for d in differences)
        return AuthorizationDecision(
            applies=False,
            code=CODE_BINDING_CHANGED,
            grant_id=resolved.id,
            differences=tuple(differences),
            reason=(
                f"{resolved.actor} authorized a different plan — {moved} changed "
                "since the authorization was granted."
            ),
            corrective_action=(
                "Review what changed and approve it: the previous signature "
                "deliberately does not carry over to a plan nobody looked at."
            ),
        )

    return AuthorizationDecision(
        applies=True,
        acknowledgments=resolved.signed(),
        scopes=resolved.scopes,
        grant_id=resolved.id,
        reason=(
            f"Standing authorization {resolved.id} granted by {resolved.actor}, "
            f"valid until {resolved.expires_at}."
        ),
    )


#: The only binding field a grant may re-bind to by itself, and only when the
#: change it moved for is one the granter authorized. Everything else needs a
#: human, because nobody signed for it.
REBINDABLE_FIELDS: frozenset[str] = frozenset({"source_schema_fingerprint"})


def rebind_authorization(
    grant: StandingAuthorization,
    *,
    binding_now: dict[str, Any],
    now: datetime | None = None,
) -> StandingAuthorization:
    """Carry a grant forward across a change it explicitly authorized.

    When a run proceeds on ``net_additive_drift`` the source baseline advances,
    which would otherwise invalidate the very grant that permitted it — the
    authorization would be single-use by accident. Re-binding is therefore
    allowed for the source fingerprint alone, keeps the original actor, reason,
    scopes and expiry, and records what it moved from so the trail still shows
    the shape that was signed for.

    Raises ``AuthorizationRefused`` if anything else moved: that is a plan nobody
    approved.
    """
    moved = {d["field"] for d in binding_differences(grant.binding, binding_now)}
    if not moved:
        return grant
    if not moved <= REBINDABLE_FIELDS:
        raise AuthorizationRefused(
            f"{CODE_BINDING_CHANGED}: cannot re-bind "
            f"{', '.join(sorted(moved - REBINDABLE_FIELDS))} without a human."
        )
    if not grant.has_scope(SCOPE_NET_ADDITIVE_DRIFT):
        raise AuthorizationRefused(
            f"{CODE_OUT_OF_SCOPE}: this authorization does not cover a source "
            "shape change."
        )
    history = list(grant.rebinds) + [{
        "at": _iso(now or _now()),
        "from": str(grant.binding.get("source_schema_fingerprint") or ""),
        "to": str(binding_now.get("source_schema_fingerprint") or ""),
    }]
    return replace(
        grant,
        binding=dict(binding_now),
        rebinds=tuple(history[-20:]),
    )


def record_use(
    grant: StandingAuthorization,
    *,
    now: datetime | None = None,
) -> StandingAuthorization:
    """Count one unattended use — how often authority was exercised is evidence."""
    return replace(grant, uses=int(grant.uses) + 1, last_used_at=_iso(now or _now()))


def audit_authorization_use(
    decision: AuthorizationDecision,
    *,
    schedule_id: str,
    details: dict[str, Any] | None = None,
) -> None:
    """Audit an unattended replay exactly as visibly as a click at Validate."""
    if not decision.applies or not decision.acknowledgments.any_claimed:
        return
    from services.acknowledgment_contract import audit_acknowledgments

    audit_acknowledgments(
        decision.acknowledgments,
        resource=f"schedule:{schedule_id}",
        details={
            **(details or {}),
            "unattended": True,
            "authorization_id": decision.grant_id,
            "authorization_scopes": list(decision.scopes),
        },
    )
