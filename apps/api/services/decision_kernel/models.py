"""Immutable Decision Artifact models — Migration Decision Kernel (Phase C1).

Map / Validate / Execute / Proof / UI must consume these structures. They do not
re-derive conversion class, DDL invent, or risk decisions independently.

Schema version: ``decision_artifact_v1``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping
from uuid import uuid4

from services.conversion_contract import ConversionClass

DECISION_ARTIFACT_SCHEMA = "decision_artifact_v1"


class RiskLevel(str, Enum):
    """Operator-facing risk band (aligns with MIGRATION_RISK_CONTRACT)."""

    SAFE = "safe"
    INFO = "info"
    REVIEW = "review"
    APPROVAL = "approval"
    BLOCKED = "blocked"


class AssignmentStrategy(str, Enum):
    """Honest labels for how a mapping was assigned (audit §4.1 Defect A)."""

    OPTIMAL_BIPARTITE_HUNGARIAN = "optimal_bipartite_hungarian"
    HUNGARIAN_WITH_GREEDY_PATCH = "hungarian_with_greedy_patch"
    OPERATOR_SUPPLIED = "operator_supplied"
    IDENTITY_PASSTHROUGH = "identity_passthrough"
    CREATE_NEW = "create_new"
    UNASSIGNED = "unassigned"


@dataclass(frozen=True, slots=True)
class CanonicalType:
    """Width-preserving canonical type stamp.

    ``logical`` is the family token (integer/float/decimal/…). Integer and float
    widths are first-class — never collapse BIGINT→INTEGER at this boundary.
    """

    logical: str
    native: str = ""
    bit_width: int | None = None  # 8/16/32/64 for integers; 32/64 for floats
    precision: int | None = None
    scale: int | None = None
    timezone_polarity: str | None = None  # ntz | tz | ltz
    nullable: bool | None = None
    extras: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _drop_empty(asdict(self))


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    """One column in the Decision Artifact schema strip."""

    name: str
    canonical: CanonicalType
    role: str | None = None  # id | metric | timestamp | …

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "canonical": self.canonical.to_dict(),
            **({"role": self.role} if self.role else {}),
        }


@dataclass(frozen=True, slots=True)
class ConversionDecision:
    """Per-column conversion classification (charter ConversionClass + risk)."""

    conversion_class: ConversionClass
    risk_level: RiskLevel
    lossy: bool
    recommended_action: str = ""
    risk_contract_id: str | None = None
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "conversion_class": self.conversion_class.value,
            "risk_level": self.risk_level.value,
            "lossy": self.lossy,
            "recommended_action": self.recommended_action,
            "risk_contract_id": self.risk_contract_id,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class MappingDecision:
    """Per-source assignment into a destination column (or omit/create-new)."""

    source: str
    target: str | None
    confidence: float
    assignment_strategy: AssignmentStrategy
    conversion: ConversionDecision
    create_new: bool = False
    omitted: bool = False
    alternatives: tuple[str, ...] = ()
    calibration_reason: str = ""
    #: Key / identity flag (PR #435). Emitted only when set, so an unkeyed
    #: row keeps its historical bytes; part of the content hash when set.
    is_primary_key: bool = False

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "source": self.source,
            "target": self.target,
            "confidence": self.confidence,
            "assignment_strategy": self.assignment_strategy.value,
            "conversion": self.conversion.to_dict(),
            "create_new": self.create_new,
            "omitted": self.omitted,
            "alternatives": list(self.alternatives),
            "calibration_reason": self.calibration_reason,
        }
        if self.is_primary_key:
            out["is_primary_key"] = True
        return out


@dataclass(frozen=True, slots=True)
class DdlPlan:
    """Materialized destination DDL stamps + identity fingerprint."""

    ddl_identity_hash: str
    column_ddl: Mapping[str, str] = field(default_factory=dict)
    dialect: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ddl_identity_hash": self.ddl_identity_hash,
            "column_ddl": dict(self.column_ddl),
            "dialect": self.dialect,
        }


@dataclass(frozen=True, slots=True)
class ProofPlan:
    """How Execute will prove the migration (checksum algorithm, sample limits)."""

    checksum_algorithm: str = "sha256"
    checksum_hex_chars: int = 64  # full digest — never truncate to 16 (audit §2.8)
    sample_limit_preflight: int = 500
    sample_is_population_proof: bool = False
    reconcile_mode: str = "full_population"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DecisionArtifact:
    """Immutable, versioned migration decision — single Execute authority."""

    artifact_id: str
    schema_version: str
    created_at: str
    tenant_id: str
    route_id: str
    source_fingerprint: str
    dest_fingerprint: str
    source_columns: tuple[ColumnSpec, ...]
    dest_columns: tuple[ColumnSpec, ...]
    mappings: tuple[MappingDecision, ...]
    ddl: DdlPlan
    proof: ProofPlan
    sync_mode: str = "full_refresh_overwrite"
    error_policy: str = "quarantine"
    capability_source_hash: str = ""
    capability_dest_hash: str = ""
    policy_signature: str = ""
    content_hash: str = ""
    #: Document model identity (services.document_model.model_hash) this
    #: decision was made for. Emitted only when set, so every artifact made
    #: without a model keeps its exact bytes and content_hash.
    document_model_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "artifact_id": self.artifact_id,
            "schema_version": self.schema_version,
            "created_at": self.created_at,
            "tenant_id": self.tenant_id,
            "route_id": self.route_id,
            "source_fingerprint": self.source_fingerprint,
            "dest_fingerprint": self.dest_fingerprint,
            "source_columns": [c.to_dict() for c in self.source_columns],
            "dest_columns": [c.to_dict() for c in self.dest_columns],
            "mappings": [m.to_dict() for m in self.mappings],
            "ddl": self.ddl.to_dict(),
            "proof": self.proof.to_dict(),
            "sync_mode": self.sync_mode,
            "error_policy": self.error_policy,
            "capability_source_hash": self.capability_source_hash,
            "capability_dest_hash": self.capability_dest_hash,
            "policy_signature": self.policy_signature,
            "content_hash": self.content_hash,
        }
        if self.document_model_hash:
            out["document_model_hash"] = self.document_model_hash
        return out

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True) + (
            "\n" if indent is not None else ""
        )


def _drop_empty(d: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        if v is None or v == "" or v == {}:
            continue
        out[k] = v
    return out


# One hash design (QA batch 1 + PR #435):
#
# * The content hash covers the structural decision: every field of the
#   artifact except its envelope (ids, clock, signature) and the per-mapping
#   scores. Scores are planner estimates, compared separately, so a rescore of
#   an unchanged Map is not drift.
# * Scores are dropped from mapping rows only. A name-keyed map such as
#   ``ddl.column_ddl`` keeps a column literally named ``confidence``,
#   ``score`` or ``created_at``.
# * Every stamp stored before this design still names its decision: the
#   feature/Cleanup digest (scores inside) and PR #435's interim digest
#   (scores and envelope stripped at every depth). They are read, never minted.
_ENVELOPE_KEYS = frozenset({
    "content_hash",
    "policy_signature",
    "created_at",
    "artifact_id",
})
_MAPPING_SCORE_KEYS = frozenset({
    "confidence",
    "calibration_reason",
    "score",
    "score_gap",
    "alternatives",
})
_KEY_FLAG_ALIASES = ("is_primary_key", "primary_key", "identity", "is_identity")
#: How the code grades a conversion, not what the operator planned: a new
#: release that reclassifies an unchanged pair must not refuse the stamp
#: (review Q1). ``risk_contract_id`` stays — the operator minted it.
_CLASSIFICATION_KEYS = frozenset({"reason", "risk_level", "recommended_action"})


def _digest(body: Any) -> str:
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _payload(artifact: DecisionArtifact | Mapping[str, Any]) -> dict[str, Any]:
    return artifact.to_dict() if isinstance(artifact, DecisionArtifact) else dict(artifact)


def _row_is_key(row: Mapping[str, Any]) -> bool:
    return any(bool(row.get(name)) for name in _KEY_FLAG_ALIASES)


def _historical_body(payload: Mapping[str, Any]) -> dict[str, Any]:
    """The body as stamped before #435: envelope off, no key flag on rows."""
    body = {k: v for k, v in payload.items() if k not in _ENVELOPE_KEYS}
    rows = body.get("mappings")
    if isinstance(rows, (list, tuple)):
        body["mappings"] = [
            {k: v for k, v in row.items() if k not in _KEY_FLAG_ALIASES}
            if isinstance(row, Mapping) else row
            for row in rows
        ]
    return body


def _structural_body(payload: Mapping[str, Any], *, classification: bool) -> dict[str, Any]:
    body = {k: v for k, v in payload.items() if k not in _ENVELOPE_KEYS}
    rows = body.get("mappings")
    if isinstance(rows, (list, tuple)):
        stripped: list[Any] = []
        for row in rows:
            if not isinstance(row, Mapping):
                stripped.append(row)
                continue
            kept = {
                str(k): v for k, v in row.items()
                if str(k) not in _MAPPING_SCORE_KEYS and str(k) not in _KEY_FLAG_ALIASES
            }
            conversion = kept.get("conversion")
            if not classification and isinstance(conversion, Mapping):
                kept["conversion"] = {
                    k: v for k, v in conversion.items() if k not in _CLASSIFICATION_KEYS
                }
            if _row_is_key(row):
                kept["is_primary_key"] = True
            stripped.append(kept)
        body["mappings"] = stripped
    return body


def compute_content_hash(payload: Mapping[str, Any]) -> str:
    """The structural decision hash.

    Excluded: the envelope, per-mapping scores, and how the code classifies a
    conversion (reason, risk level, recommended action).
    """
    return _digest(_structural_body(payload, classification=False))


def scored_structural_hash(payload: Mapping[str, Any]) -> str:
    """The structural digest before review Q1: classification still inside.

    Read-only compatibility for stamps minted by fix/qa-batch1 / PR #435.
    """
    return _digest(_structural_body(payload, classification=True))


def structural_decision_hash(artifact: DecisionArtifact | Mapping[str, Any]) -> str:
    """The content hash of ``artifact`` (one design; PR #435's name kept)."""
    return compute_content_hash(_payload(artifact))


def base_content_hash(payload: Mapping[str, Any]) -> str:
    """feature/Cleanup's digest: scores inside (read-only compatibility)."""
    return _digest(_historical_body(payload))


def _interim_strip(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _interim_strip(item)
            for key, item in value.items()
            if str(key) not in _ENVELOPE_KEYS | _MAPPING_SCORE_KEYS
        }
    if isinstance(value, (list, tuple)):
        return [_interim_strip(item) for item in value]
    return value


def interim_content_hash(payload: Mapping[str, Any]) -> str:
    """PR #435 interim digest (e83b6dc7a): scores stripped at every depth.

    Read-only compatibility: it also dropped name-keyed DDL entries, so it is
    never minted again.
    """
    return _digest(_interim_strip(_historical_body(payload)))


def accepted_stamps(artifact: DecisionArtifact | Mapping[str, Any]) -> set[str]:
    """Every digest a stored stamp of this decision may carry.

    The interim digest dropped key flags with the scores, so for a decision
    with a key it would equal the current hash of the same rows without the
    key (review Q2): it is accepted only for a decision with no key-flagged
    row, where it cannot hide a key change. The feature/Cleanup digest never
    covered key flags either; it is kept as it always verified.
    """
    payload = _payload(artifact)
    stamps = {
        compute_content_hash(payload),
        scored_structural_hash(payload),
        base_content_hash(payload),
    }
    rows = payload.get("mappings") or []
    if not any(isinstance(row, Mapping) and _row_is_key(row) for row in rows):
        stamps.add(interim_content_hash(payload))
    return stamps


def same_structural_decision(left: DecisionArtifact, right: DecisionArtifact) -> bool:
    """True when two artifacts are the same decision at different scores."""
    return compute_content_hash(_payload(left)) == compute_content_hash(_payload(right))


def previous_release_stamps(artifact: DecisionArtifact | Mapping[str, Any]) -> set[str]:
    """The digests only a stamp minted before this release can carry.

    The current content hash is not among them: a stamp in the current format
    was minted under the current type rules, so it is never matched under the
    previous release's rules (review 2, item 5).
    """
    return accepted_stamps(artifact) - {compute_content_hash(_payload(artifact))}


def stamp_is_previous_release_format(art: DecisionArtifact) -> bool:
    """``art`` (a stored artifact) carries a digest only the previous release minted."""
    stamp = (art.content_hash or "").strip().lower()
    return bool(stamp) and stamp != compute_content_hash(_payload(art))


def decision_stamps_match(approved: str, art: DecisionArtifact, *, previous_formats_only: bool = False) -> bool:
    """True when a stored stamp names this artifact.

    The current content hash, the feature/Cleanup digest, or PR #435's interim
    digest of this artifact. None of these accepts a different type, key,
    fingerprint, sync mode, or error policy. A base stamp still moves with the
    score it was taken at (that digest held it); a current stamp does not.
    """
    stamp = (approved or "").strip().lower()
    if not stamp:
        return False
    if previous_formats_only:
        return stamp in previous_release_stamps(art)
    if stamp == (art.content_hash or "").strip().lower():
        return True
    return stamp in accepted_stamps(art)


def build_decision_artifact(
    *,
    tenant_id: str,
    route_id: str,
    source_fingerprint: str,
    dest_fingerprint: str,
    source_columns: list[ColumnSpec],
    dest_columns: list[ColumnSpec],
    mappings: list[MappingDecision],
    ddl: DdlPlan,
    proof: ProofPlan | None = None,
    sync_mode: str = "full_refresh_overwrite",
    error_policy: str = "quarantine",
    capability_source_hash: str = "",
    capability_dest_hash: str = "",
    artifact_id: str | None = None,
    created_at: str | None = None,
    document_model_hash: str = "",
) -> DecisionArtifact:
    """Construct an immutable DecisionArtifact with content_hash stamped."""
    draft = DecisionArtifact(
        artifact_id=artifact_id or f"da_{uuid4().hex[:16]}",
        schema_version=DECISION_ARTIFACT_SCHEMA,
        created_at=created_at
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        tenant_id=tenant_id,
        route_id=route_id,
        source_fingerprint=source_fingerprint,
        dest_fingerprint=dest_fingerprint,
        source_columns=tuple(source_columns),
        dest_columns=tuple(dest_columns),
        mappings=tuple(mappings),
        ddl=ddl,
        proof=proof or ProofPlan(),
        sync_mode=sync_mode,
        error_policy=error_policy,
        capability_source_hash=capability_source_hash,
        capability_dest_hash=capability_dest_hash,
        policy_signature="",
        content_hash="",
        document_model_hash=document_model_hash,
    )
    content_hash = compute_content_hash(draft.to_dict())
    return DecisionArtifact(
        artifact_id=draft.artifact_id,
        schema_version=draft.schema_version,
        created_at=draft.created_at,
        tenant_id=draft.tenant_id,
        route_id=draft.route_id,
        source_fingerprint=draft.source_fingerprint,
        dest_fingerprint=draft.dest_fingerprint,
        source_columns=draft.source_columns,
        dest_columns=draft.dest_columns,
        mappings=draft.mappings,
        ddl=draft.ddl,
        proof=draft.proof,
        sync_mode=draft.sync_mode,
        error_policy=draft.error_policy,
        capability_source_hash=draft.capability_source_hash,
        capability_dest_hash=draft.capability_dest_hash,
        policy_signature=draft.policy_signature,
        content_hash=content_hash,
        document_model_hash=draft.document_model_hash,
    )


def decision_artifact_from_dict(data: Mapping[str, Any]) -> DecisionArtifact:
    """Parse a JSON object into a DecisionArtifact (fail-closed on schema mismatch)."""
    ver = str(data.get("schema_version") or "")
    if ver != DECISION_ARTIFACT_SCHEMA:
        raise ValueError(
            f"unsupported decision artifact schema {ver!r} — "
            f"expected {DECISION_ARTIFACT_SCHEMA}"
        )

    def _canon(raw: Mapping[str, Any]) -> CanonicalType:
        return CanonicalType(
            logical=str(raw.get("logical") or ""),
            native=str(raw.get("native") or ""),
            bit_width=raw.get("bit_width"),
            precision=raw.get("precision"),
            scale=raw.get("scale"),
            timezone_polarity=raw.get("timezone_polarity"),
            nullable=raw.get("nullable"),
            extras=dict(raw.get("extras") or {}),
        )

    def _col(raw: Mapping[str, Any]) -> ColumnSpec:
        return ColumnSpec(
            name=str(raw["name"]),
            canonical=_canon(raw.get("canonical") or {}),
            role=raw.get("role"),
        )

    def _conv(raw: Mapping[str, Any]) -> ConversionDecision:
        return ConversionDecision(
            conversion_class=ConversionClass(str(raw["conversion_class"])),
            risk_level=RiskLevel(str(raw.get("risk_level") or RiskLevel.REVIEW.value)),
            lossy=bool(raw.get("lossy")),
            recommended_action=str(raw.get("recommended_action") or ""),
            risk_contract_id=raw.get("risk_contract_id"),
            reason=str(raw.get("reason") or ""),
        )

    def _map(raw: Mapping[str, Any]) -> MappingDecision:
        return MappingDecision(
            source=str(raw["source"]),
            target=raw.get("target"),
            confidence=float(raw.get("confidence") or 0.0),
            assignment_strategy=AssignmentStrategy(
                str(raw.get("assignment_strategy") or AssignmentStrategy.UNASSIGNED.value)
            ),
            conversion=_conv(raw.get("conversion") or {}),
            create_new=bool(raw.get("create_new")),
            omitted=bool(raw.get("omitted")),
            alternatives=tuple(raw.get("alternatives") or ()),
            calibration_reason=str(raw.get("calibration_reason") or ""),
            is_primary_key=bool(
                raw.get("is_primary_key")
                or raw.get("primary_key")
                or raw.get("identity")
                or raw.get("is_identity")
            ),
        )

    ddl_raw = data.get("ddl") or {}
    proof_raw = data.get("proof") or {}
    art = DecisionArtifact(
        artifact_id=str(data["artifact_id"]),
        schema_version=ver,
        created_at=str(data["created_at"]),
        tenant_id=str(data.get("tenant_id") or ""),
        route_id=str(data.get("route_id") or ""),
        source_fingerprint=str(data.get("source_fingerprint") or ""),
        dest_fingerprint=str(data.get("dest_fingerprint") or ""),
        source_columns=tuple(_col(c) for c in (data.get("source_columns") or [])),
        dest_columns=tuple(_col(c) for c in (data.get("dest_columns") or [])),
        mappings=tuple(_map(m) for m in (data.get("mappings") or [])),
        ddl=DdlPlan(
            ddl_identity_hash=str(ddl_raw.get("ddl_identity_hash") or ""),
            column_ddl=dict(ddl_raw.get("column_ddl") or {}),
            dialect=str(ddl_raw.get("dialect") or ""),
        ),
        proof=ProofPlan(
            checksum_algorithm=str(proof_raw.get("checksum_algorithm") or "sha256"),
            checksum_hex_chars=int(proof_raw.get("checksum_hex_chars") or 64),
            sample_limit_preflight=int(proof_raw.get("sample_limit_preflight") or 500),
            sample_is_population_proof=bool(
                proof_raw.get("sample_is_population_proof", False)
            ),
            reconcile_mode=str(proof_raw.get("reconcile_mode") or "full_population"),
        ),
        sync_mode=str(data.get("sync_mode") or "full_refresh_overwrite"),
        error_policy=str(data.get("error_policy") or "quarantine"),
        capability_source_hash=str(data.get("capability_source_hash") or ""),
        capability_dest_hash=str(data.get("capability_dest_hash") or ""),
        policy_signature=str(data.get("policy_signature") or ""),
        content_hash=str(data.get("content_hash") or ""),
        document_model_hash=str(data.get("document_model_hash") or ""),
    )
    if art.content_hash and art.content_hash.lower() not in accepted_stamps(art):
        raise ValueError(
            "decision artifact content_hash mismatch — refuse tampered/stale artifact"
        )
    return art
