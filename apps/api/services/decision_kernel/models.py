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

    def to_dict(self) -> dict[str, Any]:
        return {
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

    def to_dict(self) -> dict[str, Any]:
        return {
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


def compute_content_hash(payload: Mapping[str, Any]) -> str:
    """Stable SHA-256 over canonical JSON (excludes content_hash / signatures)."""
    body = {
        k: v
        for k, v in payload.items()
        if k not in {"content_hash", "policy_signature", "created_at", "artifact_id"}
    }
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


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
    )
    expected = compute_content_hash(art.to_dict())
    if art.content_hash and art.content_hash != expected:
        raise ValueError(
            "decision artifact content_hash mismatch — refuse tampered/stale artifact"
        )
    return art
