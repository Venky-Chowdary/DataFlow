"""Migration Decision Kernel — single source of truth for migration decisions.

This module is the start of an enterprise-grade backend decision engine. It
produces one immutable ``MigrationDecision`` artifact that Map, Validate,
Execute, Proof, Audit, and the UI all consume.

Right now the kernel delegates to existing services for discovery, mapping,
preflight, and DDL generation. Over time those services will be refactored into
the kernel's domain/application/infrastructure layers, but the public API and
the Decision Artifact shape will remain stable.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class ConversionClass(str, Enum):
    """Canonical conversion classification for a source → target type pair."""

    IDENTITY = "identity"
    EQUIVALENT = "equivalent"
    WIDENING = "widening"
    REPRESENTATION_CHANGE = "representation_change"
    NORMALIZATION = "normalization"
    BUSINESS_REVIEW = "business_review"
    POTENTIALLY_LOSSY = "potentially_lossy"
    LOSSY = "lossy"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class TypeCarrier:
    """Parametric, connector-independent carrier for a column's type."""

    logical: str = "string"
    native: str = ""
    precision: int | None = None
    scale: int | None = None
    length: int | None = None
    nullable: bool = True
    charset: str = ""
    collation: str = ""
    timezone: str = ""
    is_identity: bool = False
    is_generated: bool = False
    is_auto_increment: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnModel:
    """Canonical column discovered from a source or destination."""

    name: str
    source_name: str = ""
    carrier: TypeCarrier = field(default_factory=TypeCarrier)
    primary_key: bool = False
    nullable: bool = True
    default_value: str | None = None
    position: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SchemaModel:
    """Normalized schema independent of connector."""

    kind: str  # database, file, api, warehouse, lakehouse
    format: str = ""  # postgresql, csv, parquet, rest_api, etc.
    name: str = ""
    columns: tuple[ColumnModel, ...] = field(default_factory=tuple)
    constraints: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    indexes: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    row_estimate: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ConversionVerdict:
    """Explainable classification for a single source → target conversion."""

    classification: ConversionClass = ConversionClass.UNKNOWN
    confidence: float = 0.0
    evidence: str = ""
    business_impact: str = ""
    technical_impact: str = ""
    recommendation: str = ""
    execution_policy: str = "quarantine"  # allowed, quarantine, block, review
    audit_info: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnMapping:
    """One source → target column mapping with authority-stamped fidelity."""

    source_name: str
    target_name: str
    source_carrier: TypeCarrier = field(default_factory=TypeCarrier)
    target_carrier: TypeCarrier = field(default_factory=TypeCarrier)
    transform: str = "none"
    verdict: ConversionVerdict = field(default_factory=ConversionVerdict)
    locked: bool = False
    user_override: bool = False
    risk_acknowledged: bool = False


@dataclass(frozen=True)
class MappingModel:
    """Full source → destination mapping produced by the kernel."""

    columns: tuple[ColumnMapping, ...] = field(default_factory=tuple)
    confidence: float = 0.0
    requires_review: bool = False
    unmapped_source: tuple[str, ...] = field(default_factory=tuple)
    unmapped_target: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class GateResult:
    """Result of a single preflight gate."""

    gate_id: str
    status: str  # pass, block, warn, skip
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationModel:
    """Validation / preflight outcome for a migration decision."""

    passed: bool = False
    mode: str = "strict"
    confidence_threshold: float = 0.85
    gates: tuple[GateResult, ...] = field(default_factory=tuple)
    risk_contracts: dict[str, Any] = field(default_factory=dict)
    write_permitted: bool = False


@dataclass(frozen=True)
class ExecutionPlan:
    """Runtime plan produced by the kernel, consumed by the executor."""

    sync_mode: str = "full_refresh_overwrite"
    chunk_size: int = 20000
    max_workers: int = 1
    resume_supported: bool = False
    delivery_semantics: str = "at_least_once"
    quarantine_policy: str = "quarantine"
    rollback_policy: str = "none"
    checkpoint_interval: int = 1000


@dataclass(frozen=True)
class ProofPlan:
    """Measurable proof the executor must produce after a run."""

    row_count: bool = True
    checksum: bool = True
    sample_size: int = 1000
    constraint_validation: bool = False
    referential_integrity: bool = False
    transformation_verification: bool = True


@dataclass(frozen=True)
class MigrationDecision:
    """Immutable artifact that all subsystems consume for one migration."""

    decision_id: str
    version: str
    created_at: str
    source: SchemaModel
    destination: SchemaModel
    mapping: MappingModel
    validation: ValidationModel
    execution_plan: ExecutionPlan
    proof_plan: ProofPlan
    hash: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def _json_default(obj: Any) -> Any:
    """Serialize dataclasses and enums for decision hashing."""
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (tuple, list)):
        return [_json_default(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _json_default(v) for k, v in obj.items()}
    if isinstance(obj, TypeCarrier | ColumnModel | SchemaModel | ConversionVerdict
                  | ColumnMapping | MappingModel | GateResult | ValidationModel
                  | ExecutionPlan | ProofPlan | MigrationDecision):
        return _json_default(obj.__dict__)
    return obj


def _decision_hash(decision: MigrationDecision) -> str:
    """Stable hash of the decision content (excludes id, timestamp, and hash field)."""
    payload = {
        "version": decision.version,
        "source": _json_default(decision.source),
        "destination": _json_default(decision.destination),
        "mapping": _json_default(decision.mapping),
        "validation": _json_default(decision.validation),
        "execution_plan": _json_default(decision.execution_plan),
        "proof_plan": _json_default(decision.proof_plan),
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class MigrationKernel:
    """Authoritative backend decision engine for migration assurance."""

    VERSION = "1.0.0"

    def __init__(self) -> None:
        # Lazy imports keep the module importable even when legacy modules move.
        from services import type_system
        from services import mapping_engine_contract
        from services import validation_mode_contract
        from services import transform_engine

        self._type_system = type_system
        self._mapping_contract = mapping_engine_contract
        self._validation_contract = validation_mode_contract
        self._transform_engine = transform_engine

    # ------------------------------------------------------------------ #
    # Canonical helpers
    # ------------------------------------------------------------------ #
    def canonicalize_type(self, native_type: str, **metadata: Any) -> TypeCarrier:
        """Native → Canonical type carrier.

        Uses ``services.type_system`` today; will be replaced by the new
        ``services.type`` package as the refactor proceeds.
        """
        logical = self._type_system.normalize_logical_type(native_type)
        precision = None
        scale = None
        length = None
        timezone = None

        if logical == "decimal":
            precision, scale = self._type_system.parse_numeric_precision_scale(native_type)
        elif logical in {"string", "text"}:
            length = self._type_system.parse_string_carrier_width(native_type)
        elif logical == "vector":
            length = self._type_system.parse_vector_dimension(native_type)
        elif logical == "binary":
            m = re.search(r"\(\s*(\d+)\s*\)", native_type or "")
            if m:
                length = int(m.group(1))
        elif logical in {"datetime", "time", "date"}:
            precision = self._type_system.parse_temporal_fractional_precision(native_type)
            tz = self._type_system.datetime_timezone_polarity(native_type)
            if tz is None and logical == "time":
                tz = self._type_system.time_timezone_polarity(native_type)
            if tz:
                timezone = tz

        return TypeCarrier(
            logical=logical,
            native=native_type,
            precision=precision,
            scale=scale,
            length=length,
            timezone=timezone,
            metadata=metadata,
        )

    def target_ddl(self, dest_db: str, carrier: TypeCarrier) -> str:
        """Canonical → Native DDL for a destination connector."""
        return self._type_system.ddl_type(dest_db, carrier.native)

    def classify_conversion(
        self,
        source_native: str,
        target_native: str,
        dest_db: str = "",
        *,
        source_col: str = "",
        target_col: str = "",
        source_samples: list[str] | None = None,
    ) -> ConversionVerdict:
        """Deterministic, explainable conversion classification.

        All connectors route through this function; no subsystem invents its own
        lossy-coercion logic.
        """
        source = self.canonicalize_type(source_native, kind="source")
        target = self.canonicalize_type(target_native, kind="target")

        # Same logical + same carrier params is identity.
        if (
            source.logical == target.logical
            and source.precision == target.precision
            and source.scale == target.scale
            and source.length == target.length
            and source.timezone == target.timezone
            and source.charset == target.charset
        ):
            return ConversionVerdict(
                classification=ConversionClass.IDENTITY,
                confidence=1.0,
                evidence=f"{source_native} and {target_native} are the same canonical carrier.",
                execution_policy="allowed",
            )

        # Let the existing lossy-coercion oracle answer the safety question.
        lossy = self._type_system.is_lossy_coercion(source_native, target_native, dest_db=dest_db)

        # Compute a transform recommendation from the transform engine if possible.
        transform = "none"
        if source_samples is not None:
            try:
                transform = self._transform_engine.infer_transform_for_mapping(
                    source_col or "source",
                    target_col or "target",
                    source_native,
                    target_native,
                    source_samples,
                    destination_db_type=dest_db,
                )
            except Exception:
                transform = "none"

        if lossy:
            return ConversionVerdict(
                classification=ConversionClass.LOSSY,
                confidence=0.4,
                evidence=(
                    f"{source_native} → {target_native} may lose precision, "
                    "truncate width, or change semantics per the canonical type contract."
                ),
                business_impact="Operator-visible data fidelity risk; may change reported values.",
                technical_impact="Conversion is not reversible; downstream consumers may see drift.",
                recommendation="Review mapping, approve a Migration Risk Contract, or widen target carrier.",
                execution_policy="quarantine",
                audit_info={"transform": transform},
            )

        # Safe widening or representation change.
        safe_widenings: dict[str, set[str]] = {
            "string": {"text", "json"},
            "integer": {"decimal", "float", "string", "text"},
            "decimal": {"string", "text"},
            "float": {"string", "text"},
            "boolean": {"string", "text", "json", "integer", "decimal"},
            "date": {"datetime", "string", "text"},
            "datetime": {"string", "text"},
            "time": {"string", "text"},
            "uuid": {"string", "text", "json"},
            "binary": {"string", "text"},
        }
        if source.logical == target.logical:
            classification = ConversionClass.REPRESENTATION_CHANGE
        elif target.logical in safe_widenings.get(source.logical, set()):
            classification = ConversionClass.WIDENING
        else:
            classification = ConversionClass.UNKNOWN

        return ConversionVerdict(
            classification=classification,
            confidence=0.9 if classification == ConversionClass.WIDENING else 0.7,
            evidence=(
                f"{source_native} → {target_native} is a {classification.value} "
                "under the canonical type system."
            ),
            execution_policy="allowed" if classification == ConversionClass.WIDENING else "review",
            audit_info={"transform": transform},
        )

    # ------------------------------------------------------------------ #
    # Decision building
    # ------------------------------------------------------------------ #
    def build_mapping(
        self,
        source_schema: SchemaModel,
        destination_schema: SchemaModel,
        *,
        explicit_mappings: dict[str, str] | None = None,
        dest_db: str = "",
        source_samples: dict[str, list[str]] | None = None,
    ) -> MappingModel:
        """Produce a deterministic source → target mapping."""
        explicit = explicit_mappings or {}
        mappings: list[ColumnMapping] = []

        for src in source_schema.columns:
            target_name = explicit.get(src.name, src.name)
            target_col = next(
                (c for c in destination_schema.columns if c.name == target_name),
                None,
            )
            if target_col is None:
                continue
            samples = (source_samples or {}).get(src.name, [])
            verdict = self.classify_conversion(
                src.carrier.native,
                target_col.carrier.native,
                dest_db=dest_db,
                source_col=src.name,
                target_col=target_col.name,
                source_samples=samples,
            )
            mappings.append(
                ColumnMapping(
                    source_name=src.name,
                    target_name=target_col.name,
                    source_carrier=src.carrier,
                    target_carrier=target_col.carrier,
                    verdict=verdict,
                    transform=verdict.audit_info.get("transform", "none"),
                )
            )

        unmapped_src = tuple(
            c.name for c in source_schema.columns
            if c.name not in {m.source_name for m in mappings}
        )
        unmapped_tgt = tuple(
            c.name for c in destination_schema.columns
            if c.name not in {m.target_name for m in mappings}
        )

        confidence = 1.0 if not mappings else sum(m.verdict.confidence for m in mappings) / len(mappings)
        requires_review = any(
            m.verdict.classification
            in {
                ConversionClass.LOSSY,
                ConversionClass.POTENTIALLY_LOSSY,
                ConversionClass.BUSINESS_REVIEW,
                ConversionClass.UNKNOWN,
            }
            for m in mappings
        )

        return MappingModel(
            columns=tuple(mappings),
            confidence=round(confidence, 4),
            requires_review=requires_review,
            unmapped_source=unmapped_src,
            unmapped_target=unmapped_tgt,
        )

    def validate(
        self,
        mapping: MappingModel,
        *,
        mode: str = "strict",
        confidence_floor: float | None = None,
    ) -> ValidationModel:
        """Validate a mapping under the authoritative validation-mode contract."""
        spec = self._validation_contract.mode_contract(mode)
        floor = float(
            confidence_floor
            if confidence_floor is not None
            else spec.get("confidence_floor", 0.85)
        )
        gates: list[GateResult] = []

        # G3: type fidelity gate
        fidelity_blocks = [
            m for m in mapping.columns
            if m.verdict.classification
            in {ConversionClass.LOSSY, ConversionClass.UNSUPPORTED, ConversionClass.UNKNOWN}
            and not m.risk_acknowledged
        ]
        if fidelity_blocks:
            gates.append(
                GateResult(
                    gate_id="G3",
                    status="block" if spec.get("hard_block_fidelity") else "warn",
                    message=f"{len(fidelity_blocks)} column(s) have lossy/unknown conversions.",
                    details={"columns": [m.source_name for m in fidelity_blocks]},
                )
            )
        else:
            gates.append(GateResult(gate_id="G3", status="pass", message="Fidelity gate passed"))

        # G4: mapping confidence gate
        if mapping.confidence < floor:
            gates.append(
                GateResult(
                    gate_id="G4",
                    status="block" if spec.get("hard_block_fidelity") else "warn",
                    message=f"Mapping confidence {mapping.confidence:.2f} below floor {floor:.2f}.",
                    details={"confidence": mapping.confidence, "floor": floor},
                )
            )
        else:
            gates.append(GateResult(gate_id="G4", status="pass", message="Confidence gate passed"))

        passed = all(g.status == "pass" for g in gates)
        write_permitted = passed and spec.get("allows_write", True)

        return ValidationModel(
            passed=passed,
            mode=self._validation_contract.normalize_validation_mode(mode),
            confidence_threshold=floor,
            gates=tuple(gates),
            write_permitted=write_permitted,
        )

    def build_decision(
        self,
        source: SchemaModel,
        destination: SchemaModel,
        *,
        dest_db: str = "",
        explicit_mappings: dict[str, str] | None = None,
        validation_mode: str = "strict",
        sync_mode: str = "full_refresh_overwrite",
        chunk_size: int = 20000,
        source_samples: dict[str, list[str]] | None = None,
    ) -> MigrationDecision:
        """Build the single Migration Decision Artifact for a route."""
        mapping = self.build_mapping(
            source,
            destination,
            explicit_mappings=explicit_mappings,
            dest_db=dest_db,
            source_samples=source_samples,
        )
        validation = self.validate(mapping, mode=validation_mode)

        execution_plan = ExecutionPlan(
            sync_mode=sync_mode,
            chunk_size=chunk_size,
            resume_supported=sync_mode in {"cdc", "incremental_append", "incremental_deduped"},
            delivery_semantics="at_least_once",
            quarantine_policy="quarantine",
        )

        proof_plan = ProofPlan(
            row_count=True,
            checksum=True,
            sample_size=1000,
            constraint_validation=False,
            transformation_verification=True,
        )

        decision = MigrationDecision(
            decision_id=str(uuid.uuid4()),
            version=self.VERSION,
            created_at=datetime.now(timezone.utc).isoformat(),
            source=source,
            destination=destination,
            mapping=mapping,
            validation=validation,
            execution_plan=execution_plan,
            proof_plan=proof_plan,
        )

        # Use object.__setattr__ because dataclass is frozen.
        object.__setattr__(decision, "hash", _decision_hash(decision))
        return decision
