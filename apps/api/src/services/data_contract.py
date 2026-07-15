"""DataContract and CircuitBreaker primitives.

A DataContract is a versioned, enforceable snapshot of the agreement between a
source schema, a semantic mapping, a destination schema, and the preflight gates
that approved it. The CircuitBreaker tracks the runtime health of a contract and
can halt transfers when a violation is detected.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class ContractStatus(str, Enum):
    DRAFT = "draft"
    SIGNED = "signed"
    BROKEN = "broken"
    DEPRECATED = "deprecated"


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class ColumnRule:
    source_name: str
    target_name: str
    source_type: str
    target_type: str
    transform: str | None = None
    nullable: bool = True
    primary_key: bool = False


@dataclass
class QualityRule:
    name: str
    expectation: str
    threshold: float | None = None
    severity: str = "warning"  # warning | block


@dataclass
class DataContract:
    id: str = ""
    name: str = ""
    version: int = 1
    status: ContractStatus = ContractStatus.DRAFT
    source: dict[str, Any] = field(default_factory=dict)
    destination: dict[str, Any] = field(default_factory=dict)
    columns: list[ColumnRule] = field(default_factory=list)
    mappings: list[dict[str, Any]] = field(default_factory=list)
    quality_rules: list[QualityRule] = field(default_factory=list)
    preflight_gates: list[dict[str, Any]] = field(default_factory=list)
    strict: bool = True
    created_at: str = ""
    updated_at: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = f"dfc-{uuid.uuid4().hex[:16]}"
        now = datetime.now(timezone.utc).isoformat()
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = now

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "version": self.version,
            "status": self.status.value,
            "source": self.source,
            "destination": self.destination,
            "columns": [
                {
                    "source_name": c.source_name,
                    "target_name": c.target_name,
                    "source_type": c.source_type,
                    "target_type": c.target_type,
                    "transform": c.transform,
                    "nullable": c.nullable,
                    "primary_key": c.primary_key,
                }
                for c in self.columns
            ],
            "mappings": self.mappings,
            "quality_rules": [
                {
                    "name": q.name,
                    "expectation": q.expectation,
                    "threshold": q.threshold,
                    "severity": q.severity,
                }
                for q in self.quality_rules
            ],
            "preflight_gates": self.preflight_gates,
            "strict": self.strict,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataContract":
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            version=data.get("version", 1),
            status=ContractStatus(data.get("status", "draft")),
            source=data.get("source", {}),
            destination=data.get("destination", {}),
            columns=[ColumnRule(**c) for c in data.get("columns", [])],
            mappings=data.get("mappings", []),
            quality_rules=[QualityRule(**q) for q in data.get("quality_rules", [])],
            preflight_gates=data.get("preflight_gates", []),
            strict=data.get("strict", True),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            metadata=data.get("metadata", {}),
        )


class ContractViolation(Exception):
    def __init__(self, message: str, violations: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.message = message
        self.violations = violations or []


class CircuitBreaker:
    """Simple circuit breaker with CLOSED → OPEN → HALF-OPEN → CLOSED state machine."""

    def __init__(
        self,
        contract_id: str,
        failure_threshold: int = 3,
        recovery_timeout_seconds: float = 60.0,
        half_open_max: int = 1,
    ):
        self.contract_id = contract_id
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.half_open_max = half_open_max
        self.state = BreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: float | None = None
        self.last_state_change: float = time.time()

    def allow(self) -> bool:
        if self.state == BreakerState.CLOSED:
            return True
        if self.state == BreakerState.OPEN:
            if self.last_failure_time is None:
                return False
            if time.time() - self.last_failure_time >= self.recovery_timeout_seconds:
                self.state = BreakerState.HALF_OPEN
                self.success_count = 0
                self.last_state_change = time.time()
                return True
            return False
        if self.state == BreakerState.HALF_OPEN:
            return self.success_count < self.half_open_max
        return False

    def record_success(self) -> None:
        if self.state == BreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.half_open_max:
                self.state = BreakerState.CLOSED
                self.failure_count = 0
                self.last_failure_time = None
                self.last_state_change = time.time()
        else:
            self.failure_count = 0
            self.last_failure_time = None

    def record_failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.state == BreakerState.HALF_OPEN:
            self.state = BreakerState.OPEN
            self.last_state_change = time.time()
        elif self.failure_count >= self.failure_threshold:
            self.state = BreakerState.OPEN
            self.last_state_change = time.time()

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time,
            "last_state_change": self.last_state_change,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout_seconds": self.recovery_timeout_seconds,
            "half_open_max": self.half_open_max,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CircuitBreaker":
        cb = cls(
            contract_id=data.get("contract_id", ""),
            failure_threshold=data.get("failure_threshold", 3),
            recovery_timeout_seconds=data.get("recovery_timeout_seconds", 60.0),
            half_open_max=data.get("half_open_max", 1),
        )
        cb.state = BreakerState(data.get("state", "closed"))
        cb.failure_count = data.get("failure_count", 0)
        cb.success_count = data.get("success_count", 0)
        cb.last_failure_time = data.get("last_failure_time")
        cb.last_state_change = data.get("last_state_change", time.time())
        return cb


class ContractEnforcer:
    """Check a transfer request against a stored contract and report violations."""

    def __init__(self, contract: DataContract):
        self.contract = contract

    def enforce(self, request: Any, *, sample_schema: dict[str, str] | None = None) -> None:
        """Raise ContractViolation if the request breaks the contract."""
        violations: list[dict[str, Any]] = []

        # Enforce source/destination endpoint shape (ignore credentials).
        if self.contract.source.get("format") and self.contract.source.get("format") != request.source.format:
            violations.append({
                "rule": "source_format",
                "expected": self.contract.source.get("format"),
                "actual": request.source.format,
                "message": f"Source format changed from {self.contract.source.get('format')} to {request.source.format}",
            })
        if self.contract.destination.get("format") and self.contract.destination.get("format") != request.destination.format:
            violations.append({
                "rule": "destination_format",
                "expected": self.contract.destination.get("format"),
                "actual": request.destination.format,
                "message": f"Destination format changed from {self.contract.destination.get('format')} to {request.destination.format}",
            })

        schema = sample_schema or {}
        contract_sources = {c.source_name for c in self.contract.columns}
        missing_required = [
            c for c in self.contract.columns
            if not c.nullable and c.source_name not in schema
        ]
        if missing_required:
            for c in missing_required:
                violations.append({
                    "rule": "required_column",
                    "column": c.source_name,
                    "message": f"Required source column '{c.source_name}' is missing",
                })

        if self.contract.strict:
            for c in self.contract.columns:
                if c.source_name not in schema:
                    continue
                actual_type = schema.get(c.source_name, "").lower()
                expected_type = c.source_type.lower()
                if expected_type and actual_type and expected_type != actual_type:
                    violations.append({
                        "rule": "source_type_change",
                        "column": c.source_name,
                        "expected": expected_type,
                        "actual": actual_type,
                        "message": f"Source column '{c.source_name}' type changed from {expected_type} to {actual_type}",
                    })

        if self.contract.status == ContractStatus.BROKEN:
            violations.append({
                "rule": "contract_status",
                "message": f"Contract {self.contract.id} is marked BROKEN; transfer is blocked until it is re-signed",
            })

        if violations:
            raise ContractViolation(
                f"Data contract {self.contract.id} violated: {violations[0]['message']}",
                violations=violations,
            )


def build_contract_from_preflight(
    request: Any,
    preflight: dict[str, Any] | None,
    schema: dict[str, str] | None = None,
    mappings: list[dict[str, Any]] | None = None,
) -> DataContract:
    """Derive a DataContract from a TransferRequest and its preflight result."""
    pf = preflight or {}
    schema = schema or {}
    mappings = mappings or []
    columns = []
    for m in mappings:
        src = m.get("source", "")
        tgt = m.get("target", "")
        columns.append(
            ColumnRule(
                source_name=src,
                target_name=tgt,
                source_type=schema.get(src, "VARCHAR"),
                target_type=m.get("target_type") or schema.get(src, "VARCHAR"),
                transform=m.get("transform"),
                nullable=True,
                primary_key=m.get("source", "").lower() in {"id", "_id"} or m.get("target", "").lower() in {"id", "_id"},
            )
        )

    quality_rules: list[QualityRule] = []
    for g in pf.get("gates", []) or []:
        if g.get("status") == "block" or g.get("status") == "BLOCK":
            quality_rules.append(
                QualityRule(
                    name=g.get("id", "gate"),
                    expectation=g.get("message", ""),
                    severity="block",
                )
            )

    source_ep = {
        "kind": request.source.kind,
        "format": request.source.format,
        "table": request.source.table,
        "collection": request.source.collection,
    }
    dest_ep = {
        "kind": request.destination.kind,
        "format": request.destination.format,
        "table": request.destination.table,
        "collection": request.destination.collection,
    }

    return DataContract(
        name=f"{request.source.format}-to-{request.destination.format}-{uuid.uuid4().hex[:8]}",
        source=source_ep,
        destination=dest_ep,
        columns=columns,
        mappings=mappings,
        quality_rules=quality_rules,
        preflight_gates=pf.get("gates", []),
        strict=True,
        metadata={
            "sync_mode": request.sync_mode,
            "validation_mode": request.validation_mode,
            "schema_policy": request.schema_policy,
            "backfill_new_fields": request.backfill_new_fields,
            "readiness_score": pf.get("readiness_score", 0),
        },
    )
