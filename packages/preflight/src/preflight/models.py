from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class GateStatus(str, Enum):
    PASS = "pass"
    BLOCK = "block"
    SKIP = "skip"


class GateId(str, Enum):
    G1_SOURCE = "g1_source"
    G2_DESTINATION = "g2_destination"
    G3_SCHEMA_CONTRACT = "g3_schema_contract"
    G4_MAPPING_CONFIDENCE = "g4_mapping_confidence"
    G5_DRY_RUN = "g5_dry_run"
    G6_TARGET_DDL = "g6_target_ddl"
    G7_CAPACITY = "g7_capacity"
    G8_RECONCILIATION = "g8_reconciliation"
    G9_DATA_INTEGRITY = "g9_data_integrity"


@dataclass
class GateResult:
    gate_id: GateId
    status: GateStatus
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    duration_ms: float = 0.0


@dataclass
class PreflightResult:
    passed: bool
    gates: list[GateResult]
    blockers: list[GateResult]

    @property
    def passed_count(self) -> int:
        return sum(1 for g in self.gates if g.status == GateStatus.PASS)

    @property
    def total_gates(self) -> int:
        return len(self.gates)


@dataclass
class ColumnSchema:
    name: str
    inferred_type: str
    nullable: bool = True
    samples: list[str] = field(default_factory=list)


@dataclass
class ColumnMapping:
    source: str
    target: str
    confidence: float
    transform: str | None = None
    user_override: bool = False
    reasoning: str = ""
    requires_review: bool = False
    score_gap: float = 1.0


@dataclass
class SourceConfig:
    kind: str  # file | database | api
    connected: bool = False
    parseable: bool = False
    encoding: str = "utf-8"
    columns: list[ColumnSchema] = field(default_factory=list)
    row_count_estimate: int = 0
    error: str | None = None


@dataclass
class DestinationConfig:
    kind: str
    connected: bool = False
    can_create_table: bool = False
    can_write: bool = False
    target_columns: list[ColumnSchema] = field(default_factory=list)
    table_exists: bool = False
    error: str | None = None


@dataclass
class TransferPlan:
    source: SourceConfig
    destination: DestinationConfig
    mappings: list[ColumnMapping] = field(default_factory=list)
    required_targets: list[str] = field(default_factory=list)
    dry_run_passed: bool = False
    dry_run_errors: list[str] = field(default_factory=list)
    ddl_compatible: bool = True
    ddl_issues: list[str] = field(default_factory=list)
    estimated_bytes: int = 0
    available_staging_bytes: int = 0
    confidence_threshold: float = 0.85
    validation_mode: str = "strict"


@dataclass
class PreflightContext:
    """Runtime adapters injected by connectors (DB probes, parsers)."""

    plan: TransferPlan

    def probe_unique_constraint(self, columns: list[str]) -> list[dict[str, Any]]:
        return []

    def run_dry_run(self, sample_size: int = 1000) -> tuple[bool, list[str]]:
        return self.plan.dry_run_passed, self.plan.dry_run_errors
