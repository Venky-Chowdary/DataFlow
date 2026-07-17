"""Persisted transfer plans — immutable mapping revisions for map → preflight → run."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir
from services.schema_fingerprint import fingerprint_mappings, fingerprint_schema

STORE_PATH = data_dir() / "transfer_plans.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class PlanRevision:
    version: int
    mappings: list[dict[str, Any]]
    transforms: list[dict[str, Any]]
    validation: dict[str, Any]
    mapping_hash: str
    source_schema_hash: str
    target_schema_hash: str
    agents_used: list[str]
    plan_summary: dict[str, Any] = field(default_factory=dict)
    llm: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now)
    approved: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlanRevision:
        return cls(
            version=int(data["version"]),
            mappings=list(data.get("mappings") or []),
            transforms=list(data.get("transforms") or []),
            validation=dict(data.get("validation") or {}),
            mapping_hash=data.get("mapping_hash", ""),
            source_schema_hash=data.get("source_schema_hash", ""),
            target_schema_hash=data.get("target_schema_hash", ""),
            agents_used=list(data.get("agents_used") or []),
            plan_summary=dict(data.get("plan_summary") or {}),
            llm=dict(data.get("llm") or {}),
            created_at=data.get("created_at", _now()),
            approved=bool(data.get("approved", False)),
        )


@dataclass
class TransferPlanRecord:
    id: str
    name: str
    status: str  # draft | mapped | preflight_passed | preflight_failed | approved | running | completed | failed
    source: dict[str, Any]
    destination: dict[str, Any]
    source_columns: list[str]
    source_schema: dict[str, str]
    target_columns: list[str]
    target_schema: dict[str, str]
    row_count_estimate: int = 0
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    policies: dict[str, Any] = field(default_factory=dict)
    revisions: list[PlanRevision] = field(default_factory=list)
    preflight_runs: list[dict[str, Any]] = field(default_factory=list)
    active_version: int = 0
    job_ids: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "revisions": [r.to_dict() for r in self.revisions],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TransferPlanRecord:
        revisions = [PlanRevision.from_dict(r) for r in data.get("revisions") or []]
        return cls(
            id=data["id"],
            name=data.get("name", "Transfer plan"),
            status=data.get("status", "draft"),
            source=dict(data.get("source") or {}),
            destination=dict(data.get("destination") or {}),
            source_columns=list(data.get("source_columns") or []),
            source_schema=dict(data.get("source_schema") or {}),
            target_columns=list(data.get("target_columns") or []),
            target_schema=dict(data.get("target_schema") or {}),
            row_count_estimate=int(data.get("row_count_estimate") or 0),
            sample_rows=list(data.get("sample_rows") or []),
            policies=dict(data.get("policies") or {}),
            revisions=revisions,
            preflight_runs=list(data.get("preflight_runs") or []),
            active_version=int(data.get("active_version") or 0),
            job_ids=list(data.get("job_ids") or []),
            created_at=data.get("created_at", _now()),
            updated_at=data.get("updated_at", _now()),
        )

    def active_revision(self) -> PlanRevision | None:
        if not self.revisions:
            return None
        for rev in reversed(self.revisions):
            if rev.version == self.active_version:
                return rev
        return self.revisions[-1]


def _load_all() -> list[TransferPlanRecord]:
    if not STORE_PATH.exists():
        return []
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        return [TransferPlanRecord.from_dict(p) for p in raw.get("plans", [])]
    except Exception:
        return []


def _save_all(plans: list[TransferPlanRecord]) -> None:
    write_json_atomic(
        STORE_PATH,
        {"plans": [p.to_dict() for p in plans]},
        indent=2,
        default=None,
    )


def list_plans(*, limit: int = 50) -> list[TransferPlanRecord]:
    plans = sorted(_load_all(), key=lambda p: p.updated_at, reverse=True)
    return plans[:limit]


def get_plan(plan_id: str) -> TransferPlanRecord | None:
    for p in _load_all():
        if p.id == plan_id:
            return p
    return None


def create_plan(data: dict[str, Any]) -> TransferPlanRecord:
    plans = _load_all()
    plan = TransferPlanRecord(
        id=str(uuid.uuid4()),
        name=data.get("name") or "Transfer plan",
        status="draft",
        source=dict(data.get("source") or {}),
        destination=dict(data.get("destination") or {}),
        source_columns=list(data.get("source_columns") or []),
        source_schema={k: str(v) for k, v in (data.get("source_schema") or {}).items()},
        target_columns=list(data.get("target_columns") or []),
        target_schema={k: str(v) for k, v in (data.get("target_schema") or {}).items()},
        row_count_estimate=int(data.get("row_count_estimate") or 0),
        sample_rows=list(data.get("sample_rows") or [])[:100],
        policies=dict(data.get("policies") or {}),
    )
    plans.append(plan)
    _save_all(plans)
    return plan


def add_mapping_revision(
    plan_id: str,
    pipeline_result: dict[str, Any],
    *,
    source_columns: list[str] | None = None,
    source_schema: dict[str, str] | None = None,
    target_columns: list[str] | None = None,
    target_schema: dict[str, str] | None = None,
) -> TransferPlanRecord | None:
    plans = _load_all()
    for i, plan in enumerate(plans):
        if plan.id != plan_id:
            continue
        src_cols = source_columns or plan.source_columns
        src_schema = source_schema or plan.source_schema
        tgt_cols = target_columns or plan.target_columns
        tgt_schema = target_schema or plan.target_schema
        mappings = list(pipeline_result.get("mappings") or [])
        version = (plan.revisions[-1].version + 1) if plan.revisions else 1
        rev = PlanRevision(
            version=version,
            mappings=mappings,
            transforms=list(pipeline_result.get("transforms") or []),
            validation=dict(pipeline_result.get("validation") or {}),
            mapping_hash=fingerprint_mappings(mappings),
            source_schema_hash=fingerprint_schema(src_cols, src_schema),
            target_schema_hash=fingerprint_schema(tgt_cols, tgt_schema),
            agents_used=list(pipeline_result.get("agents_used") or []),
            plan_summary=dict(pipeline_result.get("plan_summary") or {}),
            llm=dict(pipeline_result.get("llm") or {}),
        )
        plan.revisions.append(rev)
        plan.active_version = version
        plan.source_columns = src_cols
        plan.source_schema = src_schema
        plan.target_columns = tgt_cols
        plan.target_schema = tgt_schema
        plan.status = "mapped" if mappings else "draft"
        plan.updated_at = _now()
        plans[i] = plan
        _save_all(plans)
        return plan
    return None


def add_preflight_run(plan_id: str, preflight_result: dict[str, Any]) -> TransferPlanRecord | None:
    plans = _load_all()
    for i, plan in enumerate(plans):
        if plan.id != plan_id:
            continue
        run = {
            "id": str(uuid.uuid4()),
            "time": _now(),
            "passed": bool(preflight_result.get("passed")),
            "readiness_score": preflight_result.get("readiness_score"),
            "gates": preflight_result.get("gates", []),
            "blockers": preflight_result.get("blockers", []),
            "mapping_version": plan.active_version,
            "mapping_hash": (plan.active_revision() or PlanRevision(
                version=0, mappings=[], transforms=[], validation={},
                mapping_hash="", source_schema_hash="", target_schema_hash="", agents_used=[],
            )).mapping_hash,
        }
        plan.preflight_runs.append(run)
        plan.status = "preflight_passed" if run["passed"] else "preflight_failed"
        plan.updated_at = _now()
        plans[i] = plan
        _save_all(plans)
        return plan
    return None


def update_plan(plan_id: str, data: dict[str, Any]) -> TransferPlanRecord | None:
    """Merge destination, policies, and schema snapshots onto an existing plan."""
    plans = _load_all()
    for i, plan in enumerate(plans):
        if plan.id != plan_id:
            continue
        if "name" in data and data["name"]:
            plan.name = str(data["name"])
        if "source" in data:
            plan.source = dict(data["source"] or {})
        if "destination" in data:
            plan.destination = dict(data["destination"] or {})
        if "source_columns" in data:
            plan.source_columns = list(data["source_columns"] or [])
        if "source_schema" in data:
            plan.source_schema = {k: str(v) for k, v in (data["source_schema"] or {}).items()}
        if "target_columns" in data:
            plan.target_columns = list(data["target_columns"] or [])
        if "target_schema" in data:
            plan.target_schema = {k: str(v) for k, v in (data["target_schema"] or {}).items()}
        if "row_count_estimate" in data:
            plan.row_count_estimate = int(data["row_count_estimate"] or 0)
        if "sample_rows" in data:
            plan.sample_rows = list(data["sample_rows"] or [])[:100]
        if "policies" in data:
            plan.policies = {**plan.policies, **dict(data["policies"] or {})}
        plan.updated_at = _now()
        plans[i] = plan
        _save_all(plans)
        return plan
    return None


def sync_ui_mappings(
    plan_id: str,
    mappings: list[dict[str, Any]],
    *,
    transforms: list[dict[str, Any]] | None = None,
) -> TransferPlanRecord | None:
    """Persist user-approved mapping edits as a new revision."""
    plans = _load_all()
    for i, plan in enumerate(plans):
        if plan.id != plan_id:
            continue
        version = (plan.revisions[-1].version + 1) if plan.revisions else 1
        rev = PlanRevision(
            version=version,
            mappings=list(mappings),
            transforms=list(transforms or []),
            validation={"passed": True, "issues": [], "source": "ui_sync"},
            mapping_hash=fingerprint_mappings(mappings),
            source_schema_hash=fingerprint_schema(plan.source_columns, plan.source_schema),
            target_schema_hash=fingerprint_schema(plan.target_columns, plan.target_schema),
            agents_used=["user_review"],
            plan_summary={"edited_in_ui": True, "mapping_count": len(mappings)},
        )
        plan.revisions.append(rev)
        plan.active_version = version
        plan.status = "mapped"
        plan.updated_at = _now()
        plans[i] = plan
        _save_all(plans)
        return plan
    return None


def approve_plan_version(plan_id: str, version: int | None = None) -> TransferPlanRecord | None:
    plans = _load_all()
    for i, plan in enumerate(plans):
        if plan.id != plan_id:
            continue
        target_version = version or plan.active_version
        for rev in plan.revisions:
            rev.approved = rev.version == target_version
        plan.active_version = target_version
        plan.status = "approved"
        plan.updated_at = _now()
        plans[i] = plan
        _save_all(plans)
        return plan
    return None


def attach_job(plan_id: str, job_id: str, *, status: str = "running") -> TransferPlanRecord | None:
    plans = _load_all()
    for i, plan in enumerate(plans):
        if plan.id != plan_id:
            continue
        if job_id not in plan.job_ids:
            plan.job_ids.append(job_id)
        plan.status = status
        plan.updated_at = _now()
        plans[i] = plan
        _save_all(plans)
        return plan
    return None
