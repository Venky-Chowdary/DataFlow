"""GitOps manifest builders for declarative DataFlow resources.

Produces a versionable ``dataflow.yaml`` document (schedules + contract refs)
without applying remote state. Plan/apply is intentionally a later wedge —
export is the safe, non-breaking first step.
"""

from __future__ import annotations

from typing import Any


_SCHEDULE_RUNTIME_KEYS = frozenset(
    {
        "last_run_at",
        "next_run_at",
        "last_job_id",
        "last_status",
        "run_count",
        "running",
        "running_instance",
        "running_started_at",
        "run_history",
        "cursor_value",
    }
)


def schedule_spec(sched: Any) -> dict[str, Any]:
    data = sched.to_dict() if hasattr(sched, "to_dict") else dict(sched)
    return {k: v for k, v in data.items() if k not in _SCHEDULE_RUNTIME_KEYS}


def build_dataflow_manifest(*, include_contracts: bool = True) -> dict[str, Any]:
    """Build a multi-document-friendly manifest of schedules (and optional contracts)."""
    from services.schedule_store import list_schedules

    resources: list[dict[str, Any]] = []
    for sched in list_schedules():
        resources.append(
            {
                "apiVersion": "dataflow.space/v1",
                "kind": "PipelineSchedule",
                "metadata": {"name": sched.name, "id": sched.id},
                "spec": schedule_spec(sched),
            }
        )

    if include_contracts:
        try:
            from services.contract_store import get_contract_store
        except ImportError:  # pragma: no cover
            from src.services.contract_store import get_contract_store

        store = get_contract_store()
        contracts = []
        list_fn = getattr(store, "list_contracts", None)
        if callable(list_fn):
            contracts = list_fn() or []
        for contract in contracts:
            payload = contract.to_dict() if hasattr(contract, "to_dict") else dict(contract)
            resources.append(
                {
                    "apiVersion": "dataflow.space/v1",
                    "kind": "DataContract",
                    "metadata": {
                        "name": payload.get("name") or payload.get("id"),
                        "id": payload.get("id"),
                    },
                    "spec": payload,
                }
            )

    return {
        "apiVersion": "dataflow.space/v1",
        "kind": "DataFlowManifest",
        "metadata": {"generator": "dataflow-gitops-export"},
        "resources": resources,
    }
