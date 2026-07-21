"""GitOps manifest builders + plan/apply for declarative DataFlow resources.

Produces a versionable ``dataflow.yaml`` document (schedules + contracts).
``plan`` is read-only; ``apply`` creates/updates resources. Delivery semantics
of referenced CDC pipelines remain **at-least-once** — GitOps does not change
that honesty bar.
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

_CONTRACT_RUNTIME_KEYS = frozenset(
    {
        # Keep breaker out of declarative apply — ops resets live separately.
    }
)


def schedule_spec(sched: Any) -> dict[str, Any]:
    data = sched.to_dict() if hasattr(sched, "to_dict") else dict(sched)
    return {k: v for k, v in data.items() if k not in _SCHEDULE_RUNTIME_KEYS}


def contract_spec(contract: Any) -> dict[str, Any]:
    payload = contract.to_dict() if hasattr(contract, "to_dict") else dict(contract)
    return {k: v for k, v in payload.items() if k not in _CONTRACT_RUNTIME_KEYS}


def contract_artifact(contract: Any) -> dict[str, Any]:
    """Single-file ``dataflow-contract.yaml`` shape (kind + metadata + spec)."""
    spec = contract_spec(contract)
    return {
        "apiVersion": "dataflow.space/v1",
        "kind": "DataContract",
        "metadata": {
            "name": spec.get("name") or spec.get("id"),
            "id": spec.get("id"),
            "version": spec.get("version"),
        },
        "spec": spec,
    }


def schedule_artifact(sched: Any) -> dict[str, Any]:
    spec = schedule_spec(sched)
    return {
        "apiVersion": "dataflow.space/v1",
        "kind": "PipelineSchedule",
        "metadata": {"name": spec.get("name"), "id": spec.get("id")},
        "spec": spec,
    }


def build_dataflow_manifest(*, include_contracts: bool = True) -> dict[str, Any]:
    """Build a multi-resource manifest of schedules (and optional contracts)."""
    from services.schedule_store import list_schedules

    resources: list[dict[str, Any]] = []
    for sched in list_schedules():
        resources.append(schedule_artifact(sched))

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
            resources.append(contract_artifact(contract))

    return {
        "apiVersion": "dataflow.space/v1",
        "kind": "DataFlowManifest",
        "metadata": {"generator": "dataflow-gitops-export"},
        "resources": resources,
    }


def _normalize_resources(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if not isinstance(payload, dict):
        return []
    kind = str(payload.get("kind") or "")
    if kind == "DataFlowManifest":
        raw = payload.get("resources") or []
        return [r for r in raw if isinstance(r, dict)]
    if kind in {"PipelineSchedule", "DataContract"}:
        return [payload]
    # Bare schedule/contract dict without kind — treat as schedule if it looks like one.
    if payload.get("source_connector_id") or payload.get("source_table"):
        return [{"kind": "PipelineSchedule", "spec": payload}]
    if payload.get("columns") is not None or payload.get("mappings") is not None:
        return [{"kind": "DataContract", "spec": payload}]
    return []


def _resource_identity(resource: dict[str, Any]) -> tuple[str, str]:
    kind = str(resource.get("kind") or "")
    meta = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else resource
    rid = str(meta.get("id") or spec.get("id") or "").strip()
    name = str(meta.get("name") or spec.get("name") or "").strip()
    return kind, rid or name


def _coerce_payload(payload: dict[str, Any] | list[Any] | str) -> dict[str, Any] | list[Any]:
    """Accept JSON objects or ``{\"yaml\": \"...\"}`` / raw YAML strings."""
    if isinstance(payload, str):
        import yaml

        loaded = yaml.safe_load(payload)
        if isinstance(loaded, (dict, list)):
            return loaded
        raise ValueError("YAML must decode to a mapping or list")
    if isinstance(payload, dict) and isinstance(payload.get("yaml"), str) and len(payload) <= 2:
        import yaml

        loaded = yaml.safe_load(str(payload["yaml"]))
        if isinstance(loaded, (dict, list)):
            return loaded
        raise ValueError("YAML must decode to a mapping or list")
    return payload


def plan_manifest(payload: dict[str, Any] | list[Any] | str) -> dict[str, Any]:
    """Dry-run: report create/update/skip without mutating stores."""
    payload = _coerce_payload(payload)
    from services.schedule_store import get_schedule, list_schedules

    try:
        from services.contract_store import get_contract_store
    except ImportError:  # pragma: no cover
        from src.services.contract_store import get_contract_store

    resources = _normalize_resources(payload)
    actions: list[dict[str, Any]] = []
    schedule_names = {s.name: s.id for s in list_schedules()}
    store = get_contract_store()

    for resource in resources:
        kind, ident = _resource_identity(resource)
        spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
        if kind == "PipelineSchedule":
            sid = str(spec.get("id") or "").strip()
            name = str(spec.get("name") or "").strip()
            existing = get_schedule(sid) if sid else None
            if existing is None and name and name in schedule_names:
                existing = get_schedule(schedule_names[name])
            action = "update" if existing else "create"
            actions.append({
                "kind": kind,
                "action": action,
                "id": (existing.id if existing else sid) or None,
                "name": name or (existing.name if existing else None),
            })
        elif kind == "DataContract":
            cid = str(spec.get("id") or "").strip()
            existing = store.get_contract(cid) if cid else None
            action = "update" if existing else "create"
            actions.append({
                "kind": kind,
                "action": action,
                "id": cid or None,
                "name": str(spec.get("name") or "") or None,
            })
        else:
            actions.append({
                "kind": kind or "Unknown",
                "action": "skip",
                "id": ident or None,
                "name": None,
                "reason": f"unsupported kind {kind!r}",
            })

    return {
        "dry_run": True,
        "resource_count": len(resources),
        "creates": sum(1 for a in actions if a["action"] == "create"),
        "updates": sum(1 for a in actions if a["action"] == "update"),
        "skips": sum(1 for a in actions if a["action"] == "skip"),
        "actions": actions,
    }


def apply_manifest(
    payload: dict[str, Any] | list[Any] | str,
    *,
    dry_run: bool = False,
    require_signed_contracts: bool = False,
) -> dict[str, Any]:
    """Apply a DataFlowManifest (or single resource). ``dry_run=True`` delegates to plan.

    When ``require_signed_contracts=True`` (CD / staging), every PipelineSchedule
    must reference a SIGNED contract — even if the YAML omits
    ``require_signed_contract``. DataContract resources still land as DRAFT
    (sign via API/UI before CD apply of schedules that depend on them).
    """
    payload = _coerce_payload(payload)
    if dry_run:
        return plan_manifest(payload)

    from services.schedule_store import (
        assert_signed_contract,
        create_schedule,
        get_schedule,
        update_schedule,
    )

    try:
        from services.contract_store import get_contract_store
        from services.data_contract import ContractStatus, DataContract
    except ImportError:  # pragma: no cover
        from src.services.contract_store import get_contract_store
        from src.services.data_contract import ContractStatus, DataContract

    resources = _normalize_resources(payload)
    results: list[dict[str, Any]] = []
    store = get_contract_store()

    for resource in resources:
        kind, _ident = _resource_identity(resource)
        spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
        try:
            if kind == "PipelineSchedule":
                apply_spec = dict(spec)
                if require_signed_contracts:
                    apply_spec["require_signed_contract"] = True
                    cid = str(apply_spec.get("contract_id") or "").strip()
                    # Fail closed before create/update so CD never soft-skips.
                    assert_signed_contract(cid, require_signed=True)
                sid = str(apply_spec.get("id") or "").strip()
                if sid and get_schedule(sid):
                    updated = update_schedule(sid, apply_spec)
                    results.append({
                        "kind": kind,
                        "action": "update",
                        "ok": bool(updated),
                        "id": sid,
                        "name": str(apply_spec.get("name") or ""),
                    })
                else:
                    created = create_schedule(apply_spec)
                    results.append({
                        "kind": kind,
                        "action": "create",
                        "ok": True,
                        "id": created.id,
                        "name": created.name,
                    })
            elif kind == "DataContract":
                contract = DataContract.from_dict(spec)
                # Imported contracts stay draft until explicitly signed.
                contract.status = ContractStatus.DRAFT
                existing = store.get_contract(contract.id) if contract.id else None
                store.save_contract(contract)
                results.append({
                    "kind": kind,
                    "action": "update" if existing else "create",
                    "ok": True,
                    "id": contract.id,
                    "name": contract.name,
                    "note": "imported as DRAFT — sign before CD require_signed_contracts",
                })
            else:
                results.append({
                    "kind": kind or "Unknown",
                    "action": "skip",
                    "ok": False,
                    "reason": f"unsupported kind {kind!r}",
                })
        except Exception as exc:
            results.append({
                "kind": kind or "Unknown",
                "action": "error",
                "ok": False,
                "error": str(exc)[:400],
            })

    return {
        "dry_run": False,
        "resource_count": len(resources),
        "applied": sum(1 for r in results if r.get("ok")),
        "failed": sum(1 for r in results if not r.get("ok")),
        "require_signed_contracts": bool(require_signed_contracts),
        "results": results,
    }
