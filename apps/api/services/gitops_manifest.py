"""GitOps manifest builders + plan/apply for declarative Datawrap resources.

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
        "running_job_id",
        "run_history",
        "cursor_value",
        "retry_at",
        "retry_attempt",
        "missed_window_count",
        "last_missed_windows",
        "approval_request",
        "standing_authorization",
    }
)

# Observed at run time. GitOps owns policy, not the last-run fingerprint —
# stamping source_schema from env A onto env B hides real SOURCE_SCHEMA_DRIFT.
_SCHEDULE_OBSERVED_KEYS = frozenset(
    {
        "source_schema",
        "source_schema_fingerprint",
        "source_schema_observed_at",
        "source_primary_key",
        "fidelity_campaign",
        "created_at",
    }
)

# Studio Advanced + cadence + contract. Export is an allowlist so CD cannot
# silently drop write_via_staging / snapshot_mode the way a dump-minus-runtime
# filter does when a new observed field lands in to_dict().
_SCHEDULE_DECLARATIVE_KEYS = frozenset(
    {
        "id",
        "name",
        "source_connector_id",
        "source_table",
        "dest_connector_id",
        "dest_table",
        "interval",
        "enabled",
        "cron",
        "timezone",
        "sync_mode",
        "validation_mode",
        "schema_policy",
        "backfill_new_fields",
        "write_via_staging",
        "priority_column",
        "priority_direction",
        "row_limit",
        "delivery_guarantee",
        "snapshot_mode",
        "mappings",
        "stream_contracts",
        "cursor_column",
        "primary_key",
        "source_read_mode",
        "procedure_call",
        "source_query",
        "procedure_params",
        "workspace_id",
        "contract_id",
        "require_signed_contract",
        "date_locale",
        "number_locale",
        "shape_recipe",
        "approved_shape_recipe_hash",
        "approved_decision_artifact_hash",
        "approved_ddl_identity_hash",
        "max_retries",
        "retry_backoff_seconds",
        "notify_on_failure",
        "notify_on_success",
    }
)

_CONTRACT_RUNTIME_KEYS = frozenset(
    {
        # Keep breaker out of declarative apply — ops resets live separately.
    }
)


def schedule_spec(sched: Any) -> dict[str, Any]:
    data = sched.to_dict() if hasattr(sched, "to_dict") else dict(sched)
    return {
        k: data[k]
        for k in _SCHEDULE_DECLARATIVE_KEYS
        if k in data
        and k not in _SCHEDULE_RUNTIME_KEYS
        and k not in _SCHEDULE_OBSERVED_KEYS
    }


def apply_schedule_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Keep policy knobs from YAML; never stamp a remembered source shape."""
    if not isinstance(spec, dict):
        return {}
    return {
        k: v
        for k, v in spec.items()
        if k not in _SCHEDULE_RUNTIME_KEYS and k not in _SCHEDULE_OBSERVED_KEYS
    }


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


def mapping_bundle_spec(contract: Any) -> dict[str, Any]:
    """Thin declarative shape: mappings + endpoints (not full contract lifecycle)."""
    payload = contract.to_dict() if hasattr(contract, "to_dict") else dict(contract)
    return {
        "id": payload.get("id"),
        "name": payload.get("name"),
        "source": payload.get("source") or {},
        "destination": payload.get("destination") or {},
        "mappings": list(payload.get("mappings") or []),
        "columns": list(payload.get("columns") or []),
        "strict": bool(payload.get("strict", True)),
        "metadata": {
            **(payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}),
            "exported_as": "MappingBundle",
        },
    }


def mapping_bundle_artifact(contract: Any) -> dict[str, Any]:
    """``dataflow-mapping-bundle.yaml`` — import always lands as DRAFT contract."""
    spec = mapping_bundle_spec(contract)
    return {
        "apiVersion": "dataflow.space/v1",
        "kind": "MappingBundle",
        "metadata": {
            "name": spec.get("name") or spec.get("id"),
            "id": spec.get("id"),
        },
        "spec": spec,
        "honesty": (
            "Import creates/updates a DRAFT DataContract from mappings — "
            "sign separately before CD require_signed_contracts."
        ),
    }


def build_dataflow_manifest(
    *,
    include_contracts: bool = True,
    include_mapping_bundles: bool = False,
    workspace_id: str = "",
    isolation: bool = False,
) -> dict[str, Any]:
    """Build a multi-resource manifest of schedules (and optional contracts)."""
    from services.schedule_store import list_schedules

    resources: list[dict[str, Any]] = []
    ws = (workspace_id or "").strip()
    for sched in list_schedules():
        sws = (getattr(sched, "workspace_id", "") or "").strip()
        if ws:
            if isolation and sws != ws:
                continue
            if not isolation and sws and sws != ws:
                continue
        resources.append(schedule_artifact(sched))

    if include_contracts or include_mapping_bundles:
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
            meta = getattr(contract, "metadata", None) or {}
            cws = str(meta.get("workspace_id") or "").strip()
            if ws:
                if isolation and cws != ws:
                    continue
                if not isolation and cws and cws != ws:
                    continue
            if include_contracts:
                resources.append(contract_artifact(contract))
            if include_mapping_bundles:
                resources.append(mapping_bundle_artifact(contract))

    return {
        "apiVersion": "dataflow.space/v1",
        "kind": "DatawrapManifest",
        "metadata": {"generator": "dataflow-gitops-export"},
        "resources": resources,
    }


def _normalize_resources(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if not isinstance(payload, dict):
        return []
    kind = str(payload.get("kind") or "")
    if kind == "DatawrapManifest":
        raw = payload.get("resources") or []
        return [r for r in raw if isinstance(r, dict)]
    if kind in {"PipelineSchedule", "DataContract", "MappingBundle"}:
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
        elif kind in {"DataContract", "MappingBundle"}:
            cid = str(spec.get("id") or "").strip()
            existing = store.get_contract(cid) if cid else None
            action = "update" if existing else "create"
            actions.append({
                "kind": kind,
                "action": action,
                "id": cid or None,
                "name": str(spec.get("name") or "") or None,
                "note": (
                    "lands as DRAFT DataContract"
                    if kind == "MappingBundle"
                    else None
                ),
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
    workspace_id: str = "",
) -> dict[str, Any]:
    """Apply a DatawrapManifest (or single resource). ``dry_run=True`` delegates to plan.

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
                apply_spec = apply_schedule_spec(spec)
                bound_ws = (workspace_id or "").strip()
                if bound_ws:
                    apply_spec["workspace_id"] = bound_ws
                if require_signed_contracts:
                    apply_spec["require_signed_contract"] = True
                    cid = str(apply_spec.get("contract_id") or "").strip()
                    # Fail closed before create/update so CD never soft-skips.
                    assert_signed_contract(cid, require_signed=True)
                sid = str(apply_spec.get("id") or "").strip()
                if sid and get_schedule(sid):
                    existing = get_schedule(sid)
                    existing_ws = (getattr(existing, "workspace_id", "") or "").strip()
                    if bound_ws and existing_ws and existing_ws != bound_ws:
                        raise ValueError("Schedule belongs to another workspace")
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
            elif kind in {"DataContract", "MappingBundle"}:
                # MappingBundle is a thin mappings export; persist as DataContract DRAFT.
                contract_payload = dict(spec)
                if kind == "MappingBundle":
                    meta = contract_payload.get("metadata")
                    if not isinstance(meta, dict):
                        meta = {}
                    meta = {**meta, "imported_from": "MappingBundle"}
                    contract_payload["metadata"] = meta
                contract = DataContract.from_dict(contract_payload)
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
