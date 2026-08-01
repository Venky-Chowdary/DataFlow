"""Run transformation projects after a transfer lands.

``engine.py`` has three near-identical completion paths (buffered, DB-to-DB
streaming, file streaming). This is the single helper all three call, because
patching one of them is exactly the "works for the route you pasted" failure the
proof bar forbids — a model set that runs after a CSV load but not after a
Postgres stream is worse than one that never runs, since the operator cannot
tell which happened.

The contract with the caller is deliberately narrow: never raise, never block
the transfer from being marked complete, and always return a summary that says
plainly what ran. A transformation failure is a real problem, but the rows are
already at the destination — reporting the load as failed would be a lie, and
silently swallowing the failure would be worse.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def run_post_load_transforms(
    *,
    destination: Any,
    landed_table: str,
    workspace_id: str = "",
    dest_cfg: dict[str, Any] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute every project triggered by this transfer.

    Returns a summary for ``dest_summary["transformations"]``. Shape is stable
    whether or not anything ran, so the UI never has to special-case absence:

    ``{"ran": bool, "status": str, "projects": [...], "message": str}``

    Status values: ``skipped`` (nothing configured or matched), ``success``,
    ``partial`` (some models or data tests failed), ``failed``.
    """
    summary: dict[str, Any] = {
        "ran": False,
        "status": "skipped",
        "projects": [],
        "message": "",
    }

    try:
        from services.transform_store import get_transform_store

        store = get_transform_store()
        projects = [
            p for p in store.list(workspace_id) if p.triggered_by(landed_table)
        ]
    except Exception as exc:
        # An unreadable project store must not take the transfer down with it.
        logger.warning("Could not load transformation projects: %s", exc, exc_info=exc)
        summary["status"] = "failed"
        summary["message"] = f"Transformation projects could not be loaded: {exc}"
        return summary

    if not projects:
        summary["message"] = "No transformation project is configured for this table."
        return summary

    connector_id = _destination_connector_id(destination)
    # A missing connector_id on the transfer (inline credentials, no saved
    # connector) must NOT be treated as a wildcard. That would run every
    # trigger-matched project against whatever warehouse the transfer just
    # wrote to — including projects pointed at a different saved connector.
    # Only projects that either declare no destination or match exactly run.
    if not connector_id:
        runnable = [p for p in projects if not p.destination_connector_id]
        if not runnable:
            summary["status"] = "skipped"
            summary["message"] = (
                f"{len(projects)} project(s) match this table, but the transfer "
                "has no saved destination connector id to match against. "
                "Bind the transfer to a saved connector, or clear the project's "
                "destination_connector_id to opt into any landing warehouse."
            )
            return summary
    else:
        runnable = [
            p
            for p in projects
            if not p.destination_connector_id
            or p.destination_connector_id == connector_id
        ]
        if not runnable:
            summary["status"] = "skipped"
            summary["message"] = (
                f"{len(projects)} project(s) match this table but target a "
                "different destination connector."
            )
            return summary

    statuses: list[str] = []
    for project in runnable:
        entry = _run_one_project(project, destination, dest_cfg, dry_run=dry_run)
        summary["projects"].append(entry)
        statuses.append(entry.get("status") or "failed")

    summary["ran"] = True
    summary["status"] = _rollup(statuses)
    summary["message"] = _describe(summary["projects"], summary["status"])
    return summary


def _run_one_project(
    project: Any,
    destination: Any,
    dest_cfg: dict[str, Any] | None,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "project_id": project.id,
        "project_name": project.name,
        "status": "failed",
        "models": [],
        "message": "",
    }
    try:
        cfg = dest_cfg
        if cfg is None:
            from src.transfer.adapters import resolve_connector_config

            cfg = resolve_connector_config(destination)

        from src.transfer.connector_capabilities import resolve_driver_type
        from services.transform_runner import TransformRunner

        dialect = resolve_driver_type(cfg.get("type") or getattr(destination, "format", "") or "")
        runner = TransformRunner(
            cfg,
            dialect=dialect,
            schema=project.schema or cfg.get("schema") or "",
            dry_run=dry_run,
        )
        result = runner.run(list(project.models))
        payload = result.to_dict()
        entry.update(
            {
                "status": payload["status"],
                "models": payload["models"],
                "seconds": payload["seconds"],
                "plan": payload["plan"],
                "warnings": payload["warnings"],
                "failed_model_count": payload["failed_model_count"],
                "failed_test_count": payload["failed_test_count"],
                "message": payload["error"],
            }
        )
    except Exception as exc:
        # Covers plan-time errors (cycles, bad models) and connection failures.
        entry["status"] = "failed"
        entry["message"] = str(exc)
        logger.warning(
            "Transformation project '%s' failed: %s", project.name, exc, exc_info=exc
        )
    return entry


def _destination_connector_id(destination: Any) -> str:
    for attr in ("connector_id", "saved_connector_id", "id"):
        value = getattr(destination, attr, "")
        if value:
            return str(value)
    return ""


def _rollup(statuses: list[str]) -> str:
    if not statuses:
        return "skipped"
    if all(s == "success" for s in statuses):
        return "success"
    if all(s == "failed" for s in statuses):
        return "failed"
    if all(s == "skipped" for s in statuses):
        return "skipped"
    return "partial"


def _describe(projects: list[dict[str, Any]], status: str) -> str:
    total_models = sum(len(p.get("models") or []) for p in projects)
    failed_models = sum(int(p.get("failed_model_count") or 0) for p in projects)
    failed_tests = sum(int(p.get("failed_test_count") or 0) for p in projects)

    if status == "success":
        return (
            f"{total_models} transformation model(s) across "
            f"{len(projects)} project(s) built successfully."
        )
    parts = []
    if failed_models:
        parts.append(f"{failed_models} model(s) failed")
    if failed_tests:
        parts.append(f"{failed_tests} data test(s) failed")
    if not parts:
        first = next((p.get("message") for p in projects if p.get("message")), "")
        parts.append(first or "transformation did not complete")
    return (
        "Data landed successfully, but " + " and ".join(parts) + ". "
        "The destination tables are written; the derived models are not current."
    )
