"""Workspace scope for job censuses.

``workspace`` matches the briefing: this workspace plus legacy rows that have
no workspace id. ``all`` is every workspace and must be requested by name.
"""

from __future__ import annotations


def job_list_workspace_scope(scope: str | None) -> str | None:
    token = (scope or "workspace").strip().lower().replace("-", "_")
    if token in {"all", "all_workspaces"}:
        return None
    if token in {"", "workspace", "this"}:
        return ""
    raise ValueError("scope must be 'workspace' or 'all'")
