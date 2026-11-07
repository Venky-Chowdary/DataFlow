"""Workspace scope for job censuses.

``workspace`` is the workspace the turn acts in. ``all`` is every workspace
and must be requested by name. ``unassigned`` is the legacy rows with no
workspace — what a platform admin can read and claim; anyone else's
``unassigned`` is empty, because it is intersected with what they may read
(D219).
"""

from __future__ import annotations

from services.workspace_filter import UNASSIGNED_WORKSPACE


def job_list_workspace_scope(scope: str | None) -> str | None:
    token = (scope or "workspace").strip().lower().replace("-", "_")
    if token in {"all", "all_workspaces"}:
        return None
    if token in {"", "workspace", "this"}:
        return ""
    if token in {"unassigned", "legacy"}:
        return UNASSIGNED_WORKSPACE
    raise ValueError("scope must be 'workspace', 'all' or 'unassigned'")
