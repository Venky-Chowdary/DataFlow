"""list_jobs defaults to the same population as the workspace brief."""

from __future__ import annotations

import pytest

from services.job_list_scope import job_list_workspace_scope


def test_workspace_scope_matches_the_brief_and_all_is_explicit() -> None:
    assert job_list_workspace_scope(None) == ""
    assert job_list_workspace_scope("workspace") == ""
    assert job_list_workspace_scope("this") == ""
    assert job_list_workspace_scope("all") is None
    assert job_list_workspace_scope("all_workspaces") is None
    with pytest.raises(ValueError):
        job_list_workspace_scope("tenant-b")
