"""Unit tests for team/workspace RBAC store."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from services.team_store import (
    add_workspace_member,
    can_read_workspace,
    can_write_workspace,
    create_workspace,
    get_workspace,
    get_workspace_role,
    list_workspaces_for_user,
    remove_workspace_member,
)


def test_create_workspace_adds_owner(tmp_path, monkeypatch):
    store = tmp_path / "teams.json"
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(store))
    ws = create_workspace(name="Data Team", created_by="admin@example.com")
    assert ws.id
    assert ws.name == "Data Team"
    assert get_workspace_role(workspace_id=ws.id, email="admin@example.com") == "owner"
    assert len(list_workspaces_for_user("admin@example.com")) == 1


def test_member_permissions(tmp_path, monkeypatch):
    store = tmp_path / "teams.json"
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(store))
    ws = create_workspace(name="Engineering", created_by="admin@example.com")
    add_workspace_member(workspace_id=ws.id, email="editor@example.com", role="editor", added_by="admin@example.com")
    add_workspace_member(workspace_id=ws.id, email="viewer@example.com", role="viewer", added_by="editor@example.com")

    assert can_read_workspace(ws.id, "viewer@example.com") is True
    assert can_write_workspace(ws.id, "viewer@example.com") is False
    assert can_write_workspace(ws.id, "editor@example.com") is True
    assert can_write_workspace(ws.id, "admin@example.com") is True


def test_editor_cannot_remove_owner(tmp_path, monkeypatch):
    store = tmp_path / "teams.json"
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(store))
    ws = create_workspace(name="Finance", created_by="admin@example.com")
    add_workspace_member(workspace_id=ws.id, email="finance@example.com", role="editor", added_by="admin@example.com")
    assert remove_workspace_member(workspace_id=ws.id, email="admin@example.com", removed_by="finance@example.com") is False
    assert get_workspace(ws.id) is not None
