"""Workspace API keys carry a role. A blank legacy key is an editor, not a viewer."""

from __future__ import annotations

import pytest

from services.api_key_role import parse_requested_api_key_role, resolve_stored_api_key_role


def test_a_key_minted_before_roles_existed_is_an_editor() -> None:
    assert resolve_stored_api_key_role(None) == "editor"
    assert resolve_stored_api_key_role("") == "editor"
    assert resolve_stored_api_key_role("  ") == "editor"


def test_a_stored_role_is_honoured_and_an_unknown_label_is_a_viewer() -> None:
    assert resolve_stored_api_key_role("Viewer") == "viewer"
    assert resolve_stored_api_key_role("admin") == "admin"
    assert resolve_stored_api_key_role("operator") == "operator"
    assert resolve_stored_api_key_role("superuser") == "viewer"


def test_creating_a_key_defaults_to_editor_and_rejects_an_unnamed_role() -> None:
    assert parse_requested_api_key_role(None) == "editor"
    assert parse_requested_api_key_role("") == "editor"
    assert parse_requested_api_key_role(" operator ") == "operator"
    with pytest.raises(ValueError):
        parse_requested_api_key_role("owner")
