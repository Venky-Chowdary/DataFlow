"""Studio flags default off and round-trip through the workspace store."""

from services import workspace_store


def test_studio_flags_default_off(tmp_path, monkeypatch):
    monkeypatch.setattr(workspace_store, "STORE_PATH", tmp_path / "workspace.json")
    settings = workspace_store.get_workspace_settings()
    assert settings["studio_schema_browser"] is False
    assert settings["studio_canvas"] is False


def test_studio_flags_round_trip_without_clearing_the_org(tmp_path, monkeypatch):
    monkeypatch.setattr(workspace_store, "STORE_PATH", tmp_path / "workspace.json")
    workspace_store.update_workspace_settings(org_name="Northwind", actor="admin@example.com")
    updated = workspace_store.update_workspace_settings(studio_schema_browser=True)
    assert updated["org_name"] == "Northwind"
    assert updated["studio_schema_browser"] is True
    assert updated["studio_canvas"] is False
    again = workspace_store.update_workspace_settings(studio_canvas=True, studio_schema_browser=False)
    assert again["studio_canvas"] is True
    assert again["studio_schema_browser"] is False
