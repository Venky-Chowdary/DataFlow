"""A failed Confirm says what failed, never the driver/host text, and releases the ack."""

from __future__ import annotations

from fastapi.testclient import TestClient


class _BrokenLibrary:
    def save(self, recipe):
        raise RuntimeError("password authentication failed for user admin at 10.0.0.5:5432")


def test_failed_transform_save_hides_internals_and_releases_the_approval(monkeypatch):
    import services.transform_library as transform_library
    from services.transform_recipe_templates import TEMPLATES
    from src.ai.copilot.ack_ledger import get_ack_ledger
    from src.main import app

    ack_id = get_ack_ledger().put(
        kind="create_transform_recipe",
        payload={"template_key": next(iter(TEMPLATES))},
        preview={"label": "Create transform"},
    )
    monkeypatch.setattr(transform_library, "get_transform_library", lambda *a, **k: _BrokenLibrary())

    with TestClient(app) as client:
        r = client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id, "reason": "ok"})
        assert r.status_code == 400
        assert "password" not in r.text and "10.0.0.5" not in r.text
        assert "Failed to save transform" in r.text
        assert "press Confirm again" in r.text

        class _Saved:
            id, name, steps = "rcp-1", "recipe", [1, 2]

        class _GoodLibrary:
            def save(self, recipe):
                return _Saved()

        monkeypatch.setattr(transform_library, "get_transform_library", lambda *a, **k: _GoodLibrary())
        again = client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id, "reason": "ok"})
    assert again.status_code == 200, again.text
    assert again.json()["recipe_id"] == "rcp-1"
