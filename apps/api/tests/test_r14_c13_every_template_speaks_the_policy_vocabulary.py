"""QA C13 — every shipped Transform-Library template runs on the policy interface.

QA saw two template steps carrying ``on_error="fail"``, a word the transfer's
error-policy interface (refuse / divert / null) did not accept. Round 11 (T9)
made ``fail`` the library spelling of ``refuse``; this guard loads EVERY
shipped template (not one), and for each step proves:

* the library stores a word from its own vocabulary (``ON_ERROR_MODES``);
* the policy interface (``canonical_on_error``) accepts that word;
* the clone compiles to the transfer's shape payload, and that payload's
  steps carry only policy words (refuse is the default, so it is omitted);
* ``/shape/validate`` accepts the compiled recipe for the declared inputs.

A new template that ships a word the policy interface refuses fails here.
"""

from __future__ import annotations

import pytest

from tests import r4_route_harness as h


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    return TestClient(app)


def test_every_template_step_is_a_policy_word():
    from services.shape_models import _ERROR_POLICIES, canonical_on_error
    from services.transform_recipe import ON_ERROR_MODES
    from services.transform_recipe_templates import TEMPLATES, clone_template

    assert TEMPLATES, "no templates shipped"
    for key, spec in TEMPLATES.items():
        for i, raw in enumerate(spec["steps"]):
            # A template that spells a policy spells it in the policy words.
            if "on_error" in raw:
                assert canonical_on_error(raw["on_error"], where=f"{key} step {i}") in _ERROR_POLICIES
        recipe = clone_template(key)
        assert recipe is not None, key
        assert not recipe.check(), (key, recipe.check())
        for i, step in enumerate(recipe.steps):
            assert step.on_error in ON_ERROR_MODES, (key, i, step.on_error)
            assert canonical_on_error(step.on_error, where=f"{key} step {i}") in _ERROR_POLICIES


def test_every_template_clones_compiles_and_validates_through_rest(client):
    from services.shape_models import _ERROR_POLICIES
    from services.transform_recipe_templates import TEMPLATES

    listed = client.get("/api/v1/transform-library/templates")
    assert listed.status_code == 200, listed.text
    assert {t["key"] for t in listed.json()} == set(TEMPLATES)
    for key, spec in TEMPLATES.items():
        cloned = client.post(f"/api/v1/transform-library/templates/{key}/clone")
        assert cloned.status_code == 201, (key, cloned.text)
        body = cloned.json()
        columns = list(spec.get("input_columns") or [])
        compiled = client.post(
            f"/api/v1/transform-library/{body['id']}/compile",
            json={"source_columns": columns},
        )
        assert compiled.status_code == 200, (key, compiled.text)
        shape = compiled.json()["shape_recipe"]
        for i, step in enumerate(shape["steps"]):
            assert step.get("on_error", "refuse") in _ERROR_POLICIES, (key, i, step)
        valid = client.post(
            "/api/v1/shape/validate",
            json={"recipe": shape, "source_columns": columns},
        )
        assert valid.status_code == 200, (key, valid.text)
        assert valid.json()["recipe_hash"] == compiled.json()["recipe_hash"], key
