"""hash_pii works with no operator setup, per workspace, and never names infrastructure (QA enterprise #19).

Production 557f556c: ``shape/preview`` with ``hash_pii`` refused with "no HMAC
key is set" and told the tenant to set ``DATAFLOW_PII_HASH_KEY`` on Railway.
Each workspace now gets its own key on first use, stored encrypted, with an
admin rotation; digests are stable within a workspace and differ across them.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import PASSWORD, world  # noqa: E402,F401

RECIPE = {"version": 1, "steps": [{"op": "hash_pii", "column": "email"}]}
LEAKS = ("PII_HASH_KEY", "DATAFLOW_", "DATAWRAP_", "Railway", "openssl", "Variables", "environment")


@pytest.fixture
def keyed(world, monkeypatch):
    for name in ("DATAFLOW_PII_HASH_KEY", "DATAWRAP_PII_HASH_KEY", "DATAFLOW_SECRET", "DATAWRAP_SECRET"):
        monkeypatch.delenv(name, raising=False)
    try:
        from services import pii_hash_keys
    except ImportError:  # pre-fix build: no workspace key store; the tests fail on behaviour
        return world

    if world.db is not None:
        monkeypatch.setattr(pii_hash_keys, "_collection", lambda: world.db[pii_hash_keys.COLLECTION])
    else:
        monkeypatch.setattr(pii_hash_keys, "_collection", lambda: None)
    return world


def _digest(c, value: str = "ada@example.com") -> str:
    response = c.post(
        "/api/v1/shape/preview",
        json={"recipe": RECIPE, "sample_rows": [{"email": value}], "source_columns": ["email"]},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["refusal"] is None, body["refusal"]
    digest = body["after"][0]["email"]
    assert re.fullmatch(r"[0-9a-f]{32}", str(digest)), body["after"]
    return digest


def test_hash_pii_is_deterministic_per_workspace_without_setup(keyed):
    world = keyed
    editor = world.as_key(world.mint_key("editor"))
    first = _digest(editor)
    assert _digest(editor) == first
    assert _digest(world.owner) == first  # same workspace, other principal
    assert _digest(editor, "bob@example.com") != first

    from services import team_store

    other = team_store.create_workspace(name="Other", created_by="boss@other.test")
    world.account("boss@other.test")
    boss = world.login("boss@other.test", PASSWORD)
    boss.headers["X-Workspace-Id"] = other.id
    assert _digest(boss) != first, "two workspaces must not share digests"

    # The transfer engine runs under egress_workspace(job.workspace_id): same key.
    from services.egress_guard import egress_workspace
    from services.transform_engine import pii_hash_key
    from services.pii_hash_keys import workspace_key

    with egress_workspace(world.workspace_id):
        assert pii_hash_key() == workspace_key(world.workspace_id)

    # Stored encrypted: no plaintext key material at rest.
    from services import pii_hash_keys

    if world.db is not None:
        doc = world.db[pii_hash_keys.COLLECTION].find_one({"_id": world.workspace_id})
        stored = str(doc)
    else:
        stored = pii_hash_keys._file_path().read_text(encoding="utf-8")
    assert workspace_key(world.workspace_id) not in stored
    assert "enc:" in stored


def test_admin_rotation_changes_digests_and_exposes_no_key_material(keyed):
    world = keyed
    before = _digest(world.owner)
    for role in ("viewer", "editor"):
        key = world.as_key(world.mint_key(role))
        assert key.post("/api/v1/workspace/pii-hash-key/rotate").status_code == 403
    rotated = world.owner.post("/api/v1/workspace/pii-hash-key/rotate")
    assert rotated.status_code == 200, rotated.text
    status = rotated.json()
    assert status["active_version"] == 2
    assert [v["version"] for v in status["versions"]] == [1, 2]
    assert status["versions"][0]["retired_at"]
    from services.pii_hash_keys import workspace_key

    assert workspace_key(world.workspace_id) not in rotated.text
    assert "key_enc" not in rotated.text
    after = _digest(world.owner)
    assert after != before
    assert _digest(world.owner) == after
    assert world.audit("workspace.pii_hash_key.rotate")


def test_unavailable_key_refuses_without_naming_infrastructure(keyed, monkeypatch):
    world = keyed
    from services import pii_hash_keys

    def broken():
        raise RuntimeError("mongo down at railway.internal")

    monkeypatch.setattr(pii_hash_keys, "_collection", lambda: broken())
    response = world.owner.post(
        "/api/v1/shape/preview",
        json={"recipe": RECIPE, "sample_rows": [{"email": "ada@example.com"}], "source_columns": ["email"]},
    )
    text = response.text
    assert "hashing key" in text, text
    for leak in LEAKS + ("railway.internal", "mongo"):
        assert leak.lower() not in text.lower(), (leak, text)
