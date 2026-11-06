"""hash_pii: the refusal names the operator's action, and an admin can set the key (QA browser 2 / security 13).

Production 557f556c refused ``hash_pii`` with internal configuration in the
message (env var names, the hosting provider) and no way for a tenant to make
the transform work. Round 8 gave each workspace a generated key; what was still
missing:

* the refusal said "ask your Datawrap administrator" without saying what that
  administrator does — it now names Settings → Security;
* an admin had no way to *set* the key — to keep digests another system already
  produced, or to recover a stored key that can no longer be read. ``PUT
  /api/v1/workspace/pii-hash-key`` stores it through the secret vault (a new
  version; never returned, logged or audited as material).

Through the real HTTP routes (``r8_security_world``), on MongoDB when it is up.
"""

from __future__ import annotations

import hashlib
import hmac
import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import world  # noqa: E402,F401

RECIPE = {"version": 1, "steps": [{"op": "hash_pii", "column": "email"}]}
LEAKS = ("PII_HASH_KEY", "DATAFLOW_", "DATAWRAP_", "Railway", "environment", "mongo", ".json")
ADMIN_KEY = "4f1c9a7e2b8d03c5e6a1f9b7d2c4e8a0-workspace-digest-key"


@pytest.fixture
def keyed(world, monkeypatch):
    for name in ("DATAFLOW_PII_HASH_KEY", "DATAWRAP_PII_HASH_KEY", "DATAFLOW_SECRET", "DATAWRAP_SECRET"):
        monkeypatch.delenv(name, raising=False)
    from services import pii_hash_keys

    if world.db is not None:
        monkeypatch.setattr(pii_hash_keys, "_collection", lambda: world.db[pii_hash_keys.COLLECTION])
    else:
        monkeypatch.setattr(pii_hash_keys, "_collection", lambda: None)
    return world


def _preview(c, value: str = "ada@example.com"):
    return c.post(
        "/api/v1/shape/preview",
        json={"recipe": RECIPE, "sample_rows": [{"email": value}], "source_columns": ["email"]},
    )


def _expected(key: str, value: str) -> str:
    return hmac.new(key.encode(), value.encode(), hashlib.sha256).hexdigest()[:32]


def test_refusal_names_the_admin_action_and_no_internals(keyed, monkeypatch):
    from services import pii_hash_keys

    def broken():
        raise RuntimeError("mongo down at railway.internal /data/pii_hash_keys.json")

    monkeypatch.setattr(pii_hash_keys, "_collection", lambda: broken())
    text = _preview(keyed.owner).text
    assert "hashing key" in text, text
    # The operator is told what to do and where — not only "ask someone".
    assert "Settings → Security" in text, text
    assert "admin" in text.lower(), text
    for leak in LEAKS + ("railway.internal",):
        assert leak.lower() not in text.lower(), (leak, text)


def test_admin_sets_the_workspace_key_and_digests_follow_it(keyed):
    world = keyed
    generated = _preview(world.owner).json()["after"][0]["email"]

    for role in ("viewer", "editor"):
        key = world.as_key(world.mint_key(role))
        refused = key.put("/api/v1/workspace/pii-hash-key", json={"key": ADMIN_KEY})
        assert refused.status_code == 403, refused.text

    response = world.owner.put("/api/v1/workspace/pii-hash-key", json={"key": ADMIN_KEY})
    assert response.status_code == 200, response.text
    status = response.json()
    assert status["active_version"] == 2 and status["set_by_admin"] is True
    assert status["managed_by_deployment"] is False
    assert status["versions"][0]["retired_at"], "the generated key stays, marked retired"
    assert ADMIN_KEY not in response.text and "key_enc" not in response.text

    after = _preview(world.owner).json()["after"][0]["email"]
    assert after == _expected(ADMIN_KEY, "ada@example.com")
    assert after != generated
    # Readable back through GET without key material.
    read = world.owner.get("/api/v1/workspace/pii-hash-key")
    assert read.status_code == 200 and read.json()["active_version"] == 2
    assert ADMIN_KEY not in read.text

    events = world.audit("workspace.pii_hash_key.set")
    assert events
    assert ADMIN_KEY not in repr(events)
    if world.db is not None:
        stored = repr(list(world.db["workspace_pii_hash_keys"].find({})))
        assert ADMIN_KEY not in stored and "enc:" in stored


def test_admin_key_recovers_a_workspace_whose_stored_key_cannot_be_read(keyed, monkeypatch):
    world = keyed
    from services import pii_hash_keys

    assert _preview(world.owner).status_code == 200  # version 1 generated
    real = pii_hash_keys._active_secret
    monkeypatch.setattr(pii_hash_keys, "_active_secret", lambda doc: "")
    refused = _preview(world.owner)
    assert "Settings → Security" in refused.text
    monkeypatch.setattr(pii_hash_keys, "_active_secret", real)

    assert world.owner.put("/api/v1/workspace/pii-hash-key", json={"key": ADMIN_KEY}).status_code == 200
    assert _preview(world.owner).json()["after"][0]["email"] == _expected(ADMIN_KEY, "ada@example.com")


@pytest.mark.parametrize("bad", ["short", " " + ADMIN_KEY, "a" * 40])
def test_weak_or_malformed_keys_are_refused_with_a_reason(keyed, bad):
    response = keyed.owner.put("/api/v1/workspace/pii-hash-key", json={"key": bad})
    assert response.status_code == 422, response.text
    assert "hashing key" in response.text
    assert bad.strip() not in response.text or len(bad.strip()) < 8


def test_a_deployment_pinned_key_is_reported_not_overridden(keyed, monkeypatch):
    monkeypatch.setenv("DATAWRAP_PII_HASH_KEY", "deployment-pinned-key-0123456789abcdef")
    status = keyed.owner.get("/api/v1/workspace/pii-hash-key").json()
    assert status["managed_by_deployment"] is True
    response = keyed.owner.put("/api/v1/workspace/pii-hash-key", json={"key": ADMIN_KEY})
    assert response.status_code == 409, response.text
    for leak in LEAKS:
        assert leak.lower() not in response.text.lower(), (leak, response.text)
