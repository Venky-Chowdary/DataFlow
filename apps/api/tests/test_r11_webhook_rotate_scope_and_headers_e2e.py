"""Webhook channels (QA handover, build 9c75ada4; RCA "Webhook signing-secret rotate").

* ``POST /workspace/notifications/{id}/signing-secret/rotate`` checked write
  access only when the channel had a workspace and never ran
  ``_assert_channel_visible`` (src/routers/workspace_router.py): any workspace
  admin who knew a deployment-wide channel's id received its new signing
  secret. PATCH/DELETE/test already used the visibility + scope rules; rotate
  now uses the same two.
* ``config.headers`` (custom auth headers a receiver needs) were stored and
  returned in plaintext: ``notification_store._encrypt_config`` encrypted named
  keys only. They are now encrypted at rest, masked on every read, preserved
  when a client sends the masked value back, sent in clear to the receiver,
  and headers stored in plaintext before this change keep working.

Through the real routes on the live MongoDB (``r9_workspace_world``).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r9_workspace_world import r9  # noqa: E402,F401

HOOK = "https://hooks.example.com/r11"
SECRET_HEADER = "tok-r11-ünïcode-✓-9f2c"


def _create(client, **body):
    res = client.post("/api/v1/workspace/notifications", json={"kind": "webhook", **body})
    assert res.status_code == 200, res.text
    return res.json()


def _platform(r9):
    return r9.login("root@example.test", "Bootstrap-Admin-2026!", None)


def test_rotate_on_a_deployment_wide_channel_is_the_platform_admins_only(r9):
    root = _platform(r9)
    channel = _create(root, label="ops-wide", config={"url": HOOK})
    assert channel["workspace_id"] == ""
    path = f"/api/v1/workspace/notifications/{channel['id']}/signing-secret/rotate"

    # 9c75ada4: the Acme owner (a workspace admin) got the new secret.
    for client in (r9.owner, r9.bravo()):
        res = client.post(path)
        assert res.status_code == 404, res.text
        assert "signing_secret" not in res.text
    # Same answer as PATCH gives them.
    assert r9.owner.patch(
        f"/api/v1/workspace/notifications/{channel['id']}", json={"label": "x"}
    ).status_code == 404

    ok = root.post(path)
    assert ok.status_code == 200, ok.text
    assert ok.json()["signing_secret"]


def test_rotate_on_another_workspaces_channel_is_not_found(r9):
    channel = _create(r9.owner, label="acme-hook", config={"url": HOOK})
    res = r9.bravo().post(f"/api/v1/workspace/notifications/{channel['id']}/signing-secret/rotate")
    assert res.status_code == 404, res.text
    assert r9.owner.post(
        f"/api/v1/workspace/notifications/{channel['id']}/signing-secret/rotate"
    ).status_code == 200


def _store_text(r9) -> str:
    from services import notification_store

    return notification_store._store_path().read_text(encoding="utf-8")


def _capture_sends(monkeypatch) -> list[dict]:
    from services import notification_service

    sent: list[dict] = []

    def fake_post(url, payload, headers=None, timeout=30.0, *, raw_body=None):
        sent.append({"url": url, "headers": dict(headers or {})})
        return {"ok": True, "status": 200, "body": ""}

    monkeypatch.setattr(notification_service, "_http_post", fake_post)
    return sent


def test_custom_headers_are_encrypted_at_rest_and_masked_on_read(r9, monkeypatch):
    sent = _capture_sends(monkeypatch)
    headers = {"X-Receiver-Token": SECRET_HEADER, "X-Empty": "", "X-Space": "  "}
    channel = _create(r9.owner, label="hdr", config={"url": HOOK, "headers": headers})
    cid = channel["id"]

    def assert_masked(row):
        shown = row["config"]["headers"]
        assert set(shown) == set(headers)
        assert SECRET_HEADER not in json.dumps(row)
        assert shown["X-Receiver-Token"] != SECRET_HEADER

    assert_masked(channel)
    assert SECRET_HEADER not in _store_text(r9)
    listed = r9.owner.get("/api/v1/workspace/notifications").json()["channels"]
    assert_masked(next(c for c in listed if c["id"] == cid))

    # A client that sends back what it read (the masked value) keeps the secret.
    masked = next(c for c in listed if c["id"] == cid)["config"]
    res = r9.owner.patch(f"/api/v1/workspace/notifications/{cid}", json={"config": masked})
    assert res.status_code == 200, res.text
    assert_masked(res.json())
    assert SECRET_HEADER not in _store_text(r9)

    test = r9.owner.post(f"/api/v1/workspace/notifications/{cid}/test")
    assert test.status_code == 200, test.text
    assert sent[-1]["headers"]["X-Receiver-Token"] == SECRET_HEADER
    assert "X-Datawrap-Signature" in sent[-1]["headers"] or any(
        "signature" in h.lower() for h in sent[-1]["headers"]
    )

    # Changing one header replaces only that one.
    edited = dict(masked)
    edited["headers"] = {**masked["headers"], "X-Other": "plain-two"}
    r9.owner.patch(f"/api/v1/workspace/notifications/{cid}", json={"config": edited})
    r9.owner.post(f"/api/v1/workspace/notifications/{cid}/test")
    assert sent[-1]["headers"]["X-Receiver-Token"] == SECRET_HEADER
    assert sent[-1]["headers"]["X-Other"] == "plain-two"
    assert "plain-two" not in _store_text(r9)


def test_headers_stored_in_plaintext_before_this_change_keep_working(r9, monkeypatch):
    from services import notification_store

    sent = _capture_sends(monkeypatch)
    channel = _create(r9.owner, label="legacy", config={"url": HOOK})
    data = json.loads(_store_text(r9))
    for row in data["channels"]:
        if row["id"] == channel["id"]:
            row["config"]["headers"] = {"Authorization": "Bearer legacy-plain-r11"}
    notification_store._save(data)

    listed = r9.owner.get("/api/v1/workspace/notifications").json()["channels"]
    row = next(c for c in listed if c["id"] == channel["id"])
    assert "legacy-plain-r11" not in json.dumps(row)
    assert r9.owner.post(f"/api/v1/workspace/notifications/{channel['id']}/test").status_code == 200
    assert sent[-1]["headers"]["Authorization"] == "Bearer legacy-plain-r11"

    # Any save re-encrypts it.
    r9.owner.patch(f"/api/v1/workspace/notifications/{channel['id']}", json={"config": row["config"]})
    assert "legacy-plain-r11" not in _store_text(r9)
    r9.owner.post(f"/api/v1/workspace/notifications/{channel['id']}/test")
    assert sent[-1]["headers"]["Authorization"] == "Bearer legacy-plain-r11"
