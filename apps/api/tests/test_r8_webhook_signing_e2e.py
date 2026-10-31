"""Webhook notifications are HMAC-SHA256 signed (QA enterprise #46).

QA (production 557f556c): a delivered webhook carried no signature header, so
a receiver could not tell Datawrap from anyone who learned the URL. Root
cause: ``notification_service._send_webhook`` posted the JSON with only the
admin's custom headers; channels had no secret at all.

Through the real routes: create a webhook channel (secret shown once), send
the channel test, verify the received signature over the raw body; rotate
and see the old secret stop verifying; reads never return the secret.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from r8_ent_harness import ent  # noqa: F401


@pytest.fixture
def receiver(monkeypatch):
    received: list[dict] = []

    class Hook(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            received.append({"headers": {k.lower(): v for k, v in self.headers.items()}, "body": body})
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            return

    server = HTTPServer(("127.0.0.1", 0), Hook)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    from services import notification_service

    # The SSRF guard refuses loopback; this test is about the signature only.
    monkeypatch.setattr(notification_service, "_allowed_url", lambda url: True)
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/hook", received
    finally:
        server.shutdown()


def _verify(secret: str, delivery: dict) -> bool:
    from services.webhook_signing import verify

    return verify(secret, delivery["body"], delivery["headers"].get("x-datawrap-signature", ""))


def test_webhook_deliveries_are_signed_and_the_secret_rotates(ent, receiver):
    url, received = receiver
    ws = ent.workspace("Webhook signing")
    admin = ent.member(email="hook-admin-r8@example.com", workspace_id=ws, role="admin")
    created = admin.post(
        "/api/v1/workspace/notifications",
        json={"workspace_id": ws, "kind": "webhook", "label": "signed",
              "config": {"url": url, "headers": {"X-Datawrap-Signature": "forged"}}},
    )
    assert created.status_code == 200, created.text
    secret = created.json()["signing_secret"]
    assert secret.startswith("whsec_") and created.json()["signing"]["shown_once"] is True
    channel_id = created.json()["id"]

    # Never readable again.
    listed = admin.get(f"/api/v1/workspace/notifications?workspace_id={ws}")
    assert secret not in listed.text and "signing_secret\"" not in listed.text.replace("signing_secret_set", "")
    stored = (ent.tmp / "notifications.json").read_text()
    assert secret not in stored

    sent = admin.post(f"/api/v1/workspace/notifications/{channel_id}/test")
    assert sent.status_code == 200 and sent.json()["success"] is True, sent.text
    delivery = received[-1]
    ts = delivery["headers"]["x-datawrap-timestamp"]
    assert delivery["headers"]["x-datawrap-signature"].startswith(f"t={ts},v1=")
    assert _verify(secret, delivery)
    assert json.loads(delivery["body"])["job_id"] == "test"
    tampered = {**delivery, "body": delivery["body"].replace(b"42", b"43")}
    assert not _verify(secret, tampered)

    # A config edit keeps the secret.
    assert admin.patch(f"/api/v1/workspace/notifications/{channel_id}",
                       json={"config": {"url": url}}).status_code == 200
    admin.post(f"/api/v1/workspace/notifications/{channel_id}/test")
    assert _verify(secret, received[-1])

    rotated = admin.post(f"/api/v1/workspace/notifications/{channel_id}/signing-secret/rotate")
    assert rotated.status_code == 200, rotated.text
    new_secret = rotated.json()["signing_secret"]
    assert new_secret != secret
    admin.post(f"/api/v1/workspace/notifications/{channel_id}/test")
    assert _verify(new_secret, received[-1])
    assert not _verify(secret, received[-1])

    viewer = ent.member(email="hook-viewer-r8@example.com", workspace_id=ws, role="viewer")
    assert viewer.post(f"/api/v1/workspace/notifications/{channel_id}/signing-secret/rotate").status_code == 403
    rot_events = ent.audit(ws, action="workspace.notification.signing_secret.rotate")
    assert {e["outcome"] for e in rot_events} == {"succeeded", "failed"}
    assert new_secret not in json.dumps(rot_events)


def test_replayed_signature_is_rejected_by_the_reference_verifier():
    from services.webhook_signing import signature_headers, verify

    body = b'{"a":1}'
    old = signature_headers("whsec_x", body, now=1_000_000)
    assert verify("whsec_x", body, old["X-Datawrap-Signature"], now=1_000_100)
    assert not verify("whsec_x", body, old["X-Datawrap-Signature"], now=1_000_000 + 301)
