"""Send job/status alerts to configured notification channels.

Supported destinations:
- slack   : incoming webhook URL
- teams   : incoming webhook URL
- email   : platform mailer (SendGrid/Resend/Mailgun) or SMTP
- servicenow : table REST API endpoint + auth (basic or oauth)
- webhook : generic HTTP POST to a URL
"""

from __future__ import annotations

import json
import logging
import re
import time
from services.brand_env import getenv_brand
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from services.egress_guard import egress_url_allowed, host_is_blocked, redirect_target_allowed
from services.ops_events import emit_event
from services.notification_store import (
    NotificationChannel,
    get_channel_decrypted,
    list_channels,
)
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

#: Channel kinds send_to_channel dispatches; anything else logs as "unknown".
_CHANNEL_KINDS = frozenset({"slack", "teams", "email", "servicenow", "webhook"})


def _allowed_url(url: str) -> bool:
    # Webhooks stay default-deny. The connector allowlist is for on-prem
    # database dials, not for an HTTP client that would follow a private hop.
    return egress_url_allowed(url, allowlist=())


def _env_smtp() -> dict[str, Any]:
    return {
        "host": getenv_brand("SMTP_HOST", ""),
        "port": int(getenv_brand("SMTP_PORT", "587")),
        "user": getenv_brand("SMTP_USER", ""),
        "password": getenv_brand("SMTP_PASSWORD", ""),
        "use_tls": getenv_brand("SMTP_USE_TLS", "true").lower() in ("1", "true", "yes"),
        "from": getenv_brand("SMTP_FROM", "datawrap@localhost"),
    }


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Follow a public http(s) redirect. A private or metadata target is refused.

    The allowlist does not apply: a webhook that 302s onto 169.254.169.254
    stays refused even when that address would be a permitted direct dial.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        if not redirect_target_allowed(str(newurl or "")):
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)


#: Sent on every outbound HTTP call. Cloudflare-fronted APIs (Resend) refuse
#: the default ``Python-urllib/3.x`` agent with 403 "error code: 1010" before
#: the credential is even read (D130).
USER_AGENT = "Datawrap/1.0 (+https://www.datawrap.io)"

_EMAIL_ADDRESS = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_ERROR_BODY_CHARS = 500


def _redacted_error_body(raw: bytes) -> str:
    """A refusal body short enough to triage, with echoed addresses masked."""
    text = raw[: _ERROR_BODY_CHARS * 2].decode("utf-8", errors="ignore")
    return _EMAIL_ADDRESS.sub("<redacted-email>", text)[:_ERROR_BODY_CHARS]


def _http_post(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str] | None = None,
    timeout: float = 30.0,
    *,
    raw_body: bytes | None = None,
) -> dict[str, Any]:
    """POST JSON. A non-2xx answer returns ``ok: False`` with its ``status`` and
    a short redacted ``body`` so callers can tell a refusal from a transport
    failure; a transport failure carries no ``status``."""
    if not _allowed_url(url):
        return {"ok": False, "error": "Webhook URL is not allowed (private, loopback, or non-http(s))"}
    try:
        data = raw_body if raw_body is not None else json.dumps(payload, default=json_default).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT, **(headers or {})},
            method="POST",
        )
        opener = urllib.request.build_opener(_NoRedirect)
        with opener.open(req, timeout=timeout) as resp:  # nosec: B310 — scheme + host gated by egress_guard
            body = resp.read().decode("utf-8", errors="ignore")
        return {"ok": True, "status": resp.status, "body": body[:2000]}
    except urllib.error.HTTPError as exc:
        try:
            body = _redacted_error_body(exc.read() or b"")
        except Exception:  # noqa: BLE001 — an unreadable error body is still a refusal
            body = ""
        logger.warning("notification HTTP POST refused: status=%s", exc.code)
        refused: dict[str, Any] = {"ok": False, "status": int(exc.code), "body": body, "error": str(exc)}
        retry_after = (exc.headers.get("Retry-After") if exc.headers else None) or ""
        if retry_after.strip().isdigit():
            refused["retry_after_sec"] = int(retry_after.strip())
        return refused
    except Exception as exc:
        # Slack/Teams webhook URLs carry their secret in the path; the error
        # text can echo it. The class name is enough to triage.
        logger.warning("notification HTTP POST failed: %s", type(exc).__name__)
        return {"ok": False, "error": str(exc)}


def _send_sendgrid(recipients: list[str], subject: str, body: str, from_addr: str, api_key: str) -> dict[str, Any]:
    payload = {
        "personalizations": [{"to": [{"email": r} for r in recipients]}],
        "from": {"email": from_addr},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}],
    }
    result = _http_post(
        "https://api.sendgrid.com/v3/mail/send",
        payload,
        headers={"Authorization": f"Bearer {api_key}"},
    )
    if result.get("ok") and result.get("status") in (200, 202):
        return {"ok": True}
    return {"ok": False, "error": result.get("body") or result.get("error") or "SendGrid send failed"}


def _send_resend(recipients: list[str], subject: str, body: str, from_addr: str, api_key: str, reply_to: str = "") -> dict[str, Any]:
    payload = {"from": from_addr, "to": recipients, "subject": subject, "text": body}
    if reply_to:
        payload["reply_to"] = reply_to
    result = _http_post(
        "https://api.resend.com/emails",
        payload,
        headers={"Authorization": f"Bearer {api_key}"},
    )
    if result.get("ok") and result.get("status") in (200, 201, 202):
        return {"ok": True}
    return {"ok": False, "error": result.get("body") or result.get("error") or "Resend send failed"}


def _send_mailgun(recipients: list[str], subject: str, body: str, from_addr: str, api_key: str, domain: str, region: str = "us") -> dict[str, Any]:
    host = "api.mailgun.net" if region != "eu" else "api.eu.mailgun.net"
    url = f"https://{host}/v3/{domain}/messages"
    if not _allowed_url(url):
        return {"ok": False, "error": f"URL scheme not allowed: {url}"}
    data = urllib.parse.urlencode({"from": from_addr, "to": ",".join(recipients), "subject": subject, "text": body}).encode("utf-8")
    import base64
    credentials = base64.b64encode(f"api:{api_key}".encode()).decode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Authorization": f"Basic {credentials}", "User-Agent": USER_AGENT},
        method="POST",
    )
    try:
        opener = urllib.request.build_opener(_NoRedirect)
        with opener.open(req, timeout=30) as resp:  # nosec: B310 — scheme is https and validated above
            resp_body = resp.read().decode("utf-8", errors="ignore")
        return {"ok": True, "status": resp.status, "body": resp_body[:500]}
    except Exception as exc:
        logger.warning("Mailgun send failed: %s", type(exc).__name__)
        return {"ok": False, "error": str(exc)}


# SMTP connect/read ceiling — must stay under the mail outbox claim lease
# (mail_outbox._LEASE_SEC) so one send can never outlive its claim.
_SMTP_TIMEOUT_SEC = 30.0


def _smpt_send(
    recipients: list[str],
    subject: str,
    body: str,
    smtp_cfg: dict[str, Any] | None = None,
    *,
    html: str | None = None,
    reply_to: str | None = None,
) -> dict[str, Any]:
    cfg = smtp_cfg or _env_smtp()
    host = cfg.get("host") or _env_smtp().get("host")
    port = int(cfg.get("port") or _env_smtp().get("port"))
    user = cfg.get("user") or _env_smtp().get("user")
    password = cfg.get("password") or _env_smtp().get("password")
    use_tls = cfg.get("use_tls", True)
    from_addr = cfg.get("from") or _env_smtp().get("from")

    if not host:
        return {"ok": False, "error": "SMTP host not configured"}
    if host_is_blocked(str(host)):
        return {"ok": False, "error": "SMTP host is not allowed (private, loopback, or metadata)"}

    try:
        import smtplib
        from email.message import EmailMessage

        msg = EmailMessage()
        msg["From"] = from_addr
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        if reply_to:
            msg["Reply-To"] = reply_to
        msg.set_content(body)
        if html:
            msg.add_alternative(html, subtype="html")

        with smtplib.SMTP(host, port, timeout=_SMTP_TIMEOUT_SEC) as server:
            if use_tls:
                server.starttls()
            if user:
                server.login(user, password)
            server.send_message(msg)
        return {"ok": True}
    except Exception as exc:
        logger.warning("SMTP send failed: %s", type(exc).__name__)
        return {"ok": False, "error": str(exc)}


def _slack_acknowledged(result: dict[str, Any]) -> dict[str, Any]:
    """Slack's own acknowledgement, not merely "the host answered 200".

    Any endpoint that returns 2xx used to be reported as *Test message sent*, so
    a webhook pointed at the wrong host — or at an intranet page — read as a
    working alert route until the day an incident went unnoticed. An incoming
    webhook answers ``200`` with the body ``ok``; the Web API answers JSON with
    ``"ok": true``. Anything else is a delivery we cannot claim.
    """
    if not result.get("ok"):
        return result
    body = str(result.get("body", "")).strip()
    if body.lower() == "ok":
        return result
    try:
        if json.loads(body).get("ok") is True:
            return result
    except (ValueError, AttributeError):
        pass
    return {
        "ok": False,
        "status": result.get("status"),
        "error": (
            f"Endpoint answered HTTP {result.get('status')} but did not acknowledge as Slack "
            f"(expected body 'ok', got {body[:120]!r}) — check the webhook URL"
        ),
    }


def _teams_acknowledged(result: dict[str, Any]) -> dict[str, Any]:
    """Teams' own acknowledgement: ``1`` from a connector, ``202`` from a workflow."""
    if not result.get("ok"):
        return result
    body = str(result.get("body", "")).strip()
    if body == "1" or (result.get("status") == 202 and not body):
        return result
    return {
        "ok": False,
        "status": result.get("status"),
        "error": (
            f"Endpoint answered HTTP {result.get('status')} but did not acknowledge as Microsoft "
            f"Teams (expected '1' or 202 Accepted, got {body[:120]!r}) — check the webhook URL"
        ),
    }


def _send_slack(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    url = channel.config.get("webhook_url") or channel.config.get("url", "")
    if not url:
        return {"ok": False, "error": "Slack webhook URL missing"}
    text = payload.get("text") or _payload_text(payload)
    return _slack_acknowledged(_http_post(url, {"text": text}))


def _send_teams(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    url = channel.config.get("webhook_url") or channel.config.get("url", "")
    if not url:
        return {"ok": False, "error": "Teams webhook URL missing"}
    text = payload.get("text") or _payload_text(payload)
    return _teams_acknowledged(_http_post(
        url,
        {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": payload.get("color", "0076D7"),
            "summary": payload.get("title", "Datawrap alert"),
            "sections": [{"activityTitle": payload.get("title", ""), "text": text}],
        },
    ))


def _send_email(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    recipients = channel.config.get("recipients") or channel.config.get("to", "")
    if isinstance(recipients, str):
        recipients = [r.strip() for r in recipients.split(",") if r.strip()]
    if not recipients:
        return {"ok": False, "error": "Email recipients missing"}

    text = payload.get("text") or _payload_text(payload)
    subject = payload.get("title", "Datawrap alert")

    # 1. Channel-level explicit custom SMTP takes precedence.
    smtp_cfg = {
        "host": channel.config.get("smtp_host", ""),
        "port": channel.config.get("smtp_port", 0),
        "user": channel.config.get("smtp_user", ""),
        "password": channel.config.get("smtp_password", ""),
        "use_tls": channel.config.get("smtp_use_tls", True),
        "from": channel.config.get("from", ""),
    }
    from_addr = smtp_cfg.get("from") or _env_smtp().get("from")
    if smtp_cfg.get("host"):
        return _smpt_send(recipients, subject, text, smtp_cfg)

    # 2. Platform-managed transactional email provider (SaaS default).
    from services.platform_config import email_provider_config
    provider_cfg = email_provider_config()
    provider = (channel.config.get("provider") or provider_cfg.get("provider") or "smtp").lower()
    from_addr = channel.config.get("from") or provider_cfg.get("from") or _env_smtp().get("from")
    if provider == "sendgrid" and provider_cfg.get("api_key"):
        return _send_sendgrid(recipients, subject, text, from_addr, provider_cfg["api_key"])
    if provider == "resend" and provider_cfg.get("api_key"):
        return _send_resend(recipients, subject, text, from_addr, provider_cfg["api_key"])
    if provider == "mailgun" and provider_cfg.get("api_key") and provider_cfg.get("domain"):
        return _send_mailgun(recipients, subject, text, from_addr, provider_cfg["api_key"], provider_cfg["domain"], provider_cfg.get("region", "us"))

    # 3. Legacy / self-managed SMTP fallback.
    return _smpt_send(recipients, subject, text, smtp_cfg)


def _send_servicenow(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    url = channel.config.get("url") or channel.config.get("table_api", "")
    if not url:
        return {"ok": False, "error": "ServiceNow table API URL missing"}
    auth_mode = channel.config.get("auth_mode", "basic")
    headers: dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
    if auth_mode == "basic":
        import base64
        user = channel.config.get("username", "")
        pwd = channel.config.get("password", "")
        if not user:
            return {"ok": False, "error": "ServiceNow username missing"}
        token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    elif auth_mode == "oauth":
        token = channel.config.get("token", "")
        if not token:
            return {"ok": False, "error": "ServiceNow OAuth token missing"}
        headers["Authorization"] = f"Bearer {token}"

    record = {
        "short_description": payload.get("title", "Datawrap alert"),
        "description": payload.get("text") or _payload_text(payload),
        "urgency": payload.get("urgency", "3"),
        "impact": payload.get("impact", "3"),
    }
    if "correlation_id" in payload:
        record["correlation_id"] = payload["correlation_id"]
    return _http_post(url, record, headers)


def _send_webhook(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    url = channel.config.get("url", "")
    if not url:
        return {"ok": False, "error": "Webhook URL missing"}
    headers = channel.config.get("headers", {})
    if isinstance(headers, str):
        try:
            headers = json.loads(headers)
        except Exception:
            headers = {}
    from services.webhook_signing import SECRET_CONFIG_KEY, signature_headers

    body = json.dumps(payload, default=json_default).encode("utf-8")
    secret = str(channel.config.get(SECRET_CONFIG_KEY) or "")
    signed = dict(headers or {})
    if secret:
        # The signature covers exactly the bytes sent; it is added last so a
        # configured custom header can never replace it.
        signed.update(signature_headers(secret, body))
    return _http_post(url, payload, signed, raw_body=body)


def _payload_text(payload: dict[str, Any]) -> str:
    return payload.get("text") or json.dumps(payload, default=json_default, indent=2)


def send_to_channel(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    """Deliver ``payload`` and emit one ``notification.delivery.*`` event.

    The event names the channel kind and id, never the URL, recipients, or
    error text. A failed delivery is WARNING: it does not fail the job.
    """
    kind = str(channel.kind or "")
    fields = {
        "channel_kind": kind if kind in _CHANNEL_KINDS else "unknown",
        "channel_id": channel.id,
        "workspace_id": channel.workspace_id or None,
    }
    if not channel.enabled:
        emit_event(
            logger, "notification.delivery.skipped", outcome="skipped",
            reason="channel_disabled", **fields,
        )
        return {"ok": False, "error": "Channel disabled"}
    started = time.monotonic()
    try:
        result = _dispatch_to_channel(channel, payload)
    except Exception as exc:
        emit_event(
            logger, "notification.delivery.failed", outcome="failed",
            level=logging.WARNING, error=exc, reason="exception",
            duration_ms=(time.monotonic() - started) * 1000.0, **fields,
        )
        raise
    status = result.get("status") if isinstance(result, dict) else None
    ok = bool(isinstance(result, dict) and result.get("ok"))
    emit_event(
        logger,
        "notification.delivery.succeeded" if ok else "notification.delivery.failed",
        outcome="succeeded" if ok else "failed",
        level=None if ok else logging.WARNING,
        duration_ms=(time.monotonic() - started) * 1000.0,
        http_status=status if isinstance(status, int) else None,
        reason=None if ok else ("unknown_kind" if fields["channel_kind"] == "unknown" else "not_delivered"),
        **fields,
    )
    return result


def _dispatch_to_channel(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    kind = channel.kind
    if kind == "slack":
        return _send_slack(channel, payload)
    if kind == "teams":
        return _send_teams(channel, payload)
    if kind == "email":
        return _send_email(channel, payload)
    if kind == "servicenow":
        return _send_servicenow(channel, payload)
    if kind == "webhook":
        return _send_webhook(channel, payload)
    return {"ok": False, "error": f"Unknown channel kind: {kind}"}


def notify_workspace(workspace_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Broadcast a payload to every enabled channel in a workspace."""
    channels = list_channels(workspace_id=workspace_id, enabled_only=True)
    results = []
    for channel in channels:
        decrypted = get_channel_decrypted(channel.id)
        if not decrypted:
            continue
        results.append({"channel_id": channel.id, "kind": channel.kind, **send_to_channel(decrypted, payload)})
    return results


def log_job_notifications(job_id: str, results: list[dict[str, Any]]) -> bool:
    """Persist per-channel delivery results on the job record for the UI."""
    if not results:
        return False
    try:
        from datetime import datetime, timezone

        from bson import ObjectId

        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        db = mongo.get_database()
        collection = db["transfer_jobs"]
        collection.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {"notifications": results, "updated_at": datetime.now(timezone.utc)}},
        )
        return True
    except Exception:
        logger.exception("Failed to persist notification log for job=%s", job_id)
        return False


def build_job_payload(
    *,
    job_id: str,
    status: str,
    source: str,
    destination: str,
    records_transferred: int,
    rejected_rows: int,
    error: str,
    retry_url: str,
    workspace_id: str = "",
    base_url: str = "",
    web_url: str = "",
) -> dict[str, Any]:
    color = "28a745" if status in ("completed", "success") else "ffc107" if status in ("partial", "failed_with_quarantine", "completed_with_quarantine") else "dc3545"
    title = f"Datawrap transfer {status}: {source} → {destination}"
    api_base = (base_url or "").rstrip("/")
    web_base = (web_url or "").rstrip("/")
    absolute_retry_url = f"{api_base}/api/v1/connectors/jobs/{job_id}/resume" if api_base else retry_url
    job_url = f"{web_base}/jobs/{job_id}" if web_base else ""
    text_lines = [
        f"Job ID: {job_id}",
        f"Status: {status}",
        f"Records transferred: {records_transferred:,}",
    ]
    if rejected_rows:
        text_lines.append(f"Rejected/quarantined rows: {rejected_rows:,}")
    if error:
        text_lines.append(f"Error: {error}")
    if job_url:
        text_lines.append(f"Job details: {job_url}")
    if absolute_retry_url:
        text_lines.append(f"Retry/resume: {absolute_retry_url}")
    return {
        "title": title,
        "text": "\n".join(text_lines),
        "color": color,
        "job_id": job_id,
        "status": status,
        "records_transferred": records_transferred,
        "rejected_rows": rejected_rows,
        "error": error,
        "retry_url": absolute_retry_url,
        "job_url": job_url,
    }
