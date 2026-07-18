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
import os
import urllib.parse
from typing import Any

from services.notification_store import (
    NotificationChannel,
    get_channel_decrypted,
    list_channels,
)
from services.value_serializer import json_default

logger = logging.getLogger(__name__)


def _env_smtp() -> dict[str, Any]:
    return {
        "host": os.getenv("DATAFLOW_SMTP_HOST", ""),
        "port": int(os.getenv("DATAFLOW_SMTP_PORT", "587")),
        "user": os.getenv("DATAFLOW_SMTP_USER", ""),
        "password": os.getenv("DATAFLOW_SMTP_PASSWORD", ""),
        "use_tls": os.getenv("DATAFLOW_SMTP_USE_TLS", "true").lower() in ("1", "true", "yes"),
        "from": os.getenv("DATAFLOW_SMTP_FROM", "dataflow@localhost"),
    }


def _http_post(url: str, payload: dict[str, Any], headers: dict[str, str] | None = None) -> dict[str, Any]:
    try:
        import urllib.request

        data = json.dumps(payload, default=json_default).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", **(headers or {})},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        return {"ok": True, "status": resp.status, "body": body[:500]}
    except Exception as exc:
        logger.warning("HTTP POST to %s failed: %s", url, exc)
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


def _send_resend(recipients: list[str], subject: str, body: str, from_addr: str, api_key: str) -> dict[str, Any]:
    payload = {"from": from_addr, "to": recipients, "subject": subject, "text": body}
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
    data = urllib.parse.urlencode({"from": from_addr, "to": ",".join(recipients), "subject": subject, "text": body}).encode("utf-8")
    import base64
    credentials = base64.b64encode(f"api:{api_key}".encode()).decode()
    req = urllib.request.Request(url, data=data, headers={"Authorization": f"Basic {credentials}"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp_body = resp.read().decode("utf-8", errors="ignore")
        return {"ok": True, "status": resp.status, "body": resp_body[:500]}
    except Exception as exc:
        logger.warning("Mailgun send failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def _smpt_send(recipients: list[str], subject: str, body: str, smtp_cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = smtp_cfg or _env_smtp()
    host = cfg.get("host") or _env_smtp().get("host")
    port = int(cfg.get("port") or _env_smtp().get("port"))
    user = cfg.get("user") or _env_smtp().get("user")
    password = cfg.get("password") or _env_smtp().get("password")
    use_tls = cfg.get("use_tls", True)
    from_addr = cfg.get("from") or _env_smtp().get("from")

    if not host:
        return {"ok": False, "error": "SMTP host not configured"}

    try:
        import smtplib
        from email.message import EmailMessage

        msg = EmailMessage()
        msg["From"] = from_addr
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.set_content(body)

        with smtplib.SMTP(host, port, timeout=30) as server:
            if use_tls:
                server.starttls()
            if user:
                server.login(user, password)
            server.send_message(msg)
        return {"ok": True}
    except Exception as exc:
        logger.warning("SMTP send failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def _send_slack(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    url = channel.config.get("webhook_url") or channel.config.get("url", "")
    if not url:
        return {"ok": False, "error": "Slack webhook URL missing"}
    text = payload.get("text") or _payload_text(payload)
    return _http_post(url, {"text": text})


def _send_teams(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    url = channel.config.get("webhook_url") or channel.config.get("url", "")
    if not url:
        return {"ok": False, "error": "Teams webhook URL missing"}
    text = payload.get("text") or _payload_text(payload)
    return _http_post(
        url,
        {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": payload.get("color", "0076D7"),
            "summary": payload.get("title", "DataFlow alert"),
            "sections": [{"activityTitle": payload.get("title", ""), "text": text}],
        },
    )


def _send_email(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    recipients = channel.config.get("recipients") or channel.config.get("to", "")
    if isinstance(recipients, str):
        recipients = [r.strip() for r in recipients.split(",") if r.strip()]
    if not recipients:
        return {"ok": False, "error": "Email recipients missing"}

    text = payload.get("text") or _payload_text(payload)
    subject = payload.get("title", "DataFlow alert")

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
        "short_description": payload.get("title", "DataFlow alert"),
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
    return _http_post(url, payload, headers or {})


def _payload_text(payload: dict[str, Any]) -> str:
    return payload.get("text") or json.dumps(payload, default=json_default, indent=2)


def send_to_channel(channel: NotificationChannel, payload: dict[str, Any]) -> dict[str, Any]:
    if not channel.enabled:
        return {"ok": False, "error": "Channel disabled"}
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
    title = f"DataFlow transfer {status}: {source} → {destination}"
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
