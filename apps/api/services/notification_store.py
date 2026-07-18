"""Persisted workspace notification channels for job alerts."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir
from services.secret_vault import decrypt_secret, encrypt_secret
from services.value_serializer import json_default

STORE_PATH = data_dir() / "notifications.json"

_ALLOWED_KINDS = {"slack", "teams", "email", "servicenow", "webhook"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _store_path() -> Path:
    env = os.getenv("DATAFLOW_NOTIFICATION_STORE", "").strip()
    return Path(env) if env else STORE_PATH


@dataclass
class NotificationChannel:
    id: str
    workspace_id: str
    kind: str
    label: str
    enabled: bool = True
    config: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    created_by: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NotificationChannel":
        return cls(
            id=data.get("id", ""),
            workspace_id=data.get("workspace_id", ""),
            kind=data.get("kind", ""),
            label=data.get("label", ""),
            enabled=bool(data.get("enabled", True)),
            config=data.get("config", {}) or {},
            created_at=data.get("created_at", _now()),
            updated_at=data.get("updated_at", _now()),
            created_by=data.get("created_by", ""),
        )


def _load_raw() -> dict[str, Any]:
    path = _store_path()
    if not path.exists():
        return {"channels": []}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {"channels": []}
        return raw
    except Exception:
        return {"channels": []}


def _save(data: dict[str, Any]) -> None:
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=json_default), encoding="utf-8")
    tmp.replace(path)


def _channels(data: dict[str, Any]) -> list[NotificationChannel]:
    return [NotificationChannel.from_dict(c) for c in data.get("channels", []) if isinstance(c, dict)]


def _encrypt_config(config: dict[str, Any]) -> dict[str, Any]:
    """Encrypt sensitive string values in a channel config."""
    out = dict(config)
    for key in ("token", "password", "api_key", "client_secret", "webhook_url"):
        if key in out and isinstance(out[key], str) and out[key]:
            out[key] = encrypt_secret(out[key])
    return out


def _decrypt_config(config: dict[str, Any]) -> dict[str, Any]:
    out = dict(config)
    for key in ("token", "password", "api_key", "client_secret", "webhook_url"):
        if key in out and isinstance(out[key], str) and out[key]:
            out[key] = decrypt_secret(out[key])
    return out


def create_channel(
    *,
    workspace_id: str,
    kind: str,
    label: str,
    enabled: bool = True,
    config: dict[str, Any] | None = None,
    created_by: str = "",
) -> NotificationChannel:
    kind = kind.lower().strip()
    if kind not in _ALLOWED_KINDS:
        raise ValueError(f"Unsupported notification kind: {kind}")
    data = _load_raw()
    channel = NotificationChannel(
        id=str(uuid.uuid4()),
        workspace_id=workspace_id,
        kind=kind,
        label=label.strip()[:128] or kind,
        enabled=enabled,
        config=_encrypt_config(config or {}),
        created_at=_now(),
        updated_at=_now(),
        created_by=created_by,
    )
    data["channels"].append(channel.to_dict())
    _save(data)
    return channel


def list_channels(workspace_id: str | None = None, enabled_only: bool = False) -> list[NotificationChannel]:
    channels = _channels(_load_raw())
    if workspace_id is not None:
        channels = [c for c in channels if c.workspace_id == workspace_id]
    if enabled_only:
        channels = [c for c in channels if c.enabled]
    return channels


def get_channel(channel_id: str) -> NotificationChannel | None:
    for c in _channels(_load_raw()):
        if c.id == channel_id:
            return c
    return None


def get_channel_decrypted(channel_id: str) -> NotificationChannel | None:
    channel = get_channel(channel_id)
    if channel:
        channel.config = _decrypt_config(channel.config)
    return channel


def update_channel(channel_id: str, *, updates: dict[str, Any]) -> NotificationChannel | None:
    data = _load_raw()
    for i, raw in enumerate(data.get("channels", [])):
        if raw.get("id") == channel_id:
            if "config" in updates and isinstance(updates["config"], dict):
                raw["config"] = _encrypt_config(updates["config"])
            for key in ("label", "enabled", "kind"):
                if key in updates:
                    raw[key] = updates[key]
            raw["updated_at"] = _now()
            _save(data)
            return NotificationChannel.from_dict(raw)
    return None


def delete_channel(channel_id: str) -> bool:
    data = _load_raw()
    before = len(data.get("channels", []))
    data["channels"] = [c for c in data.get("channels", []) if c.get("id") != channel_id]
    if len(data["channels"]) == before:
        return False
    _save(data)
    return True
