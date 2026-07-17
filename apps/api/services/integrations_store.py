"""Persisted workspace integrations — SSO, AI provider keys, API keys."""

from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any

from services.platform_config import data_dir
from services.secret_vault import decrypt_secret, encrypt_secret

STORE_PATH = data_dir() / "integrations.json"

_SSO_TYPES = ("saml", "oidc", "azure_ad")
_CLOUD_PROVIDERS = ("openai", "anthropic")
_MASK = "••••••••"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_sso() -> dict[str, dict[str, Any]]:
    return {
        "saml": {
            "enabled": False,
            "entity_id": "",
            "sso_url": "",
            "x509_cert": "",
            "email_attribute": "email",
        },
        "oidc": {
            "enabled": False,
            "issuer": "",
            "client_id": "",
            "client_secret": "",
            "redirect_uri": "",
            "scopes": "openid email profile",
        },
        "azure_ad": {
            "enabled": False,
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            "redirect_uri": "",
        },
    }


def _default_ai() -> dict[str, dict[str, Any]]:
    return {
        "openai": {"enabled": True, "api_key": "", "model": "gpt-4o-mini"},
        "anthropic": {"enabled": True, "api_key": "", "model": "claude-sonnet-4-20250514"},
        "ollama": {"enabled": True, "api_key": "", "base_url": "http://localhost:11434", "model": "llama3.2"},
    }


def _load_raw() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {"sso": _default_sso(), "ai_providers": _default_ai(), "api_keys": []}
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("invalid store")
    except Exception:
        return {"sso": _default_sso(), "ai_providers": _default_ai(), "api_keys": []}

    sso = _default_sso()
    for key in _SSO_TYPES:
        if isinstance(raw.get("sso", {}).get(key), dict):
            sso[key].update(raw["sso"][key])

    ai = _default_ai()
    for key in ai:
        if isinstance(raw.get("ai_providers", {}).get(key), dict):
            ai[key].update(raw["ai_providers"][key])

    api_keys = raw.get("api_keys", [])
    if not isinstance(api_keys, list):
        api_keys = []

    return {"sso": sso, "ai_providers": ai, "api_keys": api_keys}


def _save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _mask_secret(value: str) -> str:
    if not value:
        return ""
    if value.startswith("enc:v1:"):
        return _MASK
    return _MASK if len(value) > 4 else _MASK


def _encrypt_field(value: str, keep_existing: str = "") -> str:
    if not value or value == _MASK:
        return keep_existing
    return encrypt_secret(value)


def apply_integrations_to_env() -> None:
    """Hydrate process env from persisted AI provider keys (env vars take precedence)."""
    import os

    data = _load_raw()
    env_map = {
        "openai": ("OPENAI_API_KEY", "OPENAI_MODEL"),
        "anthropic": ("ANTHROPIC_API_KEY", "ANTHROPIC_MODEL"),
    }
    for provider, (env_key, model_env_key) in env_map.items():
        if not os.environ.get(env_key):
            plain = resolve_provider_api_key(provider)
            if plain:
                os.environ[env_key] = plain
        model = data["ai_providers"].get(provider, {}).get("model")
        if model and not os.environ.get(model_env_key):
            os.environ[model_env_key] = str(model)

    ollama = data["ai_providers"].get("ollama", {})
    if ollama.get("base_url") and not os.environ.get("OLLAMA_BASE_URL"):
        os.environ["OLLAMA_BASE_URL"] = str(ollama["base_url"])
    if ollama.get("model") and not os.environ.get("OLLAMA_MODEL"):
        os.environ["OLLAMA_MODEL"] = str(ollama["model"])


# ── SSO ──────────────────────────────────────────────────────────────────────


def get_sso_configs() -> dict[str, dict[str, Any]]:
    data = _load_raw()
    out: dict[str, dict[str, Any]] = {}
    for sso_type in _SSO_TYPES:
        cfg = dict(data["sso"][sso_type])
        if cfg.get("client_secret"):
            cfg["client_secret"] = _mask_secret(cfg["client_secret"])
        if cfg.get("x509_cert") and len(str(cfg["x509_cert"])) > 40:
            cfg["x509_cert"] = str(cfg["x509_cert"])[:40] + "…"
        out[sso_type] = cfg
    return out


def update_sso_config(sso_type: str, patch: dict[str, Any]) -> dict[str, Any]:
    if sso_type not in _SSO_TYPES:
        raise ValueError(f"Unknown SSO type: {sso_type}")
    data = _load_raw()
    cfg = data["sso"][sso_type]
    existing_secret = cfg.get("client_secret", "")

    for key, value in patch.items():
        if key == "client_secret":
            cfg["client_secret"] = _encrypt_field(str(value or ""), existing_secret)
        elif key == "x509_cert":
            if value:
                cfg["x509_cert"] = str(value)
        elif key in cfg or key == "enabled":
            cfg[key] = value

    data["sso"][sso_type] = cfg
    data["updated_at"] = _now()
    _save(data)
    return get_sso_configs()[sso_type]


def validate_sso_config(sso_type: str) -> dict[str, Any]:
    data = _load_raw()
    cfg = data["sso"][sso_type]
    missing: list[str] = []

    if sso_type == "saml":
        for field in ("entity_id", "sso_url", "x509_cert"):
            if not str(cfg.get(field, "")).strip():
                missing.append(field)
    elif sso_type == "oidc":
        for field in ("issuer", "client_id", "client_secret", "redirect_uri"):
            if not str(cfg.get(field, "")).strip():
                missing.append(field)
    elif sso_type == "azure_ad":
        for field in ("tenant_id", "client_id", "client_secret", "redirect_uri"):
            if not str(cfg.get(field, "")).strip():
                missing.append(field)

    enabled = bool(cfg.get("enabled"))
    ready = len(missing) == 0
    return {
        "type": sso_type,
        "enabled": enabled,
        "ready": ready,
        "missing_fields": missing,
        "ok": enabled and ready,
        "message": "Configuration complete" if ready else f"Missing: {', '.join(missing)}" if missing else "Disabled",
    }


def list_sso_providers_public() -> list[dict[str, Any]]:
    data = _load_raw()
    labels = {"saml": "SAML 2.0", "oidc": "OpenID Connect", "azure_ad": "Azure AD"}
    rows = []
    for sso_type in _SSO_TYPES:
        cfg = data["sso"][sso_type]
        check = validate_sso_config(sso_type)
        if cfg.get("enabled") and check["ready"]:
            rows.append({"type": sso_type, "label": labels[sso_type], "login_path": f"/api/v1/auth/sso/{sso_type}/start"})
    return rows


def get_sso_config_raw(sso_type: str) -> dict[str, Any]:
    if sso_type not in _SSO_TYPES:
        raise ValueError(f"Unknown SSO type: {sso_type}")
    data = _load_raw()
    cfg = dict(data["sso"][sso_type])
    if cfg.get("client_secret"):
        cfg["client_secret"] = decrypt_secret(str(cfg["client_secret"]))
    return cfg


# ── AI providers ─────────────────────────────────────────────────────────────


def get_ai_provider_configs() -> dict[str, dict[str, Any]]:
    data = _load_raw()
    out: dict[str, dict[str, Any]] = {}
    for provider, cfg in data["ai_providers"].items():
        row = dict(cfg)
        key = row.get("api_key", "")
        row["configured"] = bool(key) or provider == "ollama"
        row["api_key"] = _mask_secret(key) if key else ""
        out[provider] = row
    return out


def update_ai_provider(provider: str, patch: dict[str, Any]) -> dict[str, Any]:
    if provider not in _default_ai():
        raise ValueError(f"Unknown AI provider: {provider}")
    data = _load_raw()
    cfg = data["ai_providers"][provider]
    existing_key = cfg.get("api_key", "")

    for key, value in patch.items():
        if key == "api_key":
            cfg["api_key"] = _encrypt_field(str(value or ""), existing_key)
        elif key in cfg or key == "enabled":
            cfg[key] = value

    data["ai_providers"][provider] = cfg
    data["updated_at"] = _now()
    _save(data)
    apply_integrations_to_env()
    return get_ai_provider_configs()[provider]


def resolve_provider_api_key(provider: str) -> str:
    import os

    env_map = {"openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY"}
    env_key = env_map.get(provider)
    if env_key:
        env_val = os.environ.get(env_key, "")
        if env_val and not _is_invalid_secret(env_val):
            return env_val

    data = _load_raw()
    cfg = data["ai_providers"].get(provider, {})
    if not cfg.get("enabled", True):
        return ""
    stored = cfg.get("api_key", "")
    if not stored:
        return ""
    plain = decrypt_secret(stored)
    return "" if _is_invalid_secret(plain) else plain


def _is_invalid_secret(value: str) -> bool:
    """True for masked, sentinel, or corrupted secret values."""
    if not value:
        return True
    stripped = value.strip()
    return stripped.startswith("[") or stripped.startswith("•") or stripped == _MASK


def resolve_provider_model(provider: str, default: str) -> str:
    import os

    env_map = {"openai": "OPENAI_MODEL", "anthropic": "ANTHROPIC_MODEL", "ollama": "OLLAMA_MODEL"}
    env_key = env_map.get(provider)
    if env_key and os.environ.get(env_key):
        return os.environ[env_key]
    data = _load_raw()
    return str(data["ai_providers"].get(provider, {}).get("model") or default)


def resolve_ollama_base_url(default: str = "http://localhost:11434") -> str:
    import os

    if os.environ.get("OLLAMA_BASE_URL"):
        return os.environ["OLLAMA_BASE_URL"]
    data = _load_raw()
    return str(data["ai_providers"].get("ollama", {}).get("base_url") or default)


# ── Workspace API keys ───────────────────────────────────────────────────────


def _hash_api_key(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def list_api_keys() -> list[dict[str, Any]]:
    data = _load_raw()
    rows = []
    for item in data.get("api_keys", []):
        rows.append({
            "id": item["id"],
            "name": item.get("name", "API key"),
            "prefix": item.get("prefix", "dfk_"),
            "created_at": item.get("created_at"),
            "created_by": item.get("created_by"),
            "last_used_at": item.get("last_used_at"),
        })
    return rows


def create_api_key(name: str, actor: str) -> dict[str, Any]:
    data = _load_raw()
    raw = f"dfk_{secrets.token_urlsafe(32)}"
    prefix = raw[:12]
    record = {
        "id": str(uuid.uuid4()),
        "name": name.strip()[:64] or "API key",
        "prefix": prefix,
        "key_hash": _hash_api_key(raw),
        "created_at": _now(),
        "created_by": actor,
        "last_used_at": None,
    }
    data.setdefault("api_keys", []).append(record)
    _save(data)
    return {"id": record["id"], "name": record["name"], "prefix": prefix, "key": raw, "created_at": record["created_at"]}


def revoke_api_key(key_id: str) -> bool:
    data = _load_raw()
    before = len(data.get("api_keys", []))
    data["api_keys"] = [k for k in data.get("api_keys", []) if k.get("id") != key_id]
    if len(data["api_keys"]) == before:
        return False
    _save(data)
    return True


def verify_workspace_api_key(raw: str) -> dict[str, Any] | None:
    if not raw or not raw.startswith("dfk_"):
        return None
    digest = _hash_api_key(raw)
    data = _load_raw()
    for item in data.get("api_keys", []):
        if item.get("key_hash") == digest:
            item["last_used_at"] = _now()
            _save(data)
            return {"id": item["id"], "name": item.get("name"), "created_by": item.get("created_by")}
    return None
