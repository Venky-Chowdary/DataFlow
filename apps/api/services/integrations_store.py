"""Persisted workspace integrations — SSO, AI provider keys, API keys."""

from __future__ import annotations

import hashlib
import json
import re
import logging
import secrets
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from services.api_key_role import parse_requested_api_key_role, resolve_stored_api_key_role
from services.platform_config import data_dir, is_railway
from services.secret_vault import SecretVaultError, decrypt_secret, encrypt_secret
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "integrations.json"
_API_KEY_COLLECTION = "workspace_api_keys"
# Same choices a personal access token offers. ``never`` is explicit: a blank
# request is the 90-day default, not an immortal key.
API_KEY_LIFETIMES: dict[str, int | None] = {
    "7d": 7,
    "30d": 30,
    "60d": 60,
    "90d": 90,
    "365d": 365,
    "never": None,
}
DEFAULT_API_KEY_LIFETIME = "90d"
#: Every prefix a workspace API key may carry. ``dfk_`` keys stay valid forever;
#: new keys take ``dwk_`` when the brand flag is on (services.brand).
API_KEY_PREFIXES = ("dfk_", "dwk_")


def is_workspace_api_key(token: str) -> bool:
    return bool(token) and token.startswith(API_KEY_PREFIXES)


def _mint_raw_key(key_id: str) -> str:
    from services import brand

    prefix = brand.name_for_new("api_key_prefix")
    if prefix != brand.artifact("api_key_prefix").legacy:
        brand.audit_new_name("api_key_prefix", prefix, f"api_key:{key_id}")
    return f"{prefix}{secrets.token_urlsafe(32)}"

_SSO_TYPES = ("saml", "oidc", "azure_ad")
_CLOUD_PROVIDERS = ("openai", "anthropic")
_MASK = "••••••••"
_PILOT_ENGINES = ("auto", "local", "hybrid", "cloud")
_API_KEY_ROTATION_LOCK = threading.Lock()


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
            # Admin-claimed email domains routed to this IdP (P4-6).
            "email_domains": [],
        },
        "oidc": {  # nosec B105
            "enabled": False,
            "issuer": "",
            "client_id": "",
            "client_secret": "",
            "redirect_uri": "",
            "scopes": "openid email profile",
            "email_domains": [],
        },
        "azure_ad": {  # nosec B105
            "enabled": False,
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            "redirect_uri": "",
            "email_domains": [],
        },
    }


def _default_ai() -> dict[str, dict[str, Any]]:
    return {
        "openai": {"enabled": True, "api_key": "", "model": "gpt-4o-mini"},
        "anthropic": {"enabled": True, "api_key": "", "model": "claude-sonnet-4-20250514"},
        "ollama": {"enabled": True, "api_key": "", "base_url": "http://localhost:11434", "model": "llama3.2"},
    }


def _default_pilot() -> dict[str, Any]:
    return {"engine": "auto"}


def _empty_store() -> dict[str, Any]:
    return {
        "sso": _default_sso(),
        "ai_providers": _default_ai(),
        "pilot": _default_pilot(),
        "mcp_policy": {"enabled": True, "allowed_tools": None},
        "api_keys": [],
    }


def _read_file() -> dict[str, Any] | None:
    if not STORE_PATH.exists():
        return None
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


def _load_raw() -> dict[str, Any]:
    from services.integrations_persistence import load_sections, save_sections

    file_raw = _read_file()
    durable = load_sections()
    if durable is None and file_raw is not None:
        # Heal: a settings file written before the database held them (or while
        # it was down) is copied up once, so the next restart keeps it.
        save_sections(file_raw)
    raw = {**(file_raw or {}), **(durable or {})}
    if not raw:
        return _empty_store()

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

    pilot = _default_pilot()
    if isinstance(raw.get("pilot"), dict):
        pilot.update(raw["pilot"])
    if str(pilot.get("engine", "auto")).strip().lower() not in _PILOT_ENGINES:
        pilot["engine"] = "auto"

    policy = raw.get("mcp_policy")
    if not isinstance(policy, dict):
        policy = {"enabled": True, "allowed_tools": None}
    allowed = policy.get("allowed_tools")
    if allowed is not None and not isinstance(allowed, list):
        allowed = None
    return {
        "sso": sso,
        "ai_providers": ai,
        "pilot": pilot,
        "mcp_policy": {"enabled": bool(policy.get("enabled", True)), "allowed_tools": allowed},
        "api_keys": api_keys,
    }


def _save(data: dict[str, Any]) -> None:
    from services.integrations_persistence import save_sections

    # Database first: it is what survives a restart; the file is a local cache.
    save_sections(json.loads(json.dumps(data, default=json_default)))
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(json.dumps(data, indent=2, default=json_default), encoding="utf-8")


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


_KEY_ENV = {"openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY"}
# Env keys this process copied out of the store, so deleting the stored key can
# un-hydrate exactly those and never an operator-set variable.
_HYDRATED_ENV: set[str] = set()


def apply_integrations_to_env() -> None:
    """Hydrate process env from persisted AI provider keys (env vars take precedence)."""
    import os

    data = _load_raw()
    env_map = {
        "openai": (_KEY_ENV["openai"], "OPENAI_MODEL"),
        "anthropic": (_KEY_ENV["anthropic"], "ANTHROPIC_MODEL"),
    }
    for provider, (env_key, model_env_key) in env_map.items():
        if not os.environ.get(env_key):
            plain = resolve_provider_api_key(provider)
            if plain:
                os.environ[env_key] = plain
                _HYDRATED_ENV.add(env_key)
        model = data["ai_providers"].get(provider, {}).get("model")
        if model and not os.environ.get(model_env_key):
            os.environ[model_env_key] = str(model)

    ollama = data["ai_providers"].get("ollama", {})
    # A value this process hydrated follows the store: otherwise the first
    # saved base URL stuck in the environment and a later change never applied.
    for env_key, field in (("OLLAMA_BASE_URL", "base_url"), ("OLLAMA_MODEL", "model")):
        if ollama.get(field) and (not os.environ.get(env_key) or env_key in _HYDRATED_ENV):
            os.environ[env_key] = str(ollama[field])
            _HYDRATED_ENV.add(env_key)


# ── SSO ──────────────────────────────────────────────────────────────────────


def get_mcp_policy() -> dict[str, Any]:
    """Return the organization-level MCP execution policy."""
    policy = _load_raw()["mcp_policy"]
    allowed = policy.get("allowed_tools")
    return {
        "enabled": bool(policy.get("enabled", True)),
        "allowed_tools": (
            sorted({name for name in allowed if isinstance(name, str)})
            if isinstance(allowed, list)
            else None
        ),
    }


def set_mcp_policy(policy: dict[str, Any]) -> dict[str, Any]:
    """Persist MCP policy, rejecting unknown registered tools."""
    from src.ai.copilot.tool_permissions import TOOL_PERMISSIONS

    enabled = bool(policy.get("enabled", True))
    allowed = policy.get("allowed_tools")
    if allowed is not None:
        if not isinstance(allowed, list) or any(not isinstance(name, str) for name in allowed):
            raise ValueError("allowed_tools must be a list of tool names or null")
        unknown = sorted(set(allowed) - set(TOOL_PERMISSIONS))
        if unknown:
            raise ValueError(f"Unknown MCP tools: {', '.join(unknown)}")
        allowed = sorted(set(allowed))
    data = _load_raw()
    data["mcp_policy"] = {"enabled": enabled, "allowed_tools": allowed}
    data["updated_at"] = _now()
    _save(data)
    return get_mcp_policy()


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


class SsoConfigRefused(ValueError):
    """An SSO save that would leave an enabled provider unusable or ambiguous."""

    def __init__(self, message: str, *, missing: list[str] | None = None):
        super().__init__(message)
        self.missing = list(missing or [])


_SSO_REQUIRED = {
    "saml": ("entity_id", "sso_url", "x509_cert"),
    "oidc": ("issuer", "client_id", "client_secret", "redirect_uri"),
    "azure_ad": ("tenant_id", "client_id", "client_secret", "redirect_uri"),
}
_DOMAIN_RE = re.compile(r"^(?=.{1,253}$)(?!-)[a-z0-9-]{1,63}(?<!-)(\.(?!-)[a-z0-9-]{1,63}(?<!-))+$")


def normalize_email_domains(raw: Any) -> list[str]:
    """Lower-cased, de-duplicated domains; a leading ``@`` is accepted.

    Raises ``SsoConfigRefused`` naming the first value that is not a domain, so
    a typo is refused instead of silently never routing anyone.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        raw = [part for part in re.split(r"[\s,]+", raw) if part]
    if not isinstance(raw, list):
        raise SsoConfigRefused("email_domains must be a list of domains")
    out: list[str] = []
    for item in raw:
        domain = str(item or "").strip().lower().lstrip("@").rstrip(".")
        if not domain:
            continue
        if not _DOMAIN_RE.match(domain):
            raise SsoConfigRefused(f"{str(item)!r} is not an email domain (expected e.g. example.com)")
        if domain not in out:
            out.append(domain)
    return out


def _sso_missing(sso_type: str, cfg: dict[str, Any]) -> list[str]:
    return [f for f in _SSO_REQUIRED.get(sso_type, ()) if not str(cfg.get(f, "") or "").strip()]


def update_sso_config(sso_type: str, patch: dict[str, Any]) -> dict[str, Any]:
    """Merge ``patch`` into the stored provider config.

    * ``client_secret`` blank (or the mask) keeps the stored secret;
      ``clear_client_secret: true`` removes it explicitly.
    * ``email_domains`` is normalized and must not be claimed by another
      provider (routing would be ambiguous).
    * The merged config may be *enabled* only when every required field is
      set; a disabled draft may be saved incomplete. Nothing is written when
      the save is refused.
    """
    if sso_type not in _SSO_TYPES:
        raise ValueError(f"Unknown SSO type: {sso_type}")
    data = _load_raw()
    cfg = dict(data["sso"][sso_type])
    existing_secret = cfg.get("client_secret", "")
    patch = dict(patch)
    clear_secret = bool(patch.pop("clear_client_secret", False))
    if clear_secret and "client_secret" not in cfg:
        raise SsoConfigRefused(f"{sso_type} has no client secret to clear")

    for key, value in patch.items():
        if key == "client_secret":
            cfg["client_secret"] = _encrypt_field(str(value or ""), existing_secret)
        elif key == "x509_cert":
            if value:
                cfg["x509_cert"] = str(value)
        elif key == "email_domains":
            cfg["email_domains"] = normalize_email_domains(value)
        elif key in cfg or key == "enabled":
            cfg[key] = value
    if clear_secret:
        cfg["client_secret"] = ""

    for other in _SSO_TYPES:
        if other == sso_type:
            continue
        taken = set(cfg.get("email_domains") or []) & {
            str(d).lower() for d in (data["sso"][other].get("email_domains") or [])
        }
        if taken:
            raise SsoConfigRefused(
                f"{sorted(taken)[0]} is already routed to {other}; a domain can belong to one provider"
            )

    missing = _sso_missing(sso_type, cfg)
    if cfg.get("enabled") and missing:
        raise SsoConfigRefused(
            f"Cannot enable {sso_type}: missing {', '.join(missing)}. "
            "Save it disabled as a draft, or fill these in first.",
            missing=missing,
        )

    data["sso"][sso_type] = cfg
    data["updated_at"] = _now()
    _save(data)
    return get_sso_configs()[sso_type]


def validate_sso_config(sso_type: str) -> dict[str, Any]:
    data = _load_raw()
    cfg = data["sso"][sso_type]
    missing: list[str] = []

    missing.extend(_sso_missing(sso_type, cfg))

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


def storage_status() -> dict[str, Any]:
    """Whether saved Settings survive a restart, in words a user can act on.

    Durable when the control-plane database holds them. Otherwise a container
    without a mounted volume loses them on every deploy; the operator-facing
    detail (path, volume) goes to the log, never to the Settings screen.
    """
    import os

    from services.integrations_persistence import durable

    if durable():
        return {"persistent": True, "reason": ""}
    persistent = True
    path = STORE_PATH
    if is_railway():
        root = path.parent.parent if path.parent.name == "data" else path.parent
        persistent = bool(os.path.ismount(root) or os.path.ismount(path.parent))
        if not persistent:
            logger.warning(
                "Settings store %s is on an ephemeral disk and no settings database "
                "is configured; saved keys are lost on restart (mount a volume at %s "
                "or configure MongoDB).", path, root,
            )
    return {
        "persistent": persistent,
        "reason": (
            ""
            if persistent
            else "Saved model keys can't be stored durably on this deployment yet, so "
            "they may need to be entered again after an update. Ask your administrator "
            "to connect the settings database."
        ),
    }


def provider_key_state(provider: str) -> str:
    """``ready`` / ``none`` / ``disabled`` / ``undecryptable`` for a provider's key.

    ``undecryptable`` means a ciphertext is stored but the current secrets key
    cannot open it — the operator rotated DATAFLOW_SECRETS_KEY (or fell back to
    a different AUTH_SECRET) after saving. Only re-saving the key fixes that.
    """
    import os

    if provider == "ollama":
        if not ai_provider_enabled("ollama"):
            return "disabled"
        return "ready" if ollama_status()["reachable"] else "unreachable"
    if not ai_provider_enabled(provider):
        return "disabled"
    env_val = os.environ.get({"openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY"}.get(provider, ""), "")
    if env_val and not _is_invalid_secret(env_val):
        return "ready"
    stored = _load_raw()["ai_providers"].get(provider, {}).get("api_key", "")
    if not stored:
        return "none"
    try:
        plain = decrypt_secret(stored)
    except SecretVaultError:
        return "undecryptable"
    return "undecryptable" if _is_invalid_secret(plain) else "ready"


def get_ai_provider_configs() -> dict[str, dict[str, Any]]:
    data = _load_raw()
    out: dict[str, dict[str, Any]] = {}
    for provider, cfg in data["ai_providers"].items():
        row = dict(cfg)
        key = row.get("api_key", "")
        # Configured means a key we can actually use (env or store), not merely
        # a byte string sitting in the file.
        row["key_state"] = provider_key_state(provider)
        if provider == "ollama":
            # Ollama has no key: it is usable only when its server answers.
            status = ollama_status()
            row["configured"] = row["key_state"] == "ready"
            row["reachable"] = status["reachable"]
            row["status_detail"] = status["detail"]
        else:
            row["configured"] = bool(resolve_provider_api_key(provider))
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
    _OLLAMA_PROBE.clear()
    return get_ai_provider_configs()[provider]


def delete_ai_provider_key(provider: str) -> dict[str, Any]:
    """Forget a saved cloud key. Pilot drops back to the local engine on auto."""
    import os

    if provider not in _CLOUD_PROVIDERS:
        raise ValueError(f"{provider} has no cloud key to remove")
    data = _load_raw()
    data["ai_providers"][provider]["api_key"] = ""
    data["updated_at"] = _now()
    _save(data)
    env_key = _KEY_ENV[provider]
    if env_key in _HYDRATED_ENV:
        os.environ.pop(env_key, None)
        _HYDRATED_ENV.discard(env_key)
    return get_ai_provider_configs()[provider]


def configured_ai_providers() -> tuple[str, ...]:
    """Cloud providers the operator has actually configured with a usable key.

    Membership means: enabled, and a key resolves from env or the encrypted
    store. Masked, sentinel and undecryptable values do not count. Ollama is
    excluded — its default base URL is pre-seeded, so reachability alone is not
    an operator decision.
    """
    return tuple(
        p for p in _CLOUD_PROVIDERS if ai_provider_enabled(p) and resolve_provider_api_key(p)
    )


def get_pilot_engine_preference() -> str:
    """Operator's saved Pilot engine choice — ``auto`` until they pick one."""
    data = _load_raw()
    return str(data["pilot"].get("engine") or "auto").strip().lower()


def set_pilot_engine_preference(engine: str) -> str:
    normalized = str(engine or "").strip().lower()
    if normalized not in _PILOT_ENGINES:
        raise ValueError(
            f"Unknown Pilot engine: {engine!r} (expected one of {', '.join(_PILOT_ENGINES)})"
        )
    data = _load_raw()
    data["pilot"]["engine"] = normalized
    data["updated_at"] = _now()
    _save(data)
    return normalized


def ai_provider_enabled(provider: str) -> bool:
    """Operator's on/off switch for a provider — an env key cannot override it."""
    data = _load_raw()
    cfg = data["ai_providers"].get(provider)
    if cfg is None:
        return False
    return bool(cfg.get("enabled", True))


def resolve_provider_api_key(provider: str) -> str:
    """The usable key for a provider, or "" — env wins over the encrypted store.

    Turning a provider off in Settings withholds the key even when the process
    env carries one: disabling is a decision, not a missing value.
    """
    import os

    if not ai_provider_enabled(provider):
        return ""

    env_map = {"openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY"}
    env_key = env_map.get(provider)
    if env_key:
        env_val = os.environ.get(env_key, "")
        if env_val and not _is_invalid_secret(env_val):
            return env_val

    data = _load_raw()
    cfg = data["ai_providers"].get(provider, {})
    stored = cfg.get("api_key", "")
    if not stored:
        return ""
    try:
        plain = decrypt_secret(stored)
    except SecretVaultError:
        return ""
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


_OLLAMA_PROBE_TTL_SEC = 10.0
_OLLAMA_PROBE: dict[str, tuple[float, dict[str, Any]]] = {}


def ollama_status(*, fresh: bool = False) -> dict[str, Any]:
    """Whether the configured Ollama server answers, and which models it has.

    ``GET {base_url}/api/tags`` with a short timeout. Cached for a few seconds
    per base URL so a Settings page refresh does not stall on a dead host;
    ``fresh`` (the Test button) always probes.
    """
    import time

    base_url = resolve_ollama_base_url().rstrip("/")
    now = time.monotonic()
    cached = _OLLAMA_PROBE.get(base_url)
    if not fresh and cached and now - cached[0] < _OLLAMA_PROBE_TTL_SEC:
        return cached[1]
    result: dict[str, Any] = {"base_url": base_url, "reachable": False, "models": [], "detail": ""}
    try:
        import httpx

        resp = httpx.get(f"{base_url}/api/tags", timeout=2.0)
        if resp.status_code == 200:
            body = resp.json() if resp.content else {}
            result["models"] = [
                str(m.get("name") or m.get("model") or "")
                for m in (body.get("models") or [])
                if isinstance(m, dict)
            ]
            result["reachable"] = True
        else:
            result["detail"] = f"Ollama answered HTTP {resp.status_code} at {base_url}."
    except Exception as exc:  # noqa: BLE001 — any failure means unreachable
        result["detail"] = f"Ollama is not reachable at {base_url} ({type(exc).__name__})."
    _OLLAMA_PROBE[base_url] = (now, result)
    return result


def resolve_ollama_base_url(default: str = "http://localhost:11434") -> str:
    import os

    if os.environ.get("OLLAMA_BASE_URL"):
        return os.environ["OLLAMA_BASE_URL"]
    data = _load_raw()
    return str(data["ai_providers"].get("ollama", {}).get("base_url") or default)


# ── Workspace API keys ───────────────────────────────────────────────────────


def _hash_api_key(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_api_key_lifetime(raw: object) -> str:
    """Lifetime an admin asked to mint. Blank means 90 days."""
    if raw is None or not str(raw).strip():
        return DEFAULT_API_KEY_LIFETIME
    token = str(raw).strip().lower()
    if token in API_KEY_LIFETIMES:
        return token
    allowed = ", ".join(API_KEY_LIFETIMES)
    raise ValueError(f"expires_in must be one of: {allowed}")


def _expires_at(lifetime: str, *, now: datetime | None = None) -> str | None:
    days = API_KEY_LIFETIMES[lifetime]
    if days is None:
        return None
    moment = now or datetime.now(timezone.utc)
    return (moment + timedelta(days=days)).isoformat()


def _parse_expires_at(raw: object) -> datetime | None:
    if raw is None or not str(raw).strip():
        return None
    text = str(raw).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _is_revoked(item: dict[str, Any]) -> bool:
    return bool(str(item.get("revoked_at") or "").strip())


def _key_expired(item: dict[str, Any], *, now: datetime | None = None) -> bool:
    """A missing expiry is a legacy key and stays valid. A corrupt date does not."""
    if "expires_at" not in item or item.get("expires_at") in (None, ""):
        return False
    expires = _parse_expires_at(item.get("expires_at"))
    if expires is None:
        return True
    moment = now or datetime.now(timezone.utc)
    return moment >= expires


def _store_errors() -> tuple[type[BaseException], ...]:
    try:
        from pymongo.errors import PyMongoError
    except ImportError:  # pragma: no cover
        return (OSError, RuntimeError, ValueError, TypeError)
    return (OSError, RuntimeError, ValueError, TypeError, PyMongoError)


def _keys_collection() -> Any | None:
    try:
        from services.control_plane_store import mongo_collection

        return mongo_collection(_API_KEY_COLLECTION)
    except _store_errors():
        logger.debug("workspace API key collection unavailable", exc_info=True)
        return None


def _file_key_records() -> list[dict[str, Any]]:
    data = _load_raw()
    return [
        dict(item)
        for item in data.get("api_keys", [])
        if isinstance(item, dict) and item.get("id") and item.get("key_hash")
    ]


def _save_file_keys(keys: list[dict[str, Any]]) -> None:
    data = _load_raw()
    data["api_keys"] = keys
    _save(data)


def _record_for_store(item: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if key != "_id"}


def load_api_key_records() -> list[dict[str, Any]]:
    """Keys this process can authenticate.

    Mongo is the control plane when it is up, so a new API process and a
    second replica see the key the operator minted. The JSON file is the
    store when Mongo is down, and any key that exists only there is copied
    into Mongo so the next session does not lose it.
    """
    file_keys = _file_key_records()
    coll = _keys_collection()
    if coll is None:
        return file_keys
    try:
        docs = [
            _record_for_store(doc)
            for doc in coll.find({})
            if isinstance(doc, dict) and doc.get("id") and doc.get("key_hash")
        ]
    except _store_errors():
        logger.warning("workspace API key read failed; using the local file", exc_info=True)
        return file_keys
    by_id = {str(doc["id"]): doc for doc in docs}
    file_rewrites: list[dict[str, Any]] = []
    for item in file_keys:
        key_id = str(item["id"])
        stored = _record_for_store(item)
        current = by_id.get(key_id)
        if current is None:
            try:
                coll.update_one({"id": key_id}, {"$set": stored}, upsert=True)
            except _store_errors():
                logger.warning("workspace API key heal into Mongo failed", exc_info=True)
                return file_keys
            by_id[key_id] = stored
            continue
        # A revoke on either side sticks. A stale file must not mint the secret
        # again, and a revoke that reached only the file must still reach Mongo.
        if _is_revoked(stored) and not _is_revoked(current):
            revoked = dict(current)
            revoked["revoked_at"] = stored.get("revoked_at")
            try:
                coll.update_one({"id": key_id}, {"$set": _record_for_store(revoked)}, upsert=True)
            except _store_errors():
                logger.warning("workspace API key revoke sync failed", exc_info=True)
            by_id[key_id] = revoked
        elif _is_revoked(current) and not _is_revoked(stored):
            file_rewrites.append(current)
    if file_rewrites:
        merged = {str(item["id"]): item for item in file_keys}
        for item in file_rewrites:
            merged[str(item["id"])] = _record_for_store(item)
        _save_file_keys(list(merged.values()))
    return list(by_id.values())


def _upsert_api_key_record(record: dict[str, Any]) -> None:
    stored = _record_for_store(record)
    key_id = str(stored["id"])
    file_keys = [item for item in _file_key_records() if str(item.get("id")) != key_id]
    file_keys.append(stored)
    _save_file_keys(file_keys)
    coll = _keys_collection()
    if coll is None:
        return
    try:
        coll.update_one({"id": key_id}, {"$set": stored}, upsert=True)
    except _store_errors():
        logger.warning("workspace API key Mongo write failed; file copy kept", exc_info=True)


def _save_file_api_key_record(record: dict[str, Any]) -> None:
    stored = _record_for_store(record)
    file_keys = [item for item in _file_key_records() if str(item.get("id")) != str(stored["id"])]
    file_keys.append(stored)
    _save_file_keys(file_keys)


class ApiKeyNotFound(LookupError):
    """A requested workspace API key does not exist."""


def _public_api_key(item: dict[str, Any], *, secret: str | None = None) -> dict[str, Any]:
    expires_at = item.get("expires_at")
    row: dict[str, Any] = {
        "id": item["id"],
        "name": item.get("name", "API key"),
        "prefix": item.get("prefix", "dfk_"),
        "role": resolve_stored_api_key_role(item.get("role")),
        "created_at": item.get("created_at"),
        "created_by": item.get("created_by"),
        "last_used_at": item.get("last_used_at"),
        "expires_at": expires_at or None,
        "lifetime": item.get("lifetime") or ("never" if not expires_at else ""),
        "expired": _key_expired(item),
        "scopes": item.get("scopes"),
        "kind": item.get("kind") or "api_key",
        "workspace_id": item.get("workspace_id") or "",
        "rotated_from": item.get("rotated_from"),
        "rotated_to": item.get("rotated_to"),
    }
    if secret is not None:
        row["key"] = secret
    return row


def list_api_keys() -> list[dict[str, Any]]:
    rows = [
        _public_api_key(item)
        for item in load_api_key_records()
        if not _is_revoked(item)
    ]
    rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    return rows


def create_api_key(
    name: str,
    actor: str,
    role: str = "editor",
    expires_in: str = DEFAULT_API_KEY_LIFETIME,
    *,
    scopes: list[str] | None = None,
    kind: str = "api_key",
    workspace_id: str = "",
) -> dict[str, Any]:
    stored_role = parse_requested_api_key_role(role)
    lifetime = parse_api_key_lifetime(expires_in)
    if kind not in {"api_key", "service_account", "scim"}:
        raise ValueError("kind must be api_key, service_account, or scim")
    if scopes is None:
        stored_scopes = None
    else:
        if not isinstance(scopes, list) or any(
            not isinstance(scope, str) for scope in scopes
        ):
            raise ValueError("scopes must be a list of permission names")
        stored_scopes = sorted(set(scopes))
        if not stored_scopes:
            raise ValueError("a token with no scopes can do nothing; omit scopes for full role")
        from services.rbac import all_permissions, role_permissions

        unknown_scopes = set(stored_scopes) - set(all_permissions())
        if unknown_scopes:
            raise ValueError(f"unknown scope: {sorted(unknown_scopes)[0]}")
        role_scopes = role_permissions(stored_role)
        disallowed_scopes = set(stored_scopes) - role_scopes
        if disallowed_scopes:
            raise ValueError(
                f"scope is outside the {stored_role} role: {sorted(disallowed_scopes)[0]}"
            )
    key_id = str(uuid.uuid4())
    raw = _mint_raw_key(key_id)
    prefix = raw[:12]
    record = {
        "id": key_id,
        "name": name.strip()[:64] or "API key",
        "prefix": prefix,
        "role": stored_role,
        "key_hash": _hash_api_key(raw),
        "created_at": _now(),
        "created_by": actor,
        "last_used_at": None,
        "lifetime": lifetime,
        "expires_at": _expires_at(lifetime),
        "scopes": stored_scopes,
        "kind": kind,
        "workspace_id": workspace_id,
    }
    _upsert_api_key_record(record)
    return _public_api_key(record, secret=raw)


def rotate_api_key(
    key_id: str,
    *,
    actor: str,
    overlap_seconds: int = 86400,
) -> dict[str, Any]:
    if not isinstance(overlap_seconds, int) or not 0 <= overlap_seconds <= 604800:
        raise ValueError("overlap_seconds must be between 0 and 604800")
    with _API_KEY_ROTATION_LOCK:
        source = next(
            (item for item in load_api_key_records() if str(item.get("id")) == key_id),
            None,
        )
        if source is None:
            raise ApiKeyNotFound(key_id)
        if _is_revoked(source):
            raise ValueError("revoked API keys cannot be rotated")
        now = datetime.now(timezone.utc)
        if _key_expired(source, now=now):
            raise ValueError("expired API keys cannot be rotated")
        if source.get("rotated_to"):
            raise ValueError("already been rotated")

        lifetime = source.get("lifetime") or (
            "never" if not source.get("expires_at") else DEFAULT_API_KEY_LIFETIME
        )
        lifetime = parse_api_key_lifetime(lifetime)
        kind = source.get("kind") or "api_key"
        if kind not in {"api_key", "service_account", "scim"}:
            raise ValueError("API key kind is invalid")
        scopes = source.get("scopes")
        if scopes is not None and (
            not isinstance(scopes, list)
            or any(not isinstance(scope, str) for scope in scopes)
            or not scopes
        ):
            raise ValueError("API key scopes are invalid")
        stored_scopes = sorted(set(scopes)) if scopes is not None else None

        new_id = str(uuid.uuid4())
        raw = _mint_raw_key(new_id)
        new_record = {
            "id": new_id,
            "name": source.get("name", "API key"),
            "prefix": raw[:12],
            "role": resolve_stored_api_key_role(source.get("role")),
            "key_hash": _hash_api_key(raw),
            "created_at": now.isoformat(),
            "created_by": actor,
            "last_used_at": None,
            "lifetime": lifetime,
            "expires_at": _expires_at(lifetime, now=now),
            "scopes": stored_scopes,
            "kind": kind,
            "rotated_from": key_id,
        }

        overlap_expiry = now + timedelta(seconds=overlap_seconds)
        source_expiry = _parse_expires_at(source.get("expires_at"))
        if source_expiry:
            source["expires_at"] = min(source_expiry, overlap_expiry).isoformat()
        else:
            source["expires_at"] = overlap_expiry.isoformat()
        source["rotated_to"] = new_id

        _upsert_api_key_record(new_record)
        coll = _keys_collection()
        if coll is None:
            _save_file_api_key_record(source)
        else:
            result = coll.update_one(
                {"id": key_id, "rotated_to": {"$exists": False}},
                {"$set": _record_for_store(source)},
            )
            if result.matched_count == 0:
                revoke_api_key(new_id)
                raise ValueError("already been rotated")
            _save_file_api_key_record(source)
        return _public_api_key(new_record, secret=raw)


def revoke_api_key(key_id: str) -> bool:
    """Mark the key revoked in both stores.

    Deleting it let a replica whose file still held the secret copy that secret
    back into Mongo on the next read. The hash stays so the old secret is
    recognized and refused. A second revoke reports that there is nothing left
    to revoke.
    """
    match = next(
        (item for item in load_api_key_records() if str(item.get("id")) == key_id),
        None,
    )
    if match is None or _is_revoked(match):
        return False
    match["revoked_at"] = _now()
    _upsert_api_key_record(match)
    return True


def verify_workspace_api_key(raw: str) -> dict[str, Any] | None:
    if not is_workspace_api_key(raw):
        return None
    digest = _hash_api_key(raw)
    for item in load_api_key_records():
        if item.get("key_hash") != digest:
            continue
        if _is_revoked(item) or _key_expired(item):
            return None
        item["last_used_at"] = _now()
        _upsert_api_key_record(item)
        return {
            "id": item["id"],
            "name": item.get("name"),
            "created_by": item.get("created_by"),
            "role": resolve_stored_api_key_role(item.get("role")),
            "scopes": item.get("scopes"),
            "kind": item.get("kind") or "api_key",
        }
    return None
