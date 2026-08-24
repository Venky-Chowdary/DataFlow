"""Central platform paths and environment — single source for dev vs production."""

from __future__ import annotations

import os
from services.brand_env import getenv_brand
import sys
from pathlib import Path
from typing import Any

_API_ROOT = Path(__file__).resolve().parents[1]


def is_railway() -> bool:
    return bool(os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_SERVICE_ID"))


def is_production() -> bool:
    """Return True when the process must enforce production security policy.

    Fail-closed rules:
    - Explicit ``ENV=production|prod`` → production
    - Explicit ``ENV=development|dev|local|test`` → not production
    - Railway (unless explicitly marked development) → production
    - ``ASSUME_PRODUCTION=1`` → production (bare-metal / k8s without ENV)
    - ``REQUIRE_AUTH=1`` with unset ENV → production (misconfigured host)
    - Otherwise unset ENV stays non-production for local developer UX
    """
    env = (getenv_brand("ENV", os.getenv("ENVIRONMENT", "")) or "").lower()
    if env in ("development", "dev", "local", "test"):
        return False
    if env in ("production", "prod"):
        return True
    if is_railway():
        return True
    if getenv_brand("ASSUME_PRODUCTION", "0").lower() in ("1", "true", "yes"):
        return True
    # Operator set auth-required without declaring ENV — treat as production so
    # vault / DEV_USER / docs gates cannot silently fail open on a public host.
    if not env and getenv_brand("REQUIRE_AUTH", "").lower() in ("1", "true", "yes"):
        return True
    return False


def _railway_volume_root() -> Path | None:
    """Railway persistent volume mount (set in dashboard)."""
    for key in ("RAILWAY_VOLUME_MOUNT_PATH", "DATAWRAP_VOLUME_PATH", "DATAFLOW_VOLUME_PATH"):
        raw = os.getenv(key, "").strip()
        if raw:
            return Path(raw)
    if is_railway() and Path("/data").exists():
        return Path("/data")
    return None


def data_dir() -> Path:
    vol = _railway_volume_root()
    raw = getenv_brand("DATA_DIR", "").strip()
    if raw:
        path = Path(raw)
    elif vol:
        path = vol / "data"
    else:
        path = _API_ROOT / "data"
    path.mkdir(parents=True, exist_ok=True)
    return path


def upload_dir() -> Path:
    vol = _railway_volume_root()
    raw = getenv_brand("UPLOAD_DIR", "").strip()
    if raw:
        path = Path(raw)
    elif vol:
        path = vol / "uploads"
    else:
        path = _API_ROOT / "uploads"
    path.mkdir(parents=True, exist_ok=True)
    return path


def vector_store_dir() -> Path:
    vol = _railway_volume_root()
    raw = getenv_brand("VECTOR_STORE_DIR", "").strip()
    if raw:
        path = Path(raw)
    elif vol:
        path = vol / "vector_store"
    else:
        path = data_dir() / "vector_store"
    path.mkdir(parents=True, exist_ok=True)
    return path


def mongodb_uri() -> str:
    """Resolve Mongo — Railway Mongo plugin exposes MONGO_URL / MONGO_PRIVATE_URL."""
    for key in (
        "MONGODB_URI",
        "MONGO_URL",
        "MONGO_PRIVATE_URL",
        "MONGODB_URL",
        "MONGO_PUBLIC_URL",
    ):
        val = os.getenv(key, "").strip()
        if val and ("mongo" in val.lower()):
            return val
    return os.getenv("MONGODB_URI", "mongodb://localhost:27017/")


def cors_origins() -> list[str]:
    default = "http://localhost:5173,http://localhost:3000,http://127.0.0.1:5173"
    raw = os.getenv("CORS_ORIGINS", default if not is_railway() else "")
    origins = [o.strip() for o in raw.split(",") if o.strip()]

    domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "").strip()
    if domain:
        origins.append(f"https://{domain}")

    web_domain = getenv_brand("WEB_DOMAIN", "").strip()
    if web_domain:
        if not web_domain.startswith("http"):
            origins.append(f"https://{web_domain}")
        else:
            origins.append(web_domain)

    # Operator-managed customer vanity URLs (pre-DNS or multi-tenant hosts).
    extra = os.getenv("CORS_EXTRA_ORIGINS", "").strip()
    if extra:
        origins.extend(o.strip() for o in extra.split(",") if o.strip())

    # Tenant custom domains configured via workspace APIs — customers set these
    # so the SPA at https://data.customer.com can call the API with credentials.
    if getenv_brand("CORS_INCLUDE_TENANT_DOMAINS", "1").lower() not in ("0", "false", "no"):
        try:
            from services.tenant_store import list_tenants

            for tenant in list_tenants():
                host = (tenant.custom_domain or "").strip().lower()
                # A record predating workspace scoping serves no workspace, so
                # its domain is not a browser origin we trust with credentials —
                # the same refusal domain resolution already makes.
                if not host or not tenant.workspace_id:
                    continue
                if host.startswith("http://") or host.startswith("https://"):
                    origins.append(host.rstrip("/"))
                else:
                    origins.append(f"https://{host}")
        except Exception:
            pass

    seen: set[str] = set()
    unique: list[str] = []
    for o in origins:
        if o not in seen:
            seen.add(o)
            unique.append(o)
    return unique or ["http://localhost:5173"]


def docs_enabled() -> bool:
    if is_production():
        return getenv_brand("ENABLE_DOCS", "0").lower() in ("1", "true", "yes")
    return getenv_brand("ENABLE_DOCS", "1").lower() not in ("0", "false", "off", "no")


def tracing_enabled() -> bool:
    """Opt-in OpenTelemetry. Off by default so a lean install is unchanged."""
    return getenv_brand("ENABLE_TRACING", "0").lower() in ("1", "true", "yes")


def _mongo_is_localhost(uri: str) -> bool:
    lower = uri.lower()
    return (
        lower.startswith("mongodb://localhost")
        or lower.startswith("mongodb://127.0.0.1")
        or lower.startswith("mongodb://mongo:")  # docker compose internal — ok for compose only
    )


def validate_production_config() -> list[str]:
    """Return fatal misconfiguration messages (empty = OK)."""
    if not is_production():
        return []

    errors: list[str] = []
    secret = getenv_brand("AUTH_SECRET", "")
    if not secret or len(secret) < 32:  # nosec B105
        errors.append(
            "DATAWRAP_AUTH_SECRET (or legacy DATAFLOW_AUTH_SECRET) must be set "
            "to a strong random value (>=32 bytes) in production"
        )

    if getenv_brand("REQUIRE_AUTH", "0").lower() not in ("1", "true", "yes"):
        errors.append("DATAWRAP_REQUIRE_AUTH (or DATAFLOW_REQUIRE_AUTH) must be 1 in production")

    # At least one real login path: admin pair and/or AUTH_USERS JSON.
    # DATAWRAP_ALLOW_DEV_USER is forbidden in production — never a login path.
    admin_email = (getenv_brand("ADMIN_EMAIL") or "").strip()
    admin_password = (getenv_brand("ADMIN_PASSWORD") or "").strip()
    users_raw = (getenv_brand("AUTH_USERS") or "").strip()
    allow_dev = getenv_brand("ALLOW_DEV_USER", "0").lower() in ("1", "true", "yes")
    if allow_dev:
        errors.append(
            "DATAWRAP_ALLOW_DEV_USER must not be enabled in production "
            "(hard-coded test@gmail.com login is forbidden for customer tenants)"
        )
    if not ((admin_email and admin_password) or users_raw):
        errors.append(
            "Set DATAWRAP_ADMIN_EMAIL + DATAWRAP_ADMIN_PASSWORD "
            "(or legacy DATAFLOW_ADMIN_*), or a valid DATAWRAP_AUTH_USERS JSON array"
        )
    if not getenv_brand("SECRETS_KEY", "").strip():
        errors.append(
            "DATAWRAP_SECRETS_KEY (or DATAFLOW_SECRETS_KEY) must be set for "
            "Fernet encryption of connector credentials in production"
        )
    try:
        import cryptography.fernet  # noqa: F401
    except Exception:
        errors.append("cryptography package must be installed in production (Fernet secret vault)")

    mongo = mongodb_uri()
    if _mongo_is_localhost(mongo) and not mongo.startswith("mongodb://mongo:"):
        if is_railway():
            errors.append(
                "MongoDB not configured — add Railway MongoDB plugin or set MONGO_URL / MONGODB_URI"
            )
        else:
            errors.append("MONGODB_URI must point to a production MongoDB instance (not localhost)")

    if is_railway() and not cors_origins():
        errors.append("CORS_ORIGINS or DATAFLOW_WEB_DOMAIN must include your Railway web URL")

    return errors


def enforce_production_config() -> None:
    errors = validate_production_config()
    if errors:
        for msg in errors:
            print(f"[FATAL] Production config: {msg}", file=sys.stderr)
        sys.exit(1)


def public_url() -> str:
    """Public API base URL used for retry/resume links in notifications."""
    explicit = getenv_brand("PUBLIC_URL", "").strip()
    if explicit:
        return explicit.rstrip("/")
    if is_railway():
        domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "").strip()
        if domain:
            return f"https://{domain}"
    return ""


def email_provider_config() -> dict[str, Any]:
    """Managed transactional email provider config for SaaS notifications.

    Providers: sendgrid, resend, mailgun, smtp (default).
    If provider is configured and its API key is present, the platform sends
    email without requiring per-tenant SMTP credentials.
    """
    provider = getenv_brand("EMAIL_PROVIDER", "smtp").lower().strip()
    cfg: dict[str, Any] = {"provider": provider}
    if provider == "sendgrid":
        cfg["api_key"] = os.getenv("SENDGRID_API_KEY", "")
        cfg["from"] = getenv_brand("EMAIL_FROM", "dataflow@example.com")
    elif provider == "resend":
        cfg["api_key"] = os.getenv("RESEND_API_KEY", "")
        cfg["from"] = getenv_brand("EMAIL_FROM", "dataflow@example.com")
    elif provider == "mailgun":
        cfg["api_key"] = os.getenv("MAILGUN_API_KEY", "")
        cfg["domain"] = os.getenv("MAILGUN_DOMAIN", "")
        cfg["region"] = os.getenv("MAILGUN_REGION", "us")
        cfg["from"] = getenv_brand("EMAIL_FROM", "dataflow@example.com")
    else:
        cfg["from"] = getenv_brand("EMAIL_FROM", getenv_brand("SMTP_FROM", "dataflow@localhost"))
    return cfg


def web_url() -> str:
    """Public web UI base URL used for clickable job links."""
    explicit = getenv_brand("WEB_URL", "").strip()
    if explicit:
        return explicit.rstrip("/")
    web_domain = getenv_brand("WEB_DOMAIN", "").strip()
    if web_domain:
        if web_domain.startswith("http"):
            return web_domain.rstrip("/")
        return f"https://{web_domain}"
    if is_railway():
        domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "").strip()
        if domain:
            return f"https://{domain}"
    return ""


def apply_railway_defaults() -> None:
    """Set sensible defaults when running on Railway or any production image."""
    if is_railway():
        os.environ.setdefault("DATAFLOW_ENV", "production")
    if not is_railway() and not is_production():
        return
    # Auth is required in production unless the operator explicitly opts out.
    os.environ.setdefault("DATAFLOW_REQUIRE_AUTH", "1")
    os.environ.setdefault("DATAFLOW_TRAINING", "off")
    os.environ.setdefault("DATAFLOW_AUTO_INSTALL_DRIVERS", "0")
    os.environ.setdefault("DATAFLOW_ENABLE_DOCS", "0")
    os.environ.setdefault("DATAFLOW_SEED_DEMO", "0")
    if is_railway() and os.getenv("MONGO_URL") and not os.getenv("MONGODB_URI"):
        os.environ["MONGODB_URI"] = os.environ["MONGO_URL"]
