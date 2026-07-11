"""Central platform paths and environment — single source for dev vs production."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]


def is_railway() -> bool:
    return bool(os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_SERVICE_ID"))


def is_production() -> bool:
    env = os.getenv("DATAFLOW_ENV", os.getenv("ENVIRONMENT", "")).lower()
    if is_railway():
        if env in ("development", "dev", "local"):
            return False
        return True
    if not env:
        return False
    return env in ("production", "prod")


def _railway_volume_root() -> Path | None:
    """Railway persistent volume mount (set in dashboard)."""
    for key in ("RAILWAY_VOLUME_MOUNT_PATH", "DATAFLOW_VOLUME_PATH"):
        raw = os.getenv(key, "").strip()
        if raw:
            return Path(raw)
    if is_railway() and Path("/data").exists():
        return Path("/data")
    return None


def data_dir() -> Path:
    vol = _railway_volume_root()
    raw = os.getenv("DATAFLOW_DATA_DIR", "").strip()
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
    raw = os.getenv("DATAFLOW_UPLOAD_DIR", "").strip()
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
    raw = os.getenv("DATAFLOW_VECTOR_STORE_DIR", "").strip()
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

    web_domain = os.getenv("DATAFLOW_WEB_DOMAIN", "").strip()
    if web_domain:
        if not web_domain.startswith("http"):
            origins.append(f"https://{web_domain}")
        else:
            origins.append(web_domain)

    seen: set[str] = set()
    unique: list[str] = []
    for o in origins:
        if o not in seen:
            seen.add(o)
            unique.append(o)
    return unique or ["http://localhost:5173"]


def docs_enabled() -> bool:
    if is_production():
        return os.getenv("DATAFLOW_ENABLE_DOCS", "0").lower() in ("1", "true", "yes")
    return os.getenv("DATAFLOW_ENABLE_DOCS", "1").lower() not in ("0", "false", "off", "no")


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
    secret = os.getenv("DATAFLOW_AUTH_SECRET", "")
    if not secret or secret == "dev-change-me-before-production":
        errors.append("DATAFLOW_AUTH_SECRET must be set to a strong random value in production")

    if os.getenv("DATAFLOW_REQUIRE_AUTH", "0").lower() not in ("1", "true", "yes"):
        errors.append("DATAFLOW_REQUIRE_AUTH must be 1 in production")

    if os.getenv("DATAFLOW_ALLOW_DEV_USER", "0").lower() not in ("1", "true", "yes"):
        users_raw = os.getenv("DATAFLOW_AUTH_USERS", "").strip()
        if not users_raw:
            errors.append("DATAFLOW_AUTH_USERS must define at least one user (or set DATAFLOW_ALLOW_DEV_USER=1 for staging only)")

    if not os.getenv("DATAFLOW_SECRETS_KEY", "").strip() and not secret:
        errors.append("DATAFLOW_SECRETS_KEY or DATAFLOW_AUTH_SECRET required for connector encryption")

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


def apply_railway_defaults() -> None:
    """Set sensible defaults when running on Railway."""
    if not is_railway():
        return
    os.environ.setdefault("DATAFLOW_ENV", "production")
    os.environ.setdefault("DATAFLOW_REQUIRE_AUTH", "1")
    os.environ.setdefault("DATAFLOW_TRAINING", "off")
    os.environ.setdefault("DATAFLOW_AUTO_INSTALL_DRIVERS", "0")
    os.environ.setdefault("DATAFLOW_ENABLE_DOCS", "0")
    os.environ.setdefault("DATAFLOW_SEED_DEMO", "0")
    if os.getenv("MONGO_URL") and not os.getenv("MONGODB_URI"):
        os.environ["MONGODB_URI"] = os.environ["MONGO_URL"]
