"""Driver availability policy — platform bundles drivers; users never run pip."""

from __future__ import annotations

import os


def allow_stub_writes() -> bool:
    """Only allow simulated writes when DATAFLOW_ALLOW_STUB_WRITES=1."""
    return os.getenv("DATAFLOW_ALLOW_STUB_WRITES", "").lower() in ("1", "true", "yes")


def stub_writes_allowed() -> bool:
    """Stub writes are never allowed in production — fail closed."""
    if allow_stub_writes():
        try:
            from services.platform_config import is_production

            return not is_production()
        except Exception:
            return True
    return False


def platform_driver_unavailable(connector_label: str) -> str:
    """User-facing message — no pip instructions."""
    return (
        f"{connector_label} connector is not ready on this platform node yet. "
        "DataFlow bundles all transfer drivers — wait a moment and retry, or contact your administrator if this persists."
    )


def require_driver(package: str, pip_name: str | None = None) -> str:
    """Ops/log message when bootstrap failed."""
    install = pip_name or package
    return (
        f"Platform driver missing: {package} ({install}). "
        "Restart the API after `npm run setup:api` or set DATAFLOW_AUTO_INSTALL_DRIVERS=1."
    )


def driver_missing_result(connector_label: str, package: str, pip_name: str | None = None) -> tuple[bool, str]:
    del package, pip_name
    return False, platform_driver_unavailable(connector_label)
