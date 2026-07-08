"""Driver availability policy — fail closed in production, explicit opt-in for dev stubs."""

from __future__ import annotations

import os


def allow_stub_writes() -> bool:
    """Only allow simulated writes when DATAFLOW_ALLOW_STUB_WRITES=1."""
    return os.getenv("DATAFLOW_ALLOW_STUB_WRITES", "").lower() in ("1", "true", "yes")


def require_driver(package: str, pip_name: str | None = None) -> str:
    install = pip_name or package
    return f"Driver not installed: {package}. Install with `pip install {install}` or set DATAFLOW_ALLOW_STUB_WRITES=1 for local dev only."


def driver_missing_result(package: str, pip_name: str | None = None) -> tuple[bool, str]:
    return False, require_driver(package, pip_name)
