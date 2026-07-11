"""Platform driver bootstrap — connectors ship with DataFlow, not end-user installs."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from typing import Any

# Transfer-live drivers bundled in requirements.txt
_PLATFORM_DRIVERS: list[tuple[str, str, str]] = [
    ("snowflake", "snowflake.connector", "snowflake-connector-python"),
    ("postgresql", "psycopg2", "psycopg2-binary"),
    ("mysql", "pymysql", "pymysql"),
    ("mongodb", "pymongo", "pymongo"),
    ("bigquery", "google.cloud.bigquery", "google-cloud-bigquery"),
    ("redshift", "psycopg2", "psycopg2-binary"),
    ("dynamodb", "boto3", "boto3"),
    ("s3", "boto3", "boto3"),
    ("gcs", "google.cloud.storage", "google-cloud-storage"),
    ("redis", "redis", "redis"),
    ("elasticsearch", "elasticsearch", "elasticsearch"),
    ("parquet", "pyarrow", "pyarrow"),
    ("excel", "openpyxl", "openpyxl"),
]


def _importable(module: str) -> bool:
    try:
        importlib.import_module(module)
        return True
    except ImportError:
        return False


def _missing_drivers() -> list[tuple[str, str, str]]:
    return [(d_id, mod, pip) for d_id, mod, pip in _PLATFORM_DRIVERS if not _importable(mod)]


def _auto_install_enabled() -> bool:
    import os
    from services.platform_config import is_production

    default = "0" if is_production() else "1"
    return os.getenv("DATAFLOW_AUTO_INSTALL_DRIVERS", default).lower() not in ("0", "false", "off", "no")


def ensure_platform_drivers(*, quiet: bool = False) -> dict[str, Any]:
    """
    Verify bundled connector drivers. When auto-install is on (default),
    missing packages from requirements.txt are installed on platform boot.
    """
    missing_before = _missing_drivers()
    installed: list[str] = []

    if missing_before and _auto_install_enabled():
        pkgs = sorted({pip for _, _, pip in missing_before})
        if not quiet:
            print(f"[*] DataFlow: provisioning {len(pkgs)} connector driver(s)…")
        cmd = [sys.executable, "-m", "pip", "install", "-q", *pkgs]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            installed = pkgs
            if not quiet:
                print(f"[+] Connector drivers ready: {', '.join(pkgs)}")
        elif not quiet:
            print(f"[!] Driver auto-install failed: {result.stderr.strip() or result.stdout.strip()}")

    missing_after = _missing_drivers()
    ready = len(missing_after) == 0

    status = {
        "ready": ready,
        "auto_install_enabled": _auto_install_enabled(),
        "auto_install_attempted": bool(missing_before and _auto_install_enabled()),
        "installed_packages": installed,
        "drivers": {
            d_id: {"import": mod, "package": pip, "available": _importable(mod)}
            for d_id, mod, pip in _PLATFORM_DRIVERS
        },
        "missing": [
            {"id": d_id, "package": pip}
            for d_id, _, pip in missing_after
        ],
    }
    if not ready and not quiet:
        names = ", ".join(m["package"] for m in status["missing"])
        print(f"[!] Platform drivers still missing after bootstrap: {names}")
    return status


def driver_status() -> dict[str, Any]:
    """Read-only driver availability for health checks."""
    missing = _missing_drivers()
    return {
        "ready": len(missing) == 0,
        "missing": [{"id": d_id, "package": pip} for d_id, _, pip in missing],
        "drivers": {
            d_id: _importable(mod)
            for d_id, mod, _ in _PLATFORM_DRIVERS
        },
    }
