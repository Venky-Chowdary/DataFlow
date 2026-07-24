"""Platform driver bootstrap — connectors ship with DataFlow, not end-user installs.

Driver↔module pairs come from ``connector_capabilities._DRIVER_MODULE`` (single
source of truth). Pip package names live only here as the install map.
"""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from typing import Any

# module import path → pip package (only for modules we auto-bootstrap)
_PIP_FOR_MODULE: dict[str, str] = {
    "snowflake.connector": "snowflake-connector-python",
    "psycopg2": "psycopg2-binary",
    "pymysql": "pymysql",
    "pymongo": "pymongo",
    "google.cloud.bigquery": "google-cloud-bigquery",
    "boto3": "boto3",
    "google.cloud.storage": "google-cloud-storage",
    "redis": "redis",
    "elasticsearch": "elasticsearch",
    "azure.storage.blob": "azure-storage-blob",
    "oracledb": "oracledb",
    "paramiko": "paramiko",
    "pyodbc": "pyodbc",
    "openpyxl": "openpyxl",
    "pyarrow": "pyarrow",
    "fastavro": "fastavro",
    "xmltodict": "xmltodict",
}


def _platform_drivers() -> list[tuple[str, str, str]]:
    """(driver_id, import_module, pip_package) from the capability registry."""
    try:
        from transfer.connector_capabilities import _DRIVER_MODULE
    except ImportError:  # pragma: no cover
        from src.transfer.connector_capabilities import _DRIVER_MODULE  # type: ignore

    out: list[tuple[str, str, str]] = []
    seen_mod: set[str] = set()
    for driver_id, mod in sorted(_DRIVER_MODULE.items()):
        if not mod or mod in seen_mod:
            continue
        pip = _PIP_FOR_MODULE.get(mod)
        if not pip:
            continue
        # Skip pure-stdlib (sqlite3) and request-only SaaS stubs for health noise.
        if driver_id in {
            "salesforce", "hubspot", "stripe", "rest_api", "influxdb",
            "neo4j", "couchbase", "qdrant", "weaviate", "pinecone", "milvus",
            "pgvector",  # same psycopg2 as postgresql
        }:
            continue
        seen_mod.add(mod)
        out.append((driver_id, mod, pip))
    return out


def _importable(module: str) -> bool:
    try:
        importlib.import_module(module)
        return True
    except ImportError:
        return False


def _missing_drivers() -> list[tuple[str, str, str]]:
    return [(d_id, mod, pip) for d_id, mod, pip in _platform_drivers() if not _importable(mod)]


def _auto_install_enabled() -> bool:
    from services.platform_config import is_production

    default = "0" if is_production() else "1"
    return os.getenv("DATAFLOW_AUTO_INSTALL_DRIVERS", default).lower() not in ("0", "false", "off", "no")


def ensure_platform_drivers(*, quiet: bool = False) -> dict[str, Any]:
    """
    Verify bundled connector drivers. When auto-install is on (default),
    missing packages from requirements.txt are installed on platform boot.
    """
    drivers = _platform_drivers()
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
            for d_id, mod, pip in drivers
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
    drivers = _platform_drivers()
    missing = _missing_drivers()
    return {
        "ready": len(missing) == 0,
        "missing": [{"id": d_id, "package": pip} for d_id, _, pip in missing],
        "drivers": {
            d_id: _importable(mod)
            for d_id, mod, _ in drivers
        },
    }
