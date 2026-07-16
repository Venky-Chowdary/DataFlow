"""Verify transfer-live drivers: Python deps + module wiring."""

from __future__ import annotations

import importlib
from typing import Any

from .connector_capabilities import _DRIVER_CAPS, _FILE_CAPS, transfer_live_driver_types
from .connector_registry import CONNECTOR_MODULES

# driver/format → pip package(s) required at runtime
_DRIVER_PACKAGES: dict[str, list[str]] = {
    "postgresql": ["psycopg2"],
    "mysql": ["pymysql"],
    "mongodb": ["pymongo"],
    "snowflake": ["snowflake.connector"],
    "bigquery": ["google.cloud.bigquery"],
    "dynamodb": ["boto3"],
    "s3": ["boto3"],
    "gcs": ["google.cloud.storage"],
    "redis": ["redis"],
    "elasticsearch": ["elasticsearch"],
    "redshift": ["psycopg2"],
    "csv": [],
    "tsv": [],
    "json": [],
    "jsonl": [],
    "ndjson": [],
    "excel": ["openpyxl"],
    "parquet": ["pyarrow"],
    "sftp": ["paramiko"],
    "email": [],
}


def _import_ok(module: str) -> tuple[bool, str | None]:
    try:
        importlib.import_module(module)
        return True, None
    except Exception as exc:
        return False, str(exc)


def check_driver_readiness(driver: str) -> dict[str, Any]:
    """Return per-driver readiness: packages, probe/reader/writer modules."""
    issues: list[str] = []
    packages = _DRIVER_PACKAGES.get(driver, [])
    pkg_status: dict[str, bool] = {}
    for pkg in packages:
        ok, err = _import_ok(pkg)
        pkg_status[pkg] = ok
        if not ok:
            issues.append(f"Missing package {pkg}: {err}")

    spec = CONNECTOR_MODULES.get(driver)
    if spec:
        if spec.probe:
            mod, fn = spec.probe
            try:
                m = importlib.import_module(mod)
                if not callable(getattr(m, fn, None)):
                    issues.append(f"Probe {mod}.{fn} not callable")
            except Exception as exc:
                issues.append(f"Probe import failed: {exc}")
        if spec.reader:
            ok, err = _import_ok(spec.reader)
            if not ok:
                issues.append(f"Reader {spec.reader}: {err}")
        if spec.writer:
            ok, err = _import_ok(spec.writer)
            if not ok:
                issues.append(f"Writer {spec.writer}: {err}")

    if driver in _FILE_CAPS:
        ok, err = _import_ok("services.file_parser")
        if not ok:
            issues.append(f"File parser: {err}")

    ready = len(issues) == 0
    return {
        "driver": driver,
        "ready": ready,
        "packages": pkg_status,
        "issues": issues,
    }


def platform_readiness_report() -> dict[str, Any]:
    drivers = sorted(set(transfer_live_driver_types()))
    checks = [check_driver_readiness(d) for d in drivers]
    not_ready = [c for c in checks if not c["ready"]]
    return {
        "ready": len(not_ready) == 0,
        "drivers_total": len(checks),
        "drivers_ready": len(checks) - len(not_ready),
        "drivers_failed": len(not_ready),
        "checks": checks,
        "failed_drivers": [c["driver"] for c in not_ready],
        "production_note": (
            "Transfer-live drivers are wired in code; each also needs valid credentials "
            "and network access at runtime. Wiring tests do not replace end-to-end transfer tests."
        ),
    }
