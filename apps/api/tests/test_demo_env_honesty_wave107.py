"""Demo-host honesty: Certified≠package-missing; Redshift stays Planned; XML probe."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.error_handling import humanize_transfer_failure
from services.runtime_checks import python_xml_runtime_ok
from src.transfer.connector_capabilities import (
    driver_available,
    endpoint_allowed_for_role,
)
from src.transfer.registry import validate_transfer


def test_redshift_stays_planned_not_certified():
    ok, msg = validate_transfer("database", "postgresql", "database", "redshift")
    assert ok is False
    assert "Planned" in msg


def test_sqlserver_package_gap_is_not_planned_wording():
    """When pymssql/pyodbc cannot load, surface environment gap — not Planned."""
    if driver_available("sqlserver"):
        ok, msg = validate_transfer("database", "postgresql", "database", "sqlserver")
        assert ok is True, msg
        return
    ok, msg = endpoint_allowed_for_role("sqlserver", "destination")
    assert ok is False
    assert "environment gap" in msg.lower()
    assert "driver package" in msg.lower()
    # Must not classify as product Planned (wording may say "not a Planned connector").
    assert "is Planned" not in msg


def test_expat_failure_maps_to_xml_runtime_remediation():
    details = humanize_transfer_failure(
        ImportError("No module named expat; use SimpleXMLTreeBuilder instead")
    )
    assert details.get("code") == "python_xml_runtime_broken"
    assert "pyexpat" in (details.get("fix") or "").lower() or "expat" in (
        details.get("fix") or ""
    ).lower()


def test_xml_runtime_probe_is_bool():
    assert isinstance(python_xml_runtime_ok(), bool)


def test_object_store_fails_closed_when_xml_runtime_broken(monkeypatch):
    """S3 must not stay Certified/Execute-ready when pyexpat cannot load."""
    from src.transfer import connector_capabilities as caps

    monkeypatch.setattr(
        "services.runtime_checks.python_xml_runtime_ok",
        lambda: False,
    )
    ok, msg = caps.endpoint_allowed_for_role("s3", "destination")
    assert ok is False
    assert "environment gap" in msg.lower()
    assert "xml" in msg.lower() or "expat" in msg.lower() or "pyexpat" in msg.lower()
