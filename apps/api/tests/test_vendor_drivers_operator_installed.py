"""Owner decision 3: SAP HANA and Teradata drivers are operator-installed.

``hdbcli`` (SAP) and ``teradatasql`` (Teradata) are proprietary, and their
SQLAlchemy dialects (``sqlalchemy-hana``, ``teradatasqlalchemy``) are what
the generic SQL path dials through. None of them ships in the API image any
more (requirements-vendor-drivers.txt, Dockerfile build-arg
INSTALL_VENDOR_DRIVERS=1). Without them, Test, save and run must refuse with
one actionable message: what to pip install, and that the operator must accept
the vendor's licence. No stack trace, no file path. With them, behaviour is
unchanged.
"""

from __future__ import annotations

import importlib.machinery
import os
import sys
import types
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

API = Path(__file__).resolve().parents[1]
REPO = API.parents[1]
WS = "ws-vendor-drivers"

VENDOR = {
    "sap_hana": ("hdbcli", "sqlalchemy_hana", "pip install hdbcli sqlalchemy-hana", "SAP"),
    "teradata": ("teradatasql", "teradatasqlalchemy", "pip install teradatasql teradatasqlalchemy", "Teradata"),
}
ALL_MODULES = [m for spec in VENDOR.values() for m in spec[:2]]


def _clear_caches() -> None:
    from src.transfer import connector_capabilities as cc

    cc._module_is_installed.cache_clear()


@pytest.fixture
def drivers_missing(monkeypatch):
    for name in ALL_MODULES:
        monkeypatch.setitem(sys.modules, name, None)  # import raises, find_spec -> None
    _clear_caches()
    yield
    _clear_caches()


@pytest.fixture
def drivers_present(monkeypatch):
    for name in ALL_MODULES:
        mod = types.ModuleType(name)
        mod.__spec__ = importlib.machinery.ModuleSpec(name, None)
        monkeypatch.setitem(sys.modules, name, mod)
    _clear_caches()
    yield
    _clear_caches()


def _assert_actionable(kind: str, text: str) -> None:
    _dbapi, _dialect, pip, vendor = VENDOR[kind]
    assert pip in text, text
    assert "licence" in text and vendor in text, text
    assert "INSTALL_VENDOR_DRIVERS=1" in text, text
    assert "Traceback" not in text and "/" not in text.replace("--build-arg", ""), text
    assert "requirements" not in text, "no repository file names in a product message"


@pytest.mark.parametrize("kind", sorted(VENDOR))
def test_planned_message_is_the_operator_install_instruction(kind, drivers_missing):
    from src.transfer.connector_capabilities import planned_driver_message

    _assert_actionable(kind, planned_driver_message(kind))


@pytest.mark.parametrize("kind", sorted(VENDOR))
def test_connector_test_refuses_cleanly(kind, drivers_missing, monkeypatch):
    import services.egress_guard as egress
    from connectors.generic_sql import test_generic_sql
    from src.transfer.connector_capabilities import planned_driver_message

    monkeypatch.setattr(egress, "assert_connector_dial_allowed", lambda *a, **k: None)
    ok, message = test_generic_sql(
        type=kind, host="127.0.0.1", port=9, database="d", username="u", password="p",
        schema="", connection_string="", ssl=False,
    )
    assert ok is False
    assert message == planned_driver_message(kind)
    _assert_actionable(kind, message)


@pytest.mark.parametrize("kind", sorted(VENDOR))
@pytest.mark.parametrize("role", ["source", "destination"])
def test_run_refuses_cleanly(kind, role, drivers_missing):
    from src.transfer.connector_capabilities import endpoint_allowed_for_role

    ok, message = endpoint_allowed_for_role(kind, role)
    assert ok is False
    _assert_actionable(kind, message)


@pytest.mark.parametrize("kind", sorted(VENDOR))
def test_rest_save_refuses_cleanly(kind, drivers_missing, monkeypatch):
    from fastapi import HTTPException

    import src.routers.saved_connectors_router as router
    from services.connector_store import list_connectors

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setattr(router, "resolve_write_workspace", lambda request, ws: WS)
    monkeypatch.setattr(router, "require_admin_connector_access", lambda request, ctype: None)
    body = router.ConnectorSaveDTO(**{
        "name": f"vendor-{kind}", "type": kind, "host": "127.0.0.1", "port": 9,
        "database": "d", "username": "u", "password": "not-a-real-password",
    })
    with pytest.raises(HTTPException) as caught:
        router.save_connector(body, None, WS)
    assert caught.value.status_code == 422
    _assert_actionable(kind, str(caught.value.detail))
    assert not [c for c in list_connectors(workspace_id=WS) if c.name == f"vendor-{kind}"]


@pytest.mark.parametrize("kind", ["sap_hana", "hana", "sap_bw_4hana", "teradata", "teradata_vantage"])
def test_catalog_says_requires_operator_installed_driver(kind, drivers_missing):
    from src.transfer.connector_capabilities import enrich_catalog_entry

    row = enrich_catalog_entry({"id": kind, "name": kind, "status": "live"})
    info = row["operator_installed_driver"]
    assert info["installed"] is False
    assert info["vendor"] in {"SAP", "Teradata"}
    assert row["capability_label"] == "Requires operator-installed driver"
    assert row["driver_available"] is False


@pytest.mark.parametrize("kind", sorted(VENDOR))
def test_with_the_driver_installed_behaviour_is_unchanged(kind, drivers_present):
    """The tiles stay Planned exactly as before (generic SQL brands are not
    certified); only the operator-install facts are added."""
    from src.transfer.connector_capabilities import (
        endpoint_allowed_for_role,
        enrich_catalog_entry,
        planned_driver_message,
    )

    assert planned_driver_message(kind) == ""
    row = enrich_catalog_entry({"id": kind, "name": kind, "status": "live"})
    assert row["driver_available"] is True
    assert row["capability_label"] == "Planned"
    assert row["certification_tier"] == "planned"
    assert row["operator_installed_driver"]["installed"] is True
    ok, message = endpoint_allowed_for_role(kind, "source")
    assert ok is False and message.startswith(f"{kind} is Planned"), message


def test_other_connectors_are_untouched(drivers_missing):
    from src.transfer.connector_capabilities import enrich_catalog_entry, planned_driver_message

    for kind in ("postgresql", "mysql", "sqlite", "oracle", "db2"):
        assert "operator" not in planned_driver_message(kind)
        assert enrich_catalog_entry({"id": kind, "name": kind})["operator_installed_driver"] is None


def _names(path: Path) -> set[str]:
    out = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line and not line.startswith("-"):
            out.add(line.split(">")[0].split("=")[0].split("<")[0].split("~")[0].strip().lower())
    return out


def test_vendor_drivers_are_not_in_the_image_by_default() -> None:
    vendor = {"hdbcli", "sqlalchemy-hana", "teradatasql", "teradatasqlalchemy"}
    assert not (_names(API / "requirements.txt") & vendor)
    assert not (_names(API / "requirements-dev.txt") & vendor)
    assert _names(API / "requirements-vendor-drivers.txt") == vendor
    dockerfile = (REPO / "Dockerfile.api").read_text(encoding="utf-8")
    assert "ARG INSTALL_VENDOR_DRIVERS=0" in dockerfile
    assert 'if [ "$INSTALL_VENDOR_DRIVERS" = "1" ]' in dockerfile
    assert "requirements-vendor-drivers.txt" in dockerfile
