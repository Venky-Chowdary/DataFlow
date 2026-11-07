"""SaaS/SDK connector fields outside the canonical set must persist end-to-end.

The connector form collects driver-specific fields (zendesk ``email``,
jira ``site``/``api_token``, mailjet ``secret_key``, monday ``board_id``, …)
under ``extra``. Regressions covered:

* ``ConnectorSaveDTO`` silently dropped ``extra`` — nothing persisted.
* ``/connectors/test`` could not see ``extra`` — the in-form probe tested a
  cfg different from what a saved transfer would run with.
* Masked secrets inside ``extra`` (``"****"``) overwrote real credentials on
  re-save — ``_preserve_masked_secrets`` only guarded the root fields.
"""

from __future__ import annotations

import sys
from pathlib import Path


_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _fresh_store(monkeypatch, tmp_path: Path) -> None:
    store = tmp_path / "connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    import services.connector_store as cs

    monkeypatch.setattr(cs, "_backend_choice", "file")


def test_save_persists_extra_and_masks_secret_keys(monkeypatch, tmp_path: Path) -> None:
    """POST /connectors/saved must persist ``extra``; GET masks secret keys."""
    _fresh_store(monkeypatch, tmp_path)
    from fastapi.testclient import TestClient

    from src.main import app

    client = TestClient(app)
    payload = {
        "name": "Zendesk prod",
        "type": "zendesk",
        "role": "source",
        "host": "acme",
        "port": 443,
        "database": "",
        "extra": {
            "domain": "acme",
            "email": "ops@acme.test",
            "api_key": "REAL-ZENDESK-TOKEN",
        },
    }
    r = client.post("/api/v1/connectors/saved", json=payload)
    assert r.status_code == 200, r.text
    connector_id = r.json()["id"]

    r = client.get("/api/v1/connectors/saved")
    assert r.status_code == 200
    rows = r.json()
    rows = rows["connectors"] if isinstance(rows, dict) else rows
    row = next(c for c in rows if c["id"] == connector_id)
    extra = row.get("extra") or {}
    # Non-secret extras are visible to the form; secret-keyed extras are masked.
    assert extra.get("domain") == "acme"
    assert extra.get("email") == "ops@acme.test"
    assert extra.get("api_key") == "****"

    # The stored record keeps the real token — masking is response-side only.
    from services.connector_store import get_connector

    stored = get_connector(connector_id)
    assert (stored.extra or {}).get("api_key") == "REAL-ZENDESK-TOKEN"


def test_update_restores_masked_extra_secrets(monkeypatch, tmp_path: Path) -> None:
    """Re-saving a masked extra secret must not overwrite the real value."""
    _fresh_store(monkeypatch, tmp_path)
    from fastapi.testclient import TestClient

    from src.main import app

    client = TestClient(app)
    base = {
        "name": "Jira cloud",
        "type": "jira",
        "role": "source",
        "host": "acme.atlassian.net",
        "port": 443,
        "database": "",
        "extra": {
            "site": "https://acme.atlassian.net",
            "email": "ops@acme.test",
            "api_token": "REAL-JIRA-TOKEN",
        },
    }
    r = client.post("/api/v1/connectors/saved", json=base)
    assert r.status_code == 200, r.text
    connector_id = r.json()["id"]

    # A form round-trip re-submits the masked secret it was shown.
    resend = dict(base)
    resend["extra"] = {
        "site": "https://acme.atlassian.net",
        "email": "ops@acme.test",
        "api_token": "****",
    }
    r = client.put(f"/api/v1/connectors/saved/{connector_id}", json=resend)
    assert r.status_code == 200, r.text

    from services.connector_store import get_connector

    stored = get_connector(connector_id)
    extra = stored.extra or {}
    assert extra.get("api_token") == "REAL-JIRA-TOKEN"
    assert extra.get("site") == "https://acme.atlassian.net"


def test_probe_cfg_carries_extra_fields(monkeypatch, tmp_path: Path) -> None:
    """POST /connectors/test merges ``extra`` into the probe cfg."""
    _fresh_store(monkeypatch, tmp_path)

    seen: dict = {}

    def _capture_probe(driver, cfg):
        seen.update(cfg)
        return True, "ok"

    import importlib

    # ``src.routers.connectors_router`` resolves to the APIRouter re-export on
    # the package, not the module — import_module returns the real module.
    router_mod = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(router_mod, "run_probe", _capture_probe)

    from fastapi.testclient import TestClient

    from src.main import app

    client = TestClient(app)
    r = client.post(
        "/api/v1/connectors/test",
        json={
            "type": "zendesk",
            "host": "acme",
            "port": 443,
            "database": "",
            "auth_mode": "api_key",
            "extra": {
                "domain": "acme",
                "email": "ops@acme.test",
                "api_key": "REAL-ZENDESK-TOKEN",
            },
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["success"] is True
    assert seen.get("domain") == "acme"
    assert seen.get("email") == "ops@acme.test"
    assert seen.get("api_key") == "REAL-ZENDESK-TOKEN"


def test_connector_extra_from_payload_carries_arbitrary_keys() -> None:
    """The store layer persists any extra keys the form sends."""
    from services.connector_store import connector_extra_from_payload

    extra = connector_extra_from_payload(
        {"extra": {"dc": "us21", "board_id": "123456789", "empty": ""}}
    )
    assert extra["dc"] == "us21"
    assert extra["board_id"] == "123456789"
    assert extra["empty"] == ""  # verbatim — payload extra is authoritative
