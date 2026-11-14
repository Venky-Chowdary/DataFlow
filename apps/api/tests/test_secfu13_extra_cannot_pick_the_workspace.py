"""Follow-up review 1, M2: a connector's ``extra`` cannot pick the workspace (or tenant or connector).

``resolve_connector_config`` merges an endpoint's ``extra`` into the resolved
config, so an inline endpoint with ``extra={"workspace_id": "VICTIM-WS"}``
resolved in ATTACKER-WS came back with ``cfg["workspace_id"] == "VICTIM-WS"``.
The egress checks load the allowlist of that workspace's tenant from it
(``generic_sql._build_engine``, ``connector_registry``, ``aws_common``): the
attacker borrowed another tenant's private-network allowlist. Same class as
security follow-up 6 (``extra.type``). The resolved workspace now wins and
``tenant_id`` / ``connector_id`` from ``extra`` are dropped.
"""

from __future__ import annotations

import pytest

from src.transfer.adapters import resolve_connector_config
from src.transfer.models import EndpointConfig


def _endpoint(**extra) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="postgresql", host="10.20.0.5", port=5432, database="d",
        username="u", password="p", table="t", extra=dict(extra),
    )


@pytest.fixture(autouse=True)
def _quiet(monkeypatch):
    import services.audit_log as audit_log

    monkeypatch.setattr(audit_log, "append_audit_event", lambda **_kw: None)


def test_the_resolved_workspace_wins_over_extra():
    cfg = resolve_connector_config(
        _endpoint(workspace_id="VICTIM-WS", host="10.20.0.5", sslrootcert="/certs/ca.pem"),
        workspace_id="ATTACKER-WS",
    )
    assert cfg["workspace_id"] == "ATTACKER-WS"
    assert cfg["sslrootcert"] == "/certs/ca.pem", "other extra settings still reach the driver"


@pytest.mark.parametrize("key", ["tenant_id", "connector_id"])
def test_extra_cannot_set_the_tenant_or_the_connector(key):
    cfg = resolve_connector_config(_endpoint(**{key: "VICTIM"}), workspace_id="ATTACKER-WS")
    assert cfg.get(key) in (None, ""), cfg.get(key)


def test_without_a_resolved_workspace_extra_still_cannot_name_one():
    cfg = resolve_connector_config(_endpoint(workspace_id="VICTIM-WS"))
    assert cfg.get("workspace_id") in (None, "")


def test_the_egress_check_reads_the_resolved_workspaces_allowlist(monkeypatch):
    """The allowlist consulted at the pre-dial check is ATTACKER-WS's, not VICTIM-WS's."""
    from connectors import generic_sql
    from services import egress_guard

    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_EGRESS_ALLOWLIST", raising=False)
    asked: list[str] = []

    def entries(workspace_id):
        asked.append(workspace_id)
        return ["10.20.0.0/16"] if workspace_id == "VICTIM-WS" else []

    monkeypatch.setattr(egress_guard, "_workspace_egress_entries", entries)
    cfg = resolve_connector_config(_endpoint(workspace_id="VICTIM-WS"), workspace_id="ATTACKER-WS")
    with pytest.raises(egress_guard.EgressRefused):
        generic_sql._build_engine(cfg)
    assert asked and set(asked) == {"ATTACKER-WS"}
