"""QA MX1-02 — "SFTP rejects every new host". Verification itself is correct
(fail closed, no TOFU), but the operator had no per-connector way through:
Pilot's create_connector had no ``host_key`` argument, its probe dropped one,
and the error only named process-wide env vars. Now the refusal shows the
fingerprint and the exact ``host_key=`` value to approve; passing it pins that
connector only. Live against atmoz/sftp on 127.0.0.1:2222.
"""

from __future__ import annotations

import inspect
import os
import re
import socket

import pytest

from src.ai.copilot.lifecycle_tools import (
    LIFECYCLE_TOOL_DEFINITIONS,
    update_connector as lifecycle_update_connector,
)
from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools
from src.transfer.connector_registry import run_probe


@pytest.fixture(autouse=True)
def _egress_hermetic(monkeypatch):
    """``sftp.example.com`` fixtures resolve to TEST-NET-1 (198.18.0.0/15) on
    this box, which the egress guard refuses without an explicit allowlist.
    Merge — never replace — so the live pin-connect test keeps working on a
    box whose SFTP container sits on loopback (127.0.0.1:2222)."""
    existing = os.environ.get("DATAFLOW_EGRESS_ALLOWLIST") or ""
    merged = ",".join(
        p
        for p in [existing, "127.0.0.0/8", "::1/128", "198.18.0.0/15"]
        if p.strip()
    )
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", merged)


# Live pin-connect creds match the qa-sftp container by default and are
# overridable per box (DATAFLOW_TEST_SFTP_*).
_SFTP_HOST = os.environ.get("DATAFLOW_TEST_SFTP_HOST", "127.0.0.1")
_SFTP_PORT = int(os.environ.get("DATAFLOW_TEST_SFTP_PORT", "2222"))
_SFTP_USER = os.environ.get("DATAFLOW_TEST_SFTP_USER", "dataflow")
_SFTP_PASSWORD = os.environ.get("DATAFLOW_TEST_SFTP_PASSWORD", "dataflow")
_SFTP_PATH = os.environ.get("DATAFLOW_TEST_SFTP_PATH", "uploads")

_CFG = {"type": "sftp", "host": _SFTP_HOST, "port": _SFTP_PORT, "username": _SFTP_USER,
        "password": _SFTP_PASSWORD, "database": _SFTP_PATH, "connection_string": "", "ssl": False}


def _up() -> bool:
    try:
        socket.create_connection((_SFTP_HOST, _SFTP_PORT), timeout=1).close()
        return True
    except OSError:
        return False


def test_create_connector_tool_accepts_host_key():
    spec = next(t for t in TOOL_DEFINITIONS if t["name"] == "create_connector")
    assert "host_key" in spec["input_schema"]["properties"]
    assert "host_key" in inspect.signature(DataPilotTools._create_connector).parameters


def test_untrusted_host_message_keeps_the_full_pinable_fingerprint():
    """D-BP: the probe used to return a 117-char driver snippet, cutting the
    host_key=SHA256:... instruction — an operator could never pin."""
    from src.transfer.connector_registry import humanize_connection_error

    fp = "SHA256:" + base64.b64encode(hashlib.sha256(b"mx1-02-live").digest()).decode().rstrip("=")
    raw = (
        f"SFTP host key for 127.0.0.1:2222 is not trusted (ssh-ed25519 {fp}). After "
        "confirming this fingerprint with the server's administrator, approve "
        f"it for this connector by saving host_key={fp} (pins this connector only). "
        "Alternatively pin it process-wide with DATAFLOW_SFTP_HOST_KEY, add it to a "
        "known_hosts file (DATAFLOW_SFTP_KNOWN_HOSTS), or set "
        "DATAFLOW_SFTP_HOST_KEY_POLICY=insecure_ignore to accept an unverified transport."
    )
    msg = humanize_connection_error("sftp", raw)
    m = re.search(r"host_key=(SHA256:[A-Za-z0-9+/=]+)", msg)
    assert m, msg
    assert m.group(1) == fp


@pytest.mark.skipif(not _up(), reason="SFTP container not on 127.0.0.1:2222")
def test_untrusted_host_names_fingerprint_and_operator_pin_then_pin_connects(monkeypatch):
    for env in ("DATAFLOW_SFTP_HOST_KEY", "DATAFLOW_SFTP_KNOWN_HOSTS", "DATAFLOW_SFTP_HOST_KEY_POLICY"):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setenv("HOME", "/nonexistent-mx1-02")
    ok, msg = run_probe("sftp", dict(_CFG))
    assert not ok and "not trusted" in msg
    m = re.search(r"host_key=(SHA256:[A-Za-z0-9+/=]+)", msg)
    assert m, msg
    ok, msg = run_probe("sftp", {**_CFG, "host_key": m.group(1)})
    if not ok and re.search(r"auth|permission denied|password", msg, re.I):
        pytest.skip(
            "SFTP login refused — set DATAFLOW_TEST_SFTP_USER / "
            "DATAFLOW_TEST_SFTP_PASSWORD / DATAFLOW_TEST_SFTP_PATH to the "
            f"container's credentials (tried {_SFTP_USER}@{_SFTP_HOST}:{_SFTP_PORT} {_SFTP_PATH}): {msg}"
        )
    assert ok, msg
    ok, msg = run_probe("sftp", {**_CFG, "host_key": "ssh-ed25519 SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"})
    assert not ok


# --- Hardening: validate the pin at the boundary and show it before Confirm --

import base64  # noqa: E402
import hashlib  # noqa: E402
import uuid  # noqa: E402

_FP = "SHA256:" + base64.b64encode(hashlib.sha256(b"mx1-02").digest()).decode().rstrip("=")


def _sftp_args(**extra):
    return {"name": f"sftp-mx1-02-{uuid.uuid4().hex[:6]}", "type": "sftp", "host": "sftp.example.com",
            "port": 22, "username": "df", "password": "df", "database": "/upload",
            "test_first": False, **extra}


def test_host_key_pin_accepts_only_sha256_fingerprints():
    from connectors.sftp_common import normalize_host_key_pin

    assert normalize_host_key_pin(_FP) == _FP
    assert normalize_host_key_pin(f"  {_FP}= ") == _FP
    assert normalize_host_key_pin("sha256:" + _FP.split(":", 1)[1]) == _FP
    for bad in (
        "SHA256:abc",
        "MD5:16:27:ac:a5:76:28:2d:36:63:1b:56:4d:eb:df:a6:48",
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl",
        _FP + "!",
        _FP[:-1] + " x",
    ):
        with pytest.raises(ValueError, match="SHA256:"):
            normalize_host_key_pin(bad)


def test_create_connector_refuses_a_malformed_pin_before_probing(monkeypatch):
    def boom(*_a, **_k):
        raise AssertionError("probe must not run for a malformed pin")

    monkeypatch.setattr("src.transfer.connector_registry.run_probe", boom)
    res = DataPilotTools().execute(
        "create_connector", _sftp_args(host_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5", test_first=True)
    )
    assert not res.success
    assert "SHA256:" in (res.error or "") and "host_key" in (res.error or ""), res.error


def test_create_connector_confirm_preview_names_the_pin():
    from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn

    res = DataPilotTools().execute("create_connector", _sftp_args(host_key=_FP))
    assert res.success, res.error
    assert res.output["preview"]["host_key"] == _FP
    turn = PilotTurn()
    turn.tool_results.append(res)
    answer = DataPilotAgent()._compose_local_answer("save it", "connector", turn, None, {})
    line = next((ln for ln in answer.splitlines() if "Pins SFTP host key" in ln), "")
    assert _FP in line and "verified it with the server admin" in line, answer


def test_create_connector_preview_has_no_pin_line_without_a_pin():
    from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn

    res = DataPilotTools().execute("create_connector", _sftp_args())
    assert res.success, res.error
    turn = PilotTurn()
    turn.tool_results.append(res)
    answer = DataPilotAgent()._compose_local_answer("save it", "connector", turn, None, {})
    assert "Pins SFTP host key" not in answer


# --- X-28: trust fields are real MCP surface on both create and update ------
#
# ConnectorSaveDTO carries host_key / known_hosts / host_key_policy /
# trust_server_certificate and the store lifts them into ``extra`` — so the
# earlier "update has no host-key field" refusal was stale: it told operators
# to re-create the connector (losing bound pipelines) for a field the PUT
# already persists.


def test_update_connector_tool_accepts_trust_fields():
    spec = next(t for t in LIFECYCLE_TOOL_DEFINITIONS if t["name"] == "update_connector")
    props = spec["input_schema"]["properties"]
    for field in ("host_key", "known_hosts", "host_key_policy", "trust_server_certificate"):
        assert field in props, field
    sig = inspect.signature(lifecycle_update_connector)
    for field in ("host_key", "known_hosts", "host_key_policy", "trust_server_certificate"):
        assert field in sig.parameters, field


def test_create_connector_tool_accepts_trust_fields():
    spec = next(t for t in TOOL_DEFINITIONS if t["name"] == "create_connector")
    props = spec["input_schema"]["properties"]
    for field in ("host_key", "known_hosts", "host_key_policy", "trust_server_certificate"):
        assert field in props, field


def test_update_connector_refuses_a_malformed_pin_before_resolving():
    # A garbage pin fails loudly at stage time — before connector resolution.
    res = DataPilotTools().execute(
        "update_connector", {"name": "no-such-connector", "host_key": "SHA256:abc"}
    )
    assert not res.success
    assert "SHA256:" in (res.error or ""), res.error


def test_update_connector_stages_host_key_and_trust_fields(monkeypatch):
    staged: list[dict] = []

    class Ledger:
        def put(self, *, kind, payload, preview):
            staged.append({"kind": kind, "payload": payload, "preview": preview})
            return f"ack-{len(staged)}"

    import src.ai.copilot.ack_ledger as ledger_mod
    from src.ai.copilot import lifecycle_tools as lt

    existing = {
        "id": "sftp-1",
        "name": "SFTP Prod",
        "type": "sftp",
        "role": "destination",
        "host": "sftp.example.com",
        "port": 22,
        "username": "df",
        "extra": {"host_key": "SHA256:old-pin"},
    }
    monkeypatch.setattr(ledger_mod, "get_ack_ledger", lambda: Ledger())
    monkeypatch.setattr(
        lt, "_connector_dict",
        lambda cid="", name="", **_kw: dict(existing) if (name or cid) in {"SFTP Prod", "sftp-1"} else None,
    )
    res = DataPilotTools().execute(
        "update_connector",
        {
            "connector_id": "sftp-1",
            "host_key": _FP,
            "host_key_policy": "strict",
            "trust_server_certificate": True,
        },
    )
    assert res.success, res.error
    changes = staged[0]["payload"]["changes"]
    assert changes["host_key"] == _FP
    assert changes["host_key_policy"] == "strict"
    assert changes["trust_server_certificate"] is True


def test_store_round_trips_trust_fields_into_extra(monkeypatch, tmp_path):
    from services import connector_store

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    conn = connector_store.create_connector(
        {
            "name": "sqlsrv-x28",
            "type": "sqlserver",
            "host": "db.example.com",
            "port": 1433,
            "username": "sa",
            "password": "x",
            "trust_server_certificate": True,
        }
    )
    assert conn.extra.get("trust_server_certificate") is True

    updated = connector_store.update_connector(
        conn.id,
        {
            "name": "sqlsrv-x28",
            "type": "sqlserver",
            "host": "db.example.com",
            "host_key": _FP,
            "host_key_policy": "strict",
        },
    )
    assert updated is not None
    # New root-level trust fields lift into extra on update…
    assert updated.extra.get("host_key") == _FP
    assert updated.extra.get("host_key_policy") == "strict"
    # …and the previously stored trust flag survives the merge.
    assert updated.extra.get("trust_server_certificate") is True
