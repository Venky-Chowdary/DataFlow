"""QA MX1-02 — "SFTP rejects every new host". Verification itself is correct
(fail closed, no TOFU), but the operator had no per-connector way through:
Pilot's create_connector had no ``host_key`` argument, its probe dropped one,
and the error only named process-wide env vars. Now the refusal shows the
fingerprint and the exact ``host_key=`` value to approve; passing it pins that
connector only. Live against atmoz/sftp on 127.0.0.1:2222.
"""

from __future__ import annotations

import inspect
import re
import socket

import pytest

from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools
from src.transfer.connector_registry import run_probe

_CFG = {"type": "sftp", "host": "127.0.0.1", "port": 2222, "username": "df", "password": "df",
        "database": "/upload", "connection_string": "", "ssl": False}


def _up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 2222), timeout=1).close()
        return True
    except OSError:
        return False


def test_create_connector_tool_accepts_host_key():
    spec = next(t for t in TOOL_DEFINITIONS if t["name"] == "create_connector")
    assert "host_key" in spec["input_schema"]["properties"]
    assert "host_key" in inspect.signature(DataPilotTools._create_connector).parameters


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


def test_update_connector_explicitly_refuses_host_key_changes():
    res = DataPilotTools().execute("update_connector", {"name": "any-sftp", "host_key": _FP})
    assert not res.success
    err = res.error or ""
    assert "host key" in err.lower() and "create_connector" in err, err
