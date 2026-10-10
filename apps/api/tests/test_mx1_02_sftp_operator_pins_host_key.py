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
