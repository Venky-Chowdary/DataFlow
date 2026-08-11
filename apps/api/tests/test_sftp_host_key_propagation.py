"""Host-key trust must survive every SFTP config rebuild.

The writer, the connection test, the Gate-8 read-back and the destination
sample each reconstruct an ``SFTPConfig`` from a flat dict. Dropping
``host_key``/``known_hosts``/``host_key_policy`` in any of them silently
downgrades that hop to an unverified transport — the write is pinned, the proof
read is not.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sftp_common import host_key_settings, parse_sftp_config  # noqa: E402

_TRUST = {
    "host_key": "SHA256:abc123",
    "known_hosts": "/etc/dataflow/known_hosts",
    "host_key_policy": "strict",
}


def test_host_key_settings_lifts_only_trust_fields():
    assert host_key_settings({**_TRUST, "password": "s3cret"}) == _TRUST


def test_host_key_settings_defaults_to_empty_strings():
    assert host_key_settings({"host": "sftp.example"}) == {
        "host_key": "",
        "known_hosts": "",
        "host_key_policy": "",
    }


def test_parse_sftp_config_keeps_trust_fields():
    cfg = parse_sftp_config(host="sftp.example", username="u", **_TRUST)
    assert cfg.host_key == "SHA256:abc123"
    assert cfg.known_hosts == "/etc/dataflow/known_hosts"
    assert cfg.host_key_policy == "strict"


def test_env_supplies_trust_when_the_caller_has_none(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SFTP_HOST_KEY", "SHA256:fromenv")
    monkeypatch.setenv("DATAFLOW_SFTP_HOST_KEY_POLICY", "strict")
    cfg = parse_sftp_config(host="sftp.example", username="u")
    assert cfg.host_key == "SHA256:fromenv"
    assert cfg.host_key_policy == "strict"


def test_inline_trust_wins_over_env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SFTP_HOST_KEY", "SHA256:fromenv")
    cfg = parse_sftp_config(host="sftp.example", username="u", host_key="SHA256:inline")
    assert cfg.host_key == "SHA256:inline"


def _captured_cfg(call_target, invoke):
    seen: dict[str, object] = {}

    def _capture(**kwargs):
        seen.update(kwargs)
        cfg = MagicMock()
        cfg.host = ""
        cfg.path = ""
        return cfg

    with patch(call_target, side_effect=_capture):
        invoke()
    return seen


def test_writer_forwards_trust():
    from connectors import sftp_writer

    seen = _captured_cfg(
        "connectors.sftp_writer.parse_sftp_config",
        lambda: sftp_writer.write_mapped_rows(
            host="sftp.example",
            table_name="out.csv",
            headers=["a"],
            data_rows=[["1"]],
            mappings=[{"source": "a", "target": "a"}],
            column_types={"a": "VARCHAR"},
            **_TRUST,
        ),
    )
    assert {k: seen[k] for k in _TRUST} == _TRUST


def test_connection_test_forwards_trust():
    from connectors import sftp_common

    seen = _captured_cfg(
        "connectors.sftp_common.parse_sftp_config",
        lambda: sftp_common.test_sftp(host="sftp.example", username="u", **_TRUST),
    )
    assert {k: seen[k] for k in _TRUST} == _TRUST


def test_gate8_read_back_forwards_trust():
    from services import reconciliation

    seen: dict[str, object] = {}

    def _capture(**kwargs):
        seen.update(kwargs)
        cfg = MagicMock()
        cfg.host = ""
        cfg.path = ""
        return cfg

    with patch("connectors.sftp_common.parse_sftp_config", side_effect=_capture):
        reconciliation.verify_sftp_object(
            host="sftp.example", table_name="out.csv", database="/exports", **_TRUST
        )
    assert {k: seen[k] for k in _TRUST} == _TRUST


def test_verify_target_lifts_trust_from_the_destination_config():
    from services import reconciliation

    with patch.object(
        reconciliation, "verify_sftp_object", return_value=(3, "chk")
    ) as mocked:
        reconciliation.verify_target(
            "sftp",
            {"host": "sftp.example", "database": "/exports", **_TRUST},
            schema="",
            table_name="orders.csv",
            fallback_rows=-1,
            fallback_checksum="",
        )
    assert {k: mocked.call_args.kwargs[k] for k in _TRUST} == _TRUST
