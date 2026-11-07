"""SFTP transport must verify the server host key (MITM proof).

Paramiko's raw ``Transport`` performs no host key check. Before this suite the
SFTP source/destination accepted any server key, so an on-path attacker could
read credentials and every migrated row of an SFTP → Snowflake route.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

paramiko = pytest.importorskip("paramiko")

from connectors.sftp_common import (  # noqa: E402
    host_key_fingerprint,
    parse_sftp_config,
    verify_host_key,
)


class _FakeTransport:
    def __init__(self, key):
        self._key = key

    def get_remote_server_key(self):
        return self._key


@pytest.fixture(scope="module")
def server_key():
    return paramiko.RSAKey.generate(2048)


@pytest.fixture(scope="module")
def other_key():
    return paramiko.RSAKey.generate(2048)


def _cfg(**kw):
    base = {"host": "sftp.example.com", "port": 22, "username": "u", "known_hosts": "/nonexistent"}
    base.update(kw)
    return parse_sftp_config(**base)


def test_unknown_host_key_is_refused(server_key):
    with pytest.raises(RuntimeError) as exc:
        verify_host_key(_cfg(), _FakeTransport(server_key))
    # The operator must be able to pin in one step — fingerprint is in the error.
    assert host_key_fingerprint(server_key) in str(exc.value)


def test_pinned_sha256_fingerprint_is_accepted(server_key):
    verify_host_key(
        _cfg(host_key=host_key_fingerprint(server_key)), _FakeTransport(server_key)
    )


def test_pinned_fingerprint_of_other_key_is_refused(server_key, other_key):
    with pytest.raises(RuntimeError):
        verify_host_key(
            _cfg(host_key=host_key_fingerprint(other_key)), _FakeTransport(server_key)
        )


def test_pinned_openssh_line_is_accepted(server_key):
    line = f"{server_key.get_name()} {server_key.get_base64()}"
    verify_host_key(_cfg(host_key=line), _FakeTransport(server_key))


def test_known_hosts_entry_is_accepted(tmp_path, server_key):
    kh = tmp_path / "known_hosts"
    kh.write_text(f"sftp.example.com {server_key.get_name()} {server_key.get_base64()}\n")
    verify_host_key(_cfg(known_hosts=str(kh)), _FakeTransport(server_key))


def test_known_hosts_nonstandard_port_entry_is_accepted(tmp_path, server_key):
    kh = tmp_path / "known_hosts"
    kh.write_text(
        f"[sftp.example.com]:2222 {server_key.get_name()} {server_key.get_base64()}\n"
    )
    verify_host_key(_cfg(port=2222, known_hosts=str(kh)), _FakeTransport(server_key))


def test_known_hosts_key_change_is_refused(tmp_path, server_key, other_key):
    kh = tmp_path / "known_hosts"
    kh.write_text(f"sftp.example.com {other_key.get_name()} {other_key.get_base64()}\n")
    with pytest.raises(RuntimeError) as exc:
        verify_host_key(_cfg(known_hosts=str(kh)), _FakeTransport(server_key))
    assert "mismatch" in str(exc.value).lower()


def test_explicit_insecure_policy_is_the_only_bypass(server_key):
    verify_host_key(
        _cfg(host_key_policy="insecure_ignore"), _FakeTransport(server_key)
    )


def _md5_fingerprint(key) -> str:
    import hashlib

    # Renders OpenSSH's legacy MD5 fingerprint so the test can prove we refuse
    # it — the weak digest is the fixture under test, never a security control.
    digest = hashlib.md5(key.asbytes(), usedforsecurity=False).hexdigest()
    return ":".join(digest[i : i + 2] for i in range(0, len(digest), 2))


def test_md5_pin_is_refused_even_when_it_matches(server_key):
    """MD5 is chosen-prefix broken, so a matching MD5 pin proves nothing."""
    with pytest.raises(RuntimeError) as exc:
        verify_host_key(
            _cfg(host_key=f"MD5:{_md5_fingerprint(server_key)}"),
            _FakeTransport(server_key),
        )
    assert "MD5" in str(exc.value)
    assert host_key_fingerprint(server_key) in str(exc.value)


def test_bare_md5_hex_pin_is_refused(server_key):
    with pytest.raises(RuntimeError) as exc:
        verify_host_key(
            _cfg(host_key=_md5_fingerprint(server_key)), _FakeTransport(server_key)
        )
    assert "MD5" in str(exc.value)
