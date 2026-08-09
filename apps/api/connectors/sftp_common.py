"""Shared SFTP connection and path helpers for source/destination connectors."""

from __future__ import annotations

import base64
import binascii
import hashlib
import logging
import os
import re
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Operator opt-out for host key verification. Trust-on-first-use is not a
# default: an unverified SFTP transport is a MITM-readable credential and data
# channel, which no regulated migration can accept.
_INSECURE_POLICIES = frozenset({"insecure_ignore", "ignore", "none", "off"})


class SFTPConfig:
    __slots__ = (
        "host",
        "port",
        "username",
        "password",
        "path",
        "private_key",
        "private_key_passphrase",
        "host_key",
        "known_hosts",
        "host_key_policy",
    )

    def __init__(self) -> None:
        self.host = ""
        self.port = 22
        self.username = ""
        self.password = ""  # nosec B105
        self.path = ""
        self.private_key = ""
        self.private_key_passphrase = ""  # nosec B105
        # Pinned server key: OpenSSH line, bare base64 blob, or ``SHA256:...``.
        self.host_key = ""
        self.known_hosts = ""
        self.host_key_policy = ""


def _default_port(scheme: str) -> int:
    return 22 if scheme in ("sftp", "ssh", "") else 22


def parse_sftp_config(
    *,
    connection_string: str = "",
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    database: str = "",
    table: str = "",
    service_account: str = "",
    api_key: str = "",
    private_key: str = "",
    private_key_passphrase: str = "",
    host_key: str = "",
    known_hosts: str = "",
    host_key_policy: str = "",
    **_kwargs: Any,
) -> SFTPConfig:
    """Merge explicit fields with an sftp:// URI."""
    cfg = SFTPConfig()
    cfg.private_key_passphrase = (private_key_passphrase or "").strip()
    cfg.host_key = (host_key or "").strip()
    cfg.known_hosts = (known_hosts or os.getenv("DATAFLOW_SFTP_KNOWN_HOSTS", "")).strip()
    cfg.host_key_policy = (host_key_policy or "").strip().lower()
    raw = (connection_string or "").strip()

    if raw:
        parsed = urlparse(raw)
        if parsed.scheme in ("sftp", "ssh"):
            cfg.host = (parsed.hostname or "").strip()
            cfg.port = parsed.port or _default_port(parsed.scheme)
            cfg.username = (parsed.username or "").strip()
            cfg.password = (parsed.password or "").strip()
            cfg.path = parsed.path or ""
        else:
            # Treat a bare connection string as a remote path.
            cfg.path = raw

    if host:
        cfg.host = host.strip()
    if port:
        cfg.port = int(port)
    if username:
        cfg.username = username.strip()
    if password:
        cfg.password = password.strip()

    # private key can be explicit, ride service_account (file path/key text) or api_key
    cfg.private_key = (private_key or service_account or api_key or "").strip()

    # If table/filename is provided separately, append it to the directory path.
    if table and database:
        cfg.path = (database.rstrip("/") + "/" + table.lstrip("/")).replace("//", "/")
    elif table and not cfg.path:
        cfg.path = table
    elif database and not cfg.path:
        cfg.path = database

    return cfg


def split_remote_path(path: str) -> tuple[str, str]:
    """Return (directory, filename) for a remote SFTP path."""
    path = path.strip()
    if not path or path == "/":
        return "", ""
    if path.endswith("/"):
        return path.rstrip("/"), ""
    directory, filename = os.path.split(path)
    return directory or "/", filename


def host_key_fingerprint(key: Any) -> str:
    """OpenSSH-style ``SHA256:base64`` fingerprint of a paramiko host key."""
    digest = hashlib.sha256(key.asbytes()).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii").rstrip("=")


def _pinned_host_key_matches(pinned: str, server_key: Any) -> bool:
    """True when the presented key matches an operator-pinned key or fingerprint."""
    token = (pinned or "").strip()
    if not token:
        return False
    if token.upper().startswith("SHA256:"):
        want = token.split(":", 1)[1].strip().rstrip("=")
        return host_key_fingerprint(server_key).split(":", 1)[1] == want
    if token.upper().startswith("MD5:") or re.fullmatch(r"(?:[0-9a-fA-F]{2}:){15}[0-9a-fA-F]{2}", token):
        # Legacy MD5 fingerprint — accepted for pinning parity with OpenSSH
        # clients, but it is not collision resistant; prefer SHA256.
        want = token.split(":", 1)[1] if token.upper().startswith("MD5:") else token
        got = hashlib.md5(server_key.asbytes()).hexdigest()  # nosec B324
        return want.replace(":", "").lower() == got
    # OpenSSH ``known_hosts``-style line or a bare base64 key blob.
    blob = token.split()[-1] if " " in token else token
    try:
        return base64.b64decode(blob, validate=True) == server_key.asbytes()
    except (binascii.Error, ValueError):
        return False


def _known_hosts_paths(cfg: SFTPConfig) -> list[str]:
    if cfg.known_hosts:
        return [p for p in cfg.known_hosts.split(os.pathsep) if p.strip()]
    default = os.path.expanduser("~/.ssh/known_hosts")
    return [default] if os.path.isfile(default) else []


def verify_host_key(cfg: SFTPConfig, transport: Any) -> None:
    """Fail closed unless the server key is pinned or in ``known_hosts``.

    Paramiko's raw ``Transport`` performs no host key check, so without this an
    SFTP route is trivially MITM-able — credentials and every migrated row.
    Trust-on-first-use is deliberately not offered; the error carries the
    observed fingerprint so an operator can pin it in one step.
    """
    server_key = transport.get_remote_server_key()
    if _pinned_host_key_matches(cfg.host_key, server_key):
        return

    import paramiko

    entry_names = [cfg.host if cfg.port == 22 else f"[{cfg.host}]:{cfg.port}"]
    for path in _known_hosts_paths(cfg):
        try:
            hostkeys = paramiko.hostkeys.HostKeys(filename=path)
        except OSError as exc:
            logger.warning("known_hosts unreadable (%s): %s", path, exc)
            continue
        for name in entry_names:
            known = hostkeys.lookup(name)
            if known is None:
                continue
            expected = known.get(server_key.get_name())
            if expected is not None and expected.asbytes() == server_key.asbytes():
                return
            if expected is not None:
                raise RuntimeError(
                    f"SFTP host key mismatch for {name}: server offered "
                    f"{host_key_fingerprint(server_key)} but known_hosts pins a "
                    f"different {server_key.get_name()} key. Refusing to "
                    "transfer — resolve the key change before re-running."
                )

    if cfg.host_key_policy in _INSECURE_POLICIES:
        logger.warning(
            "SFTP host key verification disabled by host_key_policy for %s:%s "
            "(fingerprint %s) — transport is not MITM-protected",
            cfg.host,
            cfg.port,
            host_key_fingerprint(server_key),
        )
        return

    raise RuntimeError(
        f"SFTP host key for {cfg.host}:{cfg.port} is not trusted "
        f"({server_key.get_name()} {host_key_fingerprint(server_key)}). Pin it "
        "via the connection's host_key field, add it to a known_hosts file "
        "(DATAFLOW_SFTP_KNOWN_HOSTS), or set host_key_policy=insecure_ignore "
        "to accept an unverified transport."
    )


def load_private_key(cfg: SFTPConfig) -> Any:
    """Parse the configured private key (path or PEM text), else ``None``."""
    if not cfg.private_key:
        return None
    import io

    import paramiko

    key_text = cfg.private_key
    if os.path.isfile(key_text):
        with open(key_text, "r") as f:
            key_text = f.read()
    passphrase = cfg.private_key_passphrase or None
    errors: list[str] = []
    for key_cls in (
        paramiko.Ed25519Key,
        paramiko.ECDSAKey,
        paramiko.RSAKey,
        paramiko.DSSKey,
    ):
        try:
            return key_cls.from_private_key(
                file_obj=io.StringIO(key_text), password=passphrase
            )
        except paramiko.PasswordRequiredException as exc:
            raise RuntimeError(
                "SFTP private key is encrypted — supply private_key_passphrase."
            ) from exc
        except Exception as exc:  # wrong algorithm for this parser
            errors.append(f"{key_cls.__name__}: {exc}")
    # Never fall through to password auth pretending the key was ignored.
    raise RuntimeError(
        "SFTP private key could not be parsed as Ed25519/ECDSA/RSA/DSA: "
        + "; ".join(errors[-2:])
    )


def connect_sftp(cfg: SFTPConfig):
    """Return (transport, sftp) client pair using paramiko, host key verified."""
    try:
        import paramiko
    except Exception as exc:
        raise RuntimeError(f"paramiko is not installed: {exc}") from exc

    pkey = load_private_key(cfg)

    transport = paramiko.Transport((cfg.host, cfg.port))
    try:
        transport.start_client(timeout=30)
        verify_host_key(cfg, transport)
        if pkey is not None:
            transport.auth_publickey(cfg.username, pkey)
        elif cfg.password:
            transport.auth_password(cfg.username, cfg.password)
        else:
            raise RuntimeError(
                "SFTP requires a password or private key for authentication."
            )
        sftp = paramiko.SFTPClient.from_transport(transport)
        if sftp is None:
            raise RuntimeError("Could not open SFTP client")
    except Exception:
        transport.close()
        raise
    return transport, sftp


def test_sftp(
    *,
    connection_string: str = "",
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    database: str = "",
    table: str = "",
    service_account: str = "",
    api_key: str = "",
    private_key: str = "",
    **_kwargs: Any,
) -> tuple[bool, str]:
    """Verify SFTP connectivity and optional directory access."""
    try:
        cfg = parse_sftp_config(
            connection_string=connection_string,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            table=table,
            service_account=service_account,
            api_key=api_key,
            private_key=private_key,
        )
        if not cfg.host:
            return False, "SFTP host is required. Use an sftp:// URL or the host/port fields."
        if not cfg.username:
            return False, "SFTP username is required."

        transport, sftp = connect_sftp(cfg)
        try:
            if cfg.path:
                directory, _ = split_remote_path(cfg.path)
                if directory:
                    sftp.stat(directory)
            return True, f"SFTP server {cfg.host}:{cfg.port} reachable and authenticated."
        finally:
            sftp.close()
            transport.close()
    except Exception as exc:
        return False, f"SFTP test failed: {exc}"
