"""Shared SFTP connection and path helpers for source/destination connectors."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse


class SFTPConfig:
    __slots__ = ("host", "port", "username", "password", "path", "private_key")

    def __init__(self) -> None:
        self.host = ""
        self.port = 22
        self.username = ""
        self.password = ""
        self.path = ""
        self.private_key = ""


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
    **_kwargs: Any,
) -> SFTPConfig:
    """Merge explicit fields with an sftp:// URI."""
    cfg = SFTPConfig()
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


def connect_sftp(cfg: SFTPConfig):
    """Return (transport, sftp) client pair using paramiko."""
    try:
        import paramiko
    except Exception as exc:
        raise RuntimeError(f"paramiko is not installed: {exc}") from exc

    pkey = None
    if cfg.private_key:
        # Try as a file path first, then as key text.
        key_text = cfg.private_key
        if os.path.isfile(key_text):
            with open(key_text, "r") as f:
                key_text = f.read()
        for key_cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
            try:
                pkey = key_cls.from_private_key(file_obj=__import__("io").StringIO(key_text))
                break
            except Exception:
                pass

    transport = paramiko.Transport((cfg.host, cfg.port))
    transport.connect(username=cfg.username, password=cfg.password or None, pkey=pkey)
    sftp = paramiko.SFTPClient.from_transport(transport)
    if sftp is None:
        transport.close()
        raise RuntimeError("Could not open SFTP client")
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
