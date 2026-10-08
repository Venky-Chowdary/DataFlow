"""Shared SFTP connection and path helpers for source/destination connectors."""

from __future__ import annotations

import base64
import binascii
import hashlib
import logging
import os
import re
from collections.abc import Mapping
from typing import Any

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


def host_key_settings(source: Mapping[str, Any]) -> dict[str, str]:
    """Host-key trust settings lifted out of a connection config.

    Every path that rebuilds an ``SFTPConfig`` (writer, connection test,
    Gate-8 read-back, destination sample) must carry these through: dropping
    them silently downgrades that connection to "no pinned key", so a route the
    operator pinned would verify on write and fall back to ``known_hosts`` (or
    fail) when read back.
    """
    return {
        "host_key": str(source.get("host_key") or ""),
        "known_hosts": str(source.get("known_hosts") or ""),
        "host_key_policy": str(source.get("host_key_policy") or ""),
    }


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
    # Saved connectors persist host-key trust on ``extra`` (and the
    # schedule runner lifts it onto the endpoint). The environment remains
    # the fallback when a caller supplies none; an inline value still wins.
    cfg.host_key = (host_key or os.getenv("DATAFLOW_SFTP_HOST_KEY", "")).strip()
    cfg.known_hosts = (known_hosts or os.getenv("DATAFLOW_SFTP_KNOWN_HOSTS", "")).strip()
    cfg.host_key_policy = (
        host_key_policy or os.getenv("DATAFLOW_SFTP_HOST_KEY_POLICY", "")
    ).strip().lower()
    raw = (connection_string or "").strip()

    if raw:
        from connectors.url_authority import parse_url_authority

        parsed = parse_url_authority(raw)
        if parsed.scheme in ("sftp", "ssh") and parsed.host:
            cfg.host = parsed.host.strip()
            cfg.port = parsed.port or _default_port(parsed.scheme)
            cfg.username = parsed.user.strip()
            cfg.password = parsed.password
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

    # Compose database+table only when the URI has no file path, or the
    # operator set a real directory (not form-default "/"). Otherwise
    # database="/" + table="data.csv" used to clobber /incoming/nested/data.csv.
    if table and database:
        composed = (database.rstrip("/") + "/" + table.lstrip("/")).replace("//", "/")
        uri_has_file = bool(cfg.path and cfg.path not in ("/", ""))
        database_is_root = database.strip() in ("", "/", ".")
        if not uri_has_file or not database_is_root:
            cfg.path = composed
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
    if token.upper().startswith("MD5:") or re.fullmatch(
        r"(?:[0-9a-fA-F]{2}:){15}[0-9a-fA-F]{2}", token
    ):
        # MD5 is chosen-prefix collision broken, so an MD5 fingerprint cannot
        # bind a host key: an attacker able to MITM can present a colliding key
        # that satisfies the pin. Refuse rather than pretend the transport is
        # verified — the operator is told exactly what to pin instead.
        raise RuntimeError(
            "SFTP host key pin uses an MD5 fingerprint, which is not collision "
            "resistant and cannot authenticate a host key. Re-pin with the "
            f"SHA256 fingerprint the server presented: {host_key_fingerprint(server_key)}"
        )
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
        "with DATAFLOW_SFTP_HOST_KEY, add it to a known_hosts file "
        "(DATAFLOW_SFTP_KNOWN_HOSTS), or set "
        "DATAFLOW_SFTP_HOST_KEY_POLICY=insecure_ignore to accept an "
        "unverified transport."
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
        getattr(paramiko, "DSSKey", None),
    ):
        if key_cls is None:
            continue
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


def _legacy_ssh_available() -> bool:
    """True when a last-attempt handshake can still speak group14-sha1 / ssh-rsa.

    Paramiko 3.5 ships both names. Paramiko 4 and 5 removed them from the
    default tables, which closed the socket during kex — before any auth
    packet — on appliances that offer nothing newer. This process can register
    those two algorithms on the real Transport class without putting them on
    the default preferred list. ``group1-sha1`` stays unavailable.
    A test double that replaced ``Transport`` has no algorithm table; the
    legacy attempt is then skipped so unit fakes keep the modern retry count.
    """
    try:
        import paramiko
    except ImportError:
        return False
    kex = getattr(paramiko.Transport, "_kex_info", None)
    if not isinstance(kex, dict):
        return False
    if "diffie-hellman-group14-sha1" in kex:
        return True
    try:
        from paramiko.kex_group14 import KexGroup14SHA256  # noqa: F401
        from paramiko.rsakey import RSAKey  # noqa: F401
    except ImportError:
        return False
    return True


def _register_legacy_ssh(paramiko_mod: Any) -> None:
    """Register group14-sha1 and ssh-rsa on this Paramiko, if it dropped them.

    Default ``_preferred_kex`` / ``_preferred_keys`` are not extended. Only a
    transport that calls :func:`_enable_legacy_ssh` advertises them, and only
    after the modern handshake has already failed. Host-key verification still
    runs before authentication.
    """
    from hashlib import sha1

    from cryptography.hazmat.primitives import hashes
    from paramiko.kex_group14 import KexGroup14SHA256
    from paramiko.rsakey import RSAKey

    kex_info = getattr(paramiko_mod.Transport, "_kex_info", None)
    key_info = getattr(paramiko_mod.Transport, "_key_info", None)
    if isinstance(kex_info, dict) and "diffie-hellman-group14-sha1" not in kex_info:

        class KexGroup14SHA1(KexGroup14SHA256):
            """RFC 4253 group14 with SHA-1. Offered only on the last attempt."""

            name = "diffie-hellman-group14-sha1"
            hash_algo = sha1

        kex_info["diffie-hellman-group14-sha1"] = KexGroup14SHA1
    if isinstance(key_info, dict) and "ssh-rsa" not in key_info:
        key_info["ssh-rsa"] = RSAKey
    # Verification looks the algorithm up on the key class, not the transport.
    # Advertising ssh-rsa without this entry would fail every host-key check
    # for that algorithm. Modern attempts never list ssh-rsa, so they cannot
    # select it.
    if "ssh-rsa" not in RSAKey.HASHES:
        RSAKey.HASHES["ssh-rsa"] = hashes.SHA1


def _modern_disabled_algorithms() -> dict[str, list[str]]:
    """Keep sha1 and ssh-rsa out of the first handshake attempts."""
    try:
        import paramiko
    except ImportError:
        return {}
    disabled: dict[str, list[str]] = {}
    preferred_kex = tuple(getattr(paramiko.Transport, "_preferred_kex", ()) or ())
    weak_kex = [
        name
        for name in ("diffie-hellman-group14-sha1", "diffie-hellman-group1-sha1")
        if name in preferred_kex
    ]
    if weak_kex:
        disabled["kex"] = weak_kex
    preferred_keys = tuple(getattr(paramiko.Transport, "_preferred_keys", ()) or ())
    if "ssh-rsa" in preferred_keys:
        disabled["keys"] = ["ssh-rsa"]
        disabled["pubkeys"] = ["ssh-rsa"]
    return disabled


def _enable_legacy_ssh(transport: Any) -> None:
    """Add sha1 group14 and ssh-rsa on this transport only.

    ``group1-sha1`` is not added. Host-key verification is unchanged.
    """
    import paramiko

    _register_legacy_ssh(paramiko)
    kex_info = getattr(paramiko.Transport, "_kex_info", {}) or {}
    key_info = getattr(paramiko.Transport, "_key_info", {}) or {}
    options = transport.get_security_options()
    if "diffie-hellman-group14-sha1" in kex_info:
        current = tuple(options.kex)
        if "diffie-hellman-group14-sha1" not in current:
            options.kex = current + ("diffie-hellman-group14-sha1",)
    if "ssh-rsa" in key_info:
        current_keys = tuple(options.key_types)
        if "ssh-rsa" not in current_keys:
            options.key_types = current_keys + ("ssh-rsa",)


def _open_sftp_transport(cfg: SFTPConfig) -> Any:
    """Handshake an SSH transport, then verify the host key before auth.

    Paramiko 3.4 advertises OpenSSH strict-kex by default. Tunnels and older
    sshd close that handshake before authentication (the socket dies during
    kex, so no password is ever sent). One retry without strict-kex keeps
    host-key verification and the same credentials. It does not skip auth.
    """
    import socket

    import paramiko

    # Paramiko 3.4+ advertises strict kex; 3.x/5 also sends server-sig-algs.
    # Tunnels and older sshd close the socket during that banner (EOFError,
    # not SSHException) before any auth packet is written. Each attempt still
    # verifies the host key. Auth itself happens after this function returns.
    attempts: list[dict[str, Any]] = [
        {"strict_kex": True, "server_sig_algs": True, "legacy": False},
        {"strict_kex": False, "server_sig_algs": True, "legacy": False},
        {"strict_kex": False, "server_sig_algs": False, "legacy": False},
    ]
    if _legacy_ssh_available():
        # Offered only after a modern handshake is refused. Host-key
        # verification still runs before any password or key is sent.
        attempts.append(
            {"strict_kex": False, "server_sig_algs": False, "legacy": True}
        )
    last_exc: Exception | None = None
    for index, opts in enumerate(attempts):
        sock = socket.create_connection((cfg.host, int(cfg.port or 22)), timeout=30)
        transport_kwargs: dict[str, Any] = {
            "strict_kex": opts["strict_kex"],
            "server_sig_algs": opts["server_sig_algs"],
        }
        if not opts["legacy"]:
            disabled = _modern_disabled_algorithms()
            if disabled:
                transport_kwargs["disabled_algorithms"] = disabled
        transport = paramiko.Transport(sock, **transport_kwargs)
        try:
            if opts["legacy"]:
                _enable_legacy_ssh(transport)
            transport.start_client(timeout=30)
            verify_host_key(cfg, transport)
            return transport
        except RuntimeError:
            # A refused host key is not a kex failure. Retrying it with
            # strict-kex off would not make the key trusted, and leaving the
            # socket open would keep a rejected transport alive.
            try:
                transport.close()
            except OSError:
                logger.debug("SFTP transport close after host-key refusal", exc_info=True)
            raise
        except (paramiko.SSHException, OSError, EOFError) as exc:
            last_exc = exc
            try:
                transport.close()
            except OSError:
                logger.debug("SFTP transport close after handshake failure", exc_info=True)
            if index == len(attempts) - 1:
                raise
            logger.info(
                "SFTP handshake closed before auth (%s); retrying %s:%s",
                type(exc).__name__,
                cfg.host,
                cfg.port,
            )
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("SFTP handshake failed before authentication")


def connect_sftp(cfg: SFTPConfig):
    """Return (transport, sftp) client pair using paramiko, host key verified."""
    try:
        import paramiko
    except Exception as exc:
        raise RuntimeError(f"paramiko is not installed: {exc}") from exc

    pkey = load_private_key(cfg)

    transport = _open_sftp_transport(cfg)
    try:
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
    host_key: str = "",
    known_hosts: str = "",
    host_key_policy: str = "",
    require_object: bool = True,
    **_kwargs: Any,
) -> tuple[bool, str]:
    """Verify SFTP connectivity and optional directory access.

    Host-key settings are forwarded so the test proves the *same* trust the
    transfer will use — a test that verified under looser trust than the write
    is worse than no test.

    ``require_object=False`` is the write/introspect contract: a target file the
    run is about to *create* must not be demanded up front. Requiring it turned
    every first write to a new remote path into "Destination unreachable:
    Authentication failed" on a server that had authenticated fine, and made
    introspect report ``connected: False`` for a path it could see the parent of.
    A missing *directory* still fails closed — the writer cannot invent one.
    """
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
            host_key=host_key,
            known_hosts=known_hosts,
            host_key_policy=host_key_policy,
        )
        if not cfg.host:
            return False, "SFTP host is required. Use an sftp:// URL or the host/port fields."
        if not cfg.username:
            return False, "SFTP username is required."

        transport, sftp = connect_sftp(cfg)
        try:
            if cfg.path:
                import stat as statmod

                try:
                    st = sftp.stat(cfg.path)
                except OSError as exc:
                    directory, filename = split_remote_path(cfg.path)
                    if filename and directory:
                        try:
                            sftp.stat(directory)
                        except OSError:
                            return False, f"SFTP authenticated but path not found: {cfg.path}"
                        if not require_object:
                            return True, (
                                f"SFTP directory {directory} reachable on "
                                f"{cfg.host}:{cfg.port}; {filename} does not exist yet."
                            )
                        return False, f"SFTP authenticated but file not found: {cfg.path}"
                    return False, f"SFTP authenticated but path not found: {cfg.path} ({exc})"
                if statmod.S_ISDIR(st.st_mode):
                    sftp.listdir(cfg.path)
                    return True, (
                        f"SFTP directory {cfg.path} reachable on {cfg.host}:{cfg.port}."
                    )
                with sftp.open(cfg.path, "rb") as handle:
                    handle.read(1)
                return True, (
                    f"SFTP file {cfg.path} reachable on {cfg.host}:{cfg.port}."
                )
            sftp.listdir(".")
            return True, f"SFTP server {cfg.host}:{cfg.port} reachable and authenticated."
        finally:
            sftp.close()
            transport.close()
    except Exception as exc:
        return False, f"SFTP test failed: {exc}"
