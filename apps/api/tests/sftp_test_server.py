"""A real, in-process SFTP server for transfer tests.

Every SFTP test in this repository patched ``connect_sftp`` and asserted on the
mock. That proves the call was made, not that a row survived the round trip —
and SFTP is the one named connector with no live route at all, so mocks were
the only thing standing behind it.

``paramiko`` ships both halves of the protocol, so a server rooted at a
temporary directory runs here with no daemon, no credentials and no network.
The host key is generated per run and handed to the client to pin, which means
the transfers also exercise the real host-key verification path in
:mod:`connectors.sftp_common` rather than the ``insecure_ignore`` escape.

Paths are confined to the served root: an SFTP client that walks out with
``../`` gets a permission error, the same answer a hardened server gives.
"""

from __future__ import annotations

import os
import socket
import threading
from dataclasses import dataclass
from typing import Any

import paramiko

_USERNAME = "dataflow"
_PASSWORD = "dataflow"  # nosec B105 — throwaway credential for a local fixture


class _Server(paramiko.ServerInterface):
    """Password auth for one fixed local account."""

    def check_auth_password(self, username: str, password: str) -> int:
        if username == _USERNAME and password == _PASSWORD:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username: str) -> str:
        return "password"

    def check_channel_request(self, kind: str, chanid: int) -> int:
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


class _Handle(paramiko.SFTPHandle):
    def stat(self) -> Any:
        try:
            return paramiko.SFTPAttributes.from_stat(os.fstat(self.readfile.fileno()))
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    def chattr(self, attr: Any) -> int:
        return paramiko.SFTP_OK


def _make_sftp_interface(root: str, *, allow_posix_rename: bool = True) -> type:
    class _Interface(paramiko.SFTPServerInterface):
        ROOT = root
        # Named apart from the method below: a class body treats both as the
        # same local name, so reusing it raises NameError before the def runs.
        supports_posix_rename = allow_posix_rename

        def _real(self, path: str) -> str | None:
            """Map a client path into the served root, or ``None`` if it escapes."""
            joined = os.path.join(self.ROOT, self.canonicalize(path).lstrip("/"))
            resolved = os.path.realpath(joined)
            root_real = os.path.realpath(self.ROOT)
            if resolved != root_real and not resolved.startswith(root_real + os.sep):
                return None
            return resolved

        def list_folder(self, path: str) -> Any:
            real = self._real(path)
            if real is None:
                return paramiko.SFTP_PERMISSION_DENIED
            try:
                out = []
                for name in os.listdir(real):
                    attr = paramiko.SFTPAttributes.from_stat(
                        os.stat(os.path.join(real, name))
                    )
                    attr.filename = name
                    out.append(attr)
                return out
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)

        def stat(self, path: str) -> Any:
            real = self._real(path)
            if real is None:
                return paramiko.SFTP_PERMISSION_DENIED
            try:
                return paramiko.SFTPAttributes.from_stat(os.stat(real))
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)

        def lstat(self, path: str) -> Any:
            real = self._real(path)
            if real is None:
                return paramiko.SFTP_PERMISSION_DENIED
            try:
                return paramiko.SFTPAttributes.from_stat(os.lstat(real))
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)

        def open(self, path: str, flags: int, attr: Any) -> Any:
            real = self._real(path)
            if real is None:
                return paramiko.SFTP_PERMISSION_DENIED
            try:
                fd = os.open(real, flags | getattr(os, "O_BINARY", 0), 0o666)
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)
            if flags & os.O_WRONLY:
                mode = "ab" if flags & os.O_APPEND else "wb"
            elif flags & os.O_RDWR:
                mode = "a+b" if flags & os.O_APPEND else "r+b"
            else:
                mode = "rb"
            try:
                handle_file = os.fdopen(fd, mode)
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)
            handle = _Handle(flags)
            handle.filename = real
            handle.readfile = handle_file
            handle.writefile = handle_file
            return handle

        def remove(self, path: str) -> int:
            real = self._real(path)
            if real is None:
                return paramiko.SFTP_PERMISSION_DENIED
            try:
                os.remove(real)
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)
            return paramiko.SFTP_OK

        def rename(self, oldpath: str, newpath: str) -> int:
            src, dst = self._real(oldpath), self._real(newpath)
            if src is None or dst is None:
                return paramiko.SFTP_PERMISSION_DENIED
            try:
                os.rename(src, dst)
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)
            return paramiko.SFTP_OK

        def posix_rename(self, oldpath: str, newpath: str) -> int:
            """The OpenSSH extension: replace the target in one step.

            Servers that lack it answer ``SFTP_OP_UNSUPPORTED``, which is what
            ``supports_posix_rename = False`` reproduces here.
            """
            if not self.supports_posix_rename:
                return paramiko.SFTP_OP_UNSUPPORTED
            src, dst = self._real(oldpath), self._real(newpath)
            if src is None or dst is None:
                return paramiko.SFTP_PERMISSION_DENIED
            try:
                os.replace(src, dst)
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)
            return paramiko.SFTP_OK

        def mkdir(self, path: str, attr: Any) -> int:
            real = self._real(path)
            if real is None:
                return paramiko.SFTP_PERMISSION_DENIED
            try:
                os.mkdir(real)
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)
            return paramiko.SFTP_OK

        def rmdir(self, path: str) -> int:
            real = self._real(path)
            if real is None:
                return paramiko.SFTP_PERMISSION_DENIED
            try:
                os.rmdir(real)
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)
            return paramiko.SFTP_OK

        def chattr(self, path: str, attr: Any) -> int:
            return paramiko.SFTP_OK

    return _Interface


@dataclass(frozen=True)
class SFTPTestServer:
    """Connection details for a running local SFTP server."""

    host: str
    port: int
    username: str
    password: str
    host_key: str
    root: str

    def endpoint_config(self, remote_path: str) -> dict[str, Any]:
        """Connector config for a path on this server, with the key pinned.

        The server presents its root as ``/`` the way a chrooted SFTP account
        does, so ``remote_path`` is server-absolute (``/in.csv``) and never the
        host filesystem path.
        """
        directory = os.path.dirname(remote_path) or "/"
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "host_key": self.host_key,
            "database": directory,
            "table": os.path.basename(remote_path),
        }

    def local_path(self, remote_path: str) -> str:
        """Host path backing a server-absolute remote path (for assertions)."""
        return os.path.join(self.root, remote_path.lstrip("/"))


class _Runner:
    def __init__(self, root: str, *, posix_rename: bool = True) -> None:
        self._root = root
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self._sock.settimeout(0.5)
        self._key = paramiko.RSAKey.generate(2048)
        self._iface = _make_sftp_interface(root, allow_posix_rename=posix_rename)
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._accept = threading.Thread(target=self._serve, daemon=True)

    @property
    def port(self) -> int:
        return int(self._sock.getsockname()[1])

    @property
    def host_key(self) -> str:
        return f"{self._key.get_name()} {self._key.get_base64()}"

    def start(self) -> None:
        self._accept.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                client, _addr = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            thread = threading.Thread(
                target=self._session, args=(client,), daemon=True
            )
            thread.start()
            self._threads.append(thread)

    def _session(self, client: socket.socket) -> None:
        transport = paramiko.Transport(client)
        try:
            transport.add_server_key(self._key)
            transport.set_subsystem_handler(
                "sftp", paramiko.SFTPServer, self._iface
            )
            transport.start_server(server=_Server())
            channel = transport.accept(20)
            if channel is None:
                return
            # The subsystem handler owns the channel; hold the transport open
            # until the client disconnects.
            while transport.is_active() and not self._stop.is_set():
                self._stop.wait(0.2)
        except Exception:  # noqa: BLE001 — one bad session must not kill the server
            return
        finally:
            try:
                transport.close()
            except Exception:  # noqa: BLE001
                pass

    def stop(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass


def start_sftp_server(
    root: str, *, posix_rename: bool = True
) -> tuple[SFTPTestServer, Any]:
    """Start a local SFTP server serving ``root``; returns details and a stopper.

    ``posix_rename=False`` models the managed file-transfer appliances that do
    not implement the OpenSSH extension, so the writer's portable fallback is
    exercised rather than assumed.
    """
    runner = _Runner(root, posix_rename=posix_rename)
    runner.start()
    details = SFTPTestServer(
        host="127.0.0.1",
        port=runner.port,
        username=_USERNAME,
        password=_PASSWORD,
        host_key=runner.host_key,
        root=root,
    )
    return details, runner
