"""Shared MySQL connection helper."""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from connectors.sql_dsn import private_cloud_host_hint, resolve_sql_endpoint
from connectors.write_resilience import is_public_proxy_host


def _parse_mysql_url(url: str) -> dict[str, Any]:
    from connectors.sql_dsn import parse_sql_url

    return parse_sql_url(url, family="mysql")

logger = logging.getLogger(__name__)

#: Only bulk-write connections advertise the client LOCAL_FILES capability.
#: Reads, probes, reconcile and DDL never issue LOAD DATA LOCAL, so a hostile
#: server must not be able to ask them for a client file.
LOCAL_INFILE_PURPOSES = frozenset({"write", "bulk"})

_ALLOWED_ATTR = "_df_local_infile_allowed"
_guard_lock = threading.Lock()
_guard_installed: bool | None = None


class LocalInfileRefused(Exception):
    """The server asked for a client file this connection did not stage."""


def _requested_path(filename: Any) -> bytes:
    if isinstance(filename, (bytes, bytearray, memoryview)):
        return bytes(filename)
    return os.fsencode(str(filename))


def _check_local_infile_request(conn: Any, filename: Any) -> None:
    allowed = getattr(conn, _ALLOWED_ATTR, None) or ()
    if _requested_path(filename) in allowed:
        return
    logger.warning(
        "Refused MySQL LOCAL INFILE request: the server asked for a client "
        "file this connection did not stage for LOAD DATA"
    )
    import pymysql
    from pymysql.constants import ER

    raise pymysql.err.OperationalError(
        ER.FILE_NOT_FOUND,
        "refused server LOCAL INFILE request for a file this connection "
        "did not stage",
    )


def _install_local_infile_guard() -> bool:
    """Gate PyMySQL's LOCAL INFILE file read on a per-connection allowlist.

    The server names the file in the LOAD LOCAL response, so a rogue server
    can name any path. PyMySQL sends whatever it names; this guard sends
    only a path the writer registered with :func:`local_infile_allowed`.
    Returns False when the installed PyMySQL exposes no known hook; callers
    then keep ``local_infile`` off (fail closed, writers fall back to INSERT).
    """
    global _guard_installed
    if _guard_installed is not None:
        return _guard_installed
    with _guard_lock:
        if _guard_installed is not None:
            return _guard_installed
        import pymysql.connections as pc

        send_local_file = getattr(pc, "_send_local_file", None)
        legacy_cls = getattr(pc, "LoadLocalFile", None)
        if callable(send_local_file):
            # PyMySQL >= 1.1.1: the caller writes the terminating empty packet.
            def _guarded_send_local_file(filename: Any, conn: Any) -> Any:
                _check_local_infile_request(conn, filename)
                return send_local_file(filename, conn)

            pc._send_local_file = _guarded_send_local_file
            _guard_installed = True
        elif isinstance(legacy_cls, type) and hasattr(legacy_cls, "send_data"):
            # PyMySQL 1.1.0: send_data itself writes the terminating packet.
            class _GuardedLoadLocalFile(legacy_cls):  # type: ignore[misc, valid-type]
                def send_data(self) -> Any:
                    try:
                        _check_local_infile_request(self.connection, self.filename)
                    except Exception:
                        if not getattr(self.connection, "_closed", False):
                            self.connection.write_packet(b"")
                        raise
                    return super().send_data()

            pc.LoadLocalFile = _GuardedLoadLocalFile
            _guard_installed = True
        else:
            logger.warning(
                "PyMySQL LOCAL INFILE hook not found; LOAD DATA LOCAL disabled"
            )
            _guard_installed = False
        return _guard_installed


@contextmanager
def local_infile_allowed(conn: Any, path: str) -> Iterator[None]:
    """Let the server read exactly ``path`` over ``conn`` for this block."""
    if conn is None or not path:
        raise ValueError("LOCAL INFILE allowlist needs a connection and a path")
    allowed = getattr(conn, _ALLOWED_ATTR, None)
    if allowed is None:
        allowed = set()
        setattr(conn, _ALLOWED_ATTR, allowed)
    token = os.fsencode(path)
    allowed.add(token)
    try:
        yield
    finally:
        allowed.discard(token)


# --- TLS: what was asked, what the session really is -------------------------
# ``ssl=True``  -> REQUIRED: TLS or no connection, never plaintext.
# ``ssl=False`` -> PREFERRED (secure default): TLS whenever the server offers it.
#                  It means "TLS not required", not "TLS off" — PyMySQL
#                  negotiates TLS by itself, which is why QA saw an encrypted
#                  session with ssl=false (MySQL D10).
# ``ssl-mode=DISABLED`` in the connection string (the MySQL client's own URI
#                  option; ``ssl_mode=disabled`` / ``sslmode=disable`` too) ->
#                  DISABLED: the operator's explicit choice of plaintext, used
#                  only where the server allows it. A server that requires
#                  secure transport is re-dialled with TLS — an upgrade, never a
#                  silent downgrade — and a public proxy host always keeps TLS.
TLS_REQUIRED = "required"
TLS_PREFERRED = "preferred"
TLS_DISABLED = "disabled"
_ER_SECURE_TRANSPORT_REQUIRED = 3159


def requested_tls_mode(*, ssl: bool, connection_string: str, public_proxy: bool) -> str:
    """REQUIRED / PREFERRED / DISABLED for this dial (see the table above)."""
    if ssl or public_proxy:
        return TLS_REQUIRED
    from urllib.parse import parse_qsl, urlsplit

    query = urlsplit(connection_string or "").query if "?" in (connection_string or "") else ""
    for key, value in parse_qsl(query, keep_blank_values=True):
        name = key.strip().lower().replace("-", "_")
        val = value.strip().lower()
        if name in {"ssl_mode", "sslmode"} and val in {"disabled", "disable"}:
            return TLS_DISABLED
        if name in {"ssl_mode", "sslmode"} and val in {"required", "require", "verify_ca",
                                                       "verify_identity", "verify-full"}:
            return TLS_REQUIRED
    return TLS_PREFERRED


def strip_tls_mode_params(connection_string: str) -> str:
    """The DSN without its TLS-mode option, for drivers that do not know the name.

    ``ssl-mode`` is the MySQL client's URI option, not a PyMySQL keyword, so a
    SQLAlchemy URL carrying it would hand it to ``connect()``. The mode itself
    is read by :func:`requested_tls_mode`. A DSN that was only the option
    (``?ssl-mode=DISABLED``) becomes empty: host and port come from the form.
    """
    raw = connection_string or ""
    if "?" not in raw:
        return raw
    from urllib.parse import parse_qsl, urlencode

    head, _, query = raw.partition("?")
    kept = [
        (k, v) for k, v in parse_qsl(query, keep_blank_values=True)
        if k.strip().lower().replace("-", "_") not in {"ssl_mode", "sslmode"}
    ]
    if not head.strip():
        return ""
    return f"{head}?{urlencode(kept)}" if kept else head


def session_encryption(conn: Any) -> dict[str, Any]:
    """The session's actual encryption, read from the server (``Ssl_cipher``)."""
    requested = str(getattr(conn, "_dataflow_tls_requested", "") or "")
    out: dict[str, Any] = {"requested": requested, "encrypted": False, "cipher": "",
                           "version": "", "note": str(getattr(conn, "_dataflow_tls_note", "") or "")}
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW SESSION STATUS WHERE Variable_name IN ('Ssl_cipher', 'Ssl_version')")
            status = {str(r[0]).lower(): str(r[1] or "") for r in cur.fetchall()}
        out["cipher"] = status.get("ssl_cipher", "")
        out["version"] = status.get("ssl_version", "")
        out["encrypted"] = bool(out["cipher"])
    except Exception as exc:  # noqa: BLE001 — report what the socket says instead
        logger.debug("Ssl_cipher read failed: %s", exc)
        sock = getattr(conn, "_sock", None)
        cipher = getattr(sock, "cipher", None)
        if callable(cipher) and cipher():
            out.update(encrypted=True, cipher=str(cipher()[0]), version=str(cipher()[1]))
    return out


def describe_session_encryption(enc: dict[str, Any]) -> str:
    """One line for a connector test / brief: the truth, then what was asked."""
    asked = {TLS_REQUIRED: "TLS required", TLS_PREFERRED: "TLS preferred (ssl off = not required)",
             TLS_DISABLED: "TLS disabled by ssl-mode=DISABLED"}.get(enc.get("requested") or "", "")
    if enc.get("encrypted"):
        head = f"session encrypted ({enc.get('version') or 'TLS'} {enc.get('cipher')})".replace("  ", " ")
    else:
        head = "session NOT encrypted"
    note = f"; {enc['note']}" if enc.get("note") else ""
    return f"{head} — {asked}{note}" if asked else f"{head}{note}"


def get_connection(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    purpose: str = "write",
) -> Any:
    """Open a MySQL connection.

    ``purpose``:
      - ``write`` / ``bulk`` — long I/O timeouts for large transfers on public proxies
      - ``introspect`` — bounded connect and per-read waits for schema reads
      - ``ddl`` — short connect/I/O + tight lock waits so DROP/CREATE cannot hang a
        5-row demo for minutes behind a metadata lock (fail with a clear timeout)

    Only ``write`` / ``bulk`` enable the client LOCAL INFILE capability, and
    even then the server may read only a file staged with
    :func:`local_infile_allowed`.
    """
    import pymysql

    ep = resolve_sql_endpoint(
        family="mysql",
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        default_port=3306,
    )

    public_proxy = is_public_proxy_host(ep["host"])
    purpose_l = (purpose or "write").strip().lower()
    ddl_mode = purpose_l in {"ddl", "drop", "setup", "probe"}
    # Schema reads answer a person waiting on a tool call: a server that takes
    # the connection and never speaks must not hold the 120s write timeouts
    # (D169). Per-read, so a slow catalog query still has 40s.
    introspect_mode = purpose_l == "introspect"
    # Bulk transfers routinely exceed short socket deadlines on public proxies.
    # DDL/demo paths must fail fast — lock waits, not silent multi-minute hangs.
    if introspect_mode:
        connect_timeout = 10
        io_timeout = 40
        lock_wait_s = 30
    elif ddl_mode:
        connect_timeout = 10
        # Short lock wait always (fail-fast on metadata locks). Setup on public
        # proxies may need longer socket I/O for CREATE/ALTER; DROP/probe stay tight.
        lock_wait_s = 30
        if purpose_l == "setup" and public_proxy:
            io_timeout = 180
        else:
            io_timeout = 45
    elif purpose_l == "reconcile":
        # Gate-8 reads the population after the write. A metadata lock still
        # fails at 120s. The socket wait is longer so a 100k checksum scan is
        # not killed mid-row and left looking stuck at 99%.
        connect_timeout = 15
        io_timeout = 1800
        lock_wait_s = 120
    else:
        connect_timeout = 15
        io_timeout = 300 if public_proxy else 120
        lock_wait_s = 120
    kwargs: dict[str, Any] = {
        "host": ep["host"],
        "port": ep["port"],
        "user": ep["username"],
        "password": ep["password"],
        "database": ep["database"] or None,
        "connect_timeout": connect_timeout,
        "charset": "utf8mb4",
        "read_timeout": io_timeout,
        "write_timeout": io_timeout,
        # Client LOCAL_FILES flag, bulk-write purposes only. Server must also
        # have local_infile=ON; the writer probes and falls back to INSERT.
        "local_infile": purpose_l in LOCAL_INFILE_PURPOSES
        and _install_local_infile_guard(),
    }
    tls_mode = requested_tls_mode(
        ssl=bool(ssl), connection_string=connection_string, public_proxy=public_proxy
    )
    if tls_mode == TLS_REQUIRED:
        kwargs["ssl"] = {"ssl": {}}
    elif tls_mode == TLS_DISABLED:
        kwargs["ssl_disabled"] = True
    from src.ai.copilot.tool_deadline import remaining_seconds

    left = remaining_seconds()
    if left is not None:
        # Inside a tool call no socket wait outlives the call: the client stops
        # waiting at the deadline even when the server never answers (D169,
        # D181). Transfer jobs keep their long socket timeouts.
        bound = max(2, int(left) + 1)
        kwargs["connect_timeout"] = min(kwargs["connect_timeout"], bound)
        kwargs["read_timeout"] = min(kwargs["read_timeout"], bound)
        kwargs["write_timeout"] = min(kwargs["write_timeout"], bound)
    from services.egress_guard import assert_connector_dial_allowed

    assert_connector_dial_allowed(ep["host"], connection_string)
    tls_note = ""
    try:
        try:
            conn = pymysql.connect(**kwargs)
        except pymysql.err.OperationalError as exc:
            if not (tls_mode == TLS_DISABLED and exc.args
                    and exc.args[0] == _ER_SECURE_TRANSPORT_REQUIRED):
                raise
            # The server requires secure transport: dial again with TLS. The
            # explicit plaintext choice cannot be honoured, and that is said.
            kwargs.pop("ssl_disabled", None)
            kwargs["ssl"] = {"ssl": {}}
            tls_note = "server requires secure transport, so TLS was used"
            logger.warning("MySQL %s requires TLS; ssl-mode=DISABLED not honoured", ep["host"])
            conn = pymysql.connect(**kwargs)
    except Exception as exc:
        hint = private_cloud_host_hint(ep["host"], connection_string)
        if hint:
            raise RuntimeError(f"{exc}{hint}") from exc
        raise
    try:
        conn._dataflow_tls_requested = tls_mode
        conn._dataflow_tls_note = tls_note
    except Exception:  # noqa: BLE001 — a driver object that refuses attributes
        logger.debug("could not record the requested TLS mode", exc_info=True)

    # TCP keepalive so public proxies do not drop idle bulk-write sockets.
    try:
        import socket

        sock = getattr(conn, "_sock", None) or getattr(conn, "socket", None)
        if sock is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, "TCP_KEEPIDLE"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            if hasattr(socket, "TCP_KEEPINTVL"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            if hasattr(socket, "TCP_KEEPCNT"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)

    from connectors.write_resilience import apply_mysql_session_guards

    # Fail-closed STRICT only on write/DDL purposes. Read/introspect/probe soft-log
    # when managed MySQL forbids sql_mode changes (still prefer STRICT when allowed).
    require_strict = purpose_l in {"write", "bulk", "ddl", "setup", "drop"}
    apply_mysql_session_guards(
        conn,
        lock_wait_seconds=lock_wait_s,
        require_strict_sql_mode=require_strict,
    )
    return conn


def enable_autocommit(conn: Any) -> None:
    """Enable autocommit on a PyMySQL connection without shadowing the method.

    ``conn.autocommit`` is a *method*. Assignment ``conn.autocommit = True``
    replaces it with a bool; ``get_autocommit()`` stays False, so ``close()``
    rolls back DELETE. CDC tombstones then report rowcount 1 while dest
    ``COUNT(*)`` does not drop — leftover rows after a source delete.
    psycopg2's ``autocommit`` *is* a property; this helper is MySQL-only.
    """
    setter = getattr(conn, "autocommit", None)
    if callable(setter):
        setter(True)
        return
    conn.autocommit = True
