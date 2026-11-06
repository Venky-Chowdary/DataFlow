"""One TLS decision for every MySQL-family dial (QA MySQL D10).

PyMySQL readers/writers/probes (``mysql_conn.get_connection``), LOAD DATA
(same), the connector test (same), the SQLAlchemy engine
(``generic_sql`` -> ``do_connect``) and the binlog stream
(``mysql_change_stream``) all ask :func:`mysql_tls_plan` and dial through
:func:`dial_pymysql`, so the contract cannot drift between paths:

* ``ssl=True`` or a public proxy host            -> REQUIRED (TLS or nothing)
* ``ssl=False``                                  -> PREFERRED (TLS when offered)
* DSN ``ssl-mode=DISABLED`` / ``ssl_disabled=1``  -> DISABLED: plaintext only where
  the server allows it; a server requiring secure transport (3159) is
  re-dialled with TLS and that is recorded — an upgrade, never a downgrade
* DSN ``ssl-mode=VERIFY_CA`` / ``VERIFY_IDENTITY`` -> TLS with the server
  certificate verified against ``ssl-ca`` (and the host name); refused
  without a CA, because "verified" without a CA would be a lie.

REQUIRED and PREFERRED encrypt but do not verify the certificate; the session
description says "unverified" for them.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

logger = logging.getLogger(__name__)

TLS_REQUIRED = "required"
TLS_PREFERRED = "preferred"
TLS_DISABLED = "disabled"
TLS_VERIFY_CA = "verify_ca"
TLS_VERIFY_IDENTITY = "verify_identity"
ER_SECURE_TRANSPORT_REQUIRED = 3159

#: DSN query keys that carry TLS intent or material (folded: lower case, '-' -> '_').
_TLS_KEYS = frozenset({"ssl_mode", "sslmode", "ssl_disabled", "ssl_ca", "sslrootcert",
                       "ssl_capath", "ssl_cert", "sslcert", "ssl_key", "sslkey",
                       "ssl_verify_cert", "ssl_verify_identity"})
_MODE_WORDS = {
    "disabled": TLS_DISABLED, "disable": TLS_DISABLED,
    "preferred": TLS_PREFERRED, "prefer": TLS_PREFERRED,
    "required": TLS_REQUIRED, "require": TLS_REQUIRED,
    "verify_ca": TLS_VERIFY_CA, "verify-ca": TLS_VERIFY_CA,
    "verify_identity": TLS_VERIFY_IDENTITY, "verify-full": TLS_VERIFY_IDENTITY,
    "verify_full": TLS_VERIFY_IDENTITY,
}
_STRICTNESS = {TLS_DISABLED: 0, TLS_PREFERRED: 1, TLS_REQUIRED: 2, TLS_VERIFY_CA: 3,
               TLS_VERIFY_IDENTITY: 4}


class TlsModeError(ValueError):
    """A TLS request that cannot be honoured as stated (e.g. VERIFY_CA without a CA)."""


@dataclass(frozen=True)
class TlsPlan:
    mode: str
    ca: str = ""
    #: PyMySQL connect keywords that realise ``mode`` (never combined with others).
    kwargs: dict[str, Any] = field(default_factory=dict)

    @property
    def verified(self) -> bool:
        return self.mode in {TLS_VERIFY_CA, TLS_VERIFY_IDENTITY}

    @property
    def must_encrypt(self) -> bool:
        return self.mode in {TLS_REQUIRED, TLS_VERIFY_CA, TLS_VERIFY_IDENTITY}


def _dsn_query(connection_string: str) -> list[tuple[str, str]]:
    raw = connection_string or ""
    if "?" not in raw:
        return []
    return parse_qsl(urlsplit(raw).query if "://" in raw else raw.partition("?")[2],
                     keep_blank_values=True)


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def tls_request(connection_string: str = "", options: Any = None) -> dict[str, Any]:
    """What the DSN and the form's TLS options ask for (mode word, material, verify flags)."""
    req: dict[str, Any] = {"asked": "", "ca": "", "capath": "", "cert": "", "key": "",
                           "verify_cert": False, "verify_identity": False}
    opts = dict(options or {})
    for name, slot in (("ssl_ca", "ca"), ("ssl_capath", "capath"), ("ssl_cert", "cert"),
                       ("ssl_key", "key")):
        if str(opts.get(name) or "").strip():
            req[slot] = str(opts[name]).strip()
    req["verify_cert"] = _truthy(opts.get("ssl_verify_cert", ""))
    req["verify_identity"] = _truthy(opts.get("ssl_verify_identity", ""))
    for key, value in _dsn_query(connection_string):
        name = key.strip().lower().replace("-", "_")
        val = value.strip()
        if name in {"ssl_mode", "sslmode"}:
            req["asked"] = _MODE_WORDS.get(val.lower(), req["asked"])
        elif name == "ssl_disabled" and _truthy(val) and not req["asked"]:
            req["asked"] = TLS_DISABLED
        elif name in {"ssl_ca", "sslrootcert"} and val:
            req["ca"] = val
        elif name == "ssl_capath" and val:
            req["capath"] = val
        elif name in {"ssl_cert", "sslcert"} and val:
            req["cert"] = val
        elif name in {"ssl_key", "sslkey"} and val:
            req["key"] = val
        elif name == "ssl_verify_cert":
            req["verify_cert"] = _truthy(val)
        elif name == "ssl_verify_identity":
            req["verify_identity"] = _truthy(val)
    return req


def mysql_tls_plan(
    *, ssl: bool, connection_string: str = "", host: str = "", ssl_ca: str = "",
    options: Any = None,
) -> TlsPlan:
    """The TLS plan for one dial. ``host`` is the resolved endpoint host.

    Inputs: the ``ssl`` flag, the DSN's TLS keys, and the form's TLS options
    (``ssl_ca`` / ``ssl_capath`` / ``ssl_cert`` / ``ssl_key`` /
    ``ssl_verify_*``). A CA or a verify flag *raises* the mode to VERIFY_CA
    (VERIFY_IDENTITY with ``ssl_verify_identity``); a client certificate
    (mTLS) raises it to at least REQUIRED. Nothing lowers a stricter request.
    """
    from connectors.write_resilience import is_public_proxy_host

    req = tls_request(connection_string, {**dict(options or {}),
                                          **({"ssl_ca": ssl_ca} if ssl_ca else {})})
    floor = TLS_PREFERRED
    if ssl or is_public_proxy_host(host or ""):
        floor = TLS_REQUIRED
    if req["cert"]:
        floor = max(floor, TLS_REQUIRED, key=_STRICTNESS.__getitem__)
    if req["ca"] or req["capath"] or req["verify_cert"]:
        floor = max(floor, TLS_VERIFY_CA, key=_STRICTNESS.__getitem__)
    if req["verify_identity"]:
        floor = TLS_VERIFY_IDENTITY
    asked = req["asked"]
    if asked == TLS_DISABLED:
        if _STRICTNESS[floor] <= _STRICTNESS[TLS_PREFERRED]:
            mode = TLS_DISABLED
        elif ssl or is_public_proxy_host(host or ""):
            mode = floor  # ssl=True / public proxy: TLS stays on
        else:
            # A CA / client certificate next to ssl-mode=DISABLED is a contradiction.
            raise TlsModeError(
                "ssl-mode=DISABLED contradicts the TLS material (CA / client certificate) "
                "or verification this connection also names; remove one of them."
            )
    elif asked:
        mode = max(asked, floor, key=_STRICTNESS.__getitem__)
    else:
        mode = floor
    if mode in {TLS_VERIFY_CA, TLS_VERIFY_IDENTITY} and not (req["ca"] or req["capath"]):
        raise TlsModeError(
            f"ssl-mode={mode.upper()} verifies the server certificate and needs "
            "ssl-ca=<path to the CA bundle> (or ssl_capath)."
        )
    if mode == TLS_DISABLED:
        kwargs: dict[str, Any] = {"ssl_disabled": True}
    elif mode == TLS_PREFERRED:
        kwargs = {}
    else:
        sslp: dict[str, Any] = {
            "verify_mode": mode in {TLS_VERIFY_CA, TLS_VERIFY_IDENTITY},
            "check_hostname": mode == TLS_VERIFY_IDENTITY,
        }
        for slot in ("ca", "capath", "cert", "key"):
            if req[slot]:
                sslp[slot] = req[slot]
        kwargs = {"ssl": sslp}
    return TlsPlan(mode=mode, ca=req["ca"] or req["capath"], kwargs=kwargs)


def strip_tls_params(connection_string: str) -> str:
    """The DSN without TLS keys, for drivers that would misread them.

    The plan owns TLS (and carries the CA / client certificate itself); a
    SQLAlchemy URL that kept ``ssl_disabled=true`` turned an ``ssl=True`` dial
    into plaintext. A DSN that was only TLS options becomes empty: host and
    port come from the form.
    """
    raw = connection_string or ""
    if "?" not in raw:
        return raw
    head, _, query = raw.partition("?")
    kept = [(k, v) for k, v in parse_qsl(query, keep_blank_values=True)
            if k.strip().lower().replace("-", "_") not in _TLS_KEYS]
    if not head.strip():
        return ""
    return f"{head}?{urlencode(kept)}" if kept else head


def _without_tls(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in kwargs.items()
            if k not in {"ssl", "ssl_disabled", "ssl_ca", "ssl_cert", "ssl_key",
                         "ssl_verify_cert", "ssl_verify_identity"}}


def note_tls_upgrade(host: str) -> str:
    """Record a DISABLED -> TLS re-dial: log, and the running job's record."""
    note = "server requires secure transport, so TLS was used despite ssl-mode=DISABLED"
    logger.warning("MySQL %s requires TLS; ssl-mode=DISABLED not honoured", host or "server")
    try:
        from services.job_tls_note import record_job_tls_note

        record_job_tls_note(note)
    except ImportError:  # pragma: no cover - job ledger absent in a stripped build
        logger.debug("no job ledger to record the TLS upgrade on")
    return note


def connect_kwargs_for(base: dict[str, Any], plan: TlsPlan) -> dict[str, Any]:
    """PyMySQL keywords for ``plan`` on top of ``base`` (base TLS keys dropped)."""
    return {**_without_tls(base), **plan.kwargs}


def dial_pymysql(kwargs: dict[str, Any], *, plan: TlsPlan, host: str = "") -> Any:
    """The one MySQL dial: apply ``plan``, connect, record what happened."""
    import pymysql

    args = connect_kwargs_for(kwargs, plan)
    note = ""
    try:
        conn = pymysql.connect(**args)
    except pymysql.err.OperationalError as exc:
        if not (plan.mode == TLS_DISABLED and exc.args
                and exc.args[0] == ER_SECURE_TRANSPORT_REQUIRED):
            raise
        args = connect_kwargs_for(kwargs, TlsPlan(mode=TLS_REQUIRED, kwargs={"ssl": {"verify_mode": False, "check_hostname": False}}))
        note = note_tls_upgrade(host)
        conn = pymysql.connect(**args)
    if plan.must_encrypt and not _socket_encrypted(conn):
        # A driver that fell back to plaintext (PyMySQL 1.1.x continued without
        # CLIENT.SSL) must not hand a REQUIRED / VERIFY_* caller a clear session.
        conn.close()
        raise TlsModeError(
            f"TLS is required ({plan.mode}) but the session to {host or 'the server'} "
            "is not encrypted; refusing to send data in plaintext."
        )
    conn._dataflow_tls_requested = plan.mode
    conn._dataflow_tls_verified = plan.verified
    conn._dataflow_tls_note = note
    return conn


def _socket_encrypted(conn: Any) -> bool:
    import ssl as _ssl

    sock = getattr(conn, "_sock", None)
    if isinstance(sock, _ssl.SSLSocket):
        return True
    return bool(session_encryption(conn).get("encrypted"))


def resolved_kwargs_for_external_dial(base: dict[str, Any], plan: TlsPlan, host: str = "") -> dict[str, Any]:
    """Keywords for a library that dials by itself (the binlog reader).

    It cannot re-dial on 3159, so DISABLED is tried once here: a server that
    requires secure transport gets the TLS keywords (and the upgrade is noted).
    """
    if plan.mode != TLS_DISABLED:
        return connect_kwargs_for(base, plan)
    import pymysql

    try:
        probe = pymysql.connect(**connect_kwargs_for(base, plan))
    except pymysql.err.OperationalError as exc:
        if not (exc.args and exc.args[0] == ER_SECURE_TRANSPORT_REQUIRED):
            raise
        note_tls_upgrade(host)
        return connect_kwargs_for(base, TlsPlan(mode=TLS_REQUIRED, kwargs={"ssl": {"verify_mode": False, "check_hostname": False}}))
    probe.close()
    return connect_kwargs_for(base, plan)


def server_reports_cipher(conn: Any) -> bool:
    """True when the server says this session is encrypted (``Ssl_cipher`` set).

    Works with tuple and dict cursors: the binlog reader's control connection
    uses ``DictCursor``.
    """
    with conn.cursor() as cur:
        cur.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        row = cur.fetchone()
    if not row:
        return False
    value = row.get("Value") if isinstance(row, dict) else row[1]
    return bool(str(value or "").strip())


def checked_external_connect(plan: TlsPlan, host: str = ""):
    """A ``pymysql.connect`` for a library that dials by itself (the binlog reader).

    It reads ``Ssl_cipher`` from the server on every connection it opens and
    refuses a plaintext session when ``plan`` must encrypt. This is the check
    ``dial_pymysql`` makes, applied to the reader's control and stream
    connections.
    """
    import pymysql

    def _connect(**kwargs: Any):
        conn = pymysql.connect(**kwargs)
        if plan.must_encrypt and not server_reports_cipher(conn):
            conn.close()
            raise TlsModeError(
                f"TLS is required ({plan.mode}) but the binlog session to "
                f"{host or 'the server'} is not encrypted; refusing to stream row data in "
                "plaintext."
            )
        return conn

    return _connect


def session_encryption(conn: Any) -> dict[str, Any]:
    """The session's actual encryption, read from the server (``Ssl_cipher``)."""
    out: dict[str, Any] = {
        "requested": str(getattr(conn, "_dataflow_tls_requested", "") or ""),
        "verified": bool(getattr(conn, "_dataflow_tls_verified", False)),
        "encrypted": False, "cipher": "", "version": "",
        "note": str(getattr(conn, "_dataflow_tls_note", "") or ""),
    }
    import pymysql

    try:
        with conn.cursor() as cur:
            cur.execute("SHOW SESSION STATUS WHERE Variable_name IN ('Ssl_cipher', 'Ssl_version')")
            status = {str(r[0]).lower(): str(r[1] or "") for r in cur.fetchall()}
    except pymysql.err.MySQLError as exc:
        logger.warning("Ssl_cipher read failed: %s", exc)
        out["note"] = (out["note"] + "; " if out["note"] else "") + "server status unreadable"
        return out
    out["cipher"] = status.get("ssl_cipher", "")
    out["version"] = status.get("ssl_version", "")
    out["encrypted"] = bool(out["cipher"])
    return out


_ASKED = {
    TLS_REQUIRED: "TLS required", TLS_PREFERRED: "TLS preferred (ssl off = not required)",
    TLS_DISABLED: "TLS disabled by ssl-mode=DISABLED",
    TLS_VERIFY_CA: "ssl-mode=VERIFY_CA", TLS_VERIFY_IDENTITY: "ssl-mode=VERIFY_IDENTITY",
}


def describe_session_encryption(enc: dict[str, Any]) -> str:
    """One line for a connector test / brief: the truth, then what was asked."""
    asked = _ASKED.get(enc.get("requested") or "", "")
    if enc.get("encrypted"):
        trust = "certificate verified" if enc.get("verified") else "certificate unverified"
        head = (f"session encrypted ({enc.get('version') or 'TLS'} {enc.get('cipher')}, "
                f"{trust})")
    else:
        head = "session NOT encrypted"
    note = f"; {enc['note']}" if enc.get("note") else ""
    return f"{head} — {asked}{note}" if asked else f"{head}{note}"
