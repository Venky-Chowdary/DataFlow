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

#: DSN query keys that carry TLS intent (folded: lower case, '-' -> '_').
_TLS_KEYS = frozenset({"ssl_mode", "sslmode", "ssl_disabled", "ssl_ca", "sslrootcert",
                       "ssl_verify_cert", "ssl_verify_identity"})
_MODE_WORDS = {
    "disabled": TLS_DISABLED, "disable": TLS_DISABLED,
    "preferred": TLS_PREFERRED, "prefer": TLS_PREFERRED,
    "required": TLS_REQUIRED, "require": TLS_REQUIRED,
    "verify_ca": TLS_VERIFY_CA, "verify-ca": TLS_VERIFY_CA,
    "verify_identity": TLS_VERIFY_IDENTITY, "verify-full": TLS_VERIFY_IDENTITY,
    "verify_full": TLS_VERIFY_IDENTITY,
}


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


def _dsn_query(connection_string: str) -> list[tuple[str, str]]:
    raw = connection_string or ""
    if "?" not in raw:
        return []
    return parse_qsl(urlsplit(raw).query if "://" in raw else raw.partition("?")[2],
                     keep_blank_values=True)


def _truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def mysql_tls_plan(
    *, ssl: bool, connection_string: str = "", host: str = "", ssl_ca: str = ""
) -> TlsPlan:
    """The TLS plan for one dial. ``host`` is the resolved endpoint host."""
    from connectors.write_resilience import is_public_proxy_host

    asked = ""
    ca = (ssl_ca or "").strip()
    for key, value in _dsn_query(connection_string):
        name = key.strip().lower().replace("-", "_")
        if name in {"ssl_mode", "sslmode"}:
            asked = _MODE_WORDS.get(value.strip().lower(), asked)
        elif name == "ssl_disabled" and _truthy(value) and not asked:
            asked = TLS_DISABLED
        elif name in {"ssl_ca", "sslrootcert"} and value.strip():
            ca = value.strip()
    if asked in {TLS_VERIFY_CA, TLS_VERIFY_IDENTITY}:
        if not ca:
            raise TlsModeError(
                f"ssl-mode={asked.upper()} verifies the server certificate and needs "
                "ssl-ca=<path to the CA bundle> in the connection string."
            )
        mode = asked
    elif ssl or is_public_proxy_host(host or "") or asked == TLS_REQUIRED:
        # A public proxy host always keeps TLS, whatever the DSN asks.
        mode = TLS_REQUIRED
    elif asked == TLS_DISABLED:
        mode = TLS_DISABLED
    else:
        mode = TLS_PREFERRED
    if mode == TLS_DISABLED:
        kwargs: dict[str, Any] = {"ssl_disabled": True}
    elif mode == TLS_REQUIRED:
        kwargs = {"ssl": {"ssl": {}}}
    elif mode in {TLS_VERIFY_CA, TLS_VERIFY_IDENTITY}:
        kwargs = {"ssl_ca": ca, "ssl_verify_cert": True,
                  "ssl_verify_identity": mode == TLS_VERIFY_IDENTITY}
    else:
        kwargs = {}
    return TlsPlan(mode=mode, ca=ca, kwargs=kwargs)


def strip_tls_params(connection_string: str) -> str:
    """The DSN without TLS keys, for drivers that would misread them.

    The plan owns TLS; a SQLAlchemy URL that kept ``ssl_disabled=true`` turned
    an ``ssl=True`` dial into plaintext. A DSN that was only TLS options
    (``?ssl-mode=DISABLED``) becomes empty: host and port come from the form.
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
        args = connect_kwargs_for(kwargs, TlsPlan(mode=TLS_REQUIRED, kwargs={"ssl": {"ssl": {}}}))
        note = note_tls_upgrade(host)
        conn = pymysql.connect(**args)
    conn._dataflow_tls_requested = plan.mode
    conn._dataflow_tls_verified = plan.verified
    conn._dataflow_tls_note = note
    return conn


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
        return connect_kwargs_for(base, TlsPlan(mode=TLS_REQUIRED, kwargs={"ssl": {"ssl": {}}}))
    probe.close()
    return connect_kwargs_for(base, plan)


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
