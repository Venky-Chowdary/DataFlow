"""An Oracle connect string never makes python-oracledb read the API host's files (follow-up review 3, M3 + M4).

python-oracledb reads more than an address from a DSN:

* ``config-file:///path/x.json`` (and every registered ``config-*`` provider:
  ``config-azure://``, ``config-ociobject://``, ``config-awss3://``, ...) makes
  the driver read the connection's user, password, host and ``pyo`` settings
  from a file on the API host or from a cloud service with the host's
  identity; ``ldap://`` / ``ldaps://`` look the descriptor up in a directory.
  A tenant's DSN may only name ``tcp://`` / ``tcps://`` (or no protocol: Easy
  Connect, a descriptor).
* Extended Easy Connect takes driver parameters with a ``pyo.`` prefix
  (``?pyo.wallet_location=/etc/oracle/wallet``): the wallet / config directory
  is judged as the driver itself parses the string
  (``oracledb.ConnectParams().parse_connect_string``, local, no network, run only
  once the protocol check passed so no provider hook runs).

Checked at save (:func:`settings_refusal`, from
``egress_guard.connector_save_refusal``) and on the DSN a driver is about to
receive (:func:`check_dsn`: the copy fast path and every guarded Oracle engine).
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from typing import Any

#: The protocols python-oracledb dials itself; any other ``<scheme>://`` runs a hook.
_DIAL_PROTOCOLS = frozenset({"tcp", "tcps"})
_SQLALCHEMY_URL = re.compile(r"^\s*oracle(\+\w+)?://", re.I)
#: ConnectParams attributes that name a directory the driver reads.
_DIRECTORIES = ("wallet_location", "config_dir")


class OracleDsnRefused(ValueError):
    """An Oracle DSN names a configuration provider, LDAP, or a host wallet / config directory."""


def _provider_message(where: str) -> str:
    return (
        f"The Oracle connect string {where} names a configuration provider or a protocol other "
        "than tcp / tcps (config-file://, config-azure://, ldap://, ...): Datawrap does not let "
        "the driver read a connection's settings from a file or service of the server's choosing. "
        "Use host:port/service, tcp:// or tcps://, or a connect descriptor."
    )


def dsn_refusal(dsn: Any, where: str) -> str:
    """Refusal text for an Oracle ``dsn``, or ``""``."""
    if not isinstance(dsn, str) or not dsn.strip():
        return ""
    text = dsn.strip()
    protocol = text.split("://", 1)[0].strip().lower() if "://" in text else ""
    if text.lower().startswith("config-") or (protocol and protocol not in _DIAL_PROTOCOLS):
        return _provider_message(where)
    try:
        import oracledb

        params = oracledb.ConnectParams()
        params.parse_connect_string(text)
    except Exception:  # noqa: BLE001 - the driver refuses this string too; the text readers still ran
        return ""
    from connectors.tls_file_options import _path_refused, message

    for option in _DIRECTORIES:
        if _path_refused(getattr(params, option, None)):
            return message(option, where)
    return ""


def check_dsn(dsn: Any) -> None:
    """Raise :class:`OracleDsnRefused` before python-oracledb receives ``dsn``."""
    refusal = dsn_refusal(dsn, "for this connection")
    if refusal:
        raise OracleDsnRefused(refusal)


def _is_oracle(data: Mapping[str, Any]) -> bool:
    kind = str(data.get("type") or data.get("format") or "").lower()
    return "oracle" in kind or bool(_SQLALCHEMY_URL.match(str(data.get("connection_string") or "")))


def _url_dsns(url: Any) -> Iterator[Any]:
    text = str(url or "")
    if not _SQLALCHEMY_URL.match(text):
        yield text  # a bare Easy Connect string or descriptor
        return
    try:
        from sqlalchemy.engine import make_url

        value = make_url(text).query.get("dsn")
    except Exception:  # noqa: BLE001 - the engine cannot be built from it either
        return
    yield from (value if isinstance(value, tuple) else (value,))


def settings_refusal(data: Any) -> str:
    """At save: an Oracle connector's DSN in its URL, fields, ``extra`` or ``connect_args``."""
    if not isinstance(data, Mapping) or not _is_oracle(data):
        return ""
    extra = data.get("extra") if isinstance(data.get("extra"), Mapping) else {}
    candidates: list[tuple[Any, str]] = [
        (dsn, "in this connector's connection settings") for dsn in _url_dsns(data.get("connection_string"))
    ]
    candidates += [(dsn, "in this connector's extra settings") for dsn in _url_dsns(extra.get("connection_string"))]
    candidates += [(data.get("dsn"), "in this connector's settings"), (extra.get("dsn"), "in this connector's extra settings")]
    for args in (data.get("connect_args"), extra.get("connect_args")):
        if isinstance(args, Mapping):
            candidates.append((args.get("dsn"), "in this connector's connect_args"))
    for dsn, where in candidates:
        refusal = dsn_refusal(dsn, where)
        if refusal:
            return refusal
    return ""
