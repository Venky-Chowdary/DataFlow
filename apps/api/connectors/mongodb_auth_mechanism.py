"""A tenant's MongoDB connector authenticates only with its own secrets (follow-up review 3, MongoDB OIDC).

PyMongo's ``authMechanism`` decides where the credential comes from:

* ``MONGODB-OIDC`` with ``authMechanismProperties=ENVIRONMENT:k8s`` reads the
  API pod's Kubernetes service-account token (or ``AZURE_FEDERATED_TOKEN_FILE``
  / ``AWS_WEB_IDENTITY_TOKEN_FILE``), ``ENVIRONMENT:azure`` / ``gcp`` mint a
  token from the host's instance metadata service, ``ENVIRONMENT:test`` reads
  ``OIDC_TOKEN_FILE``: every one is sent to the server the tenant runs;
* ``MONGODB-AWS`` signs with the host's ambient AWS credential chain;
* ``GSSAPI`` uses the host's Kerberos ticket cache / keytab.

A connector may name only :data:`ALLOWED` (the default negotiation, SCRAM,
PLAIN, X.509 whose certificate the TLS-file rule confines). Any
``authMechanismProperties`` with an ``ENVIRONMENT`` is refused too. Checked
in the URI (every ``?`` split, ``&`` and ``;`` delimiters, case-insensitive
names, percent-decoded values: as PyMongo reads it), client options, and the
connector's settings, ``extra`` and ``connect_args``. Messages never echo the
URI.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Mapping
from typing import Any
from urllib.parse import unquote_plus

ALLOWED = frozenset({"DEFAULT", "SCRAM-SHA-1", "SCRAM-SHA-256", "PLAIN", "MONGODB-X509"})
_MECHANISM = "authmechanism"
_PROPERTIES = "authmechanismproperties"


class MongoAuthRefused(ValueError):
    """A MongoDB connector names an authentication mechanism that uses the host's identity."""


def _fold(key: Any) -> str:
    return re.sub(r"[\s_\-.]", "", str(key or "").strip().lower())


def _message(where: str) -> str:
    return (
        f"MongoDB authMechanism {where} would authenticate with this server's own identity "
        "(MONGODB-OIDC with an ENVIRONMENT, MONGODB-AWS, GSSAPI, ...). Datawrap connects to a "
        "tenant's MongoDB only with the connector's own credentials: use SCRAM-SHA-256, "
        "SCRAM-SHA-1, PLAIN or MONGODB-X509 with an uploaded certificate."
    )


def _uri_pairs(uri: str) -> Iterator[tuple[str, str]]:
    start = uri.find("?")
    while start >= 0:
        query = uri[start + 1:].split("#", 1)[0]
        for part in re.split(r"[&;]", query):
            key, _sep, value = part.partition("=")
            yield unquote_plus(key), unquote_plus(value)
        start = uri.find("?", start + 1)


def _pairs_refused(pairs: Iterable[tuple[Any, Any]]) -> bool:
    for key, value in pairs:
        folded = _fold(key)
        mechanism = str(value or "").strip().upper()
        if folded == _MECHANISM and mechanism and mechanism not in ALLOWED:
            return True
        if folded == _PROPERTIES:
            text = str(value) if not isinstance(value, Mapping) else ",".join(f"{k}:" for k in value)
            if "environment" in text.lower():
                return True
    return False


def uri_refusal(uri: Any, where: str) -> str:
    text = str(uri or "")
    if "?" in text and _pairs_refused(_uri_pairs(text)):
        return _message(where)
    return ""


def options_refusal(options: Any, where: str) -> str:
    if isinstance(options, Mapping) and _pairs_refused(options.items()):
        return _message(where)
    return ""


def check_client(conn_str: Any, options: Mapping[str, Any]) -> None:
    """Raise :class:`MongoAuthRefused` before PyMongo builds a client for a connector."""
    refusal = uri_refusal(conn_str, "in this connection's URI") or options_refusal(
        options, "in this connection's options"
    )
    if refusal:
        raise MongoAuthRefused(refusal)


def settings_refusal(data: Any) -> str:
    """At save: the mechanism in a MongoDB URI, the connector's fields, ``extra`` or ``connect_args``."""
    if not isinstance(data, Mapping):
        return ""
    extra = data.get("extra") if isinstance(data.get("extra"), Mapping) else {}
    for field, source in (("connection_string", data), ("host", data), ("connection_string", extra)):
        value = str(source.get(field) or "")
        if value.strip().lower().startswith("mongodb"):
            refusal = uri_refusal(value, "in this connector's connection settings")
            if refusal:
                return refusal
    if "mongo" not in str(data.get("type") or data.get("format") or "").lower():
        return ""
    for options, where in ((data, "in this connector's settings"), (extra, "in this connector's extra settings"),
                           (data.get("connect_args"), "in this connector's connect_args"),
                           (extra.get("connect_args"), "in this connector's connect_args")):
        refusal = options_refusal(options, where)
        if refusal:
            return refusal
    return ""
