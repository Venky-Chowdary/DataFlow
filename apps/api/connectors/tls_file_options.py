"""A connector's TLS file options never name the API host's own files (follow-up review 2, TLS files).

Every database driver takes TLS material as a file path, read by the API
process: PyMySQL ``ssl_ca`` / ``ssl_capath`` / ``ssl_cert`` / ``ssl_key`` (and
``ssl={'ca', 'cert', 'key', ...}``), libpq ``sslrootcert`` / ``sslcert`` /
``sslkey`` / ``sslcrl`` / ``sslcrldir`` (and ``passfile`` / ``servicefile``),
ODBC ``ServerCertificate`` / ``ClientCertificate`` / ``ClientKey`` (and
``FileDSN`` / ``SaveFile``), python-oracledb ``wallet_location`` /
``config_dir`` (a descriptor's ``MY_WALLET_DIRECTORY``), PyMongo
``tlsCAFile`` / ``tlsCertificateKeyFile`` / ``tlsCRLFile``, clickhouse-driver
``ca_certs`` / ``certfile`` / ``keyfile``. A tenant controls its connector's
settings and runs the server, so a path on the API host made the platform
present its own client certificate and key (or wallet) to that server, or
trust the host's CA files for it.

Such a path may only point inside the areas a connector may open
(``services.tenant_host_files``). Inline PEM text is not a path, and libpq's
``sslrootcert=system`` (the system CA store) is a keyword. Checked at save
(:func:`connector_refusal`, from ``egress_guard.connector_save_refusal``) and
on the arguments each driver is about to receive (:func:`check_dial`,
:func:`guard_engine`). Messages name the option and never the path.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable, Iterator, Mapping
from typing import Any
from urllib.parse import parse_qsl

#: Option names (folded by :func:`_fold`) whose value is a file or directory the driver reads.
FILE_OPTIONS = frozenset({
    # PyMySQL / mysql-connector (and the libpq spellings the MySQL plan accepts)
    "sslca", "sslcapath", "sslcert", "sslkey", "sslrootcert", "sslcrl", "sslcrlpath",
    # libpq
    "sslcrldir", "sslkeylogfile", "passfile", "servicefile",
    # ODBC Driver 17 / 18 for SQL Server and the driver manager
    "servercertificate", "clientcertificate", "clientkey", "filedsn", "savefile",
    # python-oracledb
    "walletlocation", "configdir", "mywalletdirectory", "walletpath", "tnsadmin",
    # PyMongo (and its pre-4 spellings)
    "tlscafile", "tlscertificatekeyfile", "tlscrlfile", "sslcacerts", "sslcertfile", "sslkeyfile",
    "sslcrlfile", "sslpemkeyfile",
    # clickhouse-driver
    "cacerts", "certfile", "keyfile",
})
#: PyMySQL's ``ssl`` dict (and the same shape under ``tls``): keys that are files.
_SSL_DICT_KEYS = ("ca", "capath", "cert", "key", "cafile", "certfile", "keyfile", "crl")
_SSL_DICTS = frozenset({"ssl", "tls", "ssloptions", "tlsoptions"})
#: Values that are keywords, not paths.
_KEYWORDS = frozenset({"system"})
_DESCRIPTOR = re.compile(r"\(\s*(MY_WALLET_DIRECTORY|WALLET_LOCATION|DIRECTORY)\s*=\s*([^)]*)\)", re.I)
_CONNINFO = re.compile(r"([A-Za-z_][\w.-]*)\s*=\s*('(?:[^'\\]|\\.)*'|\S+)")


class TlsFileRefused(ValueError):
    """A connector option names a TLS / credential file outside the connector areas."""


def _fold(key: Any) -> str:
    return re.sub(r"[\s_\-.]", "", str(key or "").strip().lower())


def _shown(key: Any) -> str:
    return "".join(ch for ch in str(key) if ch.isprintable())[:60]


def message(option: Any, where: str) -> str:
    return (
        f"TLS file option {_shown(option)} {where} names a file outside the folders connectors may "
        "open on this server: Datawrap does not present or trust the platform's own certificate, "
        "key, wallet or password files for a connector. Upload the file in this workspace and use "
        "that upload, paste the PEM text where the option accepts it, or ask the operator to list "
        "its folder in DATAFLOW_SOURCE_PATH_ROOTS."
    )


def _path_refused(value: Any) -> bool:
    if isinstance(value, os.PathLike):
        value = os.fspath(value)
    if not isinstance(value, str):
        return False
    text = value.strip()
    if text.lower().startswith("file:"):
        text = text[5:]  # ODBC Driver 18 ``ClientCertificate=file:/path``
    if not text or text.lower() in _KEYWORDS or "-----BEGIN" in text:
        return False
    from services.tenant_host_files import host_file_allowed

    # ODBC Driver 18 opens ``file:<path>`` up to the first ``,`` (``,password:...``
    # follows): judge that too, or ``..`` in the suffix climbs back (follow-up review 3, L2).
    head = text.split(",", 1)[0].strip()
    return not host_file_allowed(text) or bool(head and not host_file_allowed(head))


def _option_refusal(key: Any, value: Any, where: str) -> str:
    folded = _fold(key)
    if folded.startswith("pyo") and folded[3:] in FILE_OPTIONS:
        folded = folded[3:]  # python-oracledb Easy Connect ``pyo.wallet_location`` (follow-up review 3, M3)
    if folded in FILE_OPTIONS and _path_refused(value):
        return message(key, where)
    if folded in _SSL_DICTS and isinstance(value, Mapping):
        for sub in _SSL_DICT_KEYS:
            if _path_refused(value.get(sub)):
                return message(f"ssl_{sub}", where)  # the option that set it
    if isinstance(value, str) and folded in {"odbcconnect", "dsn"}:
        return text_refusal(value, where)
    return ""


def _pairs_refusal(pairs: Iterable[tuple[Any, Any]], where: str) -> str:
    for key, value in pairs:
        refusal = _option_refusal(key, value, where)
        if refusal:
            return refusal
    return ""


def _query_pairs(text: str) -> Iterator[tuple[str, str]]:
    """The query as each consumer reads it: SQLAlchemy's ``make_url`` and the
    text after each ``?`` (PyMongo and libpq split at different ones, and a
    ``?`` may sit in a password)."""
    start = text.find("?")
    while start >= 0:
        yield from parse_qsl(text[start + 1:], keep_blank_values=True)
        start = text.find("?", start + 1)
    if "://" not in text:
        return
    try:
        from sqlalchemy.engine import make_url

        query = make_url(text).query
    except Exception:  # noqa: BLE001 - not a SQLAlchemy URL; the other readings stand
        return
    for key, value in query.items():
        for item in value if isinstance(value, tuple) else (value,):
            yield key, item


def _odbc_pairs(text: str) -> list[tuple[str, str]]:
    from services.residency_addressing import _odbc_pairs as braced

    pairs = braced(text)
    if pairs is not None:
        return pairs
    return [tuple(part.split("=", 1)) for part in text.split(";") if "=" in part]  # type: ignore[misc]


def _libpq_pairs(text: str) -> list[tuple[str, str]]:
    """The conninfo or URI as libpq itself reads it (multi-host, ``?`` in a password)."""
    try:
        from psycopg2.extensions import parse_dsn

        return list(parse_dsn(text).items())
    except Exception:  # noqa: BLE001 - libpq would refuse this text too; the other readings stand
        return []


def _readings(raw: str) -> Iterator[Iterable[tuple[Any, Any]]]:
    """Every consumer's reading of ``raw``: never one chosen by a substring
    (follow-up review 3, H1: a ``;`` or ``://`` inside a value hid the rest)."""
    yield _libpq_pairs(raw)
    yield [(k, v.strip("'")) for k, v in _CONNINFO.findall(raw)]
    yield _query_pairs(raw)
    yield _odbc_pairs(raw)


def text_refusal(text: Any, where: str) -> str:
    """A URL, ODBC string, libpq conninfo or Oracle descriptor naming a file outside the areas.

    Refused when ANY reading of the text (libpq, conninfo pairs, URL query,
    ODBC pairs, descriptor) finds a file option outside the areas."""
    raw = str(text or "")
    if not raw.strip():
        return ""
    if raw.strip().lower().startswith("config-"):  # python-oracledb's config providers (review 3, M4)
        from connectors.oracle_dsn_guard import dsn_refusal

        return dsn_refusal(raw, where)
    for match in _DESCRIPTOR.finditer(raw):
        if _path_refused(match.group(2)):
            return message(match.group(1), where)
    for pairs in _readings(raw):
        refusal = _pairs_refusal(pairs, where)
        if refusal:
            return refusal
    return ""


def mapping_refusal(source: Any, where: str) -> str:
    if not isinstance(source, Mapping):
        return ""
    return _pairs_refusal(source.items(), where)


def connector_refusal(data: Any) -> str:
    """At save: a TLS file option in the connector's URLs, fields, ``extra`` or ``connect_args``."""
    if not isinstance(data, Mapping):
        return ""
    for field in ("connection_string", "host", "endpoint_url", "dsn"):
        refusal = text_refusal(data.get(field), "in this connector's connection settings")
        if refusal:
            return refusal
    extra = data.get("extra") if isinstance(data.get("extra"), Mapping) else {}
    for field in ("connection_string", "dsn"):
        refusal = text_refusal(extra.get(field), "in this connector's extra settings")
        if refusal:
            return refusal
    return (
        mapping_refusal(data, "in this connector's settings")
        or mapping_refusal(extra, "in this connector's extra settings")
        or mapping_refusal(data.get("connect_args"), "in this connector's connect_args")
        or mapping_refusal(extra.get("connect_args"), "in this connector's connect_args")
    )


def check_dial(kwargs: Mapping[str, Any] | None = None, *texts: Any) -> None:
    """Raise :class:`TlsFileRefused` before a driver receives a file outside the areas."""
    where = "for this connection"
    refusal = mapping_refusal(kwargs or {}, where)
    for text in texts:
        refusal = refusal or (text_refusal(text, where) if isinstance(text, str) else "")
    if refusal:
        raise TlsFileRefused(refusal)


_GUARDED = "_dataflow_tls_file_guard"


def guard_engine(engine: Any) -> None:
    """Check the arguments each new DBAPI connection of ``engine`` receives.

    Registered before any engine-level dialing listener (``generic_sql``'s
    MySQL dial), so it sees SQLAlchemy's ``cparams`` (URL query keys included)
    and the ODBC string in ``cargs`` before a driver does. A class-level
    ``do_connect`` listener runs before it: the one that opens Oracle
    connections (``statement_budget``) calls :func:`check_guarded_dial` first.
    """
    from sqlalchemy import event

    setattr(engine.dialect, _GUARDED, True)

    @event.listens_for(engine, "do_connect")
    def _no_host_tls_files(dialect, _rec, cargs, cparams):  # noqa: ANN001
        check_guarded_dial(dialect, cargs, cparams)


def check_guarded_dial(dialect: Any, cargs: Any, cparams: Any) -> None:
    """:func:`check_dial` for a connection of an engine :func:`guard_engine` marked."""
    if getattr(dialect, _GUARDED, False):
        check_dial(cparams, *cargs)
        if getattr(dialect, "name", "") == "oracle":
            from connectors.oracle_dsn_guard import check_dsn

            check_dsn((cparams or {}).get("dsn"))  # no config-file:// / ldap:// / pyo. wallet (review 3)
