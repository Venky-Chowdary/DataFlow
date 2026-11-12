"""Which PyMySQL options a workspace's MySQL connector may set (follow-up review 2, H3 + H4).

SQLAlchemy's PyMySQL dialect passes every URL query key to ``pymysql.connect``
(``opts.update(url.query)``), and several PyMySQL options act on the API host
rather than on the tenant's server:

* ``read_default_file`` / ``read_default_group`` read a ``.cnf`` file on this
  host and take ``socket`` / ``user`` / ``password`` / ``host`` from it (the
  egress guard judged only the URL's host), and a file that is not a ``.cnf``
  raised a parser error echoing its first line;
* ``local_infile`` (and ``client_flag`` with ``CLIENT.LOCAL_FILES``) lets the
  server ask for a file on this host with LOAD DATA LOCAL;
* ``bind_address``, ``init_command``, ``auth_plugin_map``, ``conv``,
  ``cursorclass``, ``defer_connect``, ``server_public_key`` and the rest change
  how or from where the client connects, or are not strings at all.

A MySQL-family URL may carry only :data:`ALLOWED_QUERY_KEYS` (TLS keys, which
the TLS plan reads; charset / collation; timeouts; session settings; the host
and socket keys the egress guard judges). A connector's own fields, ``extra``
and ``connect_args`` may not set a key in :data:`DANGEROUS_KEYS`. The one MySQL
dial (``mysql_tls.dial_pymysql``) accepts only :data:`ALLOWED_DIAL_KEYS`, and
``local_infile`` / ``CLIENT.LOCAL_FILES`` only from the bulk writer
(``mysql_conn.get_connection``), whose server may then read only the file the
writer staged (``mysql_conn.local_infile_allowed``). Messages name the key and
never its value.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qsl

#: URL query keys a MySQL-family connector may use, spelled as ``_norm`` reads them.
ALLOWED_QUERY_KEYS = frozenset(
    {
        # TLS: read by the TLS plan (connectors.mysql_tls) and stripped from the URL.
        "ssl", "ssl_mode", "sslmode", "ssl_disabled", "ssl_ca", "sslrootcert", "ssl_capath",
        "ssl_cert", "sslcert", "ssl_key", "sslkey", "ssl_verify_cert", "ssl_verify_identity",
        "ssl_check_hostname", "ssl_cipher",
        # Session and wire settings.
        "charset", "collation", "connect_timeout", "read_timeout", "write_timeout", "autocommit",
        "sql_mode", "use_unicode", "binary_prefix", "max_allowed_packet", "program_name",
        # Addressing the egress guard judges (services.egress_guard._query_hosts).
        "host", "port", "unix_socket", "user", "username", "password", "passwd", "database", "db",
    }
)
#: Options that act on the API host or change how the client connects; never from a tenant.
DANGEROUS_KEYS = frozenset(
    {
        "read_default_file", "read_default_group", "local_infile", "init_command",
        "defer_connect", "auth_plugin_map", "server_public_key", "bind_address", "client_flag",
        "conv", "cursorclass", "named_pipe", "compress",
        # mysql-connector-python / mysqlclient spellings of the same options.
        "option_files", "option_groups", "allow_local_infile", "allow_local_infile_in_path",
        "read_default_files", "local_infile_in_path",
    }
)
#: Keywords ``dial_pymysql`` passes on; ``local_infile`` / LOCAL_FILES only for the bulk writer.
ALLOWED_DIAL_KEYS = ALLOWED_QUERY_KEYS | {"client_flag", "local_infile", "ssl_key_password"}
_CLIENT_LOCAL_FILES = 0x80
_MYSQL_SCHEMES = ("mysql", "mariadb")


class MySQLOptionRefused(ValueError):
    """A MySQL connection option Datawrap does not pass to the driver."""


def _norm(key: Any) -> str:
    return str(key).strip().lower().replace("-", "_")


def _shown(key: Any) -> str:
    return "".join(ch for ch in str(key) if ch.isprintable())[:60]


def _message(key: Any, where: str) -> str:
    return (
        f"MySQL connection option {_shown(key)} {where} is not accepted: Datawrap does not let a "
        "connector's settings read files, sockets or plugins on this server or change how its "
        "client connects. Remove it; TLS, charset, collation, timeouts and sql_mode are accepted."
    )


def is_mysql_url(text: str) -> bool:
    scheme = str(text or "").strip().lower().partition("://")
    return bool(scheme[1]) and scheme[0].split("+", 1)[0] in _MYSQL_SCHEMES


def url_option_refusal(text: str) -> str:
    """Why a MySQL URL's query hands PyMySQL an option it may not have, else ``""``.

    Reads the query as SQLAlchemy does: everything after the first ``?`` (a
    ``#`` included), split by ``parse_qsl``, each key percent-decoded once.
    """
    raw = str(text or "")
    if "?" not in raw:
        return ""
    for key, _value in parse_qsl(raw.split("?", 1)[1], keep_blank_values=True):
        if _norm(key) not in ALLOWED_QUERY_KEYS:
            return _message(key, "in the connection URL")
    return ""


def _set(value: Any) -> bool:
    """A setting is present unless it is null, blank, false, zero or empty."""
    if isinstance(value, str):
        return bool(value.strip())
    return bool(value)


def _mapping_refusal(source: Any, where: str) -> str:
    if not isinstance(source, Mapping):
        return ""
    for key, value in source.items():
        if _norm(key) in DANGEROUS_KEYS and _set(value):
            return _message(key, where)
    return ""


def connector_option_refusal(data: Any, *, mysql_family: bool) -> str:
    """At save: a MySQL URL in ``host`` / ``endpoint_url`` / ``connection_string``
    with an option outside the allowlist, or (for a MySQL-family type) a
    dangerous option in the connector's fields, ``extra`` or ``connect_args``."""
    if not isinstance(data, Mapping):
        return ""
    for field in ("connection_string", "host", "endpoint_url"):
        value = str(data.get(field) or "")
        if is_mysql_url(value) or (mysql_family and field == "connection_string"):
            refusal = url_option_refusal(value)
            if refusal:
                return refusal
    if not mysql_family:
        return ""
    extra = data.get("extra")
    extra = extra if isinstance(extra, Mapping) else {}
    return (
        _mapping_refusal(data, "in this connector's settings")
        or _mapping_refusal(extra, "in this connector's extra settings")
        or _mapping_refusal(data.get("connect_args"), "in this connector's connect_args")
        or _mapping_refusal(extra.get("connect_args"), "in this connector's connect_args")
    )


def dial_option_refusal(kwargs: Mapping[str, Any], *, local_infile_ok: bool) -> str:
    """Why ``pymysql.connect(**kwargs)`` would use an option a tenant may not set, else ``""``."""
    for key, value in kwargs.items():
        if key not in ALLOWED_DIAL_KEYS:
            return _message(key, "for this connection")
    if local_infile_ok:
        return ""
    if kwargs.get("local_infile"):
        return _message("local_infile", "for this connection")
    try:
        flags = int(kwargs.get("client_flag") or 0)
    except (TypeError, ValueError):
        return _message("client_flag", "for this connection")
    if flags & _CLIENT_LOCAL_FILES:
        return _message("client_flag", "for this connection")
    return ""
