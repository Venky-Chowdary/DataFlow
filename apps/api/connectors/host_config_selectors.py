"""Connection-string options that select the API host's own client configuration (follow-up review 4, L).

* libpq ``service=<name>`` reads that section of the host's
  ``~/.pg_service.conf`` (or ``PGSERVICEFILE``): host, port, user, password and
  TLS files the platform configured, not the tenant.
* ODBC ``Driver=<path>`` (and ``?driver=`` in a ``+pyodbc`` URL, which
  SQLAlchemy writes as ``DRIVER={...}``) makes the driver manager load that
  shared library into the API process; a driver name that is not in the
  installed list may be loaded as a library file too.
* ODBC ``DSN=`` selects a data source from the host's ``odbc.ini``; SQLAlchemy
  writes ``dsn=<host>`` for a ``+pyodbc`` URL with a host and no database.
  ``FILEDSN`` / ``SAVEFILE`` read or write a DSN file on the host.

Refused for a tenant connector: ``service`` on a PostgreSQL-family connector
(or wherever libpq itself reads the text), a ``Driver`` that is a path or a
library file (at save also one that is not an installed driver), ``DSN`` in an
ODBC string, a ``+pyodbc`` URL's query or its host-as-DSN form, and
``FILEDSN`` / ``SAVEFILE`` everywhere. Every reading of a string runs
(``tls_file_options._readings``), at save (:func:`settings_refusal`) and on
the arguments each guarded engine's driver receives (:func:`check_guarded`,
from ``tls_file_options.check_guarded_dial``) and the replication dial
(:func:`check_libpq_kwargs`). Messages name the option and the remedy, never a
value.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urlsplit

#: libpq engines (SQLAlchemy dialect names and connector driver types).
LIBPQ = frozenset({"postgresql", "postgres", "redshift", "cockroachdb"})
_DSN_FILES = frozenset({"filedsn", "savefile"})
_DRIVER_KEYS = frozenset({"driver", "odbcdriver"})
#: Shared-library / libtool / archive names the driver manager could load (follow-up review 5, L4).
_LIBRARY = re.compile(r"\.(so|la|a|sl|dll|dylib|bundle)(\.[0-9]+)*$", re.IGNORECASE)
#: SQLAlchemy dialects whose default DBAPI is pyodbc (``mssql://`` is ``mssql+pyodbc://``).
_ODBC_DEFAULT_DIALECTS = frozenset({"mssql", "sybase"})
_TEXT_FIELDS = ("connection_string", "host", "endpoint_url", "dsn", "odbc_connect")


class HostConfigRefused(ValueError):
    """A connection option selects the API host's own configuration or a library file."""


def _fold(key: Any) -> str:
    from connectors.tls_file_options import _fold as fold

    return fold(key)


def installed_odbc_drivers() -> frozenset[str] | None:
    """The driver manager's installed driver names, or ``None`` when unknown."""
    try:
        import pyodbc

        return frozenset(str(name) for name in (pyodbc.drivers() or ()))
    except Exception:  # noqa: BLE001 - no driver manager: only the path rule applies
        return None


def _service_message(where: str) -> str:
    return (
        f"PostgreSQL option service {where} selects a section of this server's connection service "
        "file (pg_service.conf), which holds the platform's own hosts and credentials. Datawrap does "
        "not accept it from a connector; enter the host, port, database and credentials in the "
        "connector's fields."
    )


def _dsn_message(where: str) -> str:
    return (
        f"ODBC option DSN {where} selects a data source from this server's ODBC configuration "
        "(odbc.ini). Datawrap does not accept it from a connector; enter the server and database in "
        "the connector's fields (a SQL Server URL needs its database: with a host and no database "
        "the driver reads the host as a DSN name)."
    )


def _file_message(key: Any, where: str) -> str:
    from connectors.tls_file_options import _shown

    return (
        f"ODBC option {_shown(key)} {where} reads or writes a DSN file on this server. Datawrap does "
        "not accept it from a connector; put the server, database and credentials in the connector's "
        "own fields."
    )


def _driver_refusal(value: Any, where: str, *, installed_only: bool) -> str:
    raw = str(value or "")
    braced = raw.strip()
    # What the driver manager looks up: inside braces, or the value as given, spaces included.
    name = braced[1:-1] if len(braced) >= 2 and braced[0] == "{" and braced[-1] == "}" else raw
    if not name.strip():
        return ""
    if "/" in name or "\\" in name or _LIBRARY.search(name.strip()):
        return (
            f"ODBC option Driver {where} names a library file on this server. Use the name of an "
            "installed ODBC driver."
        )
    installed = installed_odbc_drivers() if installed_only else None
    # unixODBC finds an odbcinst.ini section in any letter case, but not with other spacing
    # (follow-up review 6): compare case-insensitively, nothing more.
    if installed is not None and name.casefold() not in {driver.casefold() for driver in installed}:
        return (
            f"ODBC option Driver {where} is not an ODBC driver installed on this server. Use one of: "
            + ", ".join(sorted(installed)) + "."
        )
    return ""


def _odbc_pairs_refusal(pairs: Iterable[tuple[Any, Any]], where: str, *, installed_only: bool) -> str:
    for key, value in pairs:
        folded = _fold(key).strip("{}")
        if folded in _DSN_FILES:
            return _file_message(key, where)
        if folded == "dsn":
            return _dsn_message(where)
        if folded in _DRIVER_KEYS:
            refusal = _driver_refusal(value, where, installed_only=installed_only)
            if refusal:
                return refusal
    return ""


def _scheme(text: str) -> str:
    head = text.split("://", 1)[0] if "://" in text else ""
    return head.strip().lower()


def _odbc_scheme(scheme: str) -> bool:
    """A URL scheme SQLAlchemy dials through pyodbc (``mssql`` / ``sybase`` default to it)."""
    base, _, driver = scheme.partition("+")
    return "odbc" in driver if driver else base in _ODBC_DEFAULT_DIALECTS or "odbc" in base


def _odbc_url_refusal(text: str, where: str, *, installed_only: bool) -> str:
    """A ``+pyodbc`` URL as SQLAlchemy's pyodbc connector reads it."""
    from connectors.tls_file_options import _odbc_pairs, _query_pairs

    query = list(_query_pairs(text))
    for key, value in query:
        folded = _fold(key)
        if folded == "odbcconnect":
            refusal = _odbc_pairs_refusal(_odbc_pairs(str(value)), where, installed_only=installed_only)
        else:
            refusal = _odbc_pairs_refusal([(key, value)], where, installed_only=installed_only)
        if refusal:
            return refusal
    if any(_fold(key) == "odbcconnect" for key, _ in query):
        return ""
    try:
        parts = urlsplit(text)
        host, database = parts.hostname, parts.path.strip("/")
    except ValueError:
        return ""
    in_query = any(_fold(key) == "database" for key, _ in query)  # SQLAlchemy reads it as the database
    if host and not database and not in_query:  # SQLAlchemy: ``dsn=<host>`` (no ``database`` key)
        return _dsn_message(where)
    return ""


def _libpq_service_in(pairs: Iterable[tuple[Any, Any]]) -> bool:
    return any(_fold(key) == "service" for key, _ in pairs)


def text_refusal(text: Any, where: str, *, libpq: bool, odbc: bool, installed_only: bool) -> str:
    """A connection string that selects host configuration, read every way a driver reads it."""
    from connectors.tls_file_options import _libpq_pairs, _odbc_pairs, _readings

    raw = str(text or "")
    if "=" not in raw and "://" not in raw:
        return ""
    readings = [list(pairs) for pairs in _readings(raw)]
    for pairs in readings:
        for key, _value in pairs:
            if _fold(key) in _DSN_FILES:
                return _file_message(key, where)
    scheme = _scheme(raw)
    libpq = libpq or scheme.split("+", 1)[0] in LIBPQ
    if _libpq_service_in(_libpq_pairs(raw)) or (libpq and any(_libpq_service_in(pairs) for pairs in readings)):
        return _service_message(where)
    if libpq:  # generic_sql's ``?dsn=<conninfo>``: libpq reads that string too
        for pairs in readings:
            for key, value in pairs:
                if _fold(key) == "dsn" and isinstance(value, str) and "=" in value and value != raw:
                    refusal = text_refusal(value, where, libpq=True, odbc=False, installed_only=installed_only)
                    if refusal:
                        return refusal
    if _odbc_scheme(scheme):
        return _odbc_url_refusal(raw, where, installed_only=installed_only)
    if odbc or "://" not in raw:
        return _odbc_pairs_refusal(_odbc_pairs(raw), where, installed_only=installed_only)
    return ""


def mapping_refusal(source: Any, where: str, *, libpq: bool, odbc: bool, installed_only: bool,
                    depth: int = 0) -> str:
    if not isinstance(source, Mapping) or depth > 3:
        return ""
    for key, value in source.items():
        folded = _fold(key)
        refusal = ""
        if folded in _DSN_FILES:
            refusal = _file_message(key, where)
        elif folded == "service" and libpq:
            refusal = _service_message(where)
        elif folded in _DRIVER_KEYS and odbc:
            refusal = _driver_refusal(value, where, installed_only=installed_only)
        elif folded == "odbcconnect" and isinstance(value, str):
            refusal = _odbc_pairs_refusal(_odbc_pairs_of(value), where, installed_only=installed_only)
        elif isinstance(value, Mapping):
            refusal = mapping_refusal(value, where, libpq=libpq, odbc=odbc, installed_only=installed_only,
                                      depth=depth + 1)
        if refusal:
            return refusal
    return ""


def _odbc_pairs_of(text: str) -> list[tuple[str, str]]:
    from connectors.tls_file_options import _odbc_pairs

    return _odbc_pairs(text)


def _families(data: Mapping[str, Any]) -> tuple[bool, bool]:
    """(libpq, odbc) for a saved connector's type."""
    kind = str(data.get("type") or data.get("format") or "").strip().lower()
    if not kind:
        return False, False
    from src.transfer.connector_capabilities import resolve_driver_type

    driver = str(resolve_driver_type(kind) or "").lower()
    libpq = kind in LIBPQ or driver in LIBPQ
    try:
        from connectors.generic_sql import _drivername

        odbc = "pyodbc" in _drivername(kind) or "pyodbc" in _drivername(driver)
    except Exception:  # noqa: BLE001 - an unknown type: judged by its strings alone
        odbc = False
    return libpq, odbc


def settings_refusal(data: Any) -> str:
    """At save: an option in the connector's strings, fields, ``extra`` or ``connect_args``."""
    if not isinstance(data, Mapping):
        return ""
    libpq, odbc = _families(data)
    extra = data.get("extra") if isinstance(data.get("extra"), Mapping) else {}
    # A generic SQL connector is judged by the URL it dials (follow-up review 5, L4).
    for text in (data.get("connection_string"), extra.get("connection_string")):
        scheme = _scheme(str(text or ""))
        libpq = libpq or scheme.split("+", 1)[0] in LIBPQ
        odbc = odbc or _odbc_scheme(scheme)
    flags = {"libpq": libpq, "odbc": odbc, "installed_only": True}
    for source, where in ((data, "in this connector's connection settings"), (extra, "in this connector's extra settings")):
        for field in _TEXT_FIELDS:
            if field == "host" and source is data:
                continue  # a host name, judged by the egress guard
            refusal = text_refusal(source.get(field), where, **flags) if isinstance(source.get(field), str) else ""
            if refusal:
                return refusal
    return (
        mapping_refusal({k: v for k, v in data.items() if k not in _TEXT_FIELDS}, "in this connector's settings",
                        **flags)
        or mapping_refusal({k: v for k, v in extra.items() if k not in _TEXT_FIELDS},
                           "in this connector's extra settings", **flags)
    )


def check_guarded(dialect: Any, cargs: Any, cparams: Any) -> None:
    """Raise before a guarded engine's driver receives such an option (``do_connect``)."""
    where = "for this connection"
    name = str(getattr(dialect, "name", "") or "").lower()
    odbc = str(getattr(dialect, "driver", "") or "").lower() == "pyodbc"
    # The installed-driver rule applies at the dial too (follow-up review 5, L4).
    flags = {"libpq": name in LIBPQ, "odbc": odbc, "installed_only": True}
    refusal = mapping_refusal(
        {k: v for k, v in (cparams or {}).items() if _fold(k) != "dsn"}, where, **flags
    )
    texts = [arg for arg in (cargs or ()) if isinstance(arg, str)]
    dsn = (cparams or {}).get("dsn")
    if isinstance(dsn, str):
        texts.append(dsn)
    for text in texts:
        refusal = refusal or text_refusal(text, where, **flags)
    if refusal:
        raise HostConfigRefused(refusal)


def check_libpq_kwargs(kwargs: Mapping[str, Any]) -> None:
    """:func:`check_guarded` for a direct psycopg2 dial (logical replication)."""
    check_guarded(type("libpq", (), {"name": "postgresql", "driver": "psycopg2"})(), (), kwargs)
