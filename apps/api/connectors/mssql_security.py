"""SQL Server transport security and Microsoft Entra ID sign-in (NF-2).

Two DBAPIs can dial SQL Server:

- **pyodbc + Microsoft ODBC Driver 18 for SQL Server** — the secure path.
  Driver 18 encrypts by default (``Encrypt=yes``/``Mandatory``), verifies the
  server certificate chain and host name (``TrustServerCertificate=no``), and
  supports ``Encrypt=strict`` (TDS 8.0) and Microsoft Entra ID sign-in
  (``Authentication=ActiveDirectoryServicePrincipal`` /
  ``ActiveDirectoryMsi`` / ``ActiveDirectoryPassword``, or an access token
  passed as the pre-connect attribute ``SQL_COPT_SS_ACCESS_TOKEN`` = 1256).
- **pymssql (FreeTDS)** — negotiates TLS but never verifies the chain, cannot
  be told to require encryption, and has no Entra ID sign-in. Kept only as an
  explicit, labelled opt-out.

Connector options (top level or under ``extra``):

``sqlserver_driver``
    ``auto`` (default): ODBC Driver 18/17 when installed, else FreeTDS —
    unless the host sets ``DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS=1`` (the image
    built with the Microsoft driver does), in which case a missing driver is
    an error, not a silent downgrade. ``odbc``: require the Microsoft driver.
    ``freetds``: use pymssql on purpose (TLS not verified).
``authentication``
    ``sql`` (default: SQL login), ``entra_service_principal`` (username =
    application/client id, password = client secret),
    ``entra_managed_identity`` (username = client id of a user-assigned
    identity, or empty for the system-assigned one; no password),
    ``entra_password`` (Entra user name and password), ``entra_access_token``
    (password = an OAuth access token for ``https://database.windows.net/``;
    tokens expire in about an hour, so use it for one-off runs only).
    The ODBC ``Authentication`` keyword values are accepted as aliases.
``encrypt``
    ``yes`` is sent when unset, so Driver 17 (whose default is ``no``) also
    encrypts and verifies; ``strict`` needs Driver 18; ``no`` / ``optional``
    must be declared by the operator.
"""

from __future__ import annotations

import struct
from typing import Any, Mapping

#: SQL_COPT_SS_ACCESS_TOKEN (msodbcsql.h): pre-connect attribute for an Entra token.
SQL_COPT_SS_ACCESS_TOKEN = 1256

DRIVER_CHOICES = ("auto", "odbc", "freetds")

#: Our option value -> the ODBC ``Authentication`` keyword (None: token / SQL login).
_AUTH_KEYWORD: dict[str, str | None] = {
    "sql": None,
    "entra_service_principal": "ActiveDirectoryServicePrincipal",
    "entra_managed_identity": "ActiveDirectoryMsi",
    "entra_password": "ActiveDirectoryPassword",
    "entra_access_token": None,
}
#: ODBC keyword spellings accepted for the same modes.
_AUTH_ALIASES = {
    "sqlpassword": "sql",
    "activedirectoryserviceprincipal": "entra_service_principal",
    "activedirectorymsi": "entra_managed_identity",
    "activedirectorymanagedidentity": "entra_managed_identity",
    "activedirectorypassword": "entra_password",
}
_TOKEN_MODE = "entra_access_token"
_TRUTHY = {"1", "true", "yes", "on"}


class SqlServerSecurityError(ValueError):
    """A SQL Server connector option this host cannot honour."""


def _opt(cfg: Mapping[str, Any], key: str) -> str:
    value = cfg.get(key)
    if value in (None, "") and isinstance(cfg.get("extra"), Mapping):
        value = cfg["extra"].get(key)
    return str(value or "").strip()


def require_verified_tls() -> bool:
    """Host policy: no silent FreeTDS fallback (set in the image that ships Driver 18)."""
    from services.brand_env import getenv_brand_str

    # DATAWRAP_ / DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS
    return getenv_brand_str("MSSQL_REQUIRE_VERIFIED_TLS", "").strip().lower() in _TRUTHY


def driver_choice(cfg: Mapping[str, Any]) -> str:
    choice = (_opt(cfg, "sqlserver_driver") or "auto").lower()
    if choice not in DRIVER_CHOICES:
        raise SqlServerSecurityError(
            f"sqlserver_driver must be one of {', '.join(DRIVER_CHOICES)} (got {choice!r})"
        )
    return choice


def auth_mode(cfg: Mapping[str, Any]) -> str:
    """The normalised ``authentication`` option (``sql`` when unset)."""
    raw = (_opt(cfg, "authentication") or "sql").lower()
    mode = _AUTH_ALIASES.get(raw, raw)
    if mode not in _AUTH_KEYWORD:
        raise SqlServerSecurityError(
            "authentication must be one of sql, entra_service_principal, "
            f"entra_managed_identity, entra_password, entra_access_token (got {raw!r})"
        )
    return mode


def is_entra(cfg: Mapping[str, Any]) -> bool:
    return auth_mode(cfg).startswith("entra_")


def select_driver(cfg: Mapping[str, Any], *, odbc_installed: bool, pymssql_installed: bool) -> str:
    """``mssql+pyodbc`` or ``mssql+pymssql`` for this connector, or refuse.

    Never downgrades silently from what the connector or host requires.
    """
    choice = driver_choice(cfg)
    entra = is_entra(cfg)
    if choice == "freetds":
        if entra:
            raise SqlServerSecurityError(
                "Microsoft Entra ID sign-in needs the Microsoft ODBC driver; the connector "
                "chose FreeTDS (sqlserver_driver=freetds), which has no Entra ID sign-in."
            )
        if not pymssql_installed:
            raise SqlServerSecurityError("sqlserver_driver=freetds, but pymssql is not installed")
        return "mssql+pymssql"
    if odbc_installed:
        return "mssql+pyodbc"
    why = ""
    if choice == "odbc":
        why = "the connector requires it (sqlserver_driver=odbc)"
    elif entra:
        why = "Microsoft Entra ID sign-in needs it"
    elif require_verified_tls():
        why = (
            "this host requires verified TLS for SQL Server (DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS); "
            "set sqlserver_driver=freetds on the connector to accept an unverified connection"
        )
    if why:
        raise SqlServerSecurityError(
            f'"ODBC Driver 18 for SQL Server" is not installed on this host and {why}. '
            "Build the API image with --build-arg INSTALL_MSODBCSQL=1 (Microsoft licence "
            "terms apply) or install the driver."
        )
    if pymssql_installed:
        return "mssql+pymssql"
    return "mssql+pyodbc"


def odbc_security_query(cfg: Mapping[str, Any]) -> dict[str, str]:
    """The ODBC keywords for encryption and Entra ID sign-in."""
    query: dict[str, str] = {}
    encrypt = _opt(cfg, "encrypt")
    # Driver 18 already defaults to Encrypt=yes; Driver 17 defaults to no.
    query["Encrypt"] = encrypt or "yes"
    keyword = _AUTH_KEYWORD[auth_mode(cfg)]
    if keyword:
        query["Authentication"] = keyword
    return query


def url_credentials(cfg: Mapping[str, Any]) -> tuple[str | None, str | None]:
    """``(username, password)`` for the SQLAlchemy URL under the chosen sign-in."""
    mode = auth_mode(cfg)
    username = str(cfg.get("username") or "") or None
    password = str(cfg.get("password") or "") or None
    if mode == _TOKEN_MODE:
        # The token rides as a pre-connect attribute; UID/PWD must be absent.
        return None, None
    if mode == "entra_managed_identity":
        return username, None
    if mode in ("entra_service_principal", "entra_password") and not (username and password):
        raise SqlServerSecurityError(
            f"authentication={mode} needs the username "
            + ("(the application / client id) and the client secret as password"
               if mode == "entra_service_principal" else "and password of the Entra user")
        )
    return username, password


def access_token_struct(token: str) -> bytes:
    """The byte layout ``SQL_COPT_SS_ACCESS_TOKEN`` expects: length, then UTF-16-LE."""
    raw = str(token or "").strip().encode("utf-16-le")
    if not raw:
        raise SqlServerSecurityError("authentication=entra_access_token needs the token in the password field")
    return struct.pack(f"<I{len(raw)}s", len(raw), raw)


def odbc_attrs_before(cfg: Mapping[str, Any]) -> dict[int, Any]:
    """pyodbc ``attrs_before`` for this connector (the Entra access token, when used)."""
    if auth_mode(cfg) != _TOKEN_MODE:
        return {}
    return {SQL_COPT_SS_ACCESS_TOKEN: access_token_struct(str(cfg.get("password") or ""))}


def transport_summary(cfg: Mapping[str, Any], drivername: str) -> dict[str, Any]:
    """What this connector's connection guarantees, for the UI and evidence."""
    if drivername == "mssql+pymssql":
        return {
            "driver": "freetds",
            "encrypted": "negotiated",
            "certificate_verified": False,
            "label": "FreeTDS — TLS certificate not verified",
            "authentication": "sql",
        }
    trust = str(_opt(cfg, "trust_server_certificate")).lower() in _TRUTHY
    encrypt = (_opt(cfg, "encrypt") or "yes").lower()
    plaintext = encrypt in ("no", "optional", "false")
    return {
        "driver": "odbc",
        "encrypted": "no" if plaintext else encrypt,
        "certificate_verified": not (trust or plaintext),
        "label": (
            "ODBC Driver — TLS not encrypted" if plaintext
            else "ODBC Driver — certificate trusted without verification" if trust
            else "ODBC Driver — verified TLS"
        ),
        "authentication": auth_mode(cfg),
    }


_FREETDS_CANNOT = ("server_certificate", "ssl_ca", "hostname_in_certificate")


def _pymssql_installed() -> bool:
    try:
        import pymssql  # noqa: F401
    except Exception:  # noqa: BLE001 - absent or broken wheel
        return False
    return True


def _odbc_installed() -> bool:
    from connectors.generic_sql import _mssql_odbc_driver

    return bool(_mssql_odbc_driver())


def driver_for(cfg: Mapping[str, Any]) -> str:
    """The SQL Server DBAPI for this connector on this host (see :func:`select_driver`)."""
    from connectors.generic_sql import with_connection_options

    return select_driver(
        with_connection_options(cfg),
        odbc_installed=_odbc_installed(),
        pymssql_installed=_pymssql_installed(),
    )


def needs_odbc(cfg: Mapping[str, Any]) -> bool:
    """True when a SQL Server connection for ``cfg`` must not be made over FreeTDS.

    Whenever :func:`select_driver` would pick the Microsoft driver (it is
    installed and the connector did not choose ``freetds``), when the connector
    signs in with Entra ID or declares TLS FreeTDS cannot honour, or when the
    host requires verified TLS. Read-back paths that dial pymssql directly
    (Gate-8, the verification ladder, target samples) and the fast path's
    fallback use this, so a connection the writer made over verified TLS is
    never read back, or retried after a certificate failure, over FreeTDS.
    """
    choice = driver_choice(cfg)
    if choice == "odbc" or is_entra(cfg):
        return True
    if choice == "freetds":
        return False
    if require_verified_tls() or _odbc_installed():
        return True
    if any(_opt(cfg, key) for key in _FREETDS_CANNOT):
        return True
    return _opt(cfg, "encrypt").lower() in ("yes", "true", "1", "strict", "mandatory")


def with_access_token(connect_args: dict[str, Any], cfg: Mapping[str, Any]) -> dict[str, Any]:
    """pyodbc connect args with the Entra ID access token added to ``attrs_before``.

    The login timeout pyodbc already takes there (attribute 103) is kept.
    """
    token = odbc_attrs_before(cfg)
    if not token:
        return connect_args
    return {**connect_args, "attrs_before": {**dict(connect_args.get("attrs_before") or {}), **token}}


def _odbc_pairs(text: str) -> list[tuple[str, str]]:
    """``KEY=value;KEY={va;lue}`` -> ``[(key lower-cased, value)]`` (braces honoured)."""
    pairs: list[tuple[str, str]] = []
    key, buf, depth, in_value = "", "", 0, False
    for ch in str(text or "") + ";":
        if in_value and ch == "{":
            depth += 1
        elif in_value and ch == "}" and depth:
            depth -= 1
        if ch == ";" and not depth:
            if in_value or buf.strip():
                pairs.append((key.strip().lower(), buf.strip().strip("{}").strip()))
            key, buf, in_value = "", "", False
        elif ch == "=" and not in_value:
            key, buf, in_value = buf, "", True
        else:
            buf += ch
    return pairs


def _url_odbc_keywords(raw: str) -> list[tuple[str, str]]:
    """The ODBC keywords a SQLAlchemy mssql URL passes: its query and ``odbc_connect``."""
    from urllib.parse import parse_qsl

    query = str(raw or "").split("?", 1)[1] if "?" in str(raw or "") else ""
    pairs: list[tuple[str, str]] = []
    for key, value in parse_qsl(query, keep_blank_values=True):  # URL-decoded
        name = key.strip().lower()
        if name == "odbc_connect":
            pairs.extend(_odbc_pairs(value))
        else:
            pairs.append((name, value.strip()))
    return pairs


def _unverified_odbc_keyword(raw: str) -> str:
    """The first keyword that turns off certificate verification, or ``""``."""
    for key, value in _url_odbc_keywords(raw):
        low = value.lower()
        if key == "driver" and ("freetds" in low or "tdsodbc" in low):
            return f"driver={value}"
        if key == "trustservercertificate" and low in _TRUTHY:
            return f"TrustServerCertificate={value}"
        if key == "encrypt" and low in ("no", "false", "0", "optional", "off"):
            return f"Encrypt={value}"
    return ""


def check_mssql_url(raw: str, cfg: Mapping[str, Any]) -> None:
    """A SQL Server connection string obeys the same driver policy as host/port.

    ``mssql+pymssql://`` (FreeTDS, no certificate verification) is refused
    when the host requires verified TLS or the connector needs the Microsoft
    driver, unless the connector chose ``sqlserver_driver=freetds``. So is an
    ``mssql+pyodbc`` string that turns verification off itself — a FreeTDS
    ``driver``, ``TrustServerCertificate=yes`` or ``Encrypt=no|optional`` —
    in its query or inside ``odbc_connect`` (review round 3).
    """
    scheme = str(raw or "").split("://", 1)[0].strip().lower()
    dialect, _, dbapi = scheme.partition("+")
    if dialect != "mssql" or driver_choice(cfg) == "freetds":
        return
    if not (require_verified_tls() or driver_choice(cfg) == "odbc" or is_entra(cfg)):
        return
    fix = (
        "Use mssql+pyodbc with ODBC Driver 18, or set sqlserver_driver=freetds to accept "
        "an unverified connection."
    )
    if dbapi not in ("", "pyodbc"):
        # Plain mssql:// is SQLAlchemy's pyodbc default. Every other SQL
        # Server DBAPI (pymssql, pytds, ...) is outside the Microsoft driver's
        # certificate verification (review N9).
        raise SqlServerSecurityError(
            f"This SQL Server connection string uses {scheme}, not mssql+pyodbc (the only SQL "
            "Server path whose certificate verification Datawrap configures), and this host or "
            f"connector requires verified TLS. {fix}"
        )
    keyword = _unverified_odbc_keyword(raw)
    if keyword:
        raise SqlServerSecurityError(
            f"This SQL Server connection string sets {keyword}, which turns off certificate "
            f"verification, and this host or connector requires verified TLS. {fix}"
        )


def connect_sqlserver_dbapi(
    cfg: Mapping[str, Any], *, login_timeout: int = 10, timeout: int | None = 30
) -> Any:
    """A DBAPI connection for a SQL Server read-back, honouring the connector's TLS.

    The Microsoft ODBC driver (through ``generic_sql.get_connection``, whose
    cursors take ``%s`` placeholders) when :func:`needs_odbc`; otherwise
    pymssql exactly as before.
    """
    if needs_odbc(cfg):
        from connectors.generic_sql import connection_options, get_connection, with_connection_options

        merged = with_connection_options(cfg)
        return get_connection(
            host=str(merged.get("host") or ""),
            port=int(merged.get("port") or 1433),
            database=str(merged.get("database") or ""),
            username=str(merged.get("username") or merged.get("user") or ""),
            password=str(merged.get("password") or ""),
            connection_string=str(merged.get("connection_string") or ""),
            db_type="sqlserver",
            **{"connect_timeout": login_timeout, **connection_options(merged)},
        )
    import pymssql

    from connectors.mssql_login import PYMSSQL_TDS_VERSION

    extra: dict[str, Any] = {"timeout": timeout} if timeout is not None else {}
    return pymssql.connect(
        server=str(cfg.get("host") or "127.0.0.1"),
        port=int(cfg.get("port") or 1433),
        user=str(cfg.get("username") or cfg.get("user") or "sa"),
        password=str(cfg.get("password") or ""),
        database=str(cfg.get("database") or "master"),
        login_timeout=login_timeout,
        tds_version=PYMSSQL_TDS_VERSION,  # D281: no Sybase re-dial
        **extra,
    )
