"""NF-2: SQL Server transport security and Microsoft Entra ID sign-in.

FreeTDS (pymssql) never verifies the server certificate and has no Entra ID
sign-in; the secure path is pyodbc + Microsoft ODBC Driver 18. These tests pin
the connection options the product hands each driver (unit level: no SQL
Server and no Entra tenant here; the cdc-sqlserver CI legs prove both drivers
against a live server, and Entra ID needs a tenant).
"""

from __future__ import annotations

import struct
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

API = Path(__file__).resolve().parents[1]
if str(API) not in sys.path:
    sys.path.insert(0, str(API))

import connectors.generic_sql as gs  # noqa: E402
from connectors.mssql_security import (  # noqa: E402
    SQL_COPT_SS_ACCESS_TOKEN,
    SqlServerSecurityError,
    access_token_struct,
    needs_odbc,
    select_driver,
    transport_summary,
)

ODBC18 = "ODBC Driver 18 for SQL Server"


@pytest.fixture()
def odbc(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", raising=False)
    monkeypatch.setattr(gs, "_mssql_odbc_driver", lambda: ODBC18)


@pytest.fixture()
def no_odbc(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", raising=False)
    monkeypatch.setattr(gs, "_mssql_odbc_driver", lambda: None)


def _url(**cfg: Any) -> Any:
    base = {"type": "sqlserver", "host": "db.example.test", "port": 1433, "database": "sales",
            "username": "loader", "password": "pw"}
    return gs._build_url_unconfined({**base, **cfg})


# ------------------------------------------------------------ driver choice


def test_auto_prefers_the_microsoft_driver():
    assert select_driver({}, odbc_installed=True, pymssql_installed=True) == "mssql+pyodbc"


def test_auto_falls_back_to_freetds_only_without_a_host_policy(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", raising=False)
    assert select_driver({}, odbc_installed=False, pymssql_installed=True) == "mssql+pymssql"
    monkeypatch.setenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", "1")
    with pytest.raises(SqlServerSecurityError, match="requires verified TLS.*sqlserver_driver=freetds"):
        select_driver({}, odbc_installed=False, pymssql_installed=True)
    # The explicit, labelled opt-out still works under the host policy.
    assert select_driver({"sqlserver_driver": "freetds"}, odbc_installed=False,
                         pymssql_installed=True) == "mssql+pymssql"


def test_odbc_choice_never_downgrades(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", raising=False)
    with pytest.raises(SqlServerSecurityError, match="INSTALL_MSODBCSQL=1"):
        select_driver({"extra": {"sqlserver_driver": "odbc"}}, odbc_installed=False, pymssql_installed=True)


def test_freetds_choice_is_used_even_when_odbc_is_installed():
    assert select_driver({"sqlserver_driver": "freetds"}, odbc_installed=True,
                         pymssql_installed=True) == "mssql+pymssql"


def test_entra_id_needs_the_microsoft_driver(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", raising=False)
    with pytest.raises(SqlServerSecurityError, match="Entra ID sign-in needs it"):
        select_driver({"authentication": "entra_managed_identity"}, odbc_installed=False,
                      pymssql_installed=True)
    with pytest.raises(SqlServerSecurityError, match="no Entra ID sign-in"):
        select_driver({"authentication": "entra_service_principal", "sqlserver_driver": "freetds"},
                      odbc_installed=True, pymssql_installed=True)


def test_unknown_option_values_are_refused():
    with pytest.raises(SqlServerSecurityError, match="sqlserver_driver must be one of"):
        select_driver({"sqlserver_driver": "jtds"}, odbc_installed=True, pymssql_installed=True)
    with pytest.raises(SqlServerSecurityError, match="authentication must be one of"):
        select_driver({"authentication": "kerberos"}, odbc_installed=True, pymssql_installed=True)


# ------------------------------------------------------------ TLS keywords


def test_odbc_default_encrypts_and_verifies(odbc):
    url = _url()
    assert url.drivername == "mssql+pyodbc"
    assert url.query["driver"] == ODBC18
    assert url.query["Encrypt"] == "yes"  # also for Driver 17, whose default is "no"
    assert "TrustServerCertificate" not in url.query
    assert "Authentication" not in url.query


def test_encrypt_strict_and_pinned_certificate_pass_through(odbc):
    url = _url(encrypt="strict", server_certificate="/certs/sql.pem", hostname_in_certificate="sql.internal")
    assert url.query["Encrypt"] == "strict"
    assert url.query["ServerCertificate"] == "/certs/sql.pem"
    assert url.query["HostNameInCertificate"] == "sql.internal"
    assert "TrustServerCertificate" not in url.query


def test_trust_and_plaintext_only_when_declared(odbc):
    assert _url(trust_server_certificate=True).query["TrustServerCertificate"] == "Yes"
    assert _url(encrypt="no").query["Encrypt"] == "no"


def test_freetds_refuses_what_it_cannot_honour(no_odbc):
    with pytest.raises(ValueError, match="Encrypt"):
        _url(encrypt="yes")
    url = _url()
    assert url.drivername == "mssql+pymssql"
    assert "Encrypt" not in url.query


# ------------------------------------------------------------ Entra ID


def test_service_principal_sign_in(odbc):
    url = _url(authentication="entra_service_principal", username="11111111-2222-3333-4444-555555555555",
               password="client-secret")
    assert url.query["Authentication"] == "ActiveDirectoryServicePrincipal"
    assert url.username == "11111111-2222-3333-4444-555555555555"
    assert url.password == "client-secret"
    assert url.query["Encrypt"] == "yes"


def test_service_principal_needs_client_id_and_secret(odbc):
    with pytest.raises(SqlServerSecurityError, match="application / client id"):
        _url(authentication="entra_service_principal", password="")


def test_managed_identity_sends_no_password(odbc):
    system = _url(authentication="ActiveDirectoryMsi", username="", password="")
    assert system.query["Authentication"] == "ActiveDirectoryMsi"
    assert system.username is None and system.password is None
    user_assigned = _url(authentication="entra_managed_identity", username="client-id", password="stale")
    assert user_assigned.username == "client-id" and user_assigned.password is None


def test_access_token_rides_as_a_pre_connect_attribute(odbc, monkeypatch):
    url = _url(authentication="entra_access_token", username="ignored", password="eyJ0eXAi.tok")
    assert url.username is None and url.password is None
    assert "Authentication" not in url.query

    seen: dict[str, Any] = {}

    def _create_engine(url: Any, **kwargs: Any) -> Any:
        seen.update(kwargs)
        return SimpleNamespace(dialect=SimpleNamespace(name="mssql"))

    monkeypatch.setattr(gs, "create_engine", _create_engine)
    monkeypatch.setattr("connectors.tls_file_options.guard_engine", lambda engine: None)
    monkeypatch.setattr("services.egress_guard.assert_connector_dial_allowed", lambda *a, **k: None)
    import contextlib

    # Event hooks on the fake engine are not the subject: only the connect args are.
    with contextlib.suppress(Exception):
        gs._build_engine({"type": "sqlserver", "host": "db.example.test", "port": 1433, "database": "sales",
                          "authentication": "entra_access_token", "password": "eyJ0eXAi.tok"})
    attrs = seen["connect_args"]["attrs_before"]
    assert attrs[SQL_COPT_SS_ACCESS_TOKEN] == access_token_struct("eyJ0eXAi.tok")
    assert 103 in attrs  # the login timeout is still there


def test_access_token_struct_layout():
    raw = "abc".encode("utf-16-le")
    assert access_token_struct("abc") == struct.pack("<I", len(raw)) + raw
    with pytest.raises(SqlServerSecurityError):
        access_token_struct("")


def test_entra_options_read_from_the_saved_extra(odbc):
    url = gs._build_url_unconfined(gs.with_connection_options({
        "type": "sqlserver", "host": "h", "database": "d", "username": "app", "password": "s",
        "extra": {"authentication": "entra_service_principal", "encrypt": "strict"},
    }))
    assert url.query["Authentication"] == "ActiveDirectoryServicePrincipal"
    assert url.query["Encrypt"] == "strict"


# ------------------------------------------------------------ labels


def test_transport_summary_labels_freetds_as_unverified():
    assert transport_summary({}, "mssql+pymssql")["label"] == "FreeTDS — TLS certificate not verified"
    assert transport_summary({}, "mssql+pymssql")["certificate_verified"] is False
    assert transport_summary({}, "mssql+pyodbc")["certificate_verified"] is True
    assert transport_summary({"trust_server_certificate": True}, "mssql+pyodbc")["certificate_verified"] is False


# ------------------------------------------------- read-backs do not downgrade


def test_read_back_paths_follow_the_connector(monkeypatch, no_odbc):
    monkeypatch.delenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", raising=False)
    assert needs_odbc({}) is False
    assert needs_odbc({"encrypt": "yes"}) is True
    assert needs_odbc({"authentication": "entra_password"}) is True
    assert needs_odbc({"sqlserver_driver": "freetds"}) is False
    monkeypatch.setenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", "1")
    assert needs_odbc({}) is True


def test_gate8_read_back_uses_the_microsoft_driver_for_entra(monkeypatch):
    import connectors.generic_sql as generic
    from services import reconciliation

    dialled: list[dict] = []

    class _Cur:
        description = [("n",)]

        def execute(self, sql: str, *a: Any) -> None:
            raise RuntimeError("stop after the dial")

        def close(self) -> None:
            pass

    def _get_connection(**kwargs: Any) -> Any:
        dialled.append(kwargs)
        return SimpleNamespace(cursor=lambda: _Cur(), close=lambda: None)

    def _no_pymssql(**_: Any) -> Any:
        raise AssertionError("an Entra ID connector must never be read back over FreeTDS")

    monkeypatch.setattr(generic, "get_connection", _get_connection)
    monkeypatch.setitem(sys.modules, "pymssql", SimpleNamespace(connect=_no_pymssql))
    count, _ = reconciliation.verify_sqlserver_table(
        host="h", port=1433, database="d", username="app", password="s", connection_string="",
        table_name="t", options={"authentication": "entra_service_principal"},
    )
    assert count == -1  # the fake cursor stops the read; the dial is the subject
    assert dialled and dialled[0]["authentication"] == "entra_service_principal"
    assert dialled[0]["db_type"] == "sqlserver"


def test_copy_fast_path_never_retries_a_failed_verified_handshake_on_freetds(monkeypatch):
    import connectors.generic_sql as generic
    from services import copy_sqlserver_sqlserver as copy

    def _fail(**_: Any) -> Any:
        raise RuntimeError("[08001] SSL Provider: certificate verify failed: unable to connect")

    monkeypatch.setattr(generic, "get_connection", _fail)
    monkeypatch.setattr(copy, "_pymssql_connect", lambda cfg: (_ for _ in ()).throw(
        AssertionError("downgraded to FreeTDS")))
    with pytest.raises(copy.FastPathUnavailable, match="certificate verify failed"):
        copy._ss_connect({"host": "h", "database": "d", "encrypt": "yes"})


# ------------------------------------------- review: no unverified fallback


def test_read_backs_use_the_microsoft_driver_whenever_it_would_be_chosen(odbc):
    """Review: with driver auto, pyodbc installed and no host flag, Gate-8 and the
    ladder still dialled FreeTDS (unverified)."""
    assert needs_odbc({}) is True
    assert needs_odbc({"sqlserver_driver": "freetds"}) is False


def test_copy_fast_path_does_not_retry_a_certificate_failure_on_freetds_with_auto(odbc, monkeypatch):
    import connectors.generic_sql as generic
    from services import copy_sqlserver_sqlserver as copy

    def _fail(**_: Any) -> Any:
        raise RuntimeError("[08001] SSL Provider: certificate verify failed: unable to connect")

    freetds: list[dict] = []
    monkeypatch.setattr(generic, "get_connection", _fail)
    monkeypatch.setattr(copy, "_pymssql_connect", lambda cfg: freetds.append(cfg) or object())
    with pytest.raises(copy.FastPathUnavailable, match="certificate verify failed"):
        copy._ss_connect({"host": "h", "database": "d"})
    assert freetds == [], "a certificate failure was retried over FreeTDS"


def test_pymssql_connection_string_obeys_the_host_policy(monkeypatch, no_odbc):
    monkeypatch.setenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", "1")
    cfg = {"type": "sqlserver", "connection_string": "mssql+pymssql://u:p@db.example.test:1433/sales"}
    with pytest.raises(SqlServerSecurityError, match="mssql\\+pymssql"):
        gs._build_url_unconfined(dict(cfg))
    # The labelled opt-out still works.
    assert str(gs._build_url_unconfined({**cfg, "sqlserver_driver": "freetds"})).startswith("mssql+pymssql://")
    monkeypatch.delenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS")
    with pytest.raises(SqlServerSecurityError):
        gs._build_url_unconfined({**cfg, "authentication": "entra_managed_identity"})


def test_managed_identity_read_back_sends_no_default_username(monkeypatch):
    """Review: ``username or "sa"`` broke a system-assigned managed identity read-back."""
    import connectors.generic_sql as generic
    from services import reconciliation

    dialled: list[dict] = []

    def _get_connection(**kwargs: Any) -> Any:
        dialled.append(kwargs)
        raise RuntimeError("stop after the dial")

    monkeypatch.setattr(generic, "get_connection", _get_connection)
    reconciliation.verify_sqlserver_table(
        host="h", port=1433, database="d", username="", password="", connection_string="",
        table_name="t", options={"authentication": "entra_managed_identity"},
    )
    assert dialled and dialled[0]["username"] == ""
    url = gs._build_url_unconfined(gs.with_connection_options({
        "type": "sqlserver", "host": "h", "database": "d", "username": dialled[0]["username"],
        "authentication": "entra_managed_identity",
    })) if gs._mssql_odbc_driver() else None
    if url is not None:
        assert url.username is None



@pytest.mark.parametrize("scheme", ["mssql+pytds", "mssql+pymssql", "mssql+aioodbc"])
def test_every_non_pyodbc_mssql_scheme_obeys_the_host_policy(monkeypatch, no_odbc, scheme):
    """Review N9: only mssql+pymssql was refused; mssql+pytds (FreeTDS-style) passed."""
    import re

    monkeypatch.setenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", "1")
    cfg = {"type": "sqlserver", "connection_string": f"{scheme}://u:p@db.example.test:1433/sales"}
    with pytest.raises(SqlServerSecurityError, match=re.escape(scheme)):
        gs._build_url_unconfined(dict(cfg))


def test_pyodbc_and_plain_mssql_schemes_pass_the_host_policy(monkeypatch, odbc):
    monkeypatch.setenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", "1")
    for scheme in ("mssql+pyodbc", "mssql"):
        url = gs._build_url_unconfined({"type": "sqlserver", "connection_string": f"{scheme}://u:p@h:1433/d"})
        assert str(url).startswith(scheme)


def _oc(text: str) -> str:
    from urllib.parse import quote_plus

    return "odbc_connect=" + quote_plus(text)


_D18 = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=h;"


@pytest.mark.parametrize(("query", "refused"), [
    # In the SQLAlchemy URL query (any key case).
    ("driver=FreeTDS", "driver=FreeTDS"),
    ("Driver=%2Fusr%2Flib%2Fx86_64-linux-gnu%2Fodbc%2Flibtdsodbc.so", "driver="),
    ("driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes", "TrustServerCertificate=yes"),
    ("driver=ODBC+Driver+18+for+SQL+Server&trustservercertificate=True", "TrustServerCertificate=True"),
    ("driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=y", "TrustServerCertificate=y"),
    ("driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=Y", "TrustServerCertificate=Y"),
    ("driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no", "Encrypt=no"),
    ("driver=ODBC+Driver+18+for+SQL+Server&ENCRYPT=Optional", "Encrypt=Optional"),
    ("driver=ODBC+Driver+18+for+SQL+Server&Encrypt=n", "Encrypt=n"),
    ("driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&Encrypt=no", "Encrypt more than once"),
    # Inside odbc_connect, keys in any case, ODBC brace rules.
    (_oc("DRIVER={FreeTDS};SERVER=h;PORT=1433"), "driver=FreeTDS"),
    (_oc(_D18 + "trustServerCertificate=yes"), "TrustServerCertificate=yes"),
    (_oc(_D18 + "TrustServerCertificate = y"), "TrustServerCertificate=y"),   # spaces around '='
    (_oc(_D18 + "Encrypt= no"), "Encrypt=no"),
    # Review 4: '{' inside an unbraced value opens nothing; the driver applies these.
    (_oc(_D18 + "UID=u;PWD=x{;TrustServerCertificate=yes;Encrypt=no;APP=}"), "TrustServerCertificate=yes"),
    (_oc(_D18 + "PWD={a;Encrypt=no"), "unbalanced brace"),                  # unterminated brace
    (_oc(_D18 + "PWD={a}b;Encrypt=yes"), "unbalanced brace"),               # text after the brace
    (_oc(_D18 + "Encrypt=yes;Encrypt=no"), "Encrypt more than once"),       # duplicate key
    (_oc(_D18 + "TrustServerCertificate=no;trustservercertificate=yes"), "more than once"),
    (_oc("DSN=prod;UID=u"), "DSN"),
    # A second decoding (SQLAlchemy unquotes odbc_connect again) cannot hide a key.
    ("odbc_connect=" + "DRIVER%253D%257BODBC%2BDriver%2B18%2Bfor%2BSQL%2BServer%257D%253BEncrypt%253Dno", "Encrypt=no"),
    # Accepted: Driver 18, verification on.
    ("driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes", None),
    ("driver=ODBC+Driver+18+for+SQL+Server&Encrypt=strict&TrustServerCertificate=no", None),
    ("driver=ODBC+Driver+18+for+SQL+Server&Encrypt=Mandatory&TrustServerCertificate=0", None),
    (_oc(_D18 + "PWD={a;Encrypt=no}};x};Encrypt=yes"), None),            # braced: '}}' escapes
])
def test_pyodbc_strings_obey_the_verified_tls_allow_list(monkeypatch, odbc, query, refused):
    """Reviews 3-4: every mssql+pyodbc string passed as verified; then a deny-list
    missed 'y' and a '{' in the middle of a value hid the keys after it."""
    from connectors.mssql_security import enforce_mssql_url

    monkeypatch.setenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", "1")
    for scheme in ("mssql+pyodbc", "mssql"):
        raw = f"{scheme}://u:p@db.example.test:1433/sales?{query}"
        if refused is None:
            assert enforce_mssql_url(raw, {}) == raw
            continue
        with pytest.raises(SqlServerSecurityError) as caught:
            enforce_mssql_url(raw, {})
        assert refused.lower() in str(caught.value).lower(), str(caught.value)
        assert enforce_mssql_url(raw, {"sqlserver_driver": "freetds"}) == raw  # the labelled opt-out
    monkeypatch.delenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS")
    raw = f"mssql+pyodbc://u:p@h/d?{query}"
    assert enforce_mssql_url(raw, {}) == raw  # no policy: as before


@pytest.mark.parametrize("query", [
    "driver=ODBC+Driver+18+for+SQL+Server",
    _oc(_D18 + "UID=u"),
    "",
])
def test_absent_encrypt_is_forced_to_yes_under_the_policy(monkeypatch, odbc, query):
    """Encrypt absent gets the default a host/port connector gets (Driver 17 would not encrypt)."""
    from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc
    from sqlalchemy.engine import make_url

    from connectors.mssql_security import enforce_mssql_url

    monkeypatch.setenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", "1")
    raw = "mssql+pyodbc://u:p@db.example.test:1433/sales" + (f"?{query}" if query else "")
    out = enforce_mssql_url(raw, {})
    connect = MSDialect_pyodbc().create_connect_args(make_url(out))[0][0]
    assert connect.rstrip(";").endswith("Encrypt=yes"), connect
    assert enforce_mssql_url(out, {}) == out


def test_connection_string_with_trust_server_certificate_is_refused_end_to_end(monkeypatch, odbc):
    monkeypatch.setenv("DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS", "1")
    cfg = {"type": "sqlserver", "connection_string":
           "mssql+pyodbc://u:p@db.example.test:1433/sales?driver=ODBC+Driver+18+for+SQL+Server"
           "&TrustServerCertificate=yes"}
    with pytest.raises(SqlServerSecurityError, match="TrustServerCertificate"):
        gs._build_url_unconfined(dict(cfg))
