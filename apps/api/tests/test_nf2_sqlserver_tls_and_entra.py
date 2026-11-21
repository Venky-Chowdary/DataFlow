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
    try:
        gs._build_engine({"type": "sqlserver", "host": "db.example.test", "port": 1433, "database": "sales",
                          "authentication": "entra_access_token", "password": "eyJ0eXAi.tok"})
    except Exception:  # noqa: BLE001 - event hooks on the fake engine are not the subject
        pass
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


def test_read_back_paths_follow_the_connector(monkeypatch):
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
