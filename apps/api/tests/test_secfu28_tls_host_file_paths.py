"""Follow-up review 2 (TLS host files): a connector's TLS file options never name the API host's files.

A tenant controls its connector settings (``extra`` included) and runs its own
database servers. Every driver family takes TLS material as a *file path*:

* MySQL (PyMySQL): ``ssl_ca`` / ``ssl_capath`` / ``ssl_cert`` / ``ssl_key``
  (URL query, the form's TLS fields, ``extra``) become ``ssl={'ca', 'cert',
  'key', ...}``;
* PostgreSQL (libpq): ``sslrootcert`` / ``sslcert`` / ``sslkey`` / ``sslcrl`` /
  ``sslcrldir`` (and ``passfile``, the host's password file) in the URL query
  or a replication DSN;
* SQL Server (ODBC): ``ServerCertificate`` / ``ClientCertificate`` /
  ``ClientKey`` in the URL or ``odbc_connect``, and the connector's
  ``server_certificate`` / ``ssl_ca`` fields;
* Oracle (python-oracledb): ``wallet_location`` / ``config_dir``;
* MongoDB (PyMongo): ``tlsCAFile`` / ``tlsCertificateKeyFile`` / ``tlsCRLFile``.

With a path on the API host the platform presented its own client certificate
and key (or wallet) to the tenant's server, or trusted the host's CA files for
it. Now such a path may only point inside the areas a connector may open (a
workspace upload, its own export folder, a ``DATAFLOW_SOURCE_PATH_ROOTS``
folder: ``services.tenant_host_files``), refused at save and at dial, with a
message naming the option and never the path. Inline PEM text is not a path.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_HOST = "/etc/platform/tls"
_CERT = f"{_HOST}/client.pem"
_KEY = f"{_HOST}/client.key"
_CA = f"{_HOST}/ca.pem"
_PEM = "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIUQ\n-----END CERTIFICATE-----\n"


class Dialled(Exception):
    """The driver's connect was reached (recorded, never a socket)."""


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("SOURCE_PATH_ROOTS", "ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "EGRESS_ENFORCE_NEW"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )


@pytest.fixture
def drivers(monkeypatch):
    """Every driver connect records its arguments and stops."""
    import oracledb
    import psycopg2
    import pymongo
    import pymysql
    import pyodbc

    calls: list[tuple[str, tuple, dict]] = []

    def recorder(name):
        def connect(*args, **kwargs):
            calls.append((name, args, kwargs))
            raise Dialled(name)

        return connect

    for module, name in ((pymysql, "pymysql"), (psycopg2, "psycopg2"), (oracledb, "oracledb"),
                         (pyodbc, "pyodbc")):
        monkeypatch.setattr(module, "connect", recorder(name))
    monkeypatch.setattr(pymongo, "MongoClient", recorder("pymongo"))
    monkeypatch.setattr("connectors.generic_sql._mssql_odbc_driver",
                        lambda: "ODBC Driver 18 for SQL Server")
    return calls


def _refused(message: str, option: str) -> None:
    assert option.lower() in message.lower(), message
    for value in (_HOST, "client.pem", "client.key", "ca.pem", "/etc/", "/tmp/"):
        assert value not in message, message


def _engine_dial(cfg: dict) -> None:
    from connectors.generic_sql import _build_engine

    engine = _build_engine(dict(cfg))
    try:
        engine.connect()
    finally:
        engine.dispose()


def _fields(kind: str, port: int, **more) -> dict:
    return {"type": kind, "host": "db.example.com", "port": port, "database": "db",
            "username": "u", "password": "p", **more}


# ------------------------------------------------------------------ dial: engines

_ENGINE_CASES = [
    pytest.param({"type": "mysql", "connection_string":
                  f"mysql+pymysql://u:p@db.example.com/db?ssl_cert={_CERT}&ssl_key={_KEY}"},
                 "ssl_cert", id="mysql-url-client-cert"),
    pytest.param({"type": "mysql", "connection_string": f"mysql+pymysql://u:p@db.example.com/db?ssl_ca={_CA}"},
                 "ssl_ca", id="mysql-url-ca"),
    pytest.param({"type": "mysql", "connection_string":
                  f"mysql+pymysql://u:p@db.example.com/db?ssl_capath={_HOST}"},
                 "ssl_capath", id="mysql-url-capath"),
    pytest.param(_fields("mysql", 3306, extra={"ssl_cert": _CERT, "ssl_key": _KEY}), "ssl_cert",
                 id="mysql-extra-client-cert"),
    pytest.param(_fields("mysql", 3306, extra={"ssl_ca": _CA}), "ssl_ca", id="mysql-extra-ca"),
    pytest.param({"type": "postgresql", "connection_string":
                  f"postgresql+psycopg2://u:p@db.example.com/db?sslcert={_CERT}&sslkey={_KEY}"},
                 "sslcert", id="pg-url-client-cert"),
    pytest.param({"type": "postgresql", "connection_string":
                  f"postgresql+psycopg2://u:p@db.example.com/db?sslmode=verify-full&sslrootcert={_CA}"},
                 "sslrootcert", id="pg-url-rootcert"),
    pytest.param({"type": "postgresql", "connection_string":
                  f"postgresql+psycopg2://u:p@db.example.com/db?sslcrl={_CA}"}, "sslcrl", id="pg-url-crl"),
    pytest.param({"type": "postgresql", "connection_string":
                  f"postgresql+psycopg2://u:p@db.example.com/db?sslcrldir={_HOST}"},
                 "sslcrldir", id="pg-url-crldir"),
    pytest.param({"type": "postgresql", "connection_string":
                  "postgresql+psycopg2://u@db.example.com/db?passfile=/root/.pgpass"},
                 "passfile", id="pg-url-passfile"),
    pytest.param({"type": "sqlserver", "connection_string":
                  "mssql+pyodbc://u:p@db.example.com/db?driver=ODBC+Driver+18+for+SQL+Server"
                  f"&ServerCertificate={_CA}"}, "ServerCertificate", id="mssql-url-server-cert"),
    pytest.param({"type": "sqlserver", "connection_string":
                  "mssql+pyodbc://u:p@db.example.com/db?driver=ODBC+Driver+18+for+SQL+Server"
                  f"&ClientCertificate=file:{_CERT}&ClientKey=file:{_KEY}"},
                 "ClientCertificate", id="mssql-url-client-cert"),
    pytest.param({"type": "sqlserver", "connection_string":
                  "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+Driver+18+for+SQL+Server%7D%3B"
                  f"SERVER%3Ddb.example.com%3BServerCertificate%3D{_CA}"},
                 "ServerCertificate", id="mssql-odbc-connect"),
    pytest.param(_fields("sqlserver", 1433, server_certificate=_CA), "ServerCertificate",
                 id="mssql-field-server-certificate"),
    pytest.param(_fields("sqlserver", 1433, ssl_ca=_CA), "ServerCertificate", id="mssql-field-ssl-ca"),
    pytest.param({"type": "oracle", "connection_string":
                  f"oracle+oracledb://u:p@db.example.com:1521/?service_name=x&wallet_location={_HOST}"},
                 "wallet_location", id="oracle-url-wallet"),
    pytest.param({"type": "oracle", "connection_string":
                  f"oracle+oracledb://u:p@db.example.com:1521/?service_name=x&config_dir={_HOST}"},
                 "config_dir", id="oracle-url-config-dir"),
]


@pytest.mark.parametrize("cfg, option", _ENGINE_CASES)
def test_an_engine_never_hands_a_driver_a_host_tls_file(drivers, cfg, option):
    with pytest.raises(ValueError) as caught:
        _engine_dial(cfg)
    _refused(str(caught.value), option)
    assert drivers == [], drivers


# ------------------------------------------------------------------ dial: direct paths


def test_the_mysql_bulk_connection_refuses_a_host_client_certificate(drivers):
    from connectors.mysql_conn import get_connection

    with pytest.raises(ValueError) as caught:
        get_connection(host="db.example.com", port=3306, database="db", username="u", password="p",
                       connection_string=f"mysql://u:p@db.example.com/db?ssl_cert={_CERT}&ssl_key={_KEY}",
                       ssl=True)
    _refused(str(caught.value), "ssl_cert")
    assert drivers == []


def test_the_binlog_reader_refuses_a_host_ca(drivers):
    from connectors.mysql_tls import mysql_tls_plan, resolved_kwargs_for_external_dial

    plan = mysql_tls_plan(ssl=True, connection_string=f"mysql://db.example.com/db?ssl_ca={_CA}",
                          host="db.example.com")
    with pytest.raises(ValueError) as caught:
        resolved_kwargs_for_external_dial({"host": "db.example.com", "port": 3306, "user": "u",
                                           "password": "p"}, plan, host="db.example.com")
    _refused(str(caught.value), "ssl_ca")


@pytest.mark.parametrize("dsn, option", [
    (f"postgresql://u:p@db.example.com/db?sslcert={_CERT}&sslkey={_KEY}", "sslcert"),
    (f"host=db.example.com dbname=db user=u sslrootcert={_CA} sslmode=verify-ca", "sslrootcert"),
])
def test_the_replication_transport_refuses_a_host_tls_file(drivers, dsn, option):
    from connectors.postgresql_cdc_transport import StreamingReplicationTransport

    transport = StreamingReplicationTransport(dsn_kwargs={"dsn": dsn}, slot_name="s",
                                              publication_name="p")
    with pytest.raises(ValueError) as caught:
        transport._open()
    _refused(str(caught.value), option)
    assert drivers == []


@pytest.mark.parametrize("uri, option", [
    (f"mongodb://u:p@db.example.com/db?tls=true&tlsCAFile={_CA}", "tlsCAFile"),
    (f"mongodb://u:p@db.example.com/db?tls=true&tlsCertificateKeyFile={_CERT}", "tlsCertificateKeyFile"),
    (f"mongodb+srv://u:p@cluster.example.com/db?tlsCRLFile={_CA}", "tlsCRLFile"),
])
def test_the_mongo_client_refuses_a_host_tls_file_in_the_uri(drivers, uri, option):
    from connectors.mongodb_common import guarded_mongo_client

    with pytest.raises(ValueError) as caught:
        guarded_mongo_client(uri)
    _refused(str(caught.value), option)
    assert drivers == []


def test_the_mongo_client_refuses_a_host_tls_file_in_its_options(drivers):
    from connectors.mongodb_common import guarded_mongo_client

    with pytest.raises(ValueError) as caught:
        guarded_mongo_client("mongodb://db.example.com/db", tls=True, tlsCertificateKeyFile=_CERT)
    _refused(str(caught.value), "tlsCertificateKeyFile")
    assert drivers == []


# ------------------------------------------------------------------ save

_SAVE_CASES = [
    pytest.param({"type": "mysql", "connection_string":
                  f"mysql+pymysql://u:p@db.example.com/db?ssl_cert={_CERT}&ssl_key={_KEY}"},
                 "ssl_cert", id="mysql-url"),
    pytest.param(_fields("mysql", 3306, extra={"ssl_ca": _CA}), "ssl_ca", id="mysql-form-ca"),
    pytest.param(_fields("mysql", 3306, extra={"ssl_key": _KEY}), "ssl_key", id="mysql-form-key"),
    pytest.param(_fields("mysql", 3306, extra={"ssl_capath": _HOST}), "ssl_capath", id="mysql-form-capath"),
    pytest.param(_fields("mysql", 3306, ssl_cert=_CERT), "ssl_cert", id="mysql-top-level"),
    pytest.param(_fields("mysql", 3306, extra={"connect_args": {"ssl": {"cert": _CERT, "key": _KEY}}}),
                 "ssl_cert", id="mysql-connect-args-ssl-dict"),
    pytest.param({"type": "postgresql", "connection_string":
                  f"postgresql://u:p@db.example.com/db?sslrootcert={_CA}"}, "sslrootcert", id="pg-url"),
    pytest.param(_fields("postgresql", 5432, extra={"sslkey": _KEY}), "sslkey", id="pg-extra"),
    pytest.param(_fields("postgresql", 5432, extra={"connect_args": {"sslcert": _CERT}}), "sslcert",
                 id="pg-connect-args"),
    pytest.param(_fields("sqlserver", 1433, extra={"server_certificate": _CA}), "server_certificate",
                 id="mssql-extra"),
    pytest.param({"type": "sqlserver", "connection_string":
                  f"mssql+pyodbc://u:p@db.example.com/db?ServerCertificate={_CA}"},
                 "ServerCertificate", id="mssql-url"),
    pytest.param({"type": "sqlserver", "connection_string":
                  f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=db.example.com;ClientKey=file:{_KEY}"},
                 "ClientKey", id="mssql-odbc-string"),
    pytest.param(_fields("oracle", 1521, extra={"wallet_location": _HOST}), "wallet_location",
                 id="oracle-extra"),
    pytest.param({"type": "oracle", "connection_string":
                  "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=db.example.com)(PORT=2484))"
                  f"(SECURITY=(MY_WALLET_DIRECTORY={_HOST})))"}, "MY_WALLET_DIRECTORY",
                 id="oracle-descriptor"),
    pytest.param({"type": "mongodb", "connection_string":
                  f"mongodb://db.example.com/db?tlsCertificateKeyFile={_CERT}"},
                 "tlsCertificateKeyFile", id="mongo-uri"),
    pytest.param(_fields("mongodb", 27017, extra={"tlsCAFile": _CA}), "tlsCAFile", id="mongo-extra"),
]


@pytest.mark.parametrize("data, option", _SAVE_CASES)
def test_save_refuses_a_tls_file_outside_the_connector_areas(data, option):
    from services.egress_guard import connector_save_refusal

    refusal = connector_save_refusal(data)
    assert refusal
    _refused(refusal, option)


# ------------------------------------------------------------------ controls


@pytest.fixture
def listed(tmp_path, monkeypatch):
    folder = tmp_path / "tls"
    folder.mkdir()
    (folder / "ca.pem").write_text(_PEM)
    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(folder))
    return folder


def test_a_file_in_an_operator_listed_folder_is_used(drivers, listed):
    ca = str(listed / "ca.pem")
    cfg = {"type": "mysql", "connection_string": f"mysql+pymysql://u:p@db.example.com/db?ssl_ca={ca}"}
    from services.egress_guard import connector_save_refusal

    assert connector_save_refusal(dict(cfg)) == ""
    with pytest.raises(Dialled):
        _engine_dial(cfg)
    assert drivers[0][2]["ssl"]["ca"] == ca


def test_a_path_that_climbs_out_of_a_listed_folder_is_refused(drivers, listed):
    climb = f"{listed}/../../../etc/platform/tls/ca.pem"
    with pytest.raises(ValueError):
        _engine_dial({"type": "postgresql",
                      "connection_string": f"postgresql+psycopg2://u:p@db.example.com/db?sslrootcert={climb}"})
    assert drivers == []


def test_a_symlink_out_of_a_listed_folder_is_refused(listed, tmp_path):
    from services.egress_guard import connector_save_refusal

    outside = tmp_path / "outside.pem"
    outside.write_text(_PEM)
    (Path(listed) / "link.pem").symlink_to(outside)
    assert connector_save_refusal(_fields("mysql", 3306, extra={"ssl_ca": str(listed / "link.pem")}))


@pytest.mark.parametrize("data", [
    pytest.param(_fields("mongodb", 27017, extra={"tlsCAFile": _PEM}), id="inline-pem"),
    pytest.param({"type": "postgresql", "connection_string":
                  "postgresql://u:p@db.example.com/db?sslmode=verify-full&sslrootcert=system"},
                 id="pg-system-ca"),
    pytest.param({"type": "mysql", "connection_string":
                  "mysql+pymysql://u:p@db.example.com/db?ssl_mode=REQUIRED"}, id="mysql-no-files"),
    pytest.param(_fields("sqlserver", 1433, extra={"trust_server_certificate": True}), id="mssql-trust"),
])
def test_save_accepts_settings_that_name_no_host_file(data):
    from services.egress_guard import connector_save_refusal

    assert connector_save_refusal(data) == ""


@pytest.mark.parametrize("cfg, driver", [
    ({"type": "mysql", "connection_string": "mysql+pymysql://u:p@db.example.com/db?ssl_mode=REQUIRED"},
     "pymysql"),
    ({"type": "postgresql", "connection_string":
      "postgresql+psycopg2://u:p@db.example.com/db?sslmode=verify-full&sslrootcert=system"}, "psycopg2"),
    (_fields("sqlserver", 1433, trust_server_certificate=True), "pyodbc"),
    ({"type": "oracle", "connection_string": "oracle+oracledb://u:p@db.example.com:1521/?service_name=x"},
     "oracledb"),
])
def test_a_dial_with_no_host_file_reaches_the_driver(drivers, cfg, driver):
    with pytest.raises(Dialled):
        _engine_dial(cfg)
    assert [name for name, _a, _k in drivers] == [driver]
