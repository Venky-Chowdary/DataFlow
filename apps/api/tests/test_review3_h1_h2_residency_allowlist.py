"""Security review round 3 (H1, H2 and the Low ``unix_socket``): deny by default.

In a workspace with a data region every connector family has an allowlist of
the connection-string query keys and ``extra`` keys its driver may be given
(``services.residency_addressing``). Any other key is refused with a 422
naming it, in the URL query, the extras and per-request overrides; every
allowlisted address key is judged with the driver's own resolution. DuckDB
locality follows the URL the engine opens (``connection_string`` first) and
``md:`` / ``motherduck:`` is remote. Unfenced workspaces are unchanged.
"""

from __future__ import annotations

import pytest

from tenancy_world import tenancy_world  # noqa: F401

EU = "x.eu-west-1.rds.amazonaws.com"
EU2 = "y.eu-west-1.rds.amazonaws.com"
ODBC = "driver=ODBC+Driver+18+for+SQL+Server"


@pytest.fixture
def w(tenancy_world, monkeypatch):  # noqa: F811
    for n in ("DATAWRAP_RESIDENCY_STRICT", "DATAFLOW_RESIDENCY_STRICT", "DATAWRAP_DEFAULT_REGION"):
        monkeypatch.delenv(n, raising=False)
    monkeypatch.setenv("DATAFLOW_DEFAULT_REGION", "eu-west-1")
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "review3-h1-secrets-key-distinct-from-auth-0001")
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    import src.transfer.background as bg

    monkeypatch.setattr(bg, "run_transfer_async", lambda *a, **k: None)
    from services import tenant_store

    tenant_store.create_tenant(workspace_id=tenancy_world.workspace_a, name="A", data_region="eu-west-1")
    return tenancy_world


def _ep(**fields):
    from src.transfer.models import EndpointConfig

    return EndpointConfig.from_dict("database", {"table": "t", **fields})


def _refusal(w, workspace, **fields):
    from services.data_residency import residency_refusal

    return residency_refusal(workspace, _ep(**fields))


def _start(w, workspace, **fields):
    from src.transfer.job_scope import start_refusal
    from src.transfer.models import transfer_request_from_dict

    return start_refusal(transfer_request_from_dict({
        "source": {"kind": "database", "format": "sqlite", "database": ":memory:", "table": "s"},
        "destination": {"kind": "database", "table": "t", **fields},
        "mappings": [{"source": "a", "target": "a"}], "workspace_id": workspace,
    }))


REFUSED_URLS = [
    ("sqlserver", f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&Address=52.1.2.3", "Address"),
    ("sqlserver", f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&Addr=52.1.2.3", "Addr"),
    ("sqlserver", f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&Failover_Partner=52.1.2.3", "Failover_Partner"),
    ("sqlserver", f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&Network+Address=52.1.2.3", "Network Address"),
    ("sqlserver", f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&FileDSN=/tmp/x.dsn", "FileDSN"),
    ("sqlserver", f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&SaveFile=/tmp/x", "SaveFile"),
    ("clickhouse", f"clickhouse+native://u:p@{EU}:9000/db?alt_hosts=52.1.2.3:9000", "alt_hosts"),
    ("mysql", f"mysql+pymysql://u:p@{EU}:3306/db?unix_socket=/var/run/mysqld/mysqld.sock", "unix_socket"),
    ("mysql", f"mysql+pymysql://u:p@{EU}:3306/db?read_default_file=/srv/my.cnf", "read_default_file"),
    ("mysql", f"mysql+pymysql://u:p@{EU}:3306/db?local_infile=1", "local_infile"),
    ("postgresql", f"postgresql://u:p@{EU}:5432/db?service=prod", "service"),
    ("postgresql", f"postgresql://u:p@{EU}:5432/db?passfile=/tmp/p", "passfile"),
    ("postgresql", f"postgresql://u:p@{EU}:5432/db?host%3D=52.1.2.3", "host="),
    ("snowflake", "snowflake://u:p@ab12345.eu-west-1/db?region=us-east-1", "region"),
    ("generic_sql", f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&Address=52.1.2.3", "Address"),
]


@pytest.mark.parametrize("fmt,url,key", REFUSED_URLS)
def test_a_url_key_off_the_familys_allowlist_or_an_unprovable_address_is_refused(w, fmt, url, key):
    refusal = _refusal(w, w.workspace_a, format=fmt, connection_string=url)
    assert refusal and "residency" in refusal.lower(), (url, refusal)
    assert _start(w, w.workspace_a, format=fmt, connection_string=url), url


@pytest.mark.parametrize("fmt,url", [
    ("sqlserver", f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&Encrypt=yes&TrustServerCertificate=no"),
    ("sqlserver", f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&Address={EU2}&Failover_Partner={EU2}"),
    ("clickhouse", f"clickhouse+native://u:p@{EU}:9000/db?secure=true&alt_hosts={EU2}:9000"),
    ("mysql", f"mysql+pymysql://u:p@{EU}:3306/db?charset=utf8mb4&ssl_ca=/etc/ca.pem"),
    ("postgresql", f"postgresql://u:p@{EU}:5432/db?sslmode=verify-full&application_name=dw"),
])
def test_allowlisted_keys_with_in_region_addresses_still_pass(w, fmt, url):
    assert _refusal(w, w.workspace_a, format=fmt, connection_string=url) is None


def test_an_odbc_connect_string_is_held_to_the_same_allowlist(w):
    from urllib.parse import quote_plus

    inner = f"Driver={{ODBC Driver 18 for SQL Server}};Server={EU};Database=db;Address=52.1.2.3"
    url = f"mssql+pyodbc:///?odbc_connect={quote_plus(inner)}"
    assert _refusal(w, w.workspace_a, format="sqlserver", connection_string=url)
    plain = f"Driver={{ODBC Driver 18 for SQL Server}};Server={EU};Database=db;Encrypt=yes;Bogus=1"
    refusal = _refusal(w, w.workspace_a, format="sqlserver",
                       connection_string=f"mssql+pyodbc:///?odbc_connect={quote_plus(plain)}")
    assert refusal and "Bogus" in refusal


@pytest.mark.parametrize("extra,key", [
    ({"alt_hosts": "52.1.2.3:9000"}, None),
    ({"some_new_option": "x"}, "some_new_option"),
    ({"host": "52.1.2.3"}, None),  # an endpoint field: judged, as the driver reads extra first
    ({"connection_string": f"clickhouse+native://u:p@{EU}:9000/db?alt_hosts=52.1.2.3"}, None),
])
def test_an_extra_override_off_the_allowlist_is_refused_naming_it(w, extra, key):
    refusal = _refusal(w, w.workspace_a, format="clickhouse", host=EU, database="db", extra=extra)
    assert refusal
    if key:
        assert f"'{key}'" in refusal


def test_engine_options_in_extra_are_not_refused(w):
    extra = {"pg_overwrite_in_transaction": True, "batch_size": 500, "route_backups": True, "sslmode": None}
    extra.pop("sslmode")
    assert _refusal(w, w.workspace_a, format="postgresql", host=EU, database="db", extra=extra) is None


def test_a_saved_connectors_extra_is_held_to_the_allowlist(w):
    from services import connector_store

    conn = connector_store.create_connector({
        "name": "ch", "type": "clickhouse", "host": EU, "port": 9000, "database": "db",
        "username": "u", "password": "p", "workspace_id": w.workspace_a,
        "extra": {"alt_hosts": "52.1.2.3:9000"},
    })
    assert _refusal(w, w.workspace_a, format="clickhouse", connector_id=conn.id)


def test_duckdb_is_judged_on_the_url_the_engine_opens(w):
    """H2: database names a local file, the connection string MotherDuck."""
    from services.local_file_policy import local_disk_destination

    ep = _ep(format="duckdb", database="local.duckdb", connection_string="duckdb:///md:mydb?motherduck_token=TKN")
    assert local_disk_destination(ep) is None
    assert _refusal(w, w.workspace_a, format="duckdb", database="local.duckdb",
                    connection_string="duckdb:///md:mydb?motherduck_token=TKN")
    for fields in ({"database": "md:mydb"}, {"database": "local.duckdb", "host": "motherduck:x"},
                   {"connection_string": "MD:mydb"}):
        assert _refusal(w, w.workspace_a, format="duckdb", **fields), fields
    assert _refusal(w, w.workspace_a, format="duckdb", database="local.duckdb") is None


def test_redshift_staging_must_be_a_regional_aws_endpoint_in_region(w):
    host = "c.abc.eu-west-1.redshift.amazonaws.com"
    base = {"format": "redshift", "host": host, "database": "dev", "port": 5439}
    stage = {"staging_bucket": "b", "iam_role": "arn:aws:iam::123456789012:role/r"}
    assert _refusal(w, w.workspace_a, **base, extra={**stage, "region": "eu-west-1"}) is None
    assert _refusal(w, w.workspace_a, **base, extra={**stage, "region": "us-east-1"})
    assert _refusal(w, w.workspace_a, **base, extra=stage)  # boto3 default: us-east-1
    assert "not a regional AWS" in _refusal(w, w.workspace_a, **base,
                                            extra={**stage, "s3_endpoint": "https://203.0.113.9"})
    assert _refusal(w, w.workspace_a, **base, extra={**stage, "s3_endpoint": "https://s3.eu-west-1.amazonaws.com"}) is None


def test_iceberg_fileio_endpoints_and_warehouse_are_judged(w):
    rest = {"format": "iceberg", "host": "https://catalog.eu-west-1.amazonaws.com", "database": "ns",
            "extra": {"catalog_type": "rest"}}
    assert _refusal(w, w.workspace_a, **rest) is None
    assert _refusal(w, w.workspace_a, **{**rest, "extra": {"catalog_type": "rest", "s3.endpoint": "http://203.0.113.9"}})
    glue = {"format": "iceberg", "host": "eu-west-1", "warehouse": "s3://eu-bucket/wh", "extra": {"catalog_type": "glue"}}
    # A bucket no address places: only a platform-verified region speaks for it.
    assert _refusal(w, w.workspace_a, **glue)


def test_the_reviewers_http_cases_answer_422_naming_the_key(w):
    from services import connector_store

    src = connector_store.create_connector({"name": "src", "type": "postgresql", "host": EU, "database": "s",
                                            "username": "u", "password": "p", "workspace_id": w.workspace_a})
    for dest, key in (
        ({"format": "sqlserver", "connection_string": f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&FileDSN=/tmp/x"}, "FileDSN"),
        ({"format": "mysql", "connection_string": f"mysql+pymysql://u:p@{EU}:3306/db?read_default_file=/x"}, "read_default_file"),
        ({"format": "clickhouse", "host": EU, "database": "db", "extra": {"bogus": 1}}, "bogus"),
    ):
        r = w.a.post("/api/v1/transfer/execute", json={
            "source": {"kind": "database", "format": "postgresql", "connector_id": src.id, "table": "s"},
            "destination": {"kind": "database", "table": "t", **dest},
            "mappings": [{"source": "a", "target": "a"}], "async_mode": True,
        })
        assert r.status_code == 422, r.text
        assert key in r.text
    for dest in (
        {"format": "sqlserver", "connection_string": f"mssql+pyodbc://u:p@{EU}:1433/db?{ODBC}&Address=52.1.2.3"},
        {"format": "clickhouse", "connection_string": f"clickhouse+native://u:p@{EU}:9000/db?alt_hosts=52.1.2.3:9000"},
        {"format": "duckdb", "database": "local.duckdb", "connection_string": "duckdb:///md:mydb?motherduck_token=T"},
    ):
        r = w.a.post("/api/v1/transfer/execute", json={
            "source": {"kind": "database", "format": "postgresql", "connector_id": src.id, "table": "s"},
            "destination": {"kind": "database", "table": "t", **dest},
            "mappings": [{"source": "a", "target": "a"}], "async_mode": True,
        })
        assert r.status_code in (403, 422), (dest, r.text)


@pytest.mark.parametrize("fmt,url", [(f, u) for f, u, _k in REFUSED_URLS])
def test_unfenced_workspaces_are_unchanged(w, fmt, url):
    assert _refusal(w, w.workspace_a, format=fmt, connection_string=url)  # what the fence refuses
    assert _refusal(w, w.workspace_b, format=fmt, connection_string=url) is None
    assert _start(w, w.workspace_b, format=fmt, connection_string=url) is None


def test_every_family_allowlist_names_the_resolver_it_mirrors():
    from services.residency_addressing import FAMILIES

    for name in ("postgresql", "mysql", "sqlserver", "clickhouse", "oracle", "mongodb", "snowflake",
                 "iceberg", "redshift", "duckdb"):
        assert FAMILIES[name].resolver, name
