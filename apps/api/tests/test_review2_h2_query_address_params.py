"""Security review round 2 (H2): query-string parameters that carry an address are judged.

``mssql+pyodbc://u:p@x.eu-west-1.rds.amazonaws.com/db?odbc_connect=...SERVER%3D52.1.2.3``
was judged eu-west-1 from the authority, while SQLAlchemy's pyodbc dialect
connects to ``SERVER=52.1.2.3`` from ``odbc_connect`` and generic_sql keeps
the parameter. The same holds for an Oracle ``dsn=`` descriptor.

Now ``odbc_connect``, ``dsn``, ``driver``, ``server``, ``host``,
``hostaddr`` and ``service`` are address fields: ``odbc_connect``'s
``SERVER=`` and a ``dsn`` descriptor's ``HOST=`` (or an EZConnect
``host:port/service``) are read and judged, and a value that cannot be read
(an ODBC or TNS alias, a libpq service name, a ``driver`` that carries
connection keywords) refuses.
"""

from __future__ import annotations

from urllib.parse import quote_plus

import pytest
from tenancy_world import tenancy_world  # noqa: F401

EU = "x.eu-west-1.rds.amazonaws.com"
ODBC_US = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=52.1.2.3,1433;DATABASE=db"
TNS_US = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=52.1.2.3)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=x)))"
TNS_EU = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={EU})(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=x)))"


@pytest.fixture
def fenced(tenancy_world, monkeypatch):
    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    from services.tenant_store import create_tenant

    create_tenant(workspace_id=tenancy_world.workspace_a, name="EU", data_region="eu-west-1")
    import src.transfer.background as bg

    tenancy_world.queued = []
    monkeypatch.setattr(bg, "run_transfer_async", lambda *a, **k: tenancy_world.queued.append(a))
    return tenancy_world


def _refused(ws, fmt, cs, host=""):
    from services import data_residency as dr
    from src.transfer.models import EndpointConfig

    ep = EndpointConfig.from_dict("database", {"format": fmt, "host": host, "connection_string": cs, "database": "db", "table": "t"})
    return dr.residency_refusal(ws, ep) is not None


def test_http_odbc_connect_server_elsewhere_is_refused(fenced):
    cs = f"mssql+pyodbc://u:p@{EU}/db?odbc_connect={quote_plus(ODBC_US)}"
    src = {"kind": "database", "format": "sqlite", "database": ":memory:", "table": "t"}
    dst = {"kind": "database", "format": "sqlserver", "database": "db", "table": "t", "connection_string": cs}
    r = fenced.a.post(
        "/api/v1/transfer/execute",
        json={"source": src, "destination": dst, "mappings": [{"source": "id", "target": "id"}], "async_mode": True},
    )
    assert r.status_code == 403, r.text
    assert fenced.queued == []


@pytest.mark.parametrize(
    "fmt,cs",
    [
        ("sqlserver", f"mssql+pyodbc://u:p@{EU}/db?odbc_connect={quote_plus(ODBC_US)}"),
        ("sqlserver", f"mssql+pyodbc://u:p@{EU}/db?odbc_connect={quote_plus('DSN=prod_alias;UID=u')}"),
        ("sqlserver", f"mssql+pyodbc://u:p@{EU}/db?driver={quote_plus('ODBC Driver 18};SERVER=52.1.2.3;{x')}"),
        ("sqlserver", f"mssql+pyodbc://u:p@{EU}/db?dsn=prod_alias"),
        ("sqlserver", f"mssql+pymssql://u:p@{EU}/db?server=52.1.2.3"),
        ("oracle", f"oracle+oracledb://u:p@/?dsn={quote_plus(TNS_US)}"),
        ("oracle", f"oracle+oracledb://u:p@{EU}:1521/?dsn={quote_plus(TNS_US)}"),
        ("oracle", f"oracle+oracledb://u:p@{EU}:1521/?dsn=PRODALIAS"),
        ("oracle", f"oracle+oracledb://u:p@/?dsn={quote_plus('52.1.2.3:1521/svc')}"),
        ("postgresql", f"postgresql://u:p@{EU}/db?hostaddr=52.1.2.3"),
        ("postgresql", f"postgresql://u:p@{EU}/db?service=prod"),
        ("postgresql", f"postgresql://u:p@{EU}/db?HOST=52.1.2.3"),
    ],
)
def test_address_parameters_are_judged_or_refused(fenced, fmt, cs):
    assert _refused(fenced.workspace_a, fmt, cs)


@pytest.mark.parametrize(
    "fmt,cs",
    [
        ("sqlserver", f"mssql+pyodbc://u:p@{EU}/db?driver={quote_plus('ODBC Driver 18 for SQL Server')}"),
        ("sqlserver", f"mssql+pyodbc://u:p@{EU}/db?odbc_connect={quote_plus(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=tcp:{EU},1433;DATABASE=db')}"),
        ("oracle", f"oracle+oracledb://u:p@/?dsn={quote_plus(TNS_EU)}"),
        ("oracle", f"oracle+oracledb://u:p@/?dsn={quote_plus(EU + ':1521/svc')}"),
        ("postgresql", f"postgresql://u:p@{EU}/db?sslmode=require&application_name=dw"),
    ],
)
def test_readable_in_region_parameters_are_still_allowed(fenced, fmt, cs):
    """Controls: an ordinary driver name and an in-region server or descriptor pass."""
    assert not _refused(fenced.workspace_a, fmt, cs)


def test_url_hosts_reads_each_parameter():
    from services.data_residency import _url_hosts

    assert _url_hosts(f"mssql+pyodbc://u:p@{EU}/db?odbc_connect={quote_plus(ODBC_US)}") == [EU, "52.1.2.3"]
    assert _url_hosts(f"oracle+oracledb://u:p@/?dsn={quote_plus(TNS_US)}") == ["52.1.2.3"]
    assert _url_hosts(f"oracle+oracledb://u:p@/?dsn=PRODALIAS") is None
    assert _url_hosts(f"postgresql://u:p@{EU}/db?service=prod") is None


def test_a_tns_descriptor_connection_string_is_read():
    from services.data_residency import _field_hosts

    assert _field_hosts(TNS_US) == ["52.1.2.3"]
    assert _field_hosts(TNS_EU) == [EU]
    two = TNS_US.replace("(ADDRESS=", f"(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={EU})(PORT=1521))(ADDRESS=", 1)
    assert sorted(_field_hosts(two) or []) == sorted([EU, "52.1.2.3"])
