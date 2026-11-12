"""PR 403 security review 4 (HIGH): a braced ODBC value holding ';' hid the
rest of the connection string from the residency allowlist.

``APP={a;b}`` made the DSN reader give up on the whole ``odbc_connect`` value,
so ``Failover_Partner=<out-of-region host>`` and ``FileDSN=`` reached pyodbc
unjudged in a workspace fenced to a region. The reader now follows ODBC brace
quoting (``}}`` is a literal ``}``) and libpq single quotes, and a DSN it
cannot read is refused, never waved through.
"""
from __future__ import annotations

from urllib.parse import quote_plus

import pytest

from services.residency_addressing import _dsn_pairs, addressing_refusal
from src.transfer.models import EndpointConfig
from tenancy_world import tenancy_world  # noqa: F401

EU = "x.eu-west-1.rds.amazonaws.com"
DRV = "Driver={ODBC Driver 18 for SQL Server}"


def _sqlserver(cs: str) -> EndpointConfig:
    return EndpointConfig.from_dict(
        "database",
        {"format": "sqlserver", "connection_string": cs, "database": "db", "table": "t"},
    )


@pytest.fixture
def fenced_workspace(tenancy_world, monkeypatch):  # noqa: F811
    """Workspace A of the tenancy world, fenced to eu-west-1."""
    for name in ("DATAWRAP_RESIDENCY_STRICT", "DATAFLOW_RESIDENCY_STRICT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_DEFAULT_REGION", "eu-west-1")
    from services import tenant_store

    tenant_store.create_tenant(
        workspace_id=tenancy_world.workspace_a, name="A", data_region="eu-west-1"
    )
    return tenancy_world.workspace_a


def _refused(workspace_id: str, cs: str) -> bool:
    from services import data_residency

    return data_residency.residency_refusal(workspace_id, _sqlserver(cs)) is not None


def _url(odbc: str) -> str:
    return f"mssql+pyodbc://u:p@/?odbc_connect={quote_plus(odbc)}"


def test_braced_values_are_read_as_one_value():
    assert _dsn_pairs(f"{DRV};Server={EU};APP={{a;b}};Failover_Partner=52.1.2.3") == [
        ("Driver", "ODBC Driver 18 for SQL Server"),
        ("Server", EU),
        ("APP", "a;b"),
        ("Failover_Partner", "52.1.2.3"),
    ]
    assert _dsn_pairs("PWD={p}};w;d};Server=s") == [("PWD", "p};w;d"), ("Server", "s")]
    assert _dsn_pairs("host=h options='-c a=b c' hostaddr=1.2.3.4") == [
        ("host", "h"),
        ("options", "-c a=b c"),
        ("hostaddr", "1.2.3.4"),
    ]


@pytest.mark.parametrize(
    "tail",
    [
        "APP={a;b};Failover_Partner=52.1.2.3",
        "APP={a;b};FileDSN=/srv/exports/x.dsn",
        "APP={a;b};SaveFile=/srv/exports/x.dsn",
        "APP={a}};b;c};Failover_Partner=52.1.2.3",
    ],
)
@pytest.mark.parametrize("where", ["odbc_connect", "connection_string"])
def test_a_key_after_a_braced_value_is_still_judged(fenced_workspace, tail, where):
    odbc = f"{DRV};Server={EU};Database=db;{tail}"
    cs = _url(odbc) if where == "odbc_connect" else odbc
    assert _refused(fenced_workspace, cs), cs


def test_the_same_keys_without_braces_were_already_refused(fenced_workspace):
    odbc = f"{DRV};Server={EU};Database=db;Failover_Partner=52.1.2.3"
    assert _refused(fenced_workspace, _url(odbc))
    assert _refused(fenced_workspace, odbc)


@pytest.mark.parametrize(
    "odbc",
    [
        f"{DRV};Server={EU};APP={{a;b;Failover_Partner=52.1.2.3",  # unterminated brace
        f"{DRV};Server={EU};APP={{a}}x;Failover_Partner=52.1.2.3",  # text after the closing brace
        f"{DRV};Server={EU};junk;Failover_Partner=52.1.2.3",  # a part with no '='
    ],
)
def test_an_unreadable_odbc_connect_is_refused(fenced_workspace, odbc):
    assert addressing_refusal(_sqlserver(_url(odbc))), odbc
    assert _refused(fenced_workspace, _url(odbc)), odbc


def test_an_ordinary_braced_value_is_still_accepted(fenced_workspace):
    odbc = f"{DRV};Server={EU};Database=db;APP={{reporting; nightly}};Encrypt=yes"
    assert addressing_refusal(_sqlserver(_url(odbc))) is None
    assert not _refused(fenced_workspace, _url(odbc))
    assert not _refused(fenced_workspace, odbc)


# --- Security review 5 of PR 403: readings that still differed from the driver.

_FAR = "10.9.9.9"


@pytest.mark.parametrize(
    "cs",
    [
        # SQLAlchemy unquote_plus-es odbc_connect after parse_qsl: %3B is a ';'
        # for pyodbc although one decode reads it as part of APP's value.
        "mssql+pyodbc://u:p@/?odbc_connect="
        + quote_plus(f"{DRV};Server={EU};APP=x%3BFailover_Partner%3D{_FAR}"),
        # SQLAlchemy keeps '#...' inside the query; urlparse calls it a fragment.
        f"mssql+pyodbc://u:p@{EU}/db?encrypt=yes#&odbc_connect="
        + quote_plus(f"{DRV};Server={EU};Failover_Partner={_FAR}"),
        f"mssql+pyodbc://u:p@{EU}/db#?odbc_connect="
        + quote_plus(f"{DRV};Server={EU};Failover_Partner={_FAR}"),
        # An unclosed brace at the end must not make the whole string one
        # libpq value: the driver reads the pairs before it.
        f"APP=x;Server={EU};Failover_Partner={_FAR};Description={{y",
        f"Server={EU};Failover_Partner={_FAR};foo",
        # A space before '{' makes the value plain for the driver (" {a"), so
        # the keys after it are real.
        f"Server={EU};APP= {{a;Failover_Partner={_FAR};X=}}",
    ],
)
def test_review5_driver_readings_are_judged(fenced_workspace, cs):
    assert _refused(fenced_workspace, cs), cs


def test_review5_sqlalchemy_hands_pyodbc_what_the_check_judged():
    """The reading the check judges is the string SQLAlchemy gives pyodbc."""
    import sqlalchemy as sa
    from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc

    from services.residency_addressing import _query_pairs

    odbc = f"{DRV};Server={EU};APP=x%3BFailover_Partner%3D{_FAR}"
    url = "mssql+pyodbc://u:p@/?odbc_connect=" + quote_plus(odbc)
    (driver_string,), _ = MSDialect_pyodbc().create_connect_args(sa.engine.make_url(url))
    judged = {k: v for where, k, v in _query_pairs(_sqlserver(url)) if where == "odbc_connect"}
    assert judged.get("Failover_Partner") == _FAR
    assert f"Failover_Partner={_FAR}" in driver_string


@pytest.mark.parametrize(
    "cs",
    [
        f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{EU},1433;Database=db;Uid=u;Pwd=p;"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryPassword",
        "mssql+pyodbc://u:p@/?odbc_connect=" + quote_plus(
            f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{EU},1433;Database=db;Encrypt=yes"
        ),
        f"{DRV};Server={EU};Pwd={{p;a}}}}ss}};Database=db",
        f"mssql+pyodbc://u:p@{EU}/db?encrypt=yes&odbc_connect="
        + quote_plus(f"{DRV};Server={EU};Database=db"),
    ],
)
def test_review5_ordinary_strings_still_pass(fenced_workspace, cs):
    # (also: an in-region URL with an allowed query key and no fragment trick)
    assert not _refused(fenced_workspace, cs), cs
