"""Security review round 2 (H1): a URL typed into ``host`` is judged as the driver dials it.

With ``host = "postgresql://u:p@x.eu-west-1.rds.amazonaws.com/db@52.1.2.3/db"``
the residency judge (``urlparse``: the host ends at the first ``/``) saw an
eu-west-1 RDS host, while the driver's resolver (``resolve_sql_endpoint`` ->
``parse_url_authority``: the last ``@``, across the path) dialled
``52.1.2.3``. ``/transfer/execute`` answered 200 and the engine's run-time
guard allowed the job. MySQL behaved the same.

Now a URL is read by both parsers and is unreadable (refused) when they
disagree or when an ``@`` appears after the authority; for the PostgreSQL and
MySQL families the host the driver's own resolver dials is judged as well.
"""

from __future__ import annotations

import pytest
from tenancy_world import tenancy_world  # noqa: F401

PATH_AT = "{scheme}://u:p@x.eu-west-1.rds.amazonaws.com/db@52.1.2.3/db"
PATH_AT_NO_USER = "{scheme}://x.eu-west-1.rds.amazonaws.com/db@52.1.2.3/db"
HONEST = "{scheme}://u:p@x.eu-west-1.rds.amazonaws.com:5432/db"


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


def _verdict(ws, dst):
    from services import data_residency as dr
    from src.transfer.models import EndpointConfig

    return dr.residency_refusal(ws, EndpointConfig.from_dict("database", dst))


def _execute(world, dst):
    src = {"kind": "database", "format": "sqlite", "database": ":memory:", "table": "t"}
    body = {"source": src, "destination": dst, "mappings": [{"source": "id", "target": "id"}], "async_mode": True}
    return world.a.post("/api/v1/transfer/execute", json=body), body


@pytest.mark.parametrize("fmt,scheme", [("postgresql", "postgresql"), ("mysql", "mysql")])
@pytest.mark.parametrize("template", [PATH_AT, PATH_AT_NO_USER])
def test_http_url_in_host_that_dials_elsewhere_is_refused(fenced, fmt, scheme, template):
    dst = {"kind": "database", "format": fmt, "host": template.format(scheme=scheme), "database": "db", "table": "t"}
    r, body = _execute(fenced, dst)
    assert r.status_code == 403, r.text
    assert "residency" in r.text.lower()
    assert fenced.queued == []
    # The engine's run-time guard reaches the same answer for a job created another way.
    from src.transfer.job_scope import start_refusal
    from src.transfer.models import transfer_request_from_dict

    assert start_refusal(transfer_request_from_dict({**body, "workspace_id": fenced.workspace_a}))


@pytest.mark.parametrize("fmt,scheme", [("postgresql", "postgresql"), ("mysql", "mysql")])
def test_http_honest_url_in_host_is_still_allowed(fenced, fmt, scheme):
    dst = {"kind": "database", "format": fmt, "host": HONEST.format(scheme=scheme), "database": "db", "table": "t"}
    r, _ = _execute(fenced, dst)
    assert r.status_code == 200, r.text


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://u:p@x.eu-west-1.rds.amazonaws.com/db@52.1.2.3/db",
        "postgresql://x.eu-west-1.rds.amazonaws.com/db?options=a@52.1.2.3",
        "postgresql://x.eu-west-1.rds.amazonaws.com/db#@52.1.2.3",
        "mysql://u:p@x.eu-west-1.rds.amazonaws.com:3306/db@52.1.2.3:3306/db",
    ],
)
def test_url_hosts_refuses_an_at_sign_after_the_authority(url):
    from services.data_residency import _url_hosts

    assert _url_hosts(url) is None


def test_url_hosts_takes_both_parsers(fenced):
    from services.data_residency import _url_hosts

    assert _url_hosts("postgresql://u:p%40ss@x.eu-west-1.rds.amazonaws.com/db") == ["x.eu-west-1.rds.amazonaws.com"]
    # A raw '@' inside the password: urlparse (first '@') and the driver (last '@') agree on the host.
    assert _url_hosts("postgresql://u:p@ss@x.eu-west-1.rds.amazonaws.com/db") == ["x.eu-west-1.rds.amazonaws.com"]


@pytest.mark.parametrize("fmt,scheme", [("postgresql", "postgresql"), ("mysql", "mysql")])
def test_function_level_judge_matches_the_driver(fenced, fmt, scheme):
    from connectors.sql_dsn import resolve_sql_endpoint

    host = PATH_AT.format(scheme=scheme)
    dialled = resolve_sql_endpoint(
        family=fmt, host=host, port=0, database="db", username="", password="",
        connection_string="", default_port=5432,
    )["host"]
    assert dialled == "52.1.2.3"
    assert _verdict(fenced.workspace_a, {"format": fmt, "host": host, "database": "db", "table": "t"})
