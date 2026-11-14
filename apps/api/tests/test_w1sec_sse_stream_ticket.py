"""SEC-14: the job stream opens with a one-time ticket, not the 24-hour bearer in the URL.

``?token=`` was redacted in the app log, but nginx's access log, the edge and
browser history kept the session token. The web client now asks
``POST /connectors/jobs/{id}/stream-ticket`` and opens the stream with
``?ticket=``: one use, 60 seconds, this URL only. nginx logs no query string.
"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tests.tenancy_world import tenancy_world  # noqa: F401


def test_a_ticket_opens_its_own_stream_once(tenancy_world):  # noqa: F811
    job = tenancy_world.job(tenancy_world.workspace_a)
    r = tenancy_world.a.post(f"/api/v1/connectors/jobs/{job}/stream-ticket")
    assert r.status_code == 200 and r.json()["ticket"] and r.json()["expires_in"] == 60, r.text
    ticket = r.json()["ticket"]
    anon = TestClient(tenancy_world.app)
    with anon.stream("GET", f"/api/v1/connectors/jobs/{job}/stream", params={"ticket": ticket}) as first:
        assert first.status_code == 200
    again = anon.get(f"/api/v1/connectors/jobs/{job}/stream", params={"ticket": ticket})
    assert again.status_code == 401, again.text[:200]


def test_a_ticket_is_bound_to_its_job_and_its_workspace(tenancy_world):  # noqa: F811
    job_a = tenancy_world.job(tenancy_world.workspace_a)
    job_b = tenancy_world.job(tenancy_world.workspace_b)
    assert tenancy_world.b.post(f"/api/v1/connectors/jobs/{job_a}/stream-ticket").status_code == 404
    ticket = tenancy_world.b.post(f"/api/v1/connectors/jobs/{job_b}/stream-ticket").json()["ticket"]
    anon = TestClient(tenancy_world.app)
    other = anon.get(f"/api/v1/connectors/jobs/{job_a}/stream", params={"ticket": ticket})
    assert other.status_code == 401, other.text[:200]


def test_the_web_client_and_nginx_keep_the_bearer_out_of_urls():
    root = Path(__file__).resolve().parents[3]
    api_ts = (root / "apps/web/src/lib/api.ts").read_text(encoding="utf-8")
    assert "stream?token=" not in api_ts and "-ticket" in api_ts
    nginx = (root / "deploy/nginx.conf.template").read_text(encoding="utf-8")
    assert "log_format dw_noquery" in nginx and "$request_uri" not in nginx.split("log_format dw_noquery")[1].split(";")[0]
    assert "access_log /var/log/nginx/access.log dw_noquery;" in nginx
