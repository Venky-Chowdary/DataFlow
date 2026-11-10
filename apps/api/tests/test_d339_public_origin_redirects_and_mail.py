"""D339: no user-visible URL names the API service's own (internal) host.

Before the fix:
- FastAPI's trailing-slash 307 built an absolute ``Location`` from the
  request's own Host and scheme — behind the edge that is the internal API
  host over plain http.
- ``platform_config.public_url()`` / ``web_url()`` fell back to the API
  service's ``RAILWAY_PUBLIC_DOMAIN``, and ``notification_service
  .build_job_payload`` always appended "Retry/resume: <api host>/api/v1/...
  /resume" (a bearer-only API URL), even on success, and printed an empty
  "Job ID:" line for a run that never created a job.

``internal.example`` stands in for the internal host; it is never a real one.
"""

from __future__ import annotations

import pytest

from r8_ent_harness import ent  # noqa: F401

INTERNAL = "internal.example"


def _no_internal(location: str) -> None:
    assert INTERNAL not in location
    assert not location.startswith("http://")


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/api/v1/schedules", "/api/v1/schedules/"),  # route declared with a slash
        ("/api/v1/schedules?limit=5", "/api/v1/schedules/?limit=5"),
        ("/api/v1/auth/me/", "/api/v1/auth/me"),  # route declared without one
    ],
)
def test_trailing_slash_redirect_never_names_the_request_host(ent, path, expected):
    res = ent.admin.get(path, headers={"host": INTERNAL}, follow_redirects=False)
    assert res.status_code == 307, res.text
    location = res.headers["location"]
    _no_internal(location)
    # Relative: the client resolves it against the public URL it used
    # (https://<public origin>/...), so it stays on that origin and scheme.
    assert location == expected
    followed = ent.admin.get(location, headers={"host": INTERNAL})
    assert followed.status_code == 200, followed.text


def test_a_redirect_to_another_host_is_left_alone():
    from src.middleware.own_host_redirect_middleware import relative_own_host_location

    assert relative_own_host_location("https://idp.example/sso?x=1", INTERNAL) is None
    assert relative_own_host_location("/already/relative", INTERNAL) is None
    assert relative_own_host_location(f"http://{INTERNAL}/a/b/?q=1", INTERNAL) == "/a/b/?q=1"
    assert relative_own_host_location(f"http://{INTERNAL.upper()}:80/a", INTERNAL + ":80") == "/a"


@pytest.fixture
def railway_api_service(monkeypatch):
    """The API service on Railway: its own public domain is the internal host."""
    for key in ("DATAWRAP_PUBLIC_URL", "DATAFLOW_PUBLIC_URL", "DATAWRAP_PUBLIC_API_ORIGIN",
                "DATAFLOW_PUBLIC_API_ORIGIN", "DATAWRAP_WEB_URL", "DATAFLOW_WEB_URL",
                "DATAWRAP_WEB_DOMAIN", "DATAFLOW_WEB_DOMAIN"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", INTERNAL)


def test_public_url_uses_the_configured_public_origin_not_the_service_domain(
    railway_api_service, monkeypatch
):
    from services.platform_config import public_url, web_url

    assert public_url() == ""
    assert web_url() == ""
    monkeypatch.setenv("DATAWRAP_WEB_DOMAIN", "app.example")
    assert public_url() == "https://app.example"
    assert web_url() == "https://app.example"
    monkeypatch.setenv("DATAWRAP_PUBLIC_API_ORIGIN", "https://api.app.example")
    assert public_url() == "https://api.app.example"
    monkeypatch.setenv("DATAWRAP_PUBLIC_URL", "https://explicit.example/")
    assert public_url() == "https://explicit.example"


def _payload(status: str, job_id: str = "job-123", **kw):
    from services.notification_service import build_job_payload

    return build_job_payload(
        job_id=job_id,
        status=status,
        source="postgresql",
        destination="snowflake",
        records_transferred=10,
        rejected_rows=0,
        error="" if status == "completed" else "boom",
        retry_url=f"/api/v1/connectors/jobs/{job_id}/resume",
        base_url=f"https://{INTERNAL}",
        web_url="https://app.example",
        **kw,
    )


def test_success_mail_has_job_details_and_no_retry_or_api_url():
    payload = _payload("completed")
    assert "Job details: https://app.example/jobs/job-123" in payload["text"]
    assert "Retry" not in payload["text"]
    assert "/api/v1/" not in payload["text"] and INTERNAL not in payload["text"]
    assert payload["retry_url"] == ""
    assert payload["job_url"] == "https://app.example/jobs/job-123"


@pytest.mark.parametrize("status", ["failed", "completed_with_quarantine", "partial"])
def test_failure_mail_points_retry_at_the_job_page_not_a_bearer_only_api_url(status):
    payload = _payload(status)
    assert "/api/v1/" not in payload["text"] and INTERNAL not in payload["text"]
    assert payload["retry_url"] == "https://app.example/jobs/job-123"
    assert "Retry/resume: https://app.example/jobs/job-123" in payload["text"]


def test_a_run_without_a_job_has_no_empty_job_id_line_or_job_link():
    payload = _payload("failed", job_id="")
    lines = payload["text"].splitlines()
    assert not any(line.startswith("Job ID:") for line in lines)
    assert "/jobs/" not in payload["text"] and payload["job_url"] == ""
    assert "/api/v1/" not in payload["text"]


def test_engine_failure_notification_carries_no_api_host(railway_api_service, monkeypatch):
    """End to end through the real caller: background._notify_failure."""
    from types import SimpleNamespace

    from services import notification_service
    from src.transfer import background

    monkeypatch.setenv("DATAWRAP_WEB_DOMAIN", "app.example")
    sent: list[dict] = []
    monkeypatch.setattr(notification_service, "notify_workspace", lambda ws, p: sent.append(p) or [])
    monkeypatch.setattr(notification_service, "log_job_notifications", lambda *a, **k: True)
    req = SimpleNamespace(source=SimpleNamespace(kind="postgresql"),
                          destination=SimpleNamespace(kind="snowflake"), workspace_id="ws1")
    background._notify_failure(req, "job-9", "boom")
    (payload,) = sent
    assert INTERNAL not in payload["text"] and "/api/v1/" not in payload["text"]
    assert "https://app.example/jobs/job-9" in payload["text"]


def test_mcp_oauth_audience_keeps_its_deployed_resolution(monkeypatch):
    """D339 moved only user-facing links. The MCP OAuth audience is a value
    clients and the identity provider already hold: it stays on the API
    service's own host unless DATAWRAP_MCP_OAUTH_AUDIENCE says otherwise."""
    from services import mcp_oauth, platform_config

    for name in ("DATAWRAP_PUBLIC_URL", "DATAFLOW_PUBLIC_URL", "DATAWRAP_MCP_OAUTH_AUDIENCE",
                 "DATAFLOW_MCP_OAUTH_AUDIENCE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", "api-internal.example")
    monkeypatch.setenv("DATAWRAP_WEB_URL", "https://app.example")
    assert platform_config.api_service_url() == "https://api-internal.example"
    assert "api-internal.example" not in platform_config.web_url()
    assert mcp_oauth.get_mcp_oauth_config().audience == "https://api-internal.example/api/v1/mcp"
    monkeypatch.setenv("DATAWRAP_MCP_OAUTH_AUDIENCE", "https://mcp.example/api/v1/mcp")
    assert mcp_oauth.get_mcp_oauth_config().audience == "https://mcp.example/api/v1/mcp"
