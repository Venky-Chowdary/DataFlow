from __future__ import annotations

import base64
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from connectors.sdk import get_descriptor
from connectors.sdk.jira import JiraCloudSource
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer

_FIXTURE_DIR = Path(__file__).parent / "connector_certification" / "fixtures" / "jira"


def test_jira_manifest_declares_basic_auth_cursor_and_project_pagination() -> None:
    source = JiraCloudSource(
        {
            "site": "synthetic-site",
            "email": "fixture@example.test",
            "api_token": "synthetic-token",
        }
    )
    issues, projects = source.manifest.streams

    assert source._base_url == "https://synthetic-site.atlassian.net/"
    assert base64.b64decode(source.auth.headers["Authorization"][6:]).decode() == (
        "fixture@example.test:synthetic-token"
    )
    assert issues.path == "rest/api/3/search/jql"
    assert issues.cursor is not None
    assert issues.cursor.field == "fields.updated"
    assert issues.cursor.request_param == "jql"
    assert issues.cursor.lookback_s >= 60
    assert issues.cursor.request_template == (
        'updated >= "{cursor:%Y/%m/%d %H:%M}" ORDER BY updated ASC'
    )
    assert issues.paginator.cursor_param == "nextPageToken"
    assert issues.paginator.cursor_path == "nextPageToken"
    assert issues.paginator.page_size == 100
    assert projects.path == "rest/api/3/project/search"
    assert projects.paginator.type == "offset"
    assert projects.paginator.offset_param == "startAt"
    assert projects.paginator.page_size_param == "maxResults"
    assert projects.paginator.stop_path == "isLast"

    descriptor = get_descriptor("jira")
    assert descriptor is not None
    assert descriptor.docs.startswith("https://developer.atlassian.com/")
    assert (
        "certified as SDK source on synthetic fixtures; not yet wired into the transfer engine"
        in descriptor.description
    )


def test_jira_issue_search_uses_basic_auth_and_next_page_token() -> None:
    page_one = json.loads(
        (_FIXTURE_DIR / "issues_page_1.json").read_text(encoding="utf-8")
    )
    page_two = json.loads(
        (_FIXTURE_DIR / "issues_page_2.json").read_text(encoding="utf-8")
    )

    with FixtureServer() as fixture:
        fixture.add_route(
            "/rest/api/3/search/jql",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = JiraCloudSource(
            {
                "base_url": fixture.base_url,
                "site": "fixture-site",
                "email": "fixture@example.test",
                "api_token": "synthetic-token",
            }
        )
        batches = list(source.read("issues"))

    records = [record for batch in batches for record in batch.records]
    assert [record["id"] for record in records] == ["1001", "1002", "1003", "1004"]
    assert len(fixture.request_log) == 2
    first_query = parse_qs(urlsplit(fixture.request_log[0].target).query)
    second_query = parse_qs(urlsplit(fixture.request_log[1].target).query)
    assert first_query["jql"] == ["ORDER BY updated ASC"]
    assert first_query["maxResults"] == ["100"]
    assert second_query["nextPageToken"] == ["synthetic-page-two"]
    first_headers = {
        key.lower(): value for key, value in fixture.request_log[0].headers.items()
    }
    decoded_auth = base64.b64decode(first_headers["authorization"][6:]).decode()
    assert decoded_auth == "fixture@example.test:synthetic-token"


def test_jira_projects_follow_is_last_when_pages_are_short() -> None:
    page_one = json.loads(
        (_FIXTURE_DIR / "projects_page_1.json").read_text(encoding="utf-8")
    )
    page_two = json.loads(
        (_FIXTURE_DIR / "projects_page_2.json").read_text(encoding="utf-8")
    )

    with FixtureServer() as fixture:
        fixture.add_route(
            "/rest/api/3/project/search",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = JiraCloudSource(
            {
                "base_url": fixture.base_url,
                "site": "fixture-site",
                "email": "fixture@example.test",
                "api_token": "synthetic-token",
            }
        )
        batches = list(source.read("projects"))

    records = [record for batch in batches for record in batch.records]
    assert [record["id"] for record in records] == ["2001", "2002"]
    assert len(fixture.request_log) == 2
    assert [
        parse_qs(urlsplit(request.target).query)["startAt"]
        for request in fixture.request_log
    ] == [["0"], ["1"]]
    assert all(
        parse_qs(urlsplit(request.target).query)["maxResults"] == ["100"]
        for request in fixture.request_log
    )
