from __future__ import annotations

import json
from pathlib import Path

from connectors.sdk import get_descriptor
from connectors.sdk.github import GitHubSource
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer

_FIXTURE_DIR = Path(__file__).parent / "connector_certification" / "fixtures" / "github"


def test_github_manifest_has_documented_stream_contracts() -> None:
    source = GitHubSource(
        {
            "access_token": "synthetic-token",
            "owner": "fixture-owner",
            "repo": "fixture-repository",
        }
    )
    repositories, issues = source.manifest.streams

    assert repositories.path == "user/repos"
    assert repositories.primary_key == ("id",)
    assert repositories.paginator.type == "link_header"
    assert repositories.request_params["per_page"] == 100
    assert issues.path == "repos/{owner}/{repo}/issues"
    assert issues.primary_key == ("id",)
    assert issues.cursor is not None
    assert issues.cursor.field == "updated_at"
    assert issues.cursor.request_param == "since"
    assert issues.request_params == {
        "state": "all",
        "sort": "updated",
        "direction": "asc",
        "per_page": 100,
    }
    assert "pull_request" in issues.json_schema["properties"]

    descriptor = get_descriptor("github")
    assert descriptor is not None
    assert descriptor.docs.startswith("https://docs.github.com/")
    assert (
        "certified as SDK source on synthetic fixtures; not yet wired into the transfer engine"
        in descriptor.description
    )


def test_github_repository_link_pagination_and_pull_request_records_are_preserved() -> None:
    repository_page_one = json.loads(
        (_FIXTURE_DIR / "repositories_page_1.json").read_text(encoding="utf-8")
    )
    repository_page_two = json.loads(
        (_FIXTURE_DIR / "repositories_page_2.json").read_text(encoding="utf-8")
    )
    issue_page_one = json.loads(
        (_FIXTURE_DIR / "issues_page_1.json").read_text(encoding="utf-8")
    )
    link = "</user/repos?per_page=100&page=2>; rel=\"next\""

    with FixtureServer() as fixture:
        fixture.add_route(
            "/user/repos",
            responses=[
                FixtureResponse(body=repository_page_one, headers={"Link": link}),
                FixtureResponse(body=repository_page_two),
            ],
        )
        fixture.add_route(
            "/repos/fixture-owner/fixture-repository/issues",
            FixtureResponse(body=issue_page_one),
        )
        source = GitHubSource(
            {
                "base_url": fixture.base_url,
                "access_token": "synthetic-token",
                "owner": "fixture-owner",
                "repo": "fixture-repository",
            }
        )

        repository_batches = list(source.read("repositories"))
        repositories = [
            record
            for batch in repository_batches
            for record in batch.records
        ]
        issue_batches = list(source.read("issues"))
        issues = [record for batch in issue_batches for record in batch.records]

        assert [record["id"] for record in repositories] == [8801, 8802]
        assert len(fixture.request_log) == 3
        first_request = fixture.request_log[0]
        assert "per_page=100" in first_request.target
        first_headers = {
            key.lower(): value for key, value in first_request.headers.items()
        }
        assert first_headers["authorization"] == "Bearer synthetic-token"
        assert first_headers["accept"] == "application/vnd.github+json"
        assert first_headers["x-github-api-version"] == "2022-11-28"

        pull_request_issue = next(record for record in issues if "pull_request" in record)
        assert pull_request_issue["pull_request"] == issue_page_one[0]["pull_request"]
