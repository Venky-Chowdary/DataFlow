from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from connectors.sdk import SingerTapBridge
from connectors.sdk.declarative.connector import DeclarativeSource
from connectors.sdk.github import GitHubSource
from connectors.sdk.http_declarative import DeclarativeHttpConnector
from connectors.sdk.hubspot_cdk import HubSpotCDKConnector
from connectors.sdk.jira import JiraCloudSource
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer
from tests.connector_certification.harness import CertificationCase

_SECRET = "PLANTED_CERTIFICATION_SECRET"
_CURSOR_START = "2026-01-01T00:00:00Z"
_CURSOR_SECOND = "2026-01-02T00:00:00Z"
_CURSOR_THIRD = "2026-01-03T00:00:00Z"
_CURSOR_FINAL = "2026-01-04T00:00:00Z"


def _tap_source(tmp_path: Path) -> Path:
    script = tmp_path / "synthetic_certification_tap.py"
    script.write_text(
        """\
import argparse
import json
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--discover", action="store_true")
parser.add_argument("--check", action="store_true")
parser.add_argument("--config")
parser.add_argument("--state")
args = parser.parse_args()
config = json.load(open(args.config, encoding="utf-8")) if args.config else {}
phase = config.get("phase", "")
rows = config.get("records", [])
cursor_field = config.get("cursor_field", "updated_at")

def emit(message):
    print(json.dumps(message), flush=True)

if args.discover:
    emit({
        "type": "SCHEMA",
        "stream": "items",
        "schema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "updated_at": {"type": "string"},
                "name": {"type": "string"}
            }
        },
        "key_properties": ["id"],
        "metadata": [{
            "breadcrumb": [],
            "metadata": {
                "replication-method": "INCREMENTAL",
                "replication-key": "updated_at"
            }
        }]
    })
    raise SystemExit(0)

if phase == "auth":
    print("authentication rejected: " + config.get("api_key", ""), file=sys.stderr)
    raise SystemExit(1)

if args.check:
    raise SystemExit(0)

state = {}
if args.state:
    with open(args.state, encoding="utf-8") as state_file:
        state = json.load(state_file)
elif phase in {"incremental", "resume"}:
    print("required --state was not passed", file=sys.stderr)
    raise SystemExit(8)

cursor = state.get(cursor_field)
selected = [
    row for row in rows
    if cursor is None or str(row.get(cursor_field, "")) >= str(cursor)
]

if phase == "fault":
    selected = selected[:2]
    for row in selected:
        emit({"type": "RECORD", "stream": "items", "record": row})
    if selected:
        emit({"type": "STATE", "value": {cursor_field: selected[-1][cursor_field]}})
    print("synthetic failure after page one", file=sys.stderr)
    raise SystemExit(7)

for row in selected:
    emit({"type": "RECORD", "stream": "items", "record": row})
if selected:
    emit({"type": "STATE", "value": {cursor_field: selected[-1][cursor_field]}})
""",
        encoding="utf-8",
    )
    return script


def build_certification_cases(tmp_path: Path) -> dict[str, CertificationCase]:
    records: tuple[Mapping[str, Any], ...] = (
        {"id": "1", "updated_at": _CURSOR_START, "name": "one"},
        {"id": "2", "updated_at": _CURSOR_SECOND, "name": "two"},
        {"id": "3", "updated_at": _CURSOR_THIRD, "name": "three"},
        {"id": "4", "updated_at": _CURSOR_FINAL, "name": "four"},
    )

    def mutation_for(cursor_field: str):
        def mutate(
            original: Sequence[Mapping[str, Any]],
        ) -> tuple[list[dict[str, Any]], set[str], set[str]]:
            changed = [dict(row) for row in original]
            changed[1][cursor_field] = "2026-01-05T00:00:00Z"
            changed[2][cursor_field] = "2026-01-06T00:00:00Z"
            added = dict(changed[-1])
            added["id"] = "5"
            added[cursor_field] = "2026-01-07T00:00:00Z"
            changed.append(added)
            return changed, {"2", "3", "5"}, {"4"}

        return mutate

    def body(rows: Sequence[Mapping[str, Any]], next_page: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "data": [dict(row) for row in rows],
            "results": [dict(row) for row in rows],
            "items": [dict(row) for row in rows],
        }
        if next_page is not None:
            payload["next"] = next_page
        return payload

    def declarative_routes(
        fixture: FixtureServer,
        phase: str,
        fixture_records: Sequence[Mapping[str, Any]],
    ) -> None:
        rows = [dict(row) for row in fixture_records]
        first, second = rows[:2], rows[2:]
        if phase == "auth":
            fixture.add_route("/items", FixtureResponse(status=401, body={"message": "unauthorized"}))
        elif phase == "check":
            fixture.add_route("/items", FixtureResponse(body=body(first[:1])))
        elif phase == "incremental":
            fixture.add_route("/items", FixtureResponse(body=body(rows[1:])))
        elif phase == "fault":
            fixture.add_route(
                "/items",
                responses=[
                    FixtureResponse(body=body(first, "page-2")),
                    *[
                        FixtureResponse(status=500, body={"message": "synthetic fault"})
                        for _ in range(4)
                    ],
                ],
            )
        elif phase == "resume":
            fixture.add_route("/items", FixtureResponse(body=body(second)))
        elif phase == "rate_limit":
            fixture.add_route(
                "/items",
                responses=[
                    FixtureResponse(
                        status=429,
                        body={"message": "rate limited"},
                        headers={"Retry-After": "2"},
                    ),
                    FixtureResponse(body=body(first, "page-2")),
                    FixtureResponse(body=body(second)),
                ],
            )
        else:
            fixture.add_route(
                "/items",
                responses=[
                    FixtureResponse(body=body(first, "page-2")),
                    FixtureResponse(body=body(second)),
                ],
            )

    def declarative_factory(
        base_url: str,
        _phase: str,
        _fixture_records: Sequence[Mapping[str, Any]],
        _sleep: Any = None,
    ) -> DeclarativeSource:
        return DeclarativeSource(
            {
                "manifest": {
                    "name": "acme",
                    "base_url": base_url or "http://fixture.invalid",
                    "auth": {
                        "type": "api_key",
                        "location": "header",
                        "name": "X-API-Key",
                    },
                    "streams": [
                        {
                            "name": "items",
                            "path": "items",
                            "records_path": "data",
                            "primary_key": ["id"],
                            "cursor": {
                                "field": "updated_at",
                                "request_param": "updated_since",
                                "format": "iso8601",
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_param": "after",
                                "cursor_path": "next",
                                "page_size": 2,
                            },
                            "json_schema": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "string"},
                                    "updated_at": {"type": "string"},
                                    "name": {"type": "string"},
                                },
                            },
                        }
                    ],
                    "defaults": {
                        "page_size": 2,
                        "retry": {
                            "max_attempts": 3,
                            "base_delay": 0,
                            "max_delay": 0,
                        },
                    },
                },
                "credentials": {"api_key": _SECRET},
            }
        )

    def assert_declarative_cursor(
        fixture: FixtureServer, state: Mapping[str, Any]
    ) -> None:
        query = parse_qs(urlsplit(fixture.request_log[0].target).query)
        cursor = state.get("cursor")
        if not cursor:
            cursor = state.get("items", {}).get("cursor")
        if query.get("updated_since") != [cursor]:
            raise AssertionError(f"incremental request omitted saved cursor {cursor!r}")

    declarative_case = CertificationCase(
        connector_id="declarative_source",
        connector_factory=declarative_factory,
        fixture_routes=declarative_routes,
        stream="items",
        mutate_fixture=mutation_for("updated_at"),
        fixture_records=records,
        primary_key=("id",),
        cursor_field="updated_at",
        secret=_SECRET,
        assert_incremental_cursor=assert_declarative_cursor,
    )

    def shim_routes(
        fixture: FixtureServer,
        phase: str,
        fixture_records: Sequence[Mapping[str, Any]],
    ) -> None:
        rows = [dict(row) for row in fixture_records]
        if phase == "auth":
            fixture.add_route("/items", FixtureResponse(status=401, body={"message": "unauthorized"}))
        elif phase == "check":
            fixture.add_route("/items", FixtureResponse(body={"data": rows[:1]}))
        elif phase == "incremental":
            fixture.add_route("/items", FixtureResponse(body={"data": rows[1:]}))
        elif phase == "rate_limit":
            fixture.add_route(
                "/items",
                responses=[
                    FixtureResponse(
                        status=429,
                        body={"message": "rate limited"},
                        headers={"Retry-After": "2"},
                    ),
                    FixtureResponse(body={"data": rows}),
                ],
            )
        else:
            fixture.add_route("/items", FixtureResponse(body={"data": rows}))

    def shim_factory(
        base_url: str,
        _phase: str,
        _fixture_records: Sequence[Mapping[str, Any]],
        _sleep: Any = None,
    ) -> DeclarativeHttpConnector:
        return DeclarativeHttpConnector(
            {
                "api_key": _SECRET,
                "spec": {
                    "name": "acme-legacy",
                    "base_url": base_url or "http://fixture.invalid",
                    "streams": [
                        {
                            "name": "items",
                            "path": "items",
                            "records_path": "data",
                            "primary_key": ["id"],
                            "cursor_field": "updated_at",
                            "cursor_param": "after",
                            "page_size": 100,
                            "properties": {
                                "id": "string",
                                "updated_at": "string",
                                "name": "string",
                            },
                        }
                    ],
                },
            }
        )

    def assert_shim_cursor(fixture: FixtureServer, state: Mapping[str, Any]) -> None:
        query = parse_qs(urlsplit(fixture.request_log[0].target).query)
        cursor = state.get("items", {}).get("updated_at")
        if query.get("after") != [str(cursor)]:
            raise AssertionError(f"legacy request omitted saved cursor {cursor!r}")

    shim_case = CertificationCase(
        connector_id="declarative_http",
        connector_factory=shim_factory,
        fixture_routes=shim_routes,
        stream="items",
        mutate_fixture=mutation_for("updated_at"),
        fixture_records=records,
        primary_key=("id",),
        cursor_field="updated_at",
        secret=_SECRET,
        unsupported_steps=frozenset({"resume_after_failure"}),
        assert_incremental_cursor=assert_shim_cursor,
    )

    def hubspot_rows(
        fixture_records: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        return [
            {
                "id": str(row["id"]),
                "properties": {
                    "email": f"{row['id']}@example.test",
                    "firstname": str(row.get("firstname", "")),
                    "lastname": "fixture",
                    "lastmodifieddate": row["lastmodifieddate"],
                },
            }
            for row in fixture_records
        ]

    def hubspot_routes(
        fixture: FixtureServer,
        phase: str,
        fixture_records: Sequence[Mapping[str, Any]],
    ) -> None:
        path = "/crm/v3/objects/contacts"
        rows = hubspot_rows(fixture_records)
        page_one = {"results": rows[:2], "paging": {"next": {"after": "2"}}}
        page_two = {"results": rows[2:]}
        if phase == "auth":
            fixture.add_route(path, FixtureResponse(status=401, body={"message": "unauthorized"}))
        elif phase == "check":
            fixture.add_route(path, FixtureResponse(body={"results": rows[:1]}))
        elif phase == "incremental":
            fixture.add_route(
                f"{path}/search",
                FixtureResponse(body={"results": rows[1:]}),
                method="POST",
            )
        elif phase == "fault":
            fixture.add_route(
                path,
                responses=[
                    FixtureResponse(body=page_one),
                    *[
                        FixtureResponse(status=500, body={"message": "synthetic fault"})
                        for _ in range(4)
                    ],
                ],
            )
        elif phase == "resume":
            fixture.add_route(path, FixtureResponse(body=page_two))
        elif phase == "rate_limit":
            fixture.add_route(
                path,
                responses=[
                    FixtureResponse(
                        status=429,
                        body={"message": "rate limited"},
                        headers={"Retry-After": "2"},
                    ),
                    FixtureResponse(body=page_one),
                    FixtureResponse(body=page_two),
                ],
            )
        else:
            fixture.add_route(path, responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)])

    def hubspot_factory(
        base_url: str,
        _phase: str,
        _fixture_records: Sequence[Mapping[str, Any]],
        sleep: Any = None,
    ) -> HubSpotCDKConnector:
        return HubSpotCDKConnector(
            {"api_key": _SECRET, "host": base_url or "http://fixture.invalid"},
            sleep=sleep,
        )

    def assert_hubspot_cursor(fixture: FixtureServer, state: Mapping[str, Any]) -> None:
        request = next(
            record for record in fixture.request_log if record.method == "POST"
        )
        body = json.loads(request.body)
        cursor = state.get("contacts", {}).get("cursor")
        expected = int(
            datetime.fromisoformat(str(cursor).replace("Z", "+00:00"))
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1000
        )
        actual = body["filterGroups"][0]["filters"][0]
        if actual.get("propertyName") != "lastmodifieddate" or actual.get("value") != str(expected):
            raise AssertionError("HubSpot incremental search omitted the saved cursor")

    def mutate_hubspot(
        original: Sequence[Mapping[str, Any]],
    ) -> tuple[list[dict[str, Any]], set[str], set[str]]:
        rows = [dict(row) for row in original]
        for index in (1, 2):
            rows[index]["lastmodifieddate"] = "2026-01-0" + str(index + 4) + "T00:00:00Z"
        added = dict(rows[-1])
        added["id"] = "5"
        added["lastmodifieddate"] = "2026-01-07T00:00:00Z"
        rows.append(added)
        return rows, {"2", "3", "5"}, {"4"}

    hubspot_fixture_records = tuple(
        {
            "id": str(index),
            "lastmodifieddate": cursor,
            "email": f"{index}@example.test",
            "firstname": name,
            "lastname": "fixture",
        }
        for index, (cursor, name) in enumerate(
            (
                (_CURSOR_START, "one"),
                (_CURSOR_SECOND, "two"),
                (_CURSOR_THIRD, "three"),
                (_CURSOR_FINAL, "four"),
            ),
            start=1,
        )
    )
    hubspot_case = CertificationCase(
        connector_id="hubspot_cdk",
        connector_factory=hubspot_factory,
        fixture_routes=hubspot_routes,
        stream="contacts",
        mutate_fixture=mutate_hubspot,
        fixture_records=hubspot_fixture_records,
        primary_key=("id",),
        cursor_field="lastmodifieddate",
        secret=_SECRET,
        assert_incremental_cursor=assert_hubspot_cursor,
    )

    github_fixture_dir = Path(__file__).parent / "fixtures" / "github"
    github_issue_pages = [
        json.loads((github_fixture_dir / filename).read_text(encoding="utf-8"))
        for filename in ("issues_page_1.json", "issues_page_2.json")
    ]
    github_repository_pages = [
        json.loads((github_fixture_dir / filename).read_text(encoding="utf-8"))
        for filename in ("repositories_page_1.json", "repositories_page_2.json")
    ]
    github_records = tuple(
        dict(record) for page in github_issue_pages for record in page
    )
    github_issues_path = "/repos/fixture-owner/fixture-repository/issues"
    github_next_page = (
        f"<{github_issues_path}?state=all&sort=updated&direction=asc"
        "&per_page=100&page=2>; rel=\"next\""
    )

    def github_routes(
        fixture: FixtureServer,
        phase: str,
        fixture_records: Sequence[Mapping[str, Any]],
    ) -> None:
        page_one = [dict(row) for row in fixture_records[:2]]
        page_two = [dict(row) for row in fixture_records[2:]]
        if phase == "auth":
            fixture.add_route(
                "/user/repos",
                FixtureResponse(status=401, body={"message": "unauthorized"}),
            )
            fixture.add_route(
                github_issues_path,
                FixtureResponse(status=401, body={"message": "unauthorized"}),
            )
        elif phase == "check":
            fixture.add_route(
                "/user/repos",
                FixtureResponse(body=github_repository_pages[0]),
            )
        elif phase == "incremental":
            fixture.add_route(
                github_issues_path,
                FixtureResponse(body=[dict(row) for row in fixture_records[1:]]),
            )
        elif phase == "fault":
            fixture.add_route(
                github_issues_path,
                responses=[
                    FixtureResponse(
                        body=page_one,
                        headers={"Link": github_next_page},
                    ),
                    FixtureResponse(status=500, body={"message": "synthetic fault"}),
                    FixtureResponse(status=500, body={"message": "synthetic fault"}),
                ],
            )
        elif phase == "resume":
            fixture.add_route(
                github_issues_path,
                FixtureResponse(body=page_two),
            )
        elif phase == "rate_limit":
            fixture.add_route(
                github_issues_path,
                responses=[
                    FixtureResponse(
                        status=429,
                        body={"message": "rate limited"},
                        headers={"Retry-After": "2"},
                    ),
                    FixtureResponse(
                        body=page_one,
                        headers={"Link": github_next_page},
                    ),
                    FixtureResponse(body=page_two),
                ],
            )
        else:
            fixture.add_route(
                github_issues_path,
                responses=[
                    FixtureResponse(
                        body=page_one,
                        headers={"Link": github_next_page},
                    ),
                    FixtureResponse(body=page_two),
                ],
            )

    def github_factory(
        base_url: str,
        _phase: str,
        _fixture_records: Sequence[Mapping[str, Any]],
        _sleep: Any = None,
    ) -> GitHubSource:
        return GitHubSource(
            {
                "access_token": _SECRET,
                "owner": "fixture-owner",
                "repo": "fixture-repository",
                "base_url": base_url or "https://api.github.com",
            }
        )

    def mutate_github(
        original: Sequence[Mapping[str, Any]],
    ) -> tuple[list[dict[str, Any]], set[str], set[str]]:
        changed = [dict(row) for row in original]
        changed[1]["updated_at"] = "2026-01-05T00:00:00Z"
        changed[2]["updated_at"] = "2026-01-06T00:00:00Z"
        added = dict(changed[-1])
        added.update(
            {
                "id": 9005,
                "number": 5,
                "title": "Synthetic issue added",
                "updated_at": "2026-01-07T00:00:00Z",
                "created_at": "2026-01-07T00:00:00Z",
            }
        )
        changed.append(added)
        return changed, {"9002", "9003", "9005"}, {"9004"}

    def assert_github_cursor(
        fixture: FixtureServer,
        state: Mapping[str, Any],
    ) -> None:
        query = parse_qs(urlsplit(fixture.request_log[0].target).query)
        stream_state = state.get("issues", {})
        cursor = stream_state.get("cursor") if isinstance(stream_state, Mapping) else None
        if cursor is None:
            cursor = state.get("cursor")
        if query.get("since") != [str(cursor)]:
            raise AssertionError(f"GitHub issues request omitted saved cursor {cursor!r}")

    github_case = CertificationCase(
        connector_id="github",
        connector_factory=github_factory,
        fixture_routes=github_routes,
        stream="issues",
        mutate_fixture=mutate_github,
        fixture_records=github_records,
        primary_key=("id",),
        cursor_field="updated_at",
        secret=_SECRET,
        assert_incremental_cursor=assert_github_cursor,
    )

    jira_fixture_dir = Path(__file__).parent / "fixtures" / "jira"
    jira_issue_pages = [
        json.loads((jira_fixture_dir / filename).read_text(encoding="utf-8"))
        for filename in ("issues_page_1.json", "issues_page_2.json")
    ]
    jira_records = tuple(
        dict(record) for page in jira_issue_pages for record in page["issues"]
    )
    jira_issues_path = "/rest/api/3/search/jql"

    def jira_routes(
        fixture: FixtureServer,
        phase: str,
        fixture_records: Sequence[Mapping[str, Any]],
    ) -> None:
        rows = [deepcopy(dict(row)) for row in fixture_records]
        if phase == "auth":
            fixture.add_route(
                jira_issues_path,
                FixtureResponse(status=401, body={"errorMessages": ["unauthorized"]}),
            )
        elif phase == "check":
            fixture.add_route(
                jira_issues_path,
                FixtureResponse(body={"issues": rows[:2]}),
            )
        elif phase == "incremental":
            fixture.add_route(
                jira_issues_path,
                FixtureResponse(body={"issues": rows[1:]}),
            )
        elif phase == "fault":
            fixture.add_route(
                jira_issues_path,
                responses=[
                    FixtureResponse(
                        body={
                            "issues": rows[:2],
                            "nextPageToken": "synthetic-page-two",
                        }
                    ),
                    *[
                        FixtureResponse(
                            status=500,
                            body={"errorMessages": ["synthetic fault"]},
                        )
                        for _ in range(4)
                    ],
                ],
            )
        elif phase == "resume":
            fixture.add_route(
                jira_issues_path,
                FixtureResponse(body={"issues": rows[2:]}),
            )
        elif phase == "rate_limit":
            fixture.add_route(
                jira_issues_path,
                responses=[
                    FixtureResponse(
                        status=429,
                        body={"errorMessages": ["rate limited"]},
                        headers={"Retry-After": "2"},
                    ),
                    FixtureResponse(
                        body={
                            "issues": rows[:2],
                            "nextPageToken": "synthetic-page-two",
                        }
                    ),
                    FixtureResponse(body={"issues": rows[2:]}),
                ],
            )
        else:
            fixture.add_route(
                jira_issues_path,
                responses=[
                    FixtureResponse(
                        body={
                            "issues": rows[:2],
                            "nextPageToken": "synthetic-page-two",
                        }
                    ),
                    FixtureResponse(body={"issues": rows[2:]}),
                ],
            )

    def jira_factory(
        base_url: str,
        _phase: str,
        _fixture_records: Sequence[Mapping[str, Any]],
        _sleep: Any = None,
    ) -> JiraCloudSource:
        config: dict[str, Any] = {
            "site": "fixture-site",
            "email": "fixture@example.test",
            "api_token": _SECRET,
        }
        if base_url:
            config["base_url"] = base_url
        return JiraCloudSource(config)

    def mutate_jira(
        original: Sequence[Mapping[str, Any]],
    ) -> tuple[list[dict[str, Any]], set[str], set[str]]:
        changed = deepcopy([dict(row) for row in original])
        changed[1]["fields"]["updated"] = "2026-01-05T00:00:00.000+0000"
        changed[2]["fields"]["updated"] = "2026-01-06T00:00:00.000+0000"
        added = deepcopy(changed[-1])
        added.update({"id": "1005", "key": "FIX-5"})
        added["fields"]["updated"] = "2026-01-07T00:00:00.000+0000"
        changed.append(added)
        return changed, {"1002", "1003", "1005"}, {"1004"}

    def assert_jira_cursor(
        fixture: FixtureServer,
        state: Mapping[str, Any],
    ) -> None:
        cursor = state.get("cursor")
        if cursor is None:
            raise AssertionError("Jira incremental read has no saved updated cursor")
        parsed = datetime.fromisoformat(str(cursor).replace("Z", "+00:00"))
        lower_bound = (
            parsed.astimezone(timezone.utc) - timedelta(seconds=60)
        ).strftime("%Y/%m/%d %H:%M")
        query = parse_qs(urlsplit(fixture.request_log[0].target).query)
        expected = f'updated >= "{lower_bound}" ORDER BY updated ASC'
        if query.get("jql") != [expected]:
            raise AssertionError(f"Jira request omitted the lookback JQL cursor {expected!r}")

    jira_case = CertificationCase(
        connector_id="jira",
        connector_factory=jira_factory,
        fixture_routes=jira_routes,
        stream="issues",
        mutate_fixture=mutate_jira,
        fixture_records=jira_records,
        primary_key=("id",),
        cursor_field="fields.updated",
        secret=_SECRET,
        assert_incremental_cursor=assert_jira_cursor,
    )

    tap_script = _tap_source(tmp_path)

    def singer_factory(
        _base_url: str,
        phase: str,
        fixture_records: Sequence[Mapping[str, Any]],
        _sleep: Any = None,
    ) -> SingerTapBridge:
        return SingerTapBridge(
            {
                "tap_command": [sys.executable, str(tap_script)],
                "tap_config": {
                    "api_key": _SECRET,
                    "phase": phase,
                    "records": [dict(row) for row in fixture_records],
                    "cursor_field": "updated_at",
                },
            }
        )

    def no_http_routes(
        _fixture: FixtureServer,
        _phase: str,
        _fixture_records: Sequence[Mapping[str, Any]],
    ) -> None:
        return

    singer_case = CertificationCase(
        connector_id="singer_tap",
        connector_factory=singer_factory,
        fixture_routes=no_http_routes,
        stream="items",
        mutate_fixture=mutation_for("updated_at"),
        fixture_records=records,
        primary_key=("id",),
        cursor_field="updated_at",
        secret=_SECRET,
        unsupported_steps=frozenset({"rate_limit"}),
    )

    return {
        case.connector_id: case
        for case in (
            singer_case,
            hubspot_case,
            declarative_case,
            shim_case,
            github_case,
            jira_case,
        )
    }
