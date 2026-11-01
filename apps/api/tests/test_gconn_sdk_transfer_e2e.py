from __future__ import annotations

import json
import logging
import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("RETRY_BASE_DELAY_SECONDS", "0")
os.environ.setdefault("RETRY_MAX_DELAY_SECONDS", "0")
os.environ.setdefault("RETRY_JITTER", "false")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402

_FAILURE_COUNT = 20
_SECRET = "synthetic-transfer-secret"


class _EngineFakeMongo(_FakeMongo):
    def update_job_fields(self, job_id: str, fields=None, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {}).update(dict(fields or {}))
        self.jobs[job_id].update(kwargs)
        return True


def _records(kind: str, start: int, count: int) -> list[dict]:
    records = []
    for value in range(start, start + count):
        if kind == "github":
            records.append(
                {
                    "id": value,
                    "number": value,
                    "title": f"GitHub issue {value}",
                    "state": "open",
                    "updated_at": "2026-01-01T00:00:00Z",
                    "pull_request": {"url": f"https://example.test/pr/{value}"},
                }
            )
        elif kind == "jira":
            records.append(
                {
                    "id": str(value),
                    "key": f"DF-{value}",
                    "fields": {
                        "summary": f"Jira issue {value}",
                        "updated": "2026-01-01T00:00:00.000+0000",
                    },
                }
            )
        elif kind == "slack":
            records.append(
                {
                    "id": f"U{value:05d}",
                    "name": f"slack-user-{value}",
                    "real_name": f"Slack User {value}",
                    "is_bot": False,
                    "deleted": False,
                    "updated": 1767225600,
                }
            )
        elif kind == "zendesk":
            records.append(
                {
                    "id": value,
                    "subject": f"Zendesk ticket {value}",
                    "status": "open",
                    "updated_at": "2026-01-01T00:00:00Z",
                    "generated_timestamp": 1700000000 + value,
                }
            )
        elif kind == "gitlab":
            records.append(
                {
                    "id": value,
                    "iid": value,
                    "project_id": 1,
                    "title": f"GitLab issue {value}",
                    "state": "opened",
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                }
            )
        elif kind == "mailchimp":
            records.append(
                {
                    "id": f"list-{value:05d}",
                    "name": f"Mailchimp list {value}",
                    "date_created": "2026-01-01T00:00:00Z",
                    "stats": {"member_count": value},
                }
            )
        elif kind == "asana":
            records.append(
                {
                    "id": f"{value:05d}",
                    "gid": str(value),
                    "name": f"Asana workspace {value}",
                    "resource_type": "workspace",
                    "is_organization": False,
                }
            )
        elif kind == "airtable":
            records.append(
                {
                    "id": f"rec{value:05d}",
                    "createdTime": "2026-01-01T00:00:00.000Z",
                    "fields": {"Name": f"Airtable row {value}"},
                }
            )
        elif kind == "freshdesk":
            records.append(
                {
                    "id": value,
                    "subject": f"Freshdesk ticket {value}",
                    "status": 2,
                    "priority": 1,
                    "requester_id": value,
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                }
            )
        elif kind == "notion":
            records.append(
                {
                    "id": f"page-{value:05d}",
                    "object": "page",
                    "created_time": "2026-01-01T00:00:00.000Z",
                    "last_edited_time": "2026-01-01T00:00:00.000Z",
                    "archived": False,
                }
            )
        else:
            records.append(
                {
                    "id": str(value),
                    "name": f"Intercom contact {value}",
                    "email": f"contact-{value}@example.test",
                    "created_at": 1767225500,
                    "updated_at": 1767225600,
                }
            )
    return records


def _endpoint(kind: str, base_url: str, destination: Path) -> EndpointConfig:
    if kind == "github":
        extra = {
            "base_url": base_url,
            "owner": "acme",
            "repo": "widgets",
            "access_token": _SECRET,
        }
        stream = "issues"
    elif kind == "jira":
        extra = {
            "base_url": base_url,
            "site": "synthetic-site",
            "email": "fixture@example.test",
            "api_token": _SECRET,
        }
        stream = "issues"
    elif kind == "slack":
        extra = {"base_url": base_url, "access_token": _SECRET}
        stream = "users"
    elif kind == "zendesk":
        extra = {
            "base_url": base_url,
            "domain": "synthetic",
            "username": "fixture@example.test",
            "api_key": _SECRET,
        }
        stream = "tickets"
    elif kind == "gitlab":
        extra = {"base_url": base_url, "api_key": _SECRET}
        stream = "issues"
    elif kind == "mailchimp":
        extra = {"base_url": base_url, "dc": "us10", "api_key": _SECRET}
        stream = "lists"
    elif kind == "asana":
        extra = {
            "base_url": base_url,
            "api_key": _SECRET,
            "workspace": "fixture-workspace",
        }
        stream = "workspaces"
    elif kind == "airtable":
        extra = {
            "base_url": base_url,
            "api_key": _SECRET,
            "base_id": "fixture-base",
            "table_id": "fixture-table",
        }
        stream = "records"
    elif kind == "freshdesk":
        extra = {"base_url": base_url, "domain": "acme", "api_key": _SECRET}
        stream = "tickets"
    elif kind == "notion":
        extra = {"base_url": base_url, "api_key": _SECRET}
        stream = "pages"
    else:
        extra = {"base_url": base_url, "access_token": _SECRET}
        stream = "contacts"
    return EndpointConfig(
        kind="database",
        format=kind,
        table=stream,
        extra=extra,
    )


def _destination(path: Path, stream: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(path),
        table=stream,
    )


def _configure_route(
    fixture: FixtureServer,
    kind: str,
    pages: list[list[dict]],
    *,
    fail_page_two: bool,
) -> str:
    if kind == "github":
        route = "/repos/acme/widgets/issues"
        first = FixtureResponse(
            body=pages[0],
            headers={
                "Link": f"<{fixture.base_url}{route}?page=2>; rel=\"next\""
            },
        )
        second = FixtureResponse(
            body=pages[1],
            headers={
                "Link": f"<{fixture.base_url}{route}?page=3>; rel=\"next\""
            },
        )
        last = FixtureResponse(body=pages[2])
    elif kind == "jira":
        route = "/rest/api/3/search/jql"
        first = FixtureResponse(
            body={"issues": pages[0], "nextPageToken": "jira-page-2"}
        )
        second = FixtureResponse(
            body={"issues": pages[1], "nextPageToken": "jira-page-3"}
        )
        last = FixtureResponse(body={"issues": pages[2], "isLast": True})
    elif kind == "slack":
        route = "/users.list"
        first = FixtureResponse(
            body={
                "members": pages[0],
                "response_metadata": {"next_cursor": "slack-cur-2"},
            }
        )
        second = FixtureResponse(
            body={
                "members": pages[1],
                "response_metadata": {"next_cursor": "slack-cur-3"},
            }
        )
        last = FixtureResponse(
            body={"members": pages[2], "response_metadata": {}}
        )
    elif kind == "zendesk":
        route = "/incremental/tickets"
        first = FixtureResponse(
            body={
                "tickets": pages[0],
                "end_of_stream": False,
                "next_page": f"{fixture.base_url}/incremental/tickets?page=2",
            }
        )
        second = FixtureResponse(
            body={
                "tickets": pages[1],
                "end_of_stream": False,
                "next_page": f"{fixture.base_url}/incremental/tickets?page=3",
            }
        )
        # Zendesk keeps emitting a self-repeating next_page even on the
        # terminal page — end_of_stream is the real stop signal.
        last = FixtureResponse(
            body={
                "tickets": pages[2],
                "end_of_stream": True,
                "next_page": f"{fixture.base_url}/incremental/tickets?page=3",
            }
        )
    elif kind == "gitlab":
        route = "/issues"
        first = FixtureResponse(
            body=pages[0],
            headers={
                "Link": f"<{fixture.base_url}{route}?page=2>; rel=\"next\""
            },
        )
        second = FixtureResponse(
            body=pages[1],
            headers={
                "Link": f"<{fixture.base_url}{route}?page=3>; rel=\"next\""
            },
        )
        last = FixtureResponse(body=pages[2])
    elif kind == "mailchimp":
        route = "/lists"
        first = FixtureResponse(body={"lists": pages[0]})
        second = FixtureResponse(body={"lists": pages[1]})
        last = FixtureResponse(body={"lists": pages[2]})
    elif kind == "asana":
        route = "/workspaces"
        first = FixtureResponse(
            body={"data": pages[0], "next_page": {"offset": "asana-off-2"}}
        )
        second = FixtureResponse(
            body={"data": pages[1], "next_page": {"offset": "asana-off-3"}}
        )
        last = FixtureResponse(body={"data": pages[2], "next_page": None})
    elif kind == "airtable":
        route = "/v0/fixture-base/fixture-table"
        first = FixtureResponse(
            body={"records": pages[0], "offset": "airtable-off-2"}
        )
        second = FixtureResponse(
            body={"records": pages[1], "offset": "airtable-off-3"}
        )
        last = FixtureResponse(body={"records": pages[2]})
    elif kind == "freshdesk":
        route = "/tickets"
        first = FixtureResponse(body=pages[0])
        second = FixtureResponse(body=pages[1])
        last = FixtureResponse(body=pages[2])
    elif kind == "notion":
        route = "/search"
        first = FixtureResponse(
            body={"results": pages[0], "next_cursor": "notion-cur-2"}
        )
        second = FixtureResponse(
            body={"results": pages[1], "next_cursor": "notion-cur-3"}
        )
        last = FixtureResponse(body={"results": pages[2], "next_cursor": None})
    else:
        route = "/contacts"
        first = FixtureResponse(
            body={
                "data": pages[0],
                "pages": {"next": {"starting_after": "intercom-page-2"}},
            }
        )
        second = FixtureResponse(
            body={
                "data": pages[1],
                "pages": {"next": {"starting_after": "intercom-page-3"}},
            }
        )
        last = FixtureResponse(body={"data": pages[2], "pages": {}})

    responses = [first]
    if fail_page_two:
        responses.extend(
            FixtureResponse(status=500, body={"error": "synthetic retry fault"})
            for _ in range(_FAILURE_COUNT)
        )
    else:
        responses = [first, second, last]
    method = "POST" if kind == "notion" else "GET"
    fixture.add_route(route, responses=responses, method=method)
    return route


def _seed_nonempty_destination(
    fixture: FixtureServer, kind: str, request: TransferRequest
) -> None:
    """Load one record so the destination is non-empty before the faulted run.

    A failed run against an empty/absent destination is rolled back to
    byte-identical state (services.batch_undo, X-35/X-41), which is the
    intended product behaviour. These tests exercise the other path - a
    mid-stream fault against a destination that already holds data keeps the
    pages already committed - so they must not start from an empty table.
    """
    record = _records(kind, 1, 1)
    route = {
        "github": "/repos/acme/widgets/issues",
        "jira": "/rest/api/3/search/jql",
        "intercom": "/contacts",
        "slack": "/users.list",
        "zendesk": "/incremental/tickets",
        "gitlab": "/issues",
        "mailchimp": "/lists",
        "asana": "/workspaces",
        "airtable": "/v0/fixture-base/fixture-table",
        "freshdesk": "/tickets",
        "notion": "/search",
    }[kind]
    if kind == "github":
        body = record
    elif kind == "jira":
        body = {"issues": record, "isLast": True}
    elif kind == "slack":
        body = {"members": record, "response_metadata": {}}
    elif kind == "zendesk":
        body = {"tickets": record, "end_of_stream": True, "next_page": None}
    elif kind == "gitlab":
        body = record
    elif kind == "mailchimp":
        body = {"lists": record}
    elif kind == "asana":
        body = {"data": record, "next_page": None}
    elif kind == "airtable":
        body = {"records": record}
    elif kind == "freshdesk":
        body = record
    elif kind == "notion":
        body = {"results": record, "next_cursor": None}
    else:
        body = {"data": record, "pages": {}}
    method = "POST" if kind == "notion" else "GET"
    fixture.add_route(route, responses=[FixtureResponse(body=body)], method=method)
    seeded = UniversalTransferEngine().execute_tracked(
        request, f"seed-{uuid.uuid4().hex[:12]}"
    )
    assert seeded.success, seeded.error


def _table_rows(path: Path, table: str) -> list[tuple]:
    with sqlite3.connect(path) as conn:
        return conn.execute(f'SELECT id FROM "{table}" ORDER BY id').fetchall()


def _table_exists(path: Path, table: str) -> bool:
    if not path.exists():
        return False
    with sqlite3.connect(path) as conn:
        return (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
                (table,),
            ).fetchone()
            is not None
        )


def _sample_row(path: Path, table: str, row_id: int | str) -> str:
    with sqlite3.connect(path) as conn:
        row = conn.execute(
            f'SELECT * FROM "{table}" WHERE id = ?',
            (row_id,),
        ).fetchone()
    return json.dumps(row, default=str)


@pytest.mark.parametrize(
    "kind",
    [
        "github",
        "jira",
        "slack",
        "zendesk",
        "gitlab",
        "mailchimp",
        "asana",
        "airtable",
        "freshdesk",
        "notion",
    ],
)
def test_sdk_transfer_engine_pages_full_reread_upsert_without_duplicates(
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from connectors.sdk.transfer_bridge import _decode_cursor_state

    fake = _EngineFakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setenv("RETRY_BASE_DELAY_SECONDS", "0")
    monkeypatch.setenv("RETRY_MAX_DELAY_SECONDS", "0")
    monkeypatch.setenv("RETRY_JITTER", "false")

    stream = {
        "intercom": "contacts",
        "slack": "users",
        "zendesk": "tickets",
        "gitlab": "issues",
        "mailchimp": "lists",
        "asana": "workspaces",
        "airtable": "records",
        "freshdesk": "tickets",
        "notion": "pages",
    }.get(kind, "issues")
    pages = [
        _records(kind, 1, 100),
        _records(kind, 101, 100),
        _records(kind, 201, 50),
    ]
    expected_count = sum(map(len, pages))
    destination_path = tmp_path / f"{kind}.sqlite"
    job_id = f"gconn-{kind}-{uuid.uuid4().hex[:12]}"

    with FixtureServer() as fixture:
        _configure_route(fixture, kind, pages, fail_page_two=True)
        source = _endpoint(kind, fixture.base_url, destination_path)
        request = TransferRequest(
            source=source,
            destination=_destination(destination_path, stream),
            sync_mode="upsert",
            skip_preflight=True,
            validation_mode="strict",
            stream_contracts=[
                {
                    "name": stream,
                    "primary_key": ["id"],
                    "sync_mode": "upsert",
                    "selected": True,
                }
            ],
        )

        _seed_nonempty_destination(fixture, kind, request)
        _configure_route(fixture, kind, pages, fail_page_two=True)
        with caplog.at_level(logging.WARNING):
            first = UniversalTransferEngine().execute_tracked(request, job_id)

        assert not first.success
        assert first.error
        assert "500" in first.error or "HTTP" in first.error
        failed_rows = _table_rows(destination_path, stream)
        assert len(failed_rows) == 100
        assert len(set(failed_rows)) == 100

        job_checkpoint = fake.jobs.get(job_id, {}).get("checkpoint") or {}
        cursor_value = (
            job_checkpoint.get("cursor_value")
            if isinstance(job_checkpoint, dict)
            else None
        )
        assert cursor_value
        assert _decode_cursor_state(cursor_value).get("page_token")

        # Recovery is a full rescan with primary-key upsert.
        _configure_route(fixture, kind, pages, fail_page_two=False)
        requests_before_retry = len(fixture.request_log)
        with caplog.at_level(logging.WARNING):
            retried = UniversalTransferEngine().execute_tracked(
                request, f"{job_id}-retry"
            )

        assert retried.success, retried.error
        retry_requests = fixture.request_log[requests_before_retry:]
        assert len(retry_requests) >= 3
        first_retry_request = retry_requests[0].target
        if kind == "github":
            assert "page=2" not in first_retry_request
        elif kind == "jira":
            assert "nextPageToken" not in first_retry_request
        elif kind == "slack":
            assert "cursor=" not in first_retry_request
        elif kind == "zendesk":
            assert "?page=" not in first_retry_request
        elif kind == "gitlab":
            # Full reread restarts at the bare collection URL — the page
            # token only ever appears as a ``&page=N`` suffix (``per_page``
            # is a first-page param).
            assert "&page=" not in first_retry_request
        elif kind == "mailchimp":
            assert "offset=200" not in first_retry_request
            assert "offset=100" not in first_retry_request
        elif kind in {"asana", "airtable"}:
            assert "offset=" not in first_retry_request
        elif kind == "freshdesk":
            assert "page=2" not in first_retry_request
            assert "page=3" not in first_retry_request
        elif kind == "notion":
            assert b"notion-cur" not in retry_requests[0].body
        else:
            assert "starting_after" not in first_retry_request
        rows = _table_rows(destination_path, stream)
        assert len(rows) == expected_count
        assert len(set(rows)) == expected_count
        if kind == "slack":
            assert rows[0][0] == "U00001"
            assert rows[-1][0] == "U00250"
        elif kind == "mailchimp":
            assert rows[0][0] == "list-00001"
            assert rows[-1][0] == "list-00250"
        elif kind == "airtable":
            assert rows[0][0] == "rec00001"
            assert rows[-1][0] == "rec00250"
        elif kind == "notion":
            assert rows[0][0] == "page-00001"
            assert rows[-1][0] == "page-00250"
        else:
            assert int(rows[0][0]) == 1
            assert int(rows[-1][0]) == 250
        sample_id = {
            "github": 1,
            "zendesk": 1,
            "gitlab": 1,
            "freshdesk": 1,
            "slack": "U00001",
            "mailchimp": "list-00001",
            "airtable": "rec00001",
            "notion": "page-00001",
            "asana": "00001",
        }.get(kind, "1")
        sample = _sample_row(destination_path, stream, sample_id)
        expected_sample = {
            "github": "GitHub issue 1",
            "jira": "Jira issue 1",
            "intercom": "Intercom contact 1",
            "slack": "Slack User 1",
            "zendesk": "Zendesk ticket 1",
            "gitlab": "GitLab issue 1",
            "mailchimp": "Mailchimp list 1",
            "asana": "Asana workspace 1",
            "airtable": "Airtable row 1",
            "freshdesk": "Freshdesk ticket 1",
            "notion": "page-00001",
        }[kind]
        assert expected_sample in sample

        stored_text = json.dumps(fake.jobs.get(job_id, {}), default=str)
        assert _SECRET not in stored_text
        assert _SECRET not in str(fake.jobs.get(f"{job_id}-retry", {}))
        assert _SECRET not in caplog.text


def test_intercom_fresh_destination_reads_three_pages_with_epoch_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = _EngineFakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setenv("RETRY_BASE_DELAY_SECONDS", "0")
    monkeypatch.setenv("RETRY_MAX_DELAY_SECONDS", "0")
    monkeypatch.setenv("RETRY_JITTER", "false")

    pages = [_records("intercom", 1, 100), _records("intercom", 101, 100), _records("intercom", 201, 50)]
    destination_path = tmp_path / "intercom.sqlite"
    job_id = f"gconn-intercom-fresh-{uuid.uuid4().hex[:12]}"

    with FixtureServer() as fixture:
        _configure_route(fixture, "intercom", pages, fail_page_two=False)
        request = TransferRequest(
            source=_endpoint("intercom", fixture.base_url, destination_path),
            destination=_destination(destination_path, "contacts"),
            sync_mode="upsert",
            skip_preflight=True,
            validation_mode="strict",
            stream_contracts=[
                {
                    "name": "contacts",
                    "primary_key": ["id"],
                    "sync_mode": "upsert",
                    "selected": True,
                }
            ],
        )

        with caplog.at_level(logging.WARNING):
            result = UniversalTransferEngine().execute_tracked(request, job_id)

        assert result.success, result.error
        assert len(fixture.request_log) >= 3
        rows = _table_rows(destination_path, "contacts")
        assert len(rows) == 250
        assert len(set(rows)) == 250
        sample = _sample_row(destination_path, "contacts", "1")
        assert "Intercom contact 1" in sample
        assert "1767225500" in sample
        assert "1767225600" in sample
        assert _SECRET not in json.dumps(fake.jobs.get(job_id, {}), default=str)
        assert _SECRET not in caplog.text


def test_intercom_fault_then_full_reread_retry_uses_pk_upsert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = _EngineFakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setenv("RETRY_BASE_DELAY_SECONDS", "0")
    monkeypatch.setenv("RETRY_MAX_DELAY_SECONDS", "0")
    monkeypatch.setenv("RETRY_JITTER", "false")

    pages = [_records("intercom", 1, 100), _records("intercom", 101, 100), _records("intercom", 201, 50)]
    destination_path = tmp_path / "intercom-retry.sqlite"
    job_id = f"gconn-intercom-retry-{uuid.uuid4().hex[:12]}"

    with FixtureServer() as fixture:
        _configure_route(fixture, "intercom", pages, fail_page_two=True)
        request = TransferRequest(
            source=_endpoint("intercom", fixture.base_url, destination_path),
            destination=_destination(destination_path, "contacts"),
            sync_mode="upsert",
            skip_preflight=True,
            validation_mode="strict",
            stream_contracts=[
                {
                    "name": "contacts",
                    "primary_key": ["id"],
                    "sync_mode": "upsert",
                    "selected": True,
                }
            ],
        )

        _seed_nonempty_destination(fixture, "intercom", request)
        _configure_route(fixture, "intercom", pages, fail_page_two=True)
        with caplog.at_level(logging.WARNING):
            failed = UniversalTransferEngine().execute_tracked(request, job_id)
        assert not failed.success
        assert failed.error and ("500" in failed.error or "HTTP" in failed.error)
        assert len(_table_rows(destination_path, "contacts")) == 100

        _configure_route(fixture, "intercom", pages, fail_page_two=False)
        with caplog.at_level(logging.WARNING):
            retried = UniversalTransferEngine().execute_tracked(request, f"{job_id}-retry")

        assert retried.success, retried.error
        rows = _table_rows(destination_path, "contacts")
        assert len(rows) == 250
        assert len(set(rows)) == 250
        assert "1767225500" in _sample_row(destination_path, "contacts", "1")
        assert _SECRET not in json.dumps(fake.jobs, default=str)
        assert _SECRET not in caplog.text


def test_github_resume_is_refused_before_http_or_destination_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from connectors.sdk.transfer_bridge import _decode_cursor_state

    fake = _EngineFakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setenv("RETRY_BASE_DELAY_SECONDS", "0")
    monkeypatch.setenv("RETRY_MAX_DELAY_SECONDS", "0")
    monkeypatch.setenv("RETRY_JITTER", "false")

    pages = [_records("github", 1, 100), _records("github", 101, 100), _records("github", 201, 50)]
    destination_path = tmp_path / "github-resume.sqlite"
    job_id = f"gconn-github-resume-{uuid.uuid4().hex[:12]}"

    with FixtureServer() as fixture:
        _configure_route(fixture, "github", pages, fail_page_two=True)
        request = TransferRequest(
            source=_endpoint("github", fixture.base_url, destination_path),
            destination=_destination(destination_path, "issues"),
            sync_mode="upsert",
            skip_preflight=True,
            validation_mode="strict",
            stream_contracts=[
                {
                    "name": "issues",
                    "primary_key": ["id"],
                    "sync_mode": "upsert",
                    "selected": True,
                }
            ],
        )
        _seed_nonempty_destination(fixture, "github", request)
        _configure_route(fixture, "github", pages, fail_page_two=True)
        failed = UniversalTransferEngine().execute_tracked(request, job_id)
        assert not failed.success
        assert len(_table_rows(destination_path, "issues")) == 100
        checkpoint = fake.jobs.get(job_id, {}).get("checkpoint") or {}
        cursor_value = checkpoint.get("cursor_value") if isinstance(checkpoint, dict) else None
        assert cursor_value
        assert _decode_cursor_state(cursor_value).get("page_token")

        route = "/repos/acme/widgets/issues"
        fixture.add_route(
            route,
            responses=[
                FixtureResponse(
                    body=pages[1],
                    headers={
                        "Link": f"<{fixture.base_url}{route}?page=3>; rel=\"next\""
                    },
                ),
                FixtureResponse(body=pages[2]),
            ],
        )
        request_start = len(fixture.request_log)
        resumed = UniversalTransferEngine().execute_tracked(
            request, job_id, resume=True
        )
        resume_requests = fixture.request_log[request_start:]
        refusal = (
            "Resume is not supported for GitHub yet; rerun the job — "
            "rows are upserted by primary key."
        )
        assert not resumed.success
        assert resumed.error == refusal
        assert fake.jobs[job_id]["status"] == "failed"
        assert fake.jobs[job_id]["error"] == refusal
        assert len(_table_rows(destination_path, "issues")) == 100
        assert not resume_requests

        fresh_destination = tmp_path / "github-resume-fresh.sqlite"
        fresh_job_id = f"gconn-github-resume-fresh-{uuid.uuid4().hex[:12]}"
        fresh_request = TransferRequest(
            source=request.source,
            destination=_destination(fresh_destination, "issues"),
            sync_mode="upsert",
            skip_preflight=True,
            validation_mode="strict",
            stream_contracts=[
                {
                    "name": "issues",
                    "primary_key": ["id"],
                    "sync_mode": "upsert",
                    "selected": True,
                }
            ],
        )
        fresh_request_start = len(fixture.request_log)
        fresh_result = UniversalTransferEngine().execute_tracked(
            fresh_request, fresh_job_id, resume=True
        )
        assert not fresh_result.success
        assert fresh_result.error == refusal
        assert fake.jobs[fresh_job_id]["status"] == "failed"
        assert fake.jobs[fresh_job_id]["error"] == refusal
        assert not _table_exists(fresh_destination, "issues")
        assert len(fixture.request_log) == fresh_request_start
