from __future__ import annotations

import json
import logging
import os
import sqlite3
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

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
    def update_job_fields(self, job_id: str, **fields) -> bool:
        self.jobs.setdefault(job_id, {}).update(fields)
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
        else:
            records.append(
                {
                    "id": str(value),
                    "name": f"Intercom contact {value}",
                    "email": f"contact-{value}@example.test",
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
        responses.extend([second, last])
    fixture.add_route(route, responses=responses)
    return route


def _table_rows(path: Path, table: str) -> list[tuple]:
    with sqlite3.connect(path) as conn:
        return conn.execute(f'SELECT id FROM "{table}" ORDER BY id').fetchall()


@pytest.mark.parametrize("kind", ["github", "jira", "intercom"])
def test_sdk_transfer_engine_pages_resume_after_failure_without_duplicates(
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

    stream = "contacts" if kind == "intercom" else "issues"
    pages = [
        _records(kind, 1, 100),
        _records(kind, 101, 100),
        _records(kind, 201, 50),
    ]
    expected_count = sum(map(len, pages))
    destination_path = tmp_path / f"{kind}.sqlite"
    job_id = f"gconn-{kind}-{uuid.uuid4().hex[:12]}"

    with FixtureServer() as fixture:
        route = _configure_route(fixture, kind, pages, fail_page_two=True)
        source = _endpoint(kind, fixture.base_url, destination_path)
        request = TransferRequest(
            source=source,
            destination=_destination(destination_path, stream),
            sync_mode="full_refresh_append",
            skip_preflight=True,
            validation_mode="strict",
        )

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

        # The job resumes from the page-two state stored after page one.
        _configure_route(fixture, kind, pages, fail_page_two=False)
        with caplog.at_level(logging.WARNING):
            resumed = UniversalTransferEngine().execute_tracked(
                request, job_id, resume=True
            )

        assert resumed.success, resumed.error
        rows = _table_rows(destination_path, stream)
        assert len(rows) == expected_count
        assert len(set(rows)) == expected_count
        assert rows[0][0] == (1 if kind == "github" else "1")
        assert rows[-1][0] == (250 if kind == "github" else "250")

        rerun_requests = fixture.request_log[-2:]
        assert len(rerun_requests) == 2
        if kind == "github":
            assert parse_qs(urlsplit(rerun_requests[0].target).query).get("page") == ["2"]
        elif kind == "jira":
            assert parse_qs(urlsplit(rerun_requests[0].target).query).get(
                "nextPageToken"
            ) == ["jira-page-2"]
        else:
            assert parse_qs(urlsplit(rerun_requests[0].target).query).get(
                "starting_after"
            ) == ["intercom-page-2"]

        stored_text = json.dumps(fake.jobs.get(job_id, {}), default=str)
        assert _SECRET not in stored_text
        assert _SECRET not in caplog.text
