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
        responses = [first, second, last]
    fixture.add_route(route, responses=responses)
    return route


def _table_rows(path: Path, table: str) -> list[tuple]:
    with sqlite3.connect(path) as conn:
        return conn.execute(f'SELECT id FROM "{table}" ORDER BY id').fetchall()


def _sample_row(path: Path, table: str, row_id: int | str) -> str:
    with sqlite3.connect(path) as conn:
        row = conn.execute(
            f'SELECT * FROM "{table}" WHERE id = ?',
            (row_id,),
        ).fetchone()
    return json.dumps(row, default=str)


@pytest.mark.parametrize("kind", ["github", "jira"])
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
        else:
            assert "starting_after" not in first_retry_request
        rows = _table_rows(destination_path, stream)
        assert len(rows) == expected_count
        assert len(set(rows)) == expected_count
        assert int(rows[0][0]) == 1
        assert int(rows[-1][0]) == 250
        sample = _sample_row(
            destination_path,
            stream,
            1 if kind == "github" else "1",
        )
        expected_sample = {
            "github": "GitHub issue 1",
            "jira": "Jira issue 1",
            "intercom": "Intercom contact 1",
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


@pytest.mark.xfail(
    strict=True,
    reason="engine retry re-maps epoch INTEGER to TIMESTAMP; see CONNECTOR_CERTIFICATION.md known gap (a)",
)
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


@pytest.mark.xfail(
    strict=True,
    reason="SDK resume fails strict Gate-8 on a session-only write digest; see CONNECTOR_CERTIFICATION.md known gap (b)",
)
def test_github_resume_uses_saved_page_two_state_and_reconciles_population(
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
        assert resume_requests and "page=2" in resume_requests[0].target
        assert resumed.success, resumed.error
        rows = _table_rows(destination_path, "issues")
        assert len(rows) == 250
        assert len(set(rows)) == 250
