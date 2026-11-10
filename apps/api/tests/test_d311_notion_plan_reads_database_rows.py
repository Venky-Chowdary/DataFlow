"""D311 - a Notion source could be tested and sampled, but plan_transfer /
start_transfer / introspect_connector_schema could not read ``database_rows``.

QA: "Could not read the source database_rows on QA-Notion
(https://api.notion.com/v1:443): HTTPError: 400 Bad Request for url:
https://api.notion.com/v1/database_rows?page_size=25". Notion serves database
rows only on ``POST /v1/databases/{id}/query`` (``start_cursor`` in the body);
a GET on ``/v1/database_rows`` does not exist.

Root cause: sample and the transfer engine read through the SDK manifest
(``connectors.sdk.transfer_bridge`` -> manifests/notion.json, POST + body
cursor), but the source introspection behind introspect/plan/start
(``schema_introspect_saas._introspect_thin_saas``) and the legacy
``connectors.notion.read_object`` went through the generic REST reader, which
builds ``GET <base>/<object>?page_size=N``.

No internet here: Notion's HTTP API is emulated at the ``requests`` layer
(POST-only query and search endpoints, every other path answers 400, exactly
like the real API did for QA).
"""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from typing import Any

import pytest
import requests

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(300)

_DB_ID = "0f1e2d3c4b5a69788796a5b4c3d2e1f0"
_TOKEN = "secret_notion_fixture_token"  # nosec B105 - fixture value


def _row(i: int) -> dict[str, Any]:
    name = "任务 🚀" if i == 3 else f"Task {i}"
    return {
        "object": "page",
        "id": f"00000000-0000-0000-0000-{i:012d}",
        "created_time": "2026-11-01T10:00:00.000Z",
        "last_edited_time": f"2026-11-0{1 + i % 8}T10:00:00.000Z",
        "created_by": {"object": "user", "id": "u1"},
        "last_edited_by": {"object": "user", "id": "u1"},
        "cover": None,
        "icon": None,
        "parent": {"type": "database_id", "database_id": _DB_ID},
        "archived": False,
        "in_trash": False,
        "url": f"https://www.notion.so/Task-{i}",
        "public_url": None,
        "properties": {
            "Name": {"id": "title", "type": "title", "title": [{"plain_text": name}]},
            "Status": {"id": "s", "type": "select", "select": {"name": "Done" if i % 2 else "Todo"}},
            "Points": {"id": "p", "type": "number", "number": None if i == 2 else i * 1.5},
            "Due": {"id": "d", "type": "date", "date": None if i == 4 else {"start": "2026-12-01"}},
        },
    }


class _NotionApi:
    """Notion's REST surface as QA met it: POST-only reads, 400 elsewhere."""

    def __init__(self, rows: int) -> None:
        self.rows = [_row(i) for i in range(1, rows + 1)]
        self.calls: list[tuple[str, str, dict]] = []

    def _response(self, url: str, status: int, body: dict) -> requests.Response:
        resp = requests.Response()
        resp.status_code = status
        resp._content = json.dumps(body).encode()
        resp.headers["Content-Type"] = "application/json"
        resp.url = url
        resp.encoding = "utf-8"
        resp.request = requests.Request("GET", url).prepare()
        return resp

    def __call__(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        body = kwargs.get("json")
        if body is None and kwargs.get("data"):
            raw = kwargs["data"]
            body = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        body = dict(body or {})
        path = re.sub(r"^https://api\.notion\.com", "", url.split("?")[0])
        self.calls.append((method.upper(), url, body))
        if method.upper() == "GET" and path == "/v1/users/me":
            return self._response(url, 200, {"object": "user", "id": "bot"})
        if method.upper() == "POST" and path == f"/v1/databases/{_DB_ID}/query":
            size = int(body.get("page_size") or 100)
            start = int(body.get("start_cursor") or 0)
            page = self.rows[start:start + size]
            nxt = start + size if start + size < len(self.rows) else None
            return self._response(url, 200, {
                "object": "list", "results": page,
                "next_cursor": str(nxt) if nxt is not None else None,
                "has_more": nxt is not None,
            })
        if method.upper() == "POST" and path == "/v1/search":
            return self._response(url, 200, {"object": "list", "results": [], "next_cursor": None,
                                             "has_more": False})
        return self._response(url, 400, {"object": "error", "status": 400,
                                         "code": "invalid_request_url",
                                         "message": "Invalid request URL."})


@pytest.fixture
def notion(monkeypatch):
    def _install(rows: int) -> _NotionApi:
        api = _NotionApi(rows)
        monkeypatch.setattr(
            requests.sessions.Session, "request",
            lambda _self, method, url, **kw: api(method, url, **kw),
        )
        return api
    return _install


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    # api.notion.com is public; there is no DNS here, so the pre-dial resolve
    # is answered with a public address.
    import socket

    real = socket.getaddrinfo

    def _resolve(host, *args, **kwargs):
        if host == "api.notion.com":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("104.18.0.1", 443))]
        return real(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", _resolve)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _connectors(tmp_path) -> tuple[str, str, str]:
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    src = create_connector({
        "name": f"d311-notion-{token}", "type": "notion", "role": "source", "workspace_id": "",
        "host": "https://api.notion.com/v1", "database": _DB_ID, "auth_mode": "api_key",
        "api_key": _TOKEN,
    })
    path = str(tmp_path / f"d311_{token}.db")
    dst = create_connector({"name": f"d311-sqlite-{token}", "type": "sqlite",
                            "role": "destination", "workspace_id": "", "database": path})
    return src.id, dst.id, path


def _no_get_on_post_paths(api: _NotionApi) -> None:
    bad = [(m, u) for m, u, _ in api.calls if m == "GET" and not u.endswith("/v1/users/me")]
    assert not bad, bad


def test_introspect_reads_database_rows_through_the_post_query(client, notion, tmp_path):
    from src.ai.copilot.schema_tools import introspect_connector_schema

    api = notion(5)
    src_id, _dst_id, _ = _connectors(tmp_path)
    out = introspect_connector_schema(connector_id=src_id, table="database_rows")
    assert out.success, out.error
    cols = {c["name"] for c in (out.output or {}).get("columns") or []}
    assert {"id", "properties"} <= cols, out.output
    _no_get_on_post_paths(api)
    assert any(m == "POST" and u.endswith(f"/databases/{_DB_ID}/query") for m, u, _ in api.calls)


def test_plan_and_start_stage_and_land_every_row(client, notion, tmp_path):
    from src.ai.copilot.transfer_tools import plan_transfer

    api = notion(130)  # > one Notion page: start_cursor rides the POST body
    src_id, dst_id, path = _connectors(tmp_path)
    plan = plan_transfer(source_connector_id=src_id, source_table="database_rows",
                         dest_connector_id=dst_id, dest_table="notion_rows",
                         sync_mode="full_refresh_overwrite")
    assert plan.success, plan.error
    _no_get_on_post_paths(api)

    staged, blockers = _stage(src_id, dst_id)
    assert staged.success, (staged.error, blockers)
    resp = client.post("/api/v1/copilot/confirm",
                       json={"ack_id": staged.output["ack_id"], "reason": "d311"})
    assert resp.status_code == 200, resp.text
    job = h.wait_job(str(resp.json().get("job_id") or ""))
    assert job.get("status") == "completed", (job.get("status"), job.get("error"))
    _no_get_on_post_paths(api)
    assert any(b.get("start_cursor") for m, _u, b in api.calls if m == "POST")

    with sqlite3.connect(path) as conn:
        rows = conn.execute("SELECT id, properties FROM notion_rows ORDER BY id").fetchall()
    assert len(rows) == 130
    by_id = {r[0]: json.loads(r[1]) for r in rows}
    third = by_id["00000000-0000-0000-0000-000000000003"]
    assert third["Name"]["title"][0]["plain_text"] == "任务 🚀"
    assert by_id["00000000-0000-0000-0000-000000000002"]["Points"]["number"] is None
    assert by_id["00000000-0000-0000-0000-000000000004"]["Due"]["date"] is None


def _stage(src_id: str, dst_id: str):
    from src.ai.copilot.transfer_tools import start_transfer

    staged = start_transfer(source_connector_id=src_id, source_table="database_rows",
                            dest_connector_id=dst_id, dest_table="notion_rows",
                            sync_mode="full_refresh_overwrite", validation_mode="strict")
    preflight = ((staged.output or {}).get("plan") or staged.output or {}).get("preflight") or {}
    return staged, [str(b.get("message")) for b in preflight.get("blockers") or []]


def test_sample_smaller_than_a_notion_page_asks_for_that_page_size(client, notion, tmp_path):
    """The body ``page_size`` follows the limit: a fixed 100 overran a 25-row
    sample of a larger database and the SDK refused the page."""
    from src.ai.copilot.query_tools import sample_connector_object

    api = notion(130)
    src_id, _dst_id, _ = _connectors(tmp_path)
    out = sample_connector_object(connector_id=src_id, table="database_rows", limit=25)
    assert out.success, out.error
    assert out.output["row_count"] == 25
    queries = [b for m, u, b in api.calls if m == "POST" and u.endswith("/query")]
    assert queries and queries[0].get("page_size") == 25, queries


def test_notion_reader_pages_the_body_cursor_and_honours_offset(notion):
    from connectors.notion import read_object

    api = notion(130)
    batch = read_object(cfg={"api_key": _TOKEN, "database": _DB_ID}, object="database_rows",
                        limit=120, offset=5)
    ids = [row[batch.headers.index("id")] for row in batch.rows]
    assert ids[0] == "00000000-0000-0000-0000-000000000006"
    assert len(ids) == 120 and len(set(ids)) == 120
    posts = [b for m, _u, b in api.calls if m == "POST"]
    assert [b.get("start_cursor") for b in posts] == [None, "100"]
    assert all(m == "POST" for m, _u, _b in api.calls)
