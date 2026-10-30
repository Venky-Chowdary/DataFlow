"""QA demo (557f556c): Studio Cloud-storage source on an S3-compatible endpoint.

QA's connector is Amazon S3 with a custom Endpoint URL and path-style
addressing. Test connection passes ("2 object(s) listed"), but Studio ->
New transfer -> Cloud storage fails for every path form
(``s3://bucket/prefix/``, a full object key, no scheme) with
"Access denied to bucket ... check IAM policy"; Retry says "Could not read the
source schema". The same credential works with boto3 against the endpoint in
path style. MCP plan/start was fixed in round 4 (D142); the Studio REST route
was not.

The S3 wire protocol here is ``moto``'s server on a non-default port, behind a
gate that refuses virtual-hosted requests (path style forced, as on QA's
endpoint). The SDK's default S3 endpoint is pointed at a second local server
that answers every request 403 -- what AWS answers QA's demo credential -- so a
client built without the connector's endpoint reproduces QA's exact message
instead of dialling the internet.

The exact calls the web app makes (apps/web/src/lib/api.ts, TransferPage.tsx):
create the connector (``POST /connectors/saved``), Test, ``POST
/transfer/introspect`` (object listing, profile, preview), ``POST
/preflight/run`` (Validate) and ``POST /transfer/execute`` (Execute) into a new
PostgreSQL table, for a prefix and for a single key, then a second run into the
table run 1 created. Values cover NULL, '', whitespace and unicode.
"""
from __future__ import annotations

import csv
import io
import threading
import uuid
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

_HEADER = ["id", "label", "amount"]
_FILE_A = [("1", "alpha", "10.5"), ("2", "", "0"), ("3", "Zoë ✓ 中文", "-3.25")]
_FILE_B = [("4", "  padded  ", ""), ("5", "omega", "7")]
_PREFIX = "sales/2026-10/"
_KEY_A = _PREFIX + "orders_a.csv"
_KEY_B = _PREFIX + "orders_b.csv"
_KEY = "test"
_SECRET = "test-secret"


def _csv(rows) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(_HEADER)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


class _Server:
    def __init__(self, app) -> None:
        from werkzeug.serving import make_server

        self._srv = make_server("127.0.0.1", 0, app, threaded=True)
        self.port = self._srv.server_port
        self._t = threading.Thread(target=self._srv.serve_forever, daemon=True)
        self._t.start()

    def stop(self) -> None:
        self._srv.shutdown()


_DENIED = (
    b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code>'
    b"<Message>Access Denied</Message></Error>"
)


def _deny_all(environ, start_response):
    """Stands in for AWS refusing a credential that only the custom store knows."""
    start_response("403 Forbidden", [("Content-Type", "application/xml")])
    return [b"" if environ.get("REQUEST_METHOD") == "HEAD" else _DENIED]


def _path_style_only(app, expected_host: str):
    """Refuse virtual-hosted requests (``bucket.host``): path style is forced."""

    def gate(environ, start_response):
        host = str(environ.get("HTTP_HOST") or "")
        if host != expected_host:
            start_response("403 Forbidden", [("Content-Type", "application/xml")])
            return [_DENIED]
        return app(environ, start_response)

    return gate


@pytest.fixture(scope="module")
def s3_store():
    boto3 = pytest.importorskip("boto3")
    pytest.importorskip("moto.server")
    from moto.server import DomainDispatcherApplication, create_backend_app

    moto_app = DomainDispatcherApplication(create_backend_app)
    holder: dict[str, Any] = {}

    def lazy(environ, start_response):
        return holder["app"](environ, start_response)

    store = _Server(lazy)
    assert store.port not in (80, 443, 9000)
    holder["app"] = _path_style_only(moto_app, f"localhost:{store.port}")
    aws = _Server(_deny_all)
    endpoint = f"http://localhost:{store.port}"
    from botocore.config import Config

    direct = boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=_KEY,
        aws_secret_access_key=_SECRET, region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    bucket = "r7-demo-" + uuid.uuid4().hex[:8]
    direct.create_bucket(Bucket=bucket)
    direct.put_object(Bucket=bucket, Key=_KEY_A, Body=_csv(_FILE_A))
    direct.put_object(Bucket=bucket, Key=_KEY_B, Body=_csv(_FILE_B))
    # QA's control: the credential works directly in path style.
    assert direct.head_bucket(Bucket=bucket)
    try:
        yield {"endpoint": endpoint, "bucket": bucket, "aws": f"http://127.0.0.1:{aws.port}"}
    finally:
        store.stop()
        aws.stop()


@pytest.fixture
def client(tmp_path, monkeypatch, s3_store):
    h.isolate(tmp_path, monkeypatch)
    # Anything that loses the connector's endpoint dials "AWS" -> 403.
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", s3_store["aws"])
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


# ConnectorModal saves the Endpoint URL's port; a connector saved by an older
# form (or with an https endpoint that names no port) keeps the 443 default.
_SHAPES = ["port_from_url", "form_default_443"]


def _connector(client, s3_store, shape: str) -> dict:
    """What ConnectorModal posts for S3 with a custom endpoint + path style."""
    url_port = int(s3_store["endpoint"].rsplit(":", 1)[1])
    body = {
        "name": "Sales files (S3-compatible) " + uuid.uuid4().hex[:6],
        "type": "s3", "role": "source", "host": "us-east-1",
        "port": url_port if shape == "port_from_url" else 443,
        "database": s3_store["bucket"], "username": _KEY, "password": _SECRET,
        "schema": "", "ssl": False,
        "endpoint_url": s3_store["endpoint"], "path_style": True,
    }
    resp = client.post("/api/v1/connectors/saved", json=body)
    assert resp.status_code in (200, 201), resp.text
    conn = resp.json()
    test = client.post(f"/api/v1/connectors/saved/{conn['id']}/test")
    assert test.status_code == 200, test.text
    tested = test.json()
    assert tested.get("success", tested.get("ok")) is True, tested
    assert "2 object(s)" in str(tested.get("message") or ""), tested
    return conn


def _source(conn: dict, path: str) -> dict:
    """TransferPage.buildSourceEndpoint for sourceKind === 'cloud'."""
    return {
        "kind": "database", "format": "s3", "connector_id": conn["id"],
        "database": conn.get("database") or "", "table": path,
    }


def _dest(dst_id: str, table: str) -> dict:
    return {"kind": "database", "format": "postgresql", "connector_id": dst_id,
            "table": table, "schema": "public"}


def _norm(rows) -> list[tuple]:
    def amt(v):
        return None if v in (None, "") else float(v)

    return sorted(
        ((str(int(r[0])), "" if r[1] is None else str(r[1]), amt(r[2])) for r in rows),
        key=lambda r: int(r[0]),
    )


_PATHS = {
    "prefix_s3_url": ("s3://{bucket}/" + _PREFIX, _FILE_A + _FILE_B),
    "prefix_no_scheme": (_PREFIX, _FILE_A + _FILE_B),
    "key_s3_url": ("s3://{bucket}/" + _KEY_A, _FILE_A),
    "key_no_scheme": (_KEY_A, _FILE_A),
}


@pytest.mark.parametrize("shape", _SHAPES)
@pytest.mark.parametrize("form", list(_PATHS))
def test_studio_cloud_storage_source_on_s3_compatible_endpoint(client, s3_store, form, shape):
    from services.connector_store import create_connector

    path_t, expected = _PATHS[form]
    path = path_t.format(bucket=s3_store["bucket"])
    conn = _connector(client, s3_store, shape)
    dst = create_connector({
        "name": "r7s3-dst-" + uuid.uuid4().hex[:6], "role": "destination",
        "workspace_id": "", "ssl": False, **h.dest_config("postgresql"),
    })
    table = "r7s3_" + uuid.uuid4().hex[:8]
    src = _source(conn, path)
    try:
        # Object listing, profile and preview (TransferPage.introspectOneStream).
        resp = client.post("/api/v1/transfer/introspect",
                           json={"source": src, "destination": _dest(dst.id, table)})
        assert resp.status_code == 200, resp.text
        intro = resp.json()["source"]
        assert intro.get("connected") is True, intro.get("message") or intro.get("error")
        assert "Access denied" not in str(intro), intro.get("message")
        names = {o.get("name") for o in intro.get("objects") or []}
        assert {_KEY_A, _KEY_B} <= names, names
        cols = [c.get("name") if isinstance(c, dict) else c for c in intro.get("columns") or []]
        assert cols == _HEADER, (intro.get("message"), intro.get("sample_error"))
        sample = intro.get("data") or intro.get("sample_data") or []
        assert sample, intro.get("message")

        mappings = [{"source": c, "target": c, "confidence": 1.0} for c in _HEADER]
        col_types = {c: "text" for c in _HEADER}

        def validate() -> dict:
            pf = client.post("/api/v1/preflight/run", json={
                "dataset_name": table, "columns": _HEADER, "column_types": col_types,
                "mappings": mappings, "source_connector_id": conn["id"],
                "source_kind": "database", "source_type": "s3", "source_table": path,
                "connector_id": dst.id, "dest_kind": "database",
                "dest_type": "postgresql", "dest_table": table,
                "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
            })
            assert pf.status_code == 200, pf.text
            body = pf.json()
            assert "Access denied" not in str(body), body
            blockers = [b for b in body.get("blockers") or [] if isinstance(b, dict)]
            assert not blockers, [(b.get("id"), b.get("message")) for b in blockers]
            return body

        def execute() -> dict:
            resp = client.post("/api/v1/transfer/execute", json={
                "source": src, "destination": _dest(dst.id, table),
                "mappings": mappings, "sync_mode": "full_refresh_overwrite",
                "validation_mode": "strict", "schema_policy": "manual_review",
                "async_mode": False, "skip_preflight": True,
            })
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body.get("success") is True, body
            from services.mongodb_service import get_mongodb_service

            job = get_mongodb_service().get_job(str(body["job_id"])) or {}
            assert str(job.get("status")) == "completed", {
                k: job.get(k) for k in ("status", "error", "message")
            }
            return job

        for _run in (1, 2):  # run 2 lands in the table run 1 created
            validate()
            execute()
            got = h.query("postgresql", f'SELECT id, label, amount FROM "{table}"')  # nosec B608
            assert _norm(got) == _norm(expected), {"run": _run, "rows": got}
    finally:
        h.drop_table("postgresql", table)

