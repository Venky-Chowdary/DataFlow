"""D274 / D277: one api-key resolution for every HTTP vector and SaaS tile.

QA: a Zilliz Cloud key declared with ``auth_mode=api_key`` and pasted in the
password (or username) field was sent as ``Bearer root:<key>`` / ``Bearer
<key>:`` — 401 (D274). An Airtable PAT in the password field never reached
``Authorization: Bearer`` ("Bearer token is not configured"), and the bare form
host ``api.airtable.com`` was refused "only absolute HTTP(S) URLs" (D277).
Pinecone / Weaviate / Qdrant each carried their own ``api_key or password or
username`` chain; ``services.connector_auth.resolve_api_key`` is now the one rule.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

KEY = "zilliz-api-key-0123456789abcdef"


class _Seen:
    headers: list[str] = []
    paths: list[str] = []


def _handler(ok_auth: str):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *_a):  # noqa: D401
            pass

        def _answer(self):
            auth = self.headers.get("Authorization") or ""
            _Seen.headers.append(auth)
            _Seen.paths.append(self.path)
            if auth != f"Bearer {ok_auth}":
                body = json.dumps({"code": 1800, "message": "unauthorized"}).encode()
                self.send_response(401)
            else:
                body = json.dumps({"code": 0, "data": ["c1"], "records": [], "tables": []}).encode()
                self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = do_POST = _answer

    return H


@pytest.fixture
def stub():
    def _start(ok_auth: str) -> int:
        srv = HTTPServer(("127.0.0.1", 0), _handler(ok_auth))
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        _Seen.headers.clear()
        _Seen.paths.clear()
        return srv.server_address[1]

    return _start


# --- the one rule ----------------------------------------------------------


def test_declared_api_key_mode_takes_the_password_then_username():
    from services.connector_auth import resolve_api_key

    assert resolve_api_key({"auth_mode": "api_key", "password": KEY}) == KEY
    assert resolve_api_key({"auth_mode": "api_key", "username": KEY}) == KEY
    assert resolve_api_key({"auth_mode": "api_key", "password": KEY}, password_is_key=False) == KEY
    assert resolve_api_key({"api_key": "explicit", "password": KEY}) == "explicit"


def test_undeclared_user_password_is_a_login_for_milvus_but_a_key_for_key_only_tiles():
    from services.connector_auth import resolve_api_key

    login = {"username": "alice", "password": "pw"}
    # Pinecone / Weaviate / Qdrant keep their fallback: the password is the key.
    assert resolve_api_key(login) == "pw"
    # Milvus: root:Milvus style login, not a bare key.
    assert resolve_api_key(login, password_is_key=False) == ""
    assert resolve_api_key({}) == ""


def test_validate_probe_auth_accepts_a_key_pasted_in_the_password_field():
    from services.connector_auth import validate_probe_auth

    assert (
        validate_probe_auth(
            driver="airtable", auth_mode="api_key", host="api.airtable.com", port=443, password="pat-x"
        )
        is None
    )
    assert validate_probe_auth(driver="airtable", auth_mode="api_key", host="api.airtable.com") == (
        "API key is required."
    )


# --- Milvus / Zilliz (D274) ------------------------------------------------


@pytest.mark.parametrize("field", ["password", "username"])
def test_milvus_probe_sends_a_declared_key_bare(stub, field):
    from src.transfer.connector_registry import run_probe

    port = stub(KEY)
    ok, msg = run_probe(
        "milvus",
        {"host": "127.0.0.1", "port": port, "ssl": False, "auth_mode": "api_key", field: KEY, "type": "milvus"},
    )
    assert ok, msg
    assert _Seen.headers == [f"Bearer {KEY}"]
    assert _Seen.paths[0].endswith("/v2/vectordb/collections/list")


def test_milvus_probe_without_a_declared_mode_keeps_the_user_password_login(stub):
    from src.transfer.connector_registry import run_probe

    port = stub("alice:pw")
    ok, msg = run_probe(
        "milvus",
        {"host": "127.0.0.1", "port": port, "ssl": False, "username": "alice", "password": "pw", "type": "milvus"},
    )
    assert ok, msg
    assert _Seen.headers == ["Bearer alice:pw"]


def test_milvus_default_login_when_no_credentials():
    from connectors.milvus_writer import _auth_token

    assert _auth_token() == "root:Milvus"
    assert _auth_token(password=KEY, auth_mode="api_key") == KEY
    assert _auth_token(username="root", password=KEY) == f"root:{KEY}"


# --- Airtable (D277) --------------------------------------------------------


def test_airtable_bare_form_host_becomes_an_https_origin_and_the_pat_the_bearer_key():
    from connectors.sdk.airtable import AirtableSource
    from connectors.sdk.transfer_bridge import _connector_config

    cfg = _connector_config(
        {
            "type": "airtable",
            "host": "api.airtable.com",
            "port": 443,
            "ssl": True,
            "database": "appIJJuyLVheGLPlz",
            "auth_mode": "api_key",
            "password": "pat-82-bytes",
            "username": "",
            "api_key": "",
        },
        "airtable",
    )
    assert cfg["base_url"] == "https://api.airtable.com"
    assert cfg["api_key"] == "pat-82-bytes"
    # The manifest parser accepted the origin (no "only absolute HTTP(S) URLs").
    source = AirtableSource({**cfg, "base_id": "appIJJuyLVheGLPlz"})
    assert source._base_url == "https://api.airtable.com/"
    assert source.auth.headers["Authorization"] == "Bearer pat-82-bytes"


def test_airtable_check_dials_with_the_password_held_pat(stub):
    from connectors.sdk.transfer_bridge import test_sdk_connector

    port = stub("pat-82-bytes")
    ok, msg = test_sdk_connector(
        {
            "type": "airtable",
            "host": f"http://127.0.0.1:{port}",
            "database": "appIJJuyLVheGLPlz",
            "auth_mode": "api_key",
            "password": "pat-82-bytes",
        },
        "airtable",
    )
    assert ok, msg
    assert _Seen.headers and set(_Seen.headers) == {"Bearer pat-82-bytes"}
    # A base with no table yet is proven on the base's table list (the call QA
    # ran by hand), not refused for a missing table_id.
    assert _Seen.paths[0].startswith("/v0/meta/bases/appIJJuyLVheGLPlz/tables")


def test_absolute_http_url_forms():
    from connectors.saas_common import absolute_http_url

    assert absolute_http_url("api.airtable.com", port=443, ssl=True) == "https://api.airtable.com"
    assert absolute_http_url("api.airtable.com") == "https://api.airtable.com"
    assert absolute_http_url("http://127.0.0.1:8123", port=443) == "http://127.0.0.1:8123"
    assert absolute_http_url("127.0.0.1", port=8123, ssl=False) == "http://127.0.0.1:8123"
    assert absolute_http_url("", port=443) == ""
