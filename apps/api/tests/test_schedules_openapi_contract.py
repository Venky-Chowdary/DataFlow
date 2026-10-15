"""Schedules OpenAPI stays aligned with the typed web client.

The path list is apps/web/src/lib/schedulesApiContract.json. A client edit
that is not in this file fails the web test; a route edit that drops one of
these operations fails here.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
_REPO = _API_ROOT.parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.main import app

_CONTRACT = json.loads(
    (_REPO / "apps/web/src/lib/schedulesApiContract.json").read_text(encoding="utf-8")
)


def _schema(openapi: dict, ref: str) -> dict:
    name = ref.rsplit("/", 1)[-1]
    return openapi["components"]["schemas"][name]


def test_schedules_openapi_matches_typed_client():
    openapi = app.openapi()
    paths = openapi["paths"]
    for operation in _CONTRACT["operations"]:
        path = operation["openapi"]
        assert path in paths, path
        spec = paths[path].get(operation["method"])
        assert spec is not None, f"{operation['method'].upper()} {path}"

    create = paths["/api/v1/schedules/"]["post"]
    body = create["requestBody"]["content"]["application/json"]["schema"]
    if "$ref" in body:
        body = _schema(openapi, body["$ref"])
    required = set(body.get("required") or [])
    for field in _CONTRACT["createRequired"]:
        assert field in required, field

    update = paths["/api/v1/schedules/{schedule_id}"]["patch"]
    update_body = update["requestBody"]["content"]["application/json"]["schema"]
    if "$ref" in update_body:
        update_body = _schema(openapi, update_body["$ref"])
    properties = update_body.get("properties") or {}
    for field in _CONTRACT["updateFields"]:
        assert field in properties, field

    history = paths["/api/v1/schedules/{schedule_id}/history"]["get"]
    assert history is not None
    run = paths["/api/v1/schedules/{schedule_id}/run"]["post"]
    assert run is not None
