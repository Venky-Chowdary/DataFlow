"""D242: counting a vector collection/class uses the engine's own count.

rank_connector_tables and aggregate count sent Qdrant and Weaviate through
the SQL aggregation path, so every object was skipped with "SQLAlchemy
dialect/driver for 'qdrant' is not available". Qdrant reports
``points_count``; Weaviate reports ``Aggregate { meta { count } }``.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest

COUNTS = {"qaenv_seed": 30, "big_points": 1200, "QaenvSeed": 30, "BigClass": 1200}


class _Resp:
    def __init__(self, status: int, body: Any) -> None:
        self.status_code = status
        self._body = body
        self.text = json.dumps(body)

    def json(self) -> Any:
        return self._body


class _Session:
    def get(self, url: str, **_k: Any) -> _Resp:
        name = url.rstrip("/").rsplit("/", 1)[-1]
        if name not in COUNTS:
            return _Resp(404, {"status": {"error": "not found"}})
        return _Resp(200, {"result": {"status": "green", "points_count": COUNTS[name], "segments_count": 8}})

    def post(self, url: str, data: str = "", **_k: Any) -> _Resp:
        query = json.loads(data)["query"]
        cls = query.split("Aggregate {", 1)[1].split("{", 1)[0].strip()
        return _Resp(200, {"data": {"Aggregate": {cls: [{"meta": {"count": COUNTS[cls]}}]}}})


@pytest.fixture
def vector_engine(monkeypatch: pytest.MonkeyPatch):
    import connectors.qdrant_writer as qw
    import connectors.weaviate_writer as ww
    import src.ai.copilot.schema_tools as st
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.query_tools import _tool_result

    monkeypatch.setattr(qw, "_requests_session", lambda: _Session())
    monkeypatch.setattr(ww, "_requests_session", lambda: _Session())
    objects = {"qdrant": ["qaenv_seed", "big_points"], "weaviate": ["QaenvSeed", "BigClass"]}
    made: dict[str, str] = {}

    def _listed(connector_id: str = "", **_k: Any):
        kind = next(k for k, v in made.items() if v == connector_id)
        return _tool_result("list_connector_objects", success=True, output={"objects": objects[kind]})

    monkeypatch.setattr(st, "list_connector_objects", _listed)
    for kind, port in (("qdrant", 6333), ("weaviate", 8080)):
        made[kind] = create_connector(
            {"name": f"d242-{kind}-{uuid.uuid4().hex[:6]}", "type": kind, "host": "vec.d242.test", "port": port}
        ).id
    yield made
    for cid in made.values():
        delete_connector(cid)


@pytest.mark.parametrize("kind,big,small", [("qdrant", "big_points", "qaenv_seed"), ("weaviate", "BigClass", "QaenvSeed")])
def test_rank_counts_vector_objects_natively(vector_engine, kind: str, big: str, small: str) -> None:
    from src.ai.copilot.aggregate_tools import rank_connector_tables

    res = rank_connector_tables(connector_id=vector_engine[kind], limit=5)
    assert res.success, res.error
    out = res.output or {}
    assert not out.get("skipped"), out.get("skipped")
    ranked = [(r.get("table"), r.get("rows")) for r in out.get("ranked") or []]
    assert ranked[:2] == [(big, 1200), (small, 30)], out


@pytest.mark.parametrize("kind,name", [("qdrant", "qaenv_seed"), ("weaviate", "QaenvSeed")])
def test_aggregate_count_uses_native_count(vector_engine, kind: str, name: str) -> None:
    from src.ai.copilot.aggregate_tools import aggregate_connector_data

    res = aggregate_connector_data(connector_id=vector_engine[kind], table=name, metric="count")
    assert res.success, res.error
    assert (res.output or {}).get("value") == 30
