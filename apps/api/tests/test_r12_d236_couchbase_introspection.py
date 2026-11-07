"""D236: an advertised Couchbase source introspects instead of refusing.

get_transfer_capabilities lists couchbase as a live source, yet plan/start
failed "Schema introspection not implemented for couchbase". Introspection now
lists keyspaces and types a keyspace from sampled documents.
"""

from __future__ import annotations

from typing import Any

import pytest

import connectors.couchbase as cb
from services.schema_introspect import introspect_schema


def _docs() -> dict[str, Any]:
    rows = []
    for i in range(1, 16):
        doc: dict[str, Any] = {"id": i, "amount": i + 0.25, "flag": bool(i % 2), "ts": f"2024-03-10T02:30:{i:02d}.012345Z"}
        if i != 7:
            doc["name"] = f"name {i}"
        rows.append({"qaenv_seed": doc})
    return {"results": rows}


@pytest.fixture
def fake_couchbase(monkeypatch: pytest.MonkeyPatch) -> None:
    def _n1ql(_url: str, _u: str, _p: str, statement: str, *_a: Any, **_k: Any) -> Any:
        if "system:keyspaces" in statement:
            return {"results": ["travel", "qaenv_seed"]}
        return _docs()

    monkeypatch.setattr(cb, "_n1ql", _n1ql)
    monkeypatch.setattr(cb, "_url", lambda *_a, **_k: "http://cb.test:8093/query/service")


def test_lists_keyspaces(fake_couchbase: None) -> None:
    out = introspect_schema("couchbase", host="cb.test", port=8093)
    assert out["ok"], out
    assert out["tables"] == ["qaenv_seed", "travel"]


def test_types_sampled_document_keys(fake_couchbase: None) -> None:
    out = introspect_schema("couchbase", host="cb.test", port=8093, table="qaenv_seed")
    assert out["ok"], out
    cols = {c["name"]: c for c in out["columns"]}
    assert set(cols) == {"amount", "flag", "id", "name", "ts"}
    assert cols["id"]["inferred_type"] in {"INTEGER", "BIGINT"}
    assert cols["flag"]["inferred_type"] == "BOOLEAN"
    assert cols["ts"]["inferred_type"].startswith("TIMESTAMPTZ")
    assert all(c["nullable"] for c in cols.values())


def test_capability_row_advertises_introspection() -> None:
    from src.transfer.connector_capabilities import get_capabilities

    assert get_capabilities("couchbase").get("introspect") is True
