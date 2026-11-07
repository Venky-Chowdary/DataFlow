"""D236: an advertised InfluxDB source introspects instead of refusing.

get_transfer_capabilities lists influxdb as a live source, yet plan/start
failed "Schema introspection not implemented for influxdb". Introspection now
lists measurements and types a measurement from its declared field types.
"""

from __future__ import annotations

from typing import Any

import pytest

import connectors.influxdb as influx
from services.schema_introspect import introspect_schema

_SHOW = {
    "SHOW MEASUREMENTS": {
        "results": [{"series": [{"name": "measurements", "columns": ["name"], "values": [["qaenv_seed"], ["cpu"]]}]}]
    },
    'SHOW FIELD KEYS FROM "qaenv_seed"': {
        "results": [{"series": [{"name": "qaenv_seed", "columns": ["fieldKey", "fieldType"], "values": [
            ["amount", "float"], ["flag", "boolean"], ["id", "integer"], ["name", "string"],
        ]}]}]
    },
    'SHOW TAG KEYS FROM "qaenv_seed"': {
        "results": [{"series": [{"name": "qaenv_seed", "columns": ["tagKey"], "values": [["host"]]}]}]
    },
}


def _points(n: int = 15) -> dict[str, Any]:
    return {"results": [{"series": [{
        "name": "qaenv_seed",
        "tags": {"host": "h1"},
        "columns": ["time", "amount", "flag", "id", "name"],
        "values": [
            [f"2024-03-10T02:30:{i:02d}.001Z", i + 0.25, bool(i % 2), i, f"name {i}"]
            for i in range(1, n + 1)
        ],
    }]}]}


@pytest.fixture
def fake_influx(monkeypatch: pytest.MonkeyPatch) -> None:
    def _query(_url: str, _db: str, q: str, *_a: Any, **_k: Any) -> Any:
        return _SHOW.get(q) or _points()

    monkeypatch.setattr(influx, "_query", _query)
    monkeypatch.setattr(influx, "_url", lambda *_a, **_k: "http://influx.test:8086")


def test_lists_measurements(fake_influx: None) -> None:
    out = introspect_schema("influxdb", host="influx.test", port=8086, database="qaenv")
    assert out["ok"], out
    assert out["tables"] == ["cpu", "qaenv_seed"]


def test_types_come_from_declared_field_types(fake_influx: None) -> None:
    out = introspect_schema("influxdb", host="influx.test", port=8086, database="qaenv", table="qaenv_seed")
    assert out["ok"], out
    cols = {c["name"]: c for c in out["columns"]}
    assert set(cols) == {"time", "amount", "flag", "host", "id", "name"}
    # A 20-point sample of 1.25..15.25 must not shrink a float64 field to DECIMAL(4,2).
    assert cols["amount"]["inferred_type"] == "DOUBLE PRECISION"
    assert cols["id"]["inferred_type"] == "BIGINT"
    assert cols["flag"]["inferred_type"] == "BOOLEAN"
    assert cols["name"]["inferred_type"] == "TEXT"
    assert cols["host"]["inferred_type"] == "TEXT"
    assert cols["time"]["inferred_type"].startswith("TIMESTAMPTZ")
    assert cols["time"]["nullable"] is False
    assert all(cols[n]["nullable"] for n in ("amount", "flag", "id", "name", "host"))


def test_capability_row_advertises_introspection() -> None:
    from src.transfer.connector_capabilities import get_capabilities

    assert get_capabilities("influxdb").get("introspect") is True
