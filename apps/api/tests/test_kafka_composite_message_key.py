"""Kafka composite message key carries every key column, not just the first.

Defect: ``kafka_writer.write_mapped_rows`` took ``key_cols[0]`` as the produce
key. With a composite key ``(tenant, id)`` the rows ``(a, 1)`` and ``(b, 1)``
were produced under the same key ``"a"``/``"1"``-style identity, so a compacted
topic kept only one of them and a keyed consumer merged two different
entities. The key is now the canonical JSON object of all key columns in
declared order; a single-column key keeps its plain text so existing topics'
keys do not change. A row with any null key part is refused (never produced
under a partial key).
"""

from __future__ import annotations

import json


class _FakeProducer:
    def __init__(self):
        self.sent: list[dict] = []

    def send(self, topic, value=None, key=None, **kwargs):
        self.sent.append({"topic": topic, "value": value, "key": key})

        class _Ack:
            def get(self, timeout=None):
                return None

        return _Ack()

    def flush(self, timeout=None):
        pass

    def close(self, timeout=None):
        pass


def _produce(monkeypatch, rows, conflict_columns, **extra):
    from connectors import kafka_writer as kw

    fake = _FakeProducer()
    monkeypatch.setattr(kw, "_producer", lambda *a, **k: fake)
    headers = list(rows[0])
    result = kw.write_mapped_rows(
        host="localhost",
        database="ws_topic",
        table_name="ws_topic",
        headers=headers,
        data_rows=[[r[c] for c in headers] for r in rows],
        mappings=[{"source": c, "target": c} for c in headers],
        column_types={"tenant": "string", "id": "integer", "v": "string"},
        write_mode="insert",
        conflict_columns=conflict_columns,
        **extra,
    )
    return fake, result


def test_composite_key_uses_every_key_column_in_order(monkeypatch):
    rows = [
        {"tenant": "a", "id": 1, "v": "x"},
        {"tenant": "b", "id": 1, "v": "y"},
        {"tenant": "a", "id": 2, "v": "z"},
    ]
    fake, result = _produce(monkeypatch, rows, ["tenant", "id"])
    assert result.ok, result.error
    keys = [m["key"] for m in fake.sent]
    # Three distinct entities -> three distinct keys (was "a", "b", "a").
    assert len(set(keys)) == 3, keys
    assert [json.loads(k) for k in keys] == [
        {"tenant": "a", "id": 1},
        {"tenant": "b", "id": 1},
        {"tenant": "a", "id": 2},
    ]
    # Declared order, compact canonical text.
    assert keys[0] == '{"tenant":"a","id":1}'


def test_composite_key_order_follows_declared_columns(monkeypatch):
    rows = [{"tenant": "a", "id": 1, "v": "x"}]
    fake, result = _produce(monkeypatch, rows, ["id", "tenant"])
    assert result.ok, result.error
    assert fake.sent[0]["key"] == '{"id":1,"tenant":"a"}'


def test_single_column_key_is_unchanged(monkeypatch):
    rows = [{"tenant": "a", "id": 42, "v": "x"}]
    fake, result = _produce(monkeypatch, rows, ["id"])
    assert result.ok, result.error
    assert fake.sent[0]["key"] == "42"


def test_composite_key_with_null_part_is_refused(monkeypatch):
    rows = [
        {"tenant": "a", "id": 1, "v": "x"},
        # second key part missing: the old first-column key produced it as "b"
        {"tenant": "b", "id": None, "v": "y"},
    ]
    fake, result = _produce(monkeypatch, rows, ["tenant", "id"])
    # quarantine (default policy) keeps the good row, refuses the partial key
    assert [m["key"] for m in fake.sent] == ['{"tenant":"a","id":1}']
    assert result.rows_written == 1
    assert result.rejected_rows == 1
    detail = result.rejected_details[-1]
    assert detail["row"] == 2
    assert detail["column"] == "id"
    assert "missing" in detail["reason"].lower()
