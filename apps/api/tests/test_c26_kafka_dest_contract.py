"""C26/D88+D90 — Kafka destination contract.

D88: payloads must carry native JSON types (numbers, booleans, nulls, exact
decimals, ISO-8601 timestamps) and a produce key from the declared identity.
D90: non-append modes are refused at preflight; retry decisions are honest
about a topic that cannot be rolled back; the producer is idempotent.
"""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from services.type_system import ddl_type


class _StubProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value=None, key=None, **kwargs):
        self.sent.append({"topic": topic, "value": value, "key": key})

        class _R:
            def get(self, timeout=None):
                return None

        return _R()

    def flush(self, timeout=None):
        pass

    def close(self, timeout=None):
        pass


def _write_rows(
    monkeypatch,
    rows,
    dest_types=None,
    conflict_columns=None,
    extra_kwargs=None,
    write_mode="insert",
):
    from connectors import kafka_writer as kw

    stub = _StubProducer()
    monkeypatch.setattr(kw, "_producer", lambda *a, **k: stub)
    headers = list(rows[0])
    result = kw.write_mapped_rows(
        host="localhost",
        database="topic1",
        table_name="topic1",
        headers=headers,
        data_rows=[[r[c] for c in headers] for r in rows],
        mappings=[{"source": c, "target": c} for c in headers],
        column_types=dest_types or {},
        write_mode=write_mode,
        conflict_columns=conflict_columns or [],
        **(extra_kwargs or {}),
    )
    return stub, result


# ---------------------------------------------------------------- D88: types
def test_kafka_writer_native_json_types(monkeypatch):
    stub, result = _write_rows(
        monkeypatch,
        [
            {
                "id": 7,
                "amt": Decimal("19.5"),
                "big": Decimal("0.12345678901234567890123"),
                "flag": True,
                "note": None,
                "ts": "2024-03-05T10:20:30+05:30",
                "d": "2024-03-05",
            }
        ],
        dest_types={
            "id": "integer",
            "amt": "decimal",
            "big": "decimal",
            "flag": "boolean",
            "ts": "timestamptz",
            "d": "date",
        },
    )
    assert result.ok, result.error
    (msg,) = stub.sent
    payload = msg["value"]
    assert payload["id"] == 7 and isinstance(payload["id"], int)
    # exactly representable decimal → JSON number
    assert payload["amt"] == 19.5 and isinstance(payload["amt"], float)
    # not exactly representable → stays Decimal in the payload and lands on
    # the wire as an exact string, not a rounded float
    assert isinstance(payload["big"], Decimal)
    import json as _json
    from services.value_serializer import json_default as _jd
    wire = _json.loads(_json.dumps(payload, default=_jd))
    assert wire["big"] == "0.12345678901234567890123"
    assert wire["amt"] == 19.5
    assert payload["flag"] is True
    assert payload["note"] is None
    # offset-bearing instant (ISO-8601 space or T form), not bare local time
    assert "+" in payload["ts"] or payload["ts"].endswith("Z")
    assert payload["d"] == "2024-03-05"
    # nothing was stringified wholesale
    assert not isinstance(payload["id"], str)
    assert not isinstance(payload["flag"], str)


def test_kafka_ddl_types_stamp_native_carriers():
    # The SSOT carriers must not collapse to TEXT — that was the D88 root cause
    # (logical type fell through DEFAULT_DDL and every cell was stringified).
    for logical, ddl in [
        ("INTEGER", "integer"),
        ("DECIMAL", "decimal"),
        ("BOOLEAN", "boolean"),
        ("TIMESTAMPTZ", "timestamptz"),
        ("DATETIME", "datetime"),
        ("DATE", "date"),
        ("JSON", "json"),
    ]:
        assert ddl_type("kafka", logical) == ddl, (logical, ddl_type("kafka", logical))


# ------------------------------------------------------------ D88: produce key
def test_kafka_writer_sets_message_key_from_pk(monkeypatch):
    stub, result = _write_rows(
        monkeypatch,
        [{"id": 42, "v": "x"}, {"id": 43, "v": "y"}],
        dest_types={"id": "integer", "v": "string"},
        conflict_columns=["id"],
    )
    assert result.ok, result.error
    keys = [m["key"] for m in stub.sent]
    assert keys == ["42", "43"]

    # upsert_key/dedupe_key style carriers resolve through the same fallback
    stub2, result2 = _write_rows(
        monkeypatch,
        [{"id": 9, "v": "x"}],
        extra_kwargs={"message_key_columns": ["id"]},
    )
    assert result2.ok, result2.error
    assert stub2.sent[0]["key"] == "9"


def test_declared_identity_columns_reads_aliases():
    from services.sync_cursor import declared_identity_columns

    req = SimpleNamespace(
        stream_contracts=[{"primary_key": "a"}],
        read_options={},
        source_filter={},
    )
    assert declared_identity_columns(req) == ["a"]

    req = SimpleNamespace(
        stream_contracts=[],
        read_options={"upsert_key": "k1,k2"},
        source_filter={},
    )
    assert declared_identity_columns(req) == ["k1", "k2"]

    req = SimpleNamespace(
        stream_contracts=[],
        read_options={},
        source_filter={"dedupe_key": ["d"]},
    )
    assert declared_identity_columns(req) == ["d"]

    empty = SimpleNamespace(stream_contracts=[], read_options={}, source_filter={})
    assert declared_identity_columns(empty) == []


# ----------------------------------------------------- D90: preflight refusal
def _gates_for(sync_mode, dest="apache_kafka"):
    from services.preflight_service import run_transfer_policy_gates

    return run_transfer_policy_gates(
        sync_mode=sync_mode,
        dest_type=dest,
        source_type="postgresql",
        source_kind="database",
        stream_contracts=[],
        mappings=[],
    )


def test_kafka_dest_non_append_refused_at_preflight():
    for mode in (
        "full_refresh_overwrite",
        "incremental_deduped",
        "upsert",
        "mirror",
        "scd2",
        "reverse_etl",
        "cdc",
    ):
        gates = _gates_for(mode)
        gate = next(g for g in gates if g["id"] == "g_dest_append_only")
        assert gate["status"] == "block", (mode, gate)
        assert "append" in gate["message"].lower()

    for mode in ("full_refresh_append", "incremental_append"):
        gates = _gates_for(mode)
        gate = next(g for g in gates if g["id"] == "g_dest_append_only")
        assert gate["status"] == "pass", (mode, gate)


def test_kafka_family_aliases_covered():
    for dest in (
        "kafka",
        "apache_kafka",
        "confluent_kafka",
        "amazon_msk",
        "redpanda",
    ):
        gates = _gates_for("upsert", dest=dest)
        gate = next(g for g in gates if g["id"] == "g_dest_append_only")
        assert gate["status"] == "block", dest


# --------------------------------------------------------- D90: retry honesty
def test_kafka_dest_retry_decision_not_converges():
    from services.execution_engine_contract import decide_retry_from_start

    # Keyed append with committed rows must NOT claim convergence.
    d = decide_retry_from_start(
        status="failed",
        sync_mode="full_refresh_append",
        rows_committed=1500,
        rows_committed_known=True,
        operator_requested=True,
        has_key_contract=True,
        has_cursor_contract=False,
        destination_engine="apache_kafka",
    )
    assert d["allowed"] is False
    assert "1500" in d["reason"]
    assert "rolled back" in d["reason"].lower()

    # A keyed non-kafka dest still converges as before (no regression).
    d2 = decide_retry_from_start(
        status="failed",
        sync_mode="full_refresh_append",
        rows_committed=1500,
        operator_requested=True,
        has_key_contract=True,
        has_cursor_contract=False,
        destination_engine="postgresql",
    )
    assert d2["allowed"] is True

    # Incremental+cursor honestly continues from the watermark.
    d3 = decide_retry_from_start(
        status="failed",
        sync_mode="incremental_append",
        rows_committed=800,
        operator_requested=True,
        has_key_contract=False,
        has_cursor_contract=True,
        destination_engine="kafka",
    )
    assert d3["allowed"] is True
    assert d3["kind"] == "from_cursor_contract"


def test_kafka_writer_runtime_refusal_is_append_only(monkeypatch):
    _, result = _write_rows(
        monkeypatch,
        [{"a": 1}],
        write_mode="merge",
    )
    assert result.ok is False
    assert "append" in (result.error or "").lower()


def test_kafka_idempotent_producer_configured():
    import inspect
    from connectors import kafka_writer as kw

    src = inspect.getsource(kw._producer)
    assert "enable_idempotence" in src
