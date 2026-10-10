"""Composite identity, dial ports, fleet pool, and specialty read paths.

Each case is the gap the v4 audit still treated as a stub. A pass is that
fixture only — specialty engines are not marked transfer-live.
"""

from __future__ import annotations

import threading

from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)
from src.transfer.models import EndpointConfig


def _plan(*, contract: str, sync_mode: str = "upsert") -> TransferPlan:
    return TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            columns=[
                ColumnSchema(name="id", inferred_type="INTEGER"),
                ColumnSchema(name="tenant_id", inferred_type="VARCHAR"),
            ],
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[
                ColumnSchema(name="id", inferred_type="INTEGER"),
                ColumnSchema(name="tenant_id", inferred_type="VARCHAR"),
            ],
        ),
        mappings=[
            ColumnMapping(source="id", target="id", confidence=0.99),
            ColumnMapping(source="tenant_id", target="tenant_id", confidence=0.99),
        ],
        ddl_compatible=True,
        validation_mode="strict",
        sync_mode=sync_mode,
        contract_primary_key=contract,
    )


def test_comma_contract_is_not_one_column_and_does_not_invent_id():
    from services.primary_key import (
        resolve_identity_key,
        resolve_primary_key_columns,
    )

    src, tgt = resolve_identity_key(
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "email", "target": "email"},
        ],
        source_columns=["id", "email"],
        dest_kind="postgresql",
        purpose="uniqueness",
        contract_primary_key="id,tenant_id",
    )
    assert src is None and tgt is None
    sources, targets = resolve_primary_key_columns(
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "email", "target": "email"},
        ],
        source_columns=["id", "email"],
        dest_kind="postgresql",
        contract_primary_key="id,tenant_id",
    )
    assert sources == ["id", "tenant_id"]
    assert targets == ["id", "tenant_id"]
    renamed_src, renamed_tgt = resolve_primary_key_columns(
        mappings=[
            {"source": "org_id", "target": "organization_id"},
            {"source": "code", "target": "code"},
        ],
        source_columns=["org_id", "code", "name"],
        dest_kind="postgresql",
        destination_pk_columns=["organization_id", "code"],
    )
    assert renamed_src == ["org_id", "code"]
    assert renamed_tgt == ["organization_id", "code"]


def test_single_column_equality_key_is_unchanged():
    from services.type_system import composite_unique_equality_key, unique_equality_key

    alone = unique_equality_key("Acme", "VARCHAR")
    joined = composite_unique_equality_key([("Acme", "VARCHAR", False, None)])
    assert joined == alone
    assert "\x1f" not in joined


def test_sample_probe_uses_the_whole_composite():
    from services.preflight_service import FilePreflightContext

    ctx = FilePreflightContext(
        _plan(contract="id,tenant_id"),
        sample_rows=[
            {"id": "1", "tenant_id": "a"},
            {"id": "1", "tenant_id": "b"},
        ],
    )
    assert ctx.probe_unique_constraint(["id", "tenant_id"]) == []
    ctx.sample_rows = [
        {"id": "1", "tenant_id": "a"},
        {"id": "1", "tenant_id": "a"},
    ]
    dupes = ctx.probe_unique_constraint(["id", "tenant_id"])
    assert len(dupes) == 1
    assert dupes[0]["column"] == "id, tenant_id"
    assert dupes[0]["count"] == 2
    # The first column by itself is still a duplicate. That is a different key.
    first_only = ctx.probe_unique_constraint(["id"])
    assert first_only and first_only[0]["column"] == "id"


def test_g6_blocks_a_repeated_composite_and_passes_a_repeated_prefix():
    from preflight.gates import gate_g6_target_ddl
    from services.preflight_service import FilePreflightContext

    shared_prefix = FilePreflightContext(
        _plan(contract="id,tenant_id"),
        sample_rows=[
            {"id": "1", "tenant_id": "a"},
            {"id": "1", "tenant_id": "b"},
        ],
    )
    passed = gate_g6_target_ddl(shared_prefix)
    assert passed.status.value == "pass"

    same_pair = FilePreflightContext(
        _plan(contract="id,tenant_id"),
        sample_rows=[
            {"id": "1", "tenant_id": "a"},
            {"id": "1", "tenant_id": "a"},
        ],
    )
    blocked = gate_g6_target_ddl(same_pair)
    assert blocked.status.value == "block"
    assert "id, tenant_id" in blocked.message


def test_runtime_probe_hashes_every_column():
    from services.preflight_runtime import RuntimePreflightContext

    class _Rows(RuntimePreflightContext):
        def _load_sample(self):
            return (
                ["id", "tenant_id"],
                [["1", "a"], ["1", "b"], ["1", "a"]],
                {},
            )

    ctx = _Rows(_plan(contract="id,tenant_id"))
    dupes = ctx.probe_unique_constraint(["id", "tenant_id"])
    assert len(dupes) == 1
    assert dupes[0]["count"] == 2
    assert ctx.probe_unique_constraint(["id", "missing"]) == []


def test_base_context_still_has_no_sample_probe():
    assert PreflightContext(_plan(contract="id")).probe_unique_constraint(["id"]) == []


def test_inline_historical_port_does_not_override_a_saved_tunnel(monkeypatch):
    from transfer import adapters
    from transfer.adapters import resolve_connector_config

    saved = {
        "host": "cache.internal",
        "port": 6380,
        "database": "0",
        "type": "redis",
        "username": "",
        "password": "secret",
    }
    monkeypatch.setattr(adapters, "_lookup_saved_connector", lambda *_a, **_k: saved)
    cfg = resolve_connector_config(
        EndpointConfig(
            format="redis",
            connector_id="redis-box",
            host="cache.internal",
            port=5432,
        )
    )
    assert cfg["port"] == 6380

    unset = resolve_connector_config(
        EndpointConfig(format="redis", host="cache.internal", port=5432)
    )
    assert unset["port"] == 6379

    explicit = resolve_connector_config(
        EndpointConfig(format="redis", host="cache.internal", port=6381)
    )
    assert explicit["port"] == 6381

    postgres = resolve_connector_config(
        EndpointConfig(format="postgresql", host="db.internal", port=5432)
    )
    assert postgres["port"] == 5432

    neo = resolve_connector_config(
        EndpointConfig(format="neo4j", host="graph.internal", port=7687)
    )
    assert neo["port"] == 7474


def test_vector_destinations_stream_and_neo4j_is_source_only():
    from src.transfer.stream import _STREAMING_TYPES, supports_streaming

    assert "postgresql" in _STREAMING_TYPES
    assert "salesforce" not in _STREAMING_TYPES
    assert "hubspot" not in _STREAMING_TYPES
    assert "weaviate" not in _STREAMING_TYPES
    assert "neo4j" not in _STREAMING_TYPES

    pg = EndpointConfig(kind="database", format="postgresql")
    weaviate = EndpointConfig(kind="database", format="weaviate")
    assert supports_streaming(pg, weaviate) is True
    assert supports_streaming(weaviate, pg) is False
    neo = EndpointConfig(kind="database", format="neo4j")
    assert supports_streaming(neo, pg) is True
    assert supports_streaming(pg, neo) is False
    for dest in ("pinecone", "milvus"):
        assert supports_streaming(pg, EndpointConfig(kind="database", format=dest)) is True
        assert supports_streaming(EndpointConfig(kind="database", format=dest), pg) is False
    # pgvector reads through the PostgreSQL reader (MX2-12), so it streams both ways.
    pgv = EndpointConfig(kind="database", format="pgvector")
    assert supports_streaming(pg, pgv) is True
    assert supports_streaming(pgv, pg) is True


def test_buffered_kafka_read_uses_the_topic_reader(monkeypatch):
    from connectors.base import ReadBatch
    from transfer.adapters import read_source_database

    seen: dict = {}

    def fake(**kwargs):
        seen.update(kwargs)
        return ReadBatch(headers=["k"], rows=[["v"]], total_rows=1), {"delivery": "at_least_once"}

    monkeypatch.setattr("connectors.kafka_reader.read_topic_batch", fake)
    records, headers, schema = read_source_database(
        EndpointConfig(format="kafka", host="broker.internal", port=9092, table="orders"),
        raise_on_truncate=True,
    )
    assert headers == ["k"]
    assert records == [{"k": "v"}]
    assert seen["topic"] == "orders"
    assert schema["k"]


def test_neo4j_streaming_read_pages_by_offset(monkeypatch):
    from connectors.base import ReadBatch
    from src.transfer.batch_readers import _read_batch_impl

    seen: dict = {}

    def fake(**kwargs):
        seen.update(kwargs)
        return ReadBatch(headers=["_neo4j_element_id"], rows=[["e1"]])

    monkeypatch.setattr("connectors.neo4j.read_object", fake)
    batch = _read_batch_impl("neo4j", {"host": "graph.internal", "port": 7474}, "Person", None, 20, 50)
    assert batch.headers == ["_neo4j_element_id"]
    assert seen["object"] == "Person"
    assert seen["offset"] == 20
    assert seen["limit"] == 50


def test_fleet_executor_grows_to_the_later_cap():
    from services import worker_fleet as wf

    previous = wf._fleet_pool
    wf._fleet_pool = None
    wf._fleet_pool_cap = 0
    small = None
    try:
        small = wf.fleet_executor(2)
        assert small._max_workers == 2
        large = wf.fleet_executor(8)
        assert large is not small
        assert large._max_workers == 8
        assert wf.fleet_executor(4) is large
    finally:
        current = wf._fleet_pool
        wf._fleet_pool = previous
        wf._fleet_pool_cap = 0 if previous is None else wf._fleet_pool_cap
        if current is not None and current is not previous:
            current.shutdown(wait=False, cancel_futures=True)
        if small is not None and small is not current and small is not previous:
            small.shutdown(wait=False, cancel_futures=True)


def test_concurrent_fleet_loop_opens_the_pool_before_the_first_claim(monkeypatch):
    from services import worker_fleet as wf

    stop = threading.Event()
    seen: dict = {}

    def claim(_store):
        seen["cap"] = wf._fleet_pool_cap
        seen["workers"] = None if wf._fleet_pool is None else wf._fleet_pool._max_workers
        stop.set()
        return None

    monkeypatch.setattr(wf, "reclaim_stale_claims", lambda *a, **k: 0)
    monkeypatch.setattr(wf, "claim_next_job", claim)
    previous = wf._fleet_pool
    wf._fleet_pool = None
    wf._fleet_pool_cap = 0
    try:
        wf.run_fleet_loop(lambda _jid: None, poll_seconds=0.01, stop_event=stop, max_inflight=4)
        assert seen["cap"] == 4
        assert seen["workers"] == 4
    finally:
        current = wf._fleet_pool
        wf._fleet_pool = previous
        if current is not None and current is not previous:
            current.shutdown(wait=False, cancel_futures=True)


def test_serial_fleet_loop_does_not_open_a_pool(monkeypatch):
    from services import worker_fleet as wf

    stop = threading.Event()

    def claim(_store):
        stop.set()
        return None

    monkeypatch.setattr(wf, "reclaim_stale_claims", lambda *a, **k: 0)
    monkeypatch.setattr(wf, "claim_next_job", claim)
    previous = wf._fleet_pool
    wf._fleet_pool = None
    wf._fleet_pool_cap = 0
    try:
        wf.run_fleet_loop(lambda _jid: None, poll_seconds=0.01, stop_event=stop, max_inflight=1)
        assert wf._fleet_pool is None
    finally:
        wf._fleet_pool = previous
