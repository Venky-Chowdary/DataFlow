"""D300 - append/upsert into an EXISTING Elasticsearch index was always refused.

QA: Postgres -> an index DataFlow had just created (overwrite passed 30/30),
then upsert keyed on order_id (and a keyless append) into that index: Validate
refused with g6 'Could not load destination schema for existing target' and
g3 'long_txt: Empty value into existing table with unknown physical DDL',
while introspect_connector_schema on the same index listed every field.

Root cause (src/transfer/endpoint_intelligence.py): the Elasticsearch
destination probe sampled the index only when the endpoint's ``database``
was set, and the sample read ``endpoint.database or endpoint.table``. The
index a transfer names is the destination *table* (the writer and the source
reader resolve table -> collection -> database), so the preflight saw the
index listed (exists) with an empty schema, or read a connector-level default
index instead of the chosen one.

No Elasticsearch server here: a fake client answers exists / count / search /
get_mapping the way the 8.x client does.
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

INDEX = "q24_orders"
_MAPPING = {"properties": {"order_id": {"type": "long"}, "long_txt": {"type": "text"},
                           "amount": {"type": "keyword"}}}
_DOCS = [
    {"_id": "1001", "_source": {"order_id": 1001, "long_txt": "", "amount": "1.50"}},
    {"_id": "1002", "_source": {"order_id": 1002, "long_txt": "x", "amount": "2.50"}},
]


class _Indices:
    def exists(self, index):
        return index == INDEX

    def get_mapping(self, index):
        return {index: {"mappings": _MAPPING}}


class _FakeES:
    indices = _Indices()

    def count(self, index):
        return {"count": len(_DOCS) if index == INDEX else 0}

    def search(self, index, body):
        docs = _DOCS if index == INDEX else []
        return {"hits": {"hits": [{**d, "sort": [i]} for i, d in enumerate(docs)]}}

    def close(self):
        pass


@pytest.fixture
def fake_es(monkeypatch):
    import connectors.elasticsearch as es_mod
    import connectors.elasticsearch_reader as reader
    from connectors.base import ConnectResult

    monkeypatch.setattr(reader, "_client", lambda cfg: _FakeES())
    monkeypatch.setattr(
        es_mod, "test_elasticsearch",
        lambda **_k: ConnectResult(ok=True, tables=[INDEX, "other"], message="ok"),
    )


def _inspect(database: str):
    from services.preflight_destination import inspect_destination_for_preflight

    return inspect_destination_for_preflight(
        dest_type="elasticsearch", dest_host="127.0.0.1", dest_port=9200,
        dest_database=database, dest_table=INDEX,
    )


@pytest.mark.parametrize("database", ["", "other"], ids=["no_database", "connector_default_index"])
def test_preflight_reads_the_chosen_index_schema(fake_es, database):
    out = _inspect(database)
    assert out["connected"] is True and out["table_exists"] is True
    assert out["column_types"].get("order_id") == "BIGINT"
    assert {"order_id", "long_txt", "amount"} <= set(out["column_types"])


@pytest.mark.parametrize("sync_mode", ["upsert", "full_refresh_append"])
def test_existing_index_is_not_refused_for_an_unloadable_schema(fake_es, sync_mode):
    from services.ddl_compatibility import evaluate_ddl_compatibility

    out = _inspect("")
    mappings = [{"source": c, "target": c, "source_type": t, "target_type": out["column_types"].get(c, "")}
                for c, t in (("order_id", "BIGINT"), ("long_txt", "TEXT"), ("amount", "TEXT"))]
    _ok, issues = evaluate_ddl_compatibility(
        mappings=mappings,
        source_schema={"order_id": "BIGINT", "long_txt": "TEXT", "amount": "TEXT"},
        target_schema=out["column_types"],
        sample_rows=[{"order_id": "1001", "long_txt": "", "amount": "1.50"}],
        table_exists=out["table_exists"],
        dest_connected=True,
        dest_db_type="elasticsearch",
        sync_mode=sync_mode,
        destination_table=INDEX,
        destination_pk_columns=["order_id"] if sync_mode == "upsert" else None,
    )
    assert not [i for i in issues if "Could not load destination schema" in i], issues
    assert not [i for i in issues if "unknown physical DDL" in i], issues
