"""Batch-1 gate-4 regressions — upsert DDL gate correctness.

Three defects the D101 destination-key gate introduced:

1. ``pg_table_existed`` was only assigned inside ``if create_table:`` — an
   upsert into an existing table (``create_table=False``) crashed with
   ``UnboundLocalError`` (live: test_postgresql_upsert_rejects_older_lsn,
   test_postgresql_writer_upsert_does_not_undelete_lattice).
2. The covering-index probe compared ``array_agg(attname)`` (type ``name[]``)
   to a ``text[]`` literal — ``operator does not exist: name[] = text[]``
   (live: test_sync_mode_second_run[upsert-postgresql],
   test_sftp_glob_loads_in_order_and_a_rerun_does_not_double,
   test_live_engines_close_quarantine_ledger_after_replay).
3. Mongo destinations: the writer created only a *throughput* (non-unique)
   index on DataFlow-created collections and introspection never read
   ``index_information`` — the gate saw every Mongo collection as a keyless
   heap and blocked second-run upserts
   (live: test_sync_mode_second_run[upsert-mongodb]).
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# PostgreSQL: fake psycopg2 connection driving the upsert path end-to-end.
# ---------------------------------------------------------------------------


class _FakePgCursor:
    def __init__(self):
        self.queries: list[str] = []
        self.managed_comment = None
        self.covering_index_present = True
        self.rowcount = 1

    def execute(self, query, params=None):
        text = query if isinstance(query, str) else getattr(query, "string", repr(query))
        self.queries.append(str(text))
        return None

    def executemany(self, query, seq):
        self.queries.append(str(query))
        return None

    def fetchone(self):
        # Existence probes and the covering-index probe all see "row present".
        last = self.queries[-1] if self.queries else ""
        if "obj_description" in last:
            return (self.managed_comment,)
        if "pg_index" in last:
            return (1,) if self.covering_index_present else None
        return (1,)

    def fetchall(self):
        last = self.queries[-1] if self.queries else ""
        if "information_schema.columns" in last.lower() or "column_name" in last.lower():
            # Column introspection on the existing destination table:
            # (column_name, data_type, udt_name, char_len, precision, scale)
            return [
                ("id", "integer", "int4", None, None, None),
                ("name", "text", "text", None, None, None),
            ]
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        return None


class _FakePgConn:
    def __init__(self):
        self.cur = _FakePgCursor()
        self.autocommit = False
        self.committed = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True

    def rollback(self):
        return None

    def close(self):
        return None


def _pg_kwargs() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "dataflow",
        "username": "u",
        "password": "p",
        "schema": "public",
        "connection_string": "",
        "ssl": False,
        "table_name": "t_upsert_gate",
        "headers": ["id", "name"],
        "data_rows": [["1", "a"]],
        "mappings": [
            {"source": "id", "target": "id", "confidence": 0.9},
            {"source": "name", "target": "name", "confidence": 0.9},
        ],
        "column_types": {"id": "INTEGER", "name": "TEXT"},
        "create_table": False,
        "write_mode": "upsert",
        "conflict_columns": ["id"],
    }


def test_upsert_into_existing_table_probes_existence_without_nameerror():
    """create_table=False upsert reaches the covering-index probe.

    On the pre-fix code the read of ``pg_table_existed`` raised
    UnboundLocalError before the pg_index probe ever ran. A fake cursor
    cannot satisfy psycopg2's ``as_string`` later in the write, so assert
    on the probe path, not the final result.
    """
    pytest.importorskip("psycopg2")
    from connectors.postgresql_writer import write_mapped_rows

    conn = _FakePgConn()
    result = write_mapped_rows(connection=conn, **_pg_kwargs())
    assert "pg_table_existed" not in (result.error or ""), result.error
    probe_sql = [q for q in conn.cur.queries if "pg_index" in q]
    assert probe_sql, (
        f"covering-index probe never ran (error={result.error!r}); "
        "the upsert gate must probe existing-table existence when "
        "create_table=False"
    )


def test_upsert_covering_index_probe_casts_attname_array_to_text():
    pytest.importorskip("psycopg2")
    from connectors.postgresql_writer import write_mapped_rows

    conn = _FakePgConn()
    write_mapped_rows(connection=conn, **_pg_kwargs())
    probe_sql = [q for q in conn.cur.queries if "pg_index" in q]
    assert probe_sql, "covering-index probe never ran"
    assert any("::text[]" in q for q in probe_sql), (
        "pg_index probe must cast array_agg(attname) — name[] never equals text[]"
    )


def test_unmarked_table_with_covering_index_upserts_without_refusal():
    """Pre-marker table, no DDL needed: run 2 of an existing upsert job.

    An unmarked destination that ALREADY has the unique index (every table
    that upserted before, since the index was always created) must keep
    working — the gate only fires when it would actually alter the table.
    """
    pytest.importorskip("psycopg2")
    from connectors.postgresql_writer import write_mapped_rows

    conn = _FakePgConn()  # unmarked (managed_comment=None), index covered
    result = write_mapped_rows(connection=conn, **_pg_kwargs())
    assert "did not create" not in (result.error or ""), result.error
    assert "pg_table_existed" not in (result.error or ""), result.error


def test_unmarked_table_missing_index_error_says_how_to_proceed():
    """When DDL IS needed on an unmarked table, the refusal names the fix."""
    pytest.importorskip("psycopg2")
    from connectors.postgresql_writer import write_mapped_rows

    conn = _FakePgConn()
    conn.cur.covering_index_present = False
    result = write_mapped_rows(connection=conn, **_pg_kwargs())
    err = result.error or ""
    assert "did not create" in err, err
    assert "ddl_changes_acknowledged" in err, err
    assert "CREATE UNIQUE INDEX" in err and "uidx_" in err, (
        "refusal must include the exact manual DDL"
    )


def test_dataflow_managed_table_may_gain_upsert_index_without_approval():
    """A 'dataflow:managed' table is ours — replay/upsert adds the index.

    The D101 gate must not refuse index DDL on a table DataFlow itself
    created (quarantine replay upserts into exactly this case).
    """
    pytest.importorskip("psycopg2")
    from connectors.postgresql_writer import write_mapped_rows

    conn = _FakePgConn()
    conn.cur.managed_comment = "dataflow:managed"
    result = write_mapped_rows(connection=conn, **_pg_kwargs())
    assert "did not create" not in (result.error or ""), result.error
    assert any("CREATE UNIQUE INDEX" in q for q in conn.cur.queries), (
        "managed table should get the upsert unique index without the "
        "operator-table gate"
    )


# ---------------------------------------------------------------------------
# MongoDB: fake client/collection; DataFlow-created collections get a UNIQUE
# index on the upsert key, existing collections keep the throughput index.
# ---------------------------------------------------------------------------


class _FakeMongoCursor:
    def __init__(self, docs):
        self._docs = docs

    def max_time_ms(self, _ms):
        return self

    def limit(self, _n):
        return self

    def __iter__(self):
        return iter(self._docs)


class _FakeMongoColl:
    def __init__(self, indexes=None):
        self.indexes = dict(indexes or {"_id_": {"key": {"_id": 1}, "unique": True}})
        self.created_indexes: list[dict] = []

    def find(self, *a, **k):
        return _FakeMongoCursor([{"_id": "x", "id": "1", "name": "a"}])

    def index_information(self):
        return dict(self.indexes)

    def estimated_document_count(self, **k):
        return 1

    def create_index(self, keys, **kwargs):
        self.created_indexes.append({"keys": list(keys), **kwargs})
        return "idx"

    def bulk_write(self, ops, ordered=False):
        from types import SimpleNamespace

        return SimpleNamespace(
            upserted_count=len(ops),
            matched_count=len(ops),
            modified_count=0,
            inserted_count=0,
            deleted_count=0,
            bulk_api_result={},
        )


class _FakeMongoDb:
    def __init__(self, coll, names):
        self._coll = coll
        self._names = list(names)

    def __getitem__(self, name):
        return self._coll

    def list_collection_names(self, *a, **k):
        return list(self._names)


class _FakeMongoClient:
    def __init__(self, db):
        self._db = db

    def __getitem__(self, name):
        return self._db

    def close(self):
        return None


def _mongo_coll(*, existed: bool):
    coll = _FakeMongoColl()
    names = ["t_upsert"] if existed else []
    return coll, _FakeMongoDb(coll, names)


def test_mongo_writer_creates_unique_index_on_dataflow_created_collection(monkeypatch):
    pytest.importorskip("pymongo")
    import connectors.mongodb_common as mc
    from connectors import mongodb_writer

    coll, db = _mongo_coll(existed=False)
    monkeypatch.setattr(mc, "_mongo_client", lambda conn_str: _FakeMongoClient(db))

    result = mongodb_writer.write_mapped_rows(
        host="127.0.0.1",
        port=27017,
        database="dataflow",
        username="",
        password="",
        connection_string="",
        schema="",
        ssl=False,
        table_name="t_upsert",
        headers=["id", "name"],
        data_rows=[["1", "a"]],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.9},
            {"source": "name", "target": "name", "confidence": 0.9},
        ],
        column_types={"id": "string", "name": "string"},
        create_table=True,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert result.ok, result.error
    key_indexes = [i for i in coll.created_indexes if [c for c, _d in i["keys"]] == ["id"]]
    assert key_indexes, "no index created on the upsert key"
    assert key_indexes[0].get("unique") is True, (
        "DataFlow-created upsert target must get a UNIQUE key index — "
        "the D101 gate verifies it on the next run"
    )


def test_mongo_writer_keeps_nonunique_index_on_existing_collection(monkeypatch):
    pytest.importorskip("pymongo")
    import connectors.mongodb_common as mc
    from connectors import mongodb_writer

    coll, db = _mongo_coll(existed=True)
    monkeypatch.setattr(mc, "_mongo_client", lambda conn_str: _FakeMongoClient(db))

    result = mongodb_writer.write_mapped_rows(
        host="127.0.0.1",
        port=27017,
        database="dataflow",
        username="",
        password="",
        connection_string="",
        schema="",
        ssl=False,
        table_name="t_upsert",
        headers=["id", "name"],
        data_rows=[["1", "a"]],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.9},
            {"source": "name", "target": "name", "confidence": 0.9},
        ],
        column_types={"id": "string", "name": "string"},
        create_table=True,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert result.ok, result.error
    key_indexes = [i for i in coll.created_indexes if [c for c, _d in i["keys"]] == ["id"]]
    assert key_indexes, "no index created on the upsert key"
    assert key_indexes[0].get("unique") is not True, (
        "unique index on an operator's existing collection is undisclosed DDL — "
        "the Validate gate discloses it, the writer must not silently create it"
    )


def test_mongo_introspection_reports_unique_indexes(monkeypatch):
    pytest.importorskip("pymongo")
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig
    import src.transfer.endpoint_intelligence as ei

    coll = _FakeMongoColl(
        indexes={
            "_id_": {"key": {"_id": 1}, "unique": True},
            "uidx_t_upsert_id": {"key": {"id": 1}, "unique": True},
        }
    )
    db = _FakeMongoDb(coll, ["t_upsert"])
    monkeypatch.setattr(ei, "_mongo_client", lambda conn_str: _FakeMongoClient(db))

    endpoint = EndpointConfig(
        kind="database",
        format="mongodb",
        host="127.0.0.1",
        port=27017,
        database="dataflow",
        collection="t_upsert",
    )
    info = introspect_endpoint(endpoint)
    uk_cols = {tuple(sorted(uk.get("columns") or [])) for uk in info.get("unique_keys") or []}
    assert ("id",) in uk_cols, f"unique keys not reported: {info.get('unique_keys')}"
    assert info.get("primary_key_columns") == ["_id"]
