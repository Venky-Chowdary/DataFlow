"""D179 — the second incremental_upsert run into a Mongo collection DataFlow made.

Run 1 writes a Postgres ``DATE`` as a BSON date (midnight UTC) and a ``JSONB``
value as an embedded document. Run 2 introspects the collection it created and
must recognise both: a DATE value fits the BSON-date carrier it already landed
on, and a JSON/JSONB document onto an existing OBJECT field is identity, not a
text parse. The live leg runs PG -> Mongo twice with preflight ON.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import socket
import uuid
from pathlib import Path

import pytest

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")

_PG = {
    "host": os.getenv("PGHOST", ""),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", ""),
    "username": os.getenv("PGUSER", ""),
    "password": os.getenv("PGPASSWORD", ""),
}


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
        return True
    except OSError:
        return False


def _mongo_up() -> bool:
    try:
        import pymongo

        pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).server_info()
        return True
    except Exception:
        return False


_live = pytest.mark.skipif(
    not (_PG["host"] and _reachable(_PG["host"], _PG["port"]) and _mongo_up()),
    reason="live PG + MongoDB: set PG* and DATAFLOW_TEST_MONGO_URI (reachable)",
)


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _request(table: str, db: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table=table, **_PG),
        destination=EndpointConfig(
            kind="database",
            format="mongodb",
            connection_string=MONGO_URI,
            database=db,
            collection=table,
        ),
        sync_mode="incremental_upsert",
        source_timezone="UTC",
        stream_contracts=[
            {
                "name": table,
                "cursor_field": "updated_at",
                "cursor_semantics": "monotonic_sequence",
                "sync_mode": "incremental_upsert",
                "primary_key": ["id"],
                "selected": True,
            }
        ],
        validation_mode="strict",
    )


def _run(fake, table: str, db: str):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "d179" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(table, db), job_id)


@_live
def test_pg_to_mongo_incremental_run_two_into_own_collection_applies_the_delta(isolated):
    import psycopg2
    import pymongo

    table = "d179_" + uuid.uuid4().hex[:10]
    db = "df_d179_test"
    pg = psycopg2.connect(
        host=_PG["host"], port=_PG["port"], user=_PG["username"],
        password=_PG["password"], dbname=_PG["database"],
    )
    cur = pg.cursor()
    client = pymongo.MongoClient(MONGO_URI)
    try:
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, d DATE, "  # nosec B608
            "payload JSONB, updated_at TIMESTAMP)"
        )
        for i in range(1, 11):
            cur.execute(
                f"INSERT INTO {table} VALUES (%s, %s, %s, %s)",  # nosec B608
                (i, dt.date(2024, 3, i), json.dumps({"k": i, "n": {"a": [i, "x"]}}),
                 dt.datetime(2024, 3, i, 10, 0, 0)),
            )
        pg.commit()

        first = _run(isolated, table, db)
        assert first.success, first.error
        stored = client[db][table].find_one({"id": 3})
        assert stored["d"] == dt.datetime(2024, 3, 3)
        assert stored["payload"] == {"k": 3, "n": {"a": [3, "x"]}}

        for i in range(2, 6):
            cur.execute(
                f"UPDATE {table} SET payload = %s, d = %s, "  # nosec B608
                "updated_at = %s WHERE id = %s",
                (json.dumps({"k": i * 10, "n": {"a": []}}), dt.date(2025, 1, i),
                 dt.datetime(2024, 4, i, 9, 0, 0), i),
            )
        pg.commit()

        second = _run(isolated, table, db)
        assert second.success, (second.error, second.blocking_reasons if hasattr(second, "blocking_reasons") else None)
        coll = client[db][table]
        assert coll.count_documents({}) == 10
        for i in range(2, 6):
            doc = coll.find_one({"id": i})
            assert doc["payload"] == {"k": i * 10, "n": {"a": []}}
            assert doc["d"] == dt.datetime(2025, 1, i)
        untouched = coll.find_one({"id": 7})
        assert untouched["payload"] == {"k": 7, "n": {"a": [7, "x"]}}
    finally:
        client.drop_database(db)
        client.close()
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.commit()
        pg.close()


# --------------------------------------------------------------------------
# Unit legs (no services).
# --------------------------------------------------------------------------

_SRC_SCHEMAS = [
    {"name": "id", "inferred_type": "bigint", "samples": ["1", "2"]},
    {"name": "payload", "inferred_type": "jsonb", "samples": [{"k": 1}, {"k": [2, "x"]}]},
]
_RUN1_COLLECTION = [
    {"name": "id", "inferred_type": "BIGINT"},
    {"name": "payload", "inferred_type": "OBJECT"},
]


def test_a_driver_parsed_document_sample_is_json_evidence_not_a_python_repr():
    from services.value_serializer import evidence_samples

    assert evidence_samples([{"k": 1}, [1, "x"], "plain"]) == [
        '{"k": 1}',
        '[1, "x"]',
        "plain",
    ]


def test_plan_scorer_binds_jsonb_onto_its_own_object_field_without_review():
    from services.mapping_pipeline import run_mapping_pipeline

    out = run_mapping_pipeline(
        ["id", "payload"],
        ["id", "payload"],
        source_schemas=[dict(s) for s in _SRC_SCHEMAS],
        target_schemas=[dict(s) for s in _RUN1_COLLECTION],
        destination_db_type="mongodb",
        destination_table_exists=True,
        source_db_type="postgresql",
        use_llm=False,
    )
    payload = {m["source"]: m for m in out["mappings"]}["payload"]
    assert payload["target"] == "payload"
    assert not payload.get("requires_review"), payload
    assert float(payload["confidence"]) >= 0.85, payload
    assert "parse failures" not in payload["reasoning"], payload


def test_plan_scorer_and_map_connector_schemas_agree_on_jsonb_to_object():
    from services.mapping_pipeline import run_mapping_pipeline
    from services.semantic_mapper import map_columns

    plan = {
        m["source"]: m
        for m in run_mapping_pipeline(
            ["id", "payload"],
            ["id", "payload"],
            source_schemas=[dict(s) for s in _SRC_SCHEMAS],
            target_schemas=[dict(s) for s in _RUN1_COLLECTION],
            destination_db_type="mongodb",
            destination_table_exists=True,
            source_db_type="postgresql",
            use_llm=False,
        )["mappings"]
    }
    # map_connector_schemas introspects both sides and calls map_columns with
    # no samples.
    tool = {
        m["source"]: m
        for m in map_columns(
            ["id", "payload"],
            ["id", "payload"],
            source_schemas=[{**s, "samples": []} for s in _SRC_SCHEMAS],
            target_schemas=[{**s, "samples": []} for s in _RUN1_COLLECTION],
            threshold=0.85,
            destination_db_type="mongodb",
            source_db_type="postgresql",
        )
    }
    for col in ("id", "payload"):
        assert plan[col]["target"] == tool[col]["target"] == col
        assert bool(plan[col].get("requires_review")) == bool(tool[col].get("requires_review"))
        assert float(plan[col]["confidence"]) >= 0.85
        assert float(tool[col]["confidence"]) >= 0.85


def test_date_source_fits_the_bson_date_carrier_run_one_wrote():
    from services.population_fit_scan import bounded_targets, fit_predicate_for

    mappings = [{"source": "d", "target": "d", "source_type": "date", "target_type": "DATE"}]
    targets, _undecidable, _safe = bounded_targets(
        mappings,
        dest_types={"d": "TIMESTAMPTZ"},
        source_types={"d": "date"},
        dest_db="mongodb",
        job_error_policy="fail_job",
        source_kind="database",
        source_format="postgresql",
        sync_mode="incremental_upsert",
        dest_table_exists=True,
    )
    assert len(targets) == 1
    fits = fit_predicate_for(targets[0], dest_db="mongodb", dialect_label="MongoDB")
    assert fits(dt.date(2024, 3, 3)) is None
    assert fits("2024-03-03") is None
    assert fits("2024-02-31") is not None  # a date that is not a day stays refused


def test_datetime_source_still_needs_a_zone_for_the_instant_carrier():
    from services.population_fit_scan import bounded_targets, fit_predicate_for

    mappings = [{"source": "t", "target": "t", "source_type": "timestamp", "target_type": "TIMESTAMP"}]
    targets, _u, _s = bounded_targets(
        mappings,
        dest_types={"t": "TIMESTAMPTZ"},
        source_types={"t": "timestamp"},
        dest_db="mongodb",
        job_error_policy="fail_job",
        source_kind="database",
        source_format="postgresql",
        sync_mode="incremental_upsert",
        dest_table_exists=True,
    )
    assert len(targets) == 1
    fits = fit_predicate_for(targets[0], dest_db="mongodb", dialect_label="MongoDB")
    assert fits(dt.datetime(2024, 3, 3, 10, 0, 0)) is not None
