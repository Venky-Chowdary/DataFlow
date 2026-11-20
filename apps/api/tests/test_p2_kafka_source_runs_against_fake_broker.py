"""Kafka -> PostgreSQL runs against an in-memory broker (D86, D286, D287, D404).

No Kafka broker runs in the API test job, so the live tests for these stories
skip there. These run the real engine (reader, Debezium bridge, type census,
Gate-8, offset commit) against ``tests.helpers.fake_kafka.FakeBroker``, which
replaces only kafka-python's client classes, and against the local
PostgreSQL. Each story stays open for a production retest on a real broker.

Kafka consumer semantics checked here (Apache Kafka docs, "Consumer
Configs"/``enable.auto.commit``; kafka-python ``KafkaConsumer.commit``): a
group's committed offset is the next offset to read and moves only on an
explicit commit, which the product makes once the job is accepted (R1).
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.config  # noqa: E402,F401 - load Settings before a fixture blanks env
import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.helpers.fake_kafka import FakeBroker  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402

psycopg2 = pytest.importorskip("psycopg2")


def _pg():
    try:
        conn = psycopg2.connect(host=os.environ.get("PGHOST", "127.0.0.1"), port=5432,
                                dbname="dataflow", user="dataflow", password="dataflow",
                                connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture
def pg():
    conn = _pg()
    made: list[str] = []
    yield conn, made
    with conn.cursor() as cur:
        for name in made:
            cur.execute(f'DROP TABLE IF EXISTS public."{name}"')  # noqa: S608 - test-owned name
            cur.execute(f'DROP TABLE IF EXISTS public."{name}_df_quarantine"')  # noqa: S608
    conn.close()


@pytest.fixture
def broker(monkeypatch):
    fake = FakeBroker().install(monkeypatch)

    class _Mongo(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields or {})
            return True

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    return fake


def _request(topic, dst, *, sync_mode, mappings, validation_mode="lenient", extra=None):
    return TransferRequest(
        source=EndpointConfig(kind="database", format="kafka", host="127.0.0.1", port=9092,
                              table=topic, database=topic, extra=dict(extra or {})),
        destination=EndpointConfig(kind="database", format="postgresql", host="127.0.0.1",
                                   port=5432, database="dataflow", username="dataflow",
                                   password="dataflow", schema="public", table=dst),
        mappings=mappings,
        sync_mode=sync_mode,
        skip_preflight=True,
        validation_mode=validation_mode,
    )


def _schema(topic):
    from connectors.kafka_reader import infer_topic_schema

    schema, _native, _warning = infer_topic_schema({"host": "127.0.0.1", "port": 9092}, topic)
    return schema


def _run(req):
    job_id = uuid.uuid4().hex[:24]
    return job_id, UniversalTransferEngine().execute_tracked(req, job_id)


def _rows(conn, dst, cols="id, name"):
    with conn.cursor() as cur:
        cur.execute(f'SELECT {cols} FROM public."{dst}" ORDER BY 1 NULLS LAST')  # noqa: S608
        return cur.fetchall()


def _names(tag):
    return f"p2k_{tag}_{uuid.uuid4().hex[:8]}", f"p2k_{tag}_d_{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------- D286 + D86

def test_d286_one_poison_message_is_quarantined_and_13_rows_land(broker, pg):
    conn, made = pg
    topic, dst = _names("poison")
    made.append(dst)
    names = ["a", "", "  ", None, "東京"]
    for i in range(1, 14):
        broker.produce(topic, json.dumps({"id": i, "name": names[i % 5], "amt": i + 0.5}))
    broker.produce(topic, b"{not json at all")

    schema = _schema(topic)
    assert "_kafka_value" not in schema and "_kafka_poison" not in schema
    req = _request(topic, dst, sync_mode="full_refresh_append",
                   mappings=[{"source": k, "target": k} for k in sorted(schema)])
    job_id, result = _run(req)

    assert result.success is True, result.error
    assert result.records_transferred == 13
    landed = _rows(conn, dst)
    assert len(landed) == 13
    by_id = {int(r[0]): r[1] for r in landed}
    assert by_id[1] == "" and by_id[2] == "  " and by_id[3] is None and by_id[5] == "a"
    from services.quarantine_dlq import quarantine_details_from_dlq

    poison = [d for d in quarantine_details_from_dlq(job_id) if "poison" in str(d.get("reason"))]
    assert len(poison) == 1
    assert poison[0]["row"]["_kafka_value"] == "{not json at all"
    # R1: the run was accepted, so the group now sits past all 14 messages,
    # the poison included (it is in quarantine, not lost).
    group = broker.only_group(topic)
    assert broker.group_offsets(group) == {(topic, 0): 14}


def test_d86_null_key_rows_load_and_gate8_never_binds_the_null_sentinel(broker, pg, monkeypatch):
    conn, made = pg
    topic, dst = _names("nullkey")
    made.append(dst)
    for i in range(1, 6):
        broker.produce(topic, json.dumps({"id": i, "name": f"n{i}"}))
    broker.produce(topic, json.dumps({"id": None, "name": "no key"}))

    import services.target_sample as target_sample

    bound: list = []
    real = target_sample.read_target_sample

    def _spy(*args, **kwargs):
        bound.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(target_sample, "read_target_sample", _spy)
    schema = _schema(topic)
    req = _request(topic, dst, sync_mode="full_refresh_append",
                   mappings=[{"source": k, "target": k} for k in sorted(schema)])
    _job, result = _run(req)

    assert result.success is True, result.error
    landed = _rows(conn, dst)
    assert len(landed) == 6 and landed[-1] == (None, "no key")
    assert bound, "Gate-8 read no keyed destination sample"
    assert "__DF_SQL_NULL__" not in json.dumps(bound, default=str)


def test_d86_a_rejected_run_leaves_the_group_where_it_was_and_the_rerun_lands_all(
    broker, pg, monkeypatch
):
    """AC-2: offsets equal the pre-run position after a run that is not accepted."""
    conn, made = pg
    topic, dst = _names("rewind")
    made.append(dst)
    for i in range(1, 9):
        broker.produce(topic, json.dumps({"id": i, "name": f"v{i}"}))
    schema = _schema(topic)
    mappings = [{"source": k, "target": k} for k in sorted(schema)]

    import src.transfer.engine as eng

    real_recon = eng.run_reconciliation

    def _gate8_fails(*args, **kwargs):
        report = dict(real_recon(*args, **kwargs))
        report.update(passed=False, message="Gate-8 refused this run (test)")
        return report

    monkeypatch.setattr(eng, "run_reconciliation", _gate8_fails)
    _job, failed = _run(_request(topic, dst, sync_mode="incremental_append",
                                 mappings=mappings))
    assert failed.success is False
    assert broker.commit_calls == []  # nothing committed on a rejected run
    monkeypatch.setattr(eng, "run_reconciliation", real_recon)

    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{dst}"')  # noqa: S608
    _job, ok = _run(_request(topic, dst, sync_mode="incremental_append", mappings=mappings))
    assert ok.success is True, ok.error
    assert [int(r[0]) for r in _rows(conn, dst)] == list(range(1, 9))
    group = broker.only_group(topic)
    assert broker.group_offsets(group) == {(topic, 0): 8}


# ---------------------------------------------------------------- D287

def test_d287_overwrite_finalizes_on_every_one_of_five_runs(broker, pg):
    conn, made = pg
    topic, dst = _names("ovw")
    made.append(dst)
    for i in range(1, 21):
        broker.produce(topic, json.dumps({"id": i, "name": f"r{i}"}))
    schema = _schema(topic)
    mappings = [{"source": k, "target": k} for k in sorted(schema)]
    for attempt in range(5):
        _job, result = _run(_request(topic, dst, sync_mode="full_refresh_overwrite",
                                     mappings=mappings, validation_mode="strict"))
        assert result.success is True, (attempt, result.error)
        assert result.records_transferred == 20
        assert len(_rows(conn, dst)) == 20  # a full refresh re-reads from earliest


def test_d287_an_unavailable_coordinator_fails_the_run_with_its_cause(broker, pg):
    """An incremental run whose group coordinator is gone ends; it does not hang."""
    conn, made = pg
    topic, dst = _names("coord")
    made.append(dst)
    for i in range(1, 6):
        broker.produce(topic, json.dumps({"id": i, "name": f"c{i}"}))
    schema = _schema(topic)
    broker.coordinator_down = True
    _job, result = _run(_request(topic, dst, sync_mode="incremental_append",
                                 mappings=[{"source": k, "target": k} for k in sorted(schema)]))
    assert result.success is False
    # The bounded lookup's own timeout is the cause the operator reads; a
    # lookup with no deadline would have blocked here forever (the fake raises).
    assert "group coordinator not available" in str(result.error)
    assert "forever" not in str(result.error)
    assert broker.commit_calls == []


# ---------------------------------------------------------------- D404

def test_d404_incremental_append_pages_by_offset_and_appends_new_versions(broker, pg):
    """Three runs: each appends only the messages past the committed offset,
    and a later message for an existing key is a new version (R6), not a refusal."""
    conn, made = pg
    topic, dst = _names("inc")
    made.append(dst)
    for i in range(1, 6):
        broker.produce(topic, json.dumps({"id": i, "name": f"v1-{i}"}))
    schema = _schema(topic)
    mappings = [{"source": k, "target": k} for k in sorted(schema)]

    _job, run1 = _run(_request(topic, dst, sync_mode="incremental_append", mappings=mappings))
    assert run1.success is True, run1.error
    assert run1.records_transferred == 5

    broker.produce(topic, json.dumps({"id": 5, "name": "v2-5"}))
    broker.produce(topic, json.dumps({"id": 3, "name": "v2-3"}))
    broker.produce(topic, json.dumps({"id": 6, "name": "v1-6"}))
    _job, run2 = _run(_request(topic, dst, sync_mode="incremental_append", mappings=mappings))
    assert run2.success is True, run2.error
    assert run2.records_transferred == 3

    broker.produce(topic, json.dumps({"id": 5, "name": "v3-5"}))
    broker.produce(topic, json.dumps({"id": 3, "name": "v3-3"}))
    _job, run3 = _run(_request(topic, dst, sync_mode="incremental_append", mappings=mappings))
    assert run3.success is True, run3.error
    assert run3.records_transferred == 2

    # A run with nothing new is a correct no-op, not a failure.
    _job, run4 = _run(_request(topic, dst, sync_mode="incremental_append", mappings=mappings))
    assert run4.success is True, run4.error
    assert run4.records_transferred == 0

    landed = sorted((int(r[0]), r[1]) for r in _rows(conn, dst))
    assert len(landed) == 10
    assert [n for k, n in landed if k == 5] == ["v1-5", "v2-5", "v3-5"]
    assert [n for k, n in landed if k == 3] == ["v1-3", "v2-3", "v3-3"]
    group = broker.only_group(topic)
    assert broker.group_offsets(group) == {(topic, 0): 10}


def _with_cursor(req, topic, cursor):
    req.stream_contracts = [{
        "name": topic, "selected": True, "sync_mode": "incremental_append",
        "cursor_field": cursor, "cursor_semantics": "monotonic_sequence",
    }]
    return req


@pytest.mark.parametrize("cursor", ["_kafka_offset"])
def test_d404_the_offset_cursors_the_plan_accepts_run_every_time(broker, pg, cursor):
    """R8: a cursor the sync-contract gate passes must not fail at Execute."""
    conn, made = pg
    topic, dst = _names("offc")
    made.append(dst)
    for i in range(1, 4):
        broker.produce(topic, json.dumps({"id": i, "name": f"a{i}", "offset": 100 - i}))
    schema = _schema(topic)
    mappings = [{"source": k, "target": k} for k in sorted(schema)]
    for expected, more in ((3, 2), (2, 0)):
        req = _with_cursor(_request(topic, dst, sync_mode="incremental_append",
                                    mappings=mappings), topic, cursor)
        _job, result = _run(req)
        assert result.success is True, (cursor, result.error)
        assert result.records_transferred == expected
        for i in range(more):
            broker.produce(topic, json.dumps({"id": 1, "name": f"b{i}", "offset": 1 + i}))
    assert len(_rows(conn, dst)) == 5


def test_d404_a_payload_column_cursor_is_refused_before_anything_is_read(broker, pg):
    conn, made = pg
    topic, dst = _names("colc")
    made.append(dst)
    for i in range(1, 4):
        broker.produce(topic, json.dumps({"id": i, "updated_at": f"2024-01-0{i}T00:00:00Z"}))
    schema = _schema(topic)
    req = _with_cursor(_request(topic, dst, sync_mode="incremental_append",
                                mappings=[{"source": k, "target": k} for k in sorted(schema)]),
                       topic, "updated_at")
    _job, result = _run(req)
    assert result.success is False
    assert "offset is the cursor" in str(result.error)
    assert broker.commit_calls == []
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f'public."{dst}"',))
        assert cur.fetchone()[0] is None  # nothing created, nothing written


def test_d404_only_the_broker_offset_column_is_the_offset_cursor():
    from src.transfer.engine_kafka import kafka_run_cursor, kafka_unpageable_cursor

    # A payload field named "offset" is message data, not the broker position.
    assert "offset is the cursor" in kafka_unpageable_cursor("kafka", "offset")
    assert kafka_unpageable_cursor("kafka", "_kafka_offset") == ""
    assert kafka_run_cursor("kafka", True, "_kafka_offset") == ""
    assert kafka_run_cursor("kafka", True, "") == ""
    with pytest.raises(ValueError, match="offset is the cursor"):
        kafka_run_cursor("kafka", True, "updated_at")
    # A full refresh has no cursor to refuse; other sources keep theirs.
    assert kafka_run_cursor("kafka", False, "updated_at") == "updated_at"
    assert kafka_run_cursor("postgresql", True, "updated_at") == "updated_at"


# ---------------------------------------------------------------- D83

from test_d190_d194_d197_declared_string_types_e2e import transfer_client  # noqa: E402,F401

_SASL_SHAPES = {
    "extra": {"extra": {"security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "PLAIN"}},
    "form": {"schema": "SASL_PLAINTEXT", "database": "PLAIN"},
}


@pytest.mark.parametrize("shape", sorted(_SASL_SHAPES))
def test_d83_sasl_plaintext_connector_introspects_and_plans_on_its_own_listener(
    transfer_client, monkeypatch, shape  # noqa: F811
):
    """AC-1: introspect and plan_transfer dial SASL_PLAINTEXT and succeed."""
    conn = _pg()
    conn.close()
    broker = FakeBroker().install(monkeypatch)
    from services.connector_store import create_connector
    from src.ai.copilot.schema_tools import introspect_connector_schema
    from src.ai.copilot.transfer_tools import plan_transfer

    topic = f"p2k_sasl_{shape}_{uuid.uuid4().hex[:6]}"
    for i in range(1, 4):
        broker.produce(topic, json.dumps({"id": i, "v": f"x{i}"}))
    src = str(create_connector({
        "name": f"p2k-sasl-{uuid.uuid4().hex[:6]}", "type": "kafka", "role": "source",
        "host": "127.0.0.1", "port": 9094, "username": "svc", "password": "not-a-real-secret",
        "ssl": False, "workspace_id": "", **_SASL_SHAPES[shape],
    }).id)
    dst = str(create_connector({
        "name": f"p2k-pg-{uuid.uuid4().hex[:6]}", "type": "postgresql", "role": "destination",
        "host": "127.0.0.1", "port": 5432, "database": "dataflow", "username": "dataflow",
        "password": "dataflow", "schema": "public", "ssl": False, "workspace_id": "",
    }).id)

    info = introspect_connector_schema(connector_id=src, table=topic)
    assert info.success, info.error
    assert [c["name"] for c in info.output["columns"]][:2] == ["id", "v"]
    plan = plan_transfer(source_connector_id=src, source_table=topic, dest_connector_id=dst,
                         dest_table=f"{topic}_d", sync_mode="full_refresh_overwrite")
    assert plan.success, plan.error

    assert broker.client_configs, "no Kafka client was opened"
    listeners = {
        (cfg.get("security_protocol"), cfg.get("sasl_mechanism"))
        for cfg in broker.client_configs
    }
    assert listeners == {("SASL_PLAINTEXT", "PLAIN")}
    secrets = json.dumps([info.output, plan.output], default=str)
    assert "not-a-real-secret" not in secrets
