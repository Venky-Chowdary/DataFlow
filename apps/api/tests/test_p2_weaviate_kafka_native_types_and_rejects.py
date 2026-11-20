"""Kafka -> new Weaviate class: native types, honest rejects, no residue (D293, D304, D285).

QA (D293, job 6af1...e6): 30 Kafka JSON messages (``flag`` boolean, the last
message all NULL except ``id``) into a new Weaviate class, overwrite strict.
The plan mapped BOOLEAN -> TEXT and the run failed "Weaviate rejected 29
batch object(s)" leaving one object. QA (D304, job 6af2...53): the same pair
then failed with the raw "invalid literal for int() with base 10: <uuid>"
and 0/0 counts while one object had been written.

Root causes on PR head 8252bd3b5:
* the writer kept a refused object's UUID as the finding's ``row``; the
  stream's quarantine renumbering did ``int(<uuid>)`` (D304);
* the reason was ``str(result.errors)``, a Python dict repr; a strict stop
  did not count the objects Weaviate had stored from the same batch;
* the Weaviate value proof expected every source row as an object, so a
  lenient run that quarantined one refused object failed its own Gate-8;
* BOOLEAN / IEEE float were declared ``text`` (D293 asks for native types).

No Kafka broker or Weaviate server runs here: ``tests.helpers.fake_kafka``
replaces kafka-python's clients and ``TypedWeaviate`` answers the REST batch
API with per-object ``result.errors.error[].message`` and refuses a value
whose JSON type is not the property's, as the server does
(https://weaviate.io/developers/weaviate/manage-data/import#error-handling).
Verified against faithful fakes: a production retest on the original pair is
still required.
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("DATAFLOW_EMBEDDING_MODEL", "hash/32")

import connectors.weaviate_writer as weaviate_writer  # noqa: E402
import src.transfer.engine as engine_mod  # noqa: E402
from connectors.weaviate_batch_result import (  # noqa: E402
    weaviate_batch_rejects,
    weaviate_object_errors,
)
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.job_quarantine import rebase_batch_rejects  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.helpers.fake_kafka import FakeBroker  # noqa: E402
from tests.test_d285_qdrant_to_weaviate_decimal_and_failed_run_residue import (  # noqa: E402
    TypedWeaviate,
)
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402

BIG = "99999999999.999999"


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields, **_kw):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


class _SilentDrop(TypedWeaviate):
    """Answers SUCCESS for an object but never stores it (a lying server)."""

    def __init__(self, drop: str, refuse: frozenset[str] = frozenset()):
        super().__init__(refuse_source_ids=refuse)
        self.drop = drop

    def _store(self, cls, obj):
        if (obj.get("properties") or {}).get("source_id") == self.drop:
            return None
        return super()._store(cls, obj)


def _seed(broker: FakeBroker, topic: str) -> None:
    """The canonical shape: id 9 carries the decimal edge, id 30 is all NULL."""
    for i in range(1, 31):
        if i == 30:
            broker.produce(topic, json.dumps({"id": 30, "flag": None, "amount": None,
                                              "d": None, "ts": None, "note": None}))
            continue
        broker.produce(topic, (
            f'{{"id": {i}, "flag": {"true" if i % 2 else "false"}, '
            f'"amount": {BIG if i == 9 else f"{i}.25"}, "d": "2024-01-{i % 28 + 1:02d}", '
            f'"ts": "2024-03-01T10:00:{i:02d}.123456Z", "note": "n{i}"}}'
        ))


def _run(monkeypatch, wv, *, validation_mode="strict", cls=None):
    broker = FakeBroker().install(monkeypatch)
    topic = f"p2wv_{uuid.uuid4().hex[:8]}"
    _seed(broker, topic)
    cls = cls or f"P2Wv{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    mongo = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: mongo)
    monkeypatch.setattr(weaviate_writer, "_requests_session", lambda: wv)
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="kafka", host="127.0.0.1", port=9092,
                              table=topic, database=topic),
        destination=EndpointConfig(kind="database", format="weaviate", host="127.0.0.1",
                                   port=8080, table=cls, extra={"embedding_model": "hash/32"}),
        sync_mode="full_refresh_overwrite",
        validation_mode=validation_mode,
    )
    job_id = uuid.uuid4().hex[:24]
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    return result, mongo.jobs.get(job_id) or {}, weaviate_writer._class_name(cls)


def _stored(wv, cls):
    return {o["properties"]["source_id"]: o["properties"] for o in wv.objects.get(cls, {}).values()}


# ------------------------------------------------------------------ D293

def test_d293_new_class_gets_native_types_and_all_30_objects_land(monkeypatch):
    wv = TypedWeaviate()
    result, _job, cls = _run(monkeypatch, wv)
    assert result.success is True, result.error
    props = {p["name"]: p["dataType"][0] for p in wv.classes[cls]["properties"]}
    assert props["flag"] == "boolean"
    # Kafka JSON numbers are typed DECIMAL (D203): exact text, never float64 (R3).
    assert props["amount"] == "text"
    assert props["d"] == "text" and props["ts"] == "text"
    stored = _stored(wv, cls)
    assert len(stored) == 30
    assert stored["1"]["flag"] is True and stored["2"]["flag"] is False
    assert stored["9"]["amount"] == BIG  # D285: every digit
    assert stored["3"]["ts"] == "2024-03-01T10:00:03.123456Z"
    assert not {"flag", "amount", "d", "ts", "note"} & set(stored["30"])  # NULL stays absent
    message = str((result.reconciliation or {}).get("message") or "")
    assert "hold every source value of 30 row(s)" in message, result.reconciliation


def test_d293_plan_names_the_types_the_class_declares():
    from services.decision_kernel import InventContext, invent_dest_type

    stamps = {
        src: invent_dest_type(src, dest_db="weaviate", context=InventContext.CREATE_NEW)
        for src in ("BOOLEAN", "DOUBLE PRECISION", "REAL", "NUMERIC(20,6)", "BIGINT")
    }
    assert stamps == {"BOOLEAN": "boolean", "DOUBLE PRECISION": "number", "REAL": "number",
                      "NUMERIC(20,6)": "text", "BIGINT": "int"}
    declared = weaviate_writer.weaviate_mapped_property_types(
        [{"source": c, "target": c, "target_type": t} for c, t in stamps.items()],
        {c: c for c in stamps},
    )
    assert declared == stamps
    # A "number" stamp on an exact decimal is never honoured as float64 (R3).
    assert weaviate_writer.weaviate_mapped_property_types(
        [{"source": "amt", "target": "amt", "target_type": "number"}], {"amt": "NUMERIC(20,6)"}
    ) == {"amt": "text"}


# ------------------------------------------------------------------ D304 / D285 residue

def test_d304_strict_failure_names_weaviates_reason_and_leaves_no_class(monkeypatch):
    wv = TypedWeaviate(refuse_source_ids=frozenset({"7"}))
    result, job, cls = _run(monkeypatch, wv)
    assert result.success is False
    error = str(result.error)
    assert "Weaviate rejected source row 7: refused by test" in error
    assert "invalid literal" not in error and "{'error'" not in error
    assert "29 row(s) committed" in error  # what Weaviate had stored before the stop
    assert cls not in wv.classes and not wv.objects.get(cls)  # nothing left behind (AC-2)
    details = (result.destination_summary or {}).get("rejected_details") or []
    assert [d.get("source_id") for d in details if d.get("object_id")] == ["7"]


def test_d304_lenient_run_lands_29_and_quarantines_the_refused_object(monkeypatch):
    wv = TypedWeaviate(refuse_source_ids=frozenset({"7"}))
    result, job, cls = _run(monkeypatch, wv, validation_mode="lenient")
    assert result.success is True, result.error
    assert result.records_transferred == 29
    assert len(_stored(wv, cls)) == 29 and "7" not in _stored(wv, cls)
    assert job.get("status") == "completed_with_quarantine"
    details = (result.destination_summary or {}).get("rejected_details") or []
    refused = [d for d in details if d.get("object_id")]
    assert len(refused) == 1
    assert refused[0]["reason"] == "Weaviate rejected source row 7: refused by test"
    assert refused[0]["row"]["note"] == "n7"  # the record, for inspect and replay
    proof = (result.reconciliation or {}).get("document_value_proof") or {}
    assert proof.get("passed") is True and proof.get("quarantined_rows") == 1, proof


def test_d304_proof_still_fails_when_an_object_is_missing_without_a_finding(monkeypatch):
    """R9: the quarantine accounts only for its own record, never for a lost one."""
    wv = _SilentDrop(drop="12", refuse=frozenset({"7"}))
    result, _job, _cls = _run(monkeypatch, wv, validation_mode="lenient")
    assert result.success is False
    assert "do not hold 1 of 30" in str(result.error), result.error


def test_weaviate_batch_reasons_are_weaviates_own_messages():
    batch = [{"id": "a", "properties": {"source_id": "1", "chunk_index": 0, "x": 1, "content": "c"}},
             {"id": "b", "properties": {"source_id": "2", "chunk_index": 2, "x": 2, "content": "c"}}]
    answer = [
        {"id": "a", "result": {"status": "SUCCESS"}},
        {"id": "b", "result": {"errors": {"error": [{"message": "invalid boolean property 'x'"},
                                                    {"message": "second"}]}}},
    ]
    stored, rejects = weaviate_batch_rejects(answer, batch, class_name="C", policy="quarantine")
    assert stored == 1 and len(rejects) == 1
    assert rejects[0]["reason"] == (
        "Weaviate rejected source row 2 chunk 2: invalid boolean property 'x'; second"
    )
    assert rejects[0]["row"] == {"source_id": "2", "x": 2}
    assert weaviate_object_errors({"result": {"status": "FAILED"}})
    assert weaviate_object_errors({"id": "a", "result": {}}) == []
    assert weaviate_object_errors("nonsense")


def test_a_text_row_label_is_never_renumbered_or_int_parsed():
    out = rebase_batch_rejects(
        [{"row": "385cc828-d977-5f50-b7f8-a735442f7f25", "reason": "x"},
         {"row": 3, "reason": "y"}, {"row": "4", "reason": "z"}],
        batch_start=100,
    )
    assert out[0]["row"] == "385cc828-d977-5f50-b7f8-a735442f7f25"
    assert out[1]["row"] == 103 and out[2]["row"] == 104


def test_d293_postgres_double_real_boolean_numeric_land_native_or_exact(monkeypatch):
    """IEEE floats are ``number``, a boolean ``boolean``, a NUMERIC exact text; the
    plan's gates pass them (Weaviate ``number`` is float64, not Oracle NUMBER)
    and the value proof holds every row, a REAL at 3.4e38 included."""
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(host=os.environ.get("PGHOST", "127.0.0.1"), port=5432,
                                dbname="dataflow", user="dataflow", password="dataflow",
                                connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    conn.autocommit = True
    table = f"p2wv_pg_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id int PRIMARY KEY, ratio double precision, "  # noqa: S608
                    "r4 real, flag boolean, amt numeric(20,6), note text)")
        cur.execute(f"INSERT INTO {table} VALUES "  # noqa: S608
                    f"(1, 1.5, 0.1, true, {BIG}, 'a'), (2, 1e-300, 3.4e38, false, 0.000001, 'b'), "
                    "(3, NULL, NULL, NULL, NULL, NULL)")
    wv = TypedWeaviate()
    cls = f"P2WvPg{uuid.uuid4().hex[:6]}"
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    monkeypatch.setattr(weaviate_writer, "_requests_session", lambda: wv)
    try:
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="postgresql", host="127.0.0.1",
                                  port=5432, database="dataflow", username="dataflow",
                                  password="dataflow", schema="public", table=table),
            destination=EndpointConfig(kind="database", format="weaviate", host="127.0.0.1",
                                       port=8080, table=cls, extra={"embedding_model": "hash/32"}),
            sync_mode="full_refresh_overwrite", validation_mode="strict",
        )
        result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # noqa: S608
        conn.close()
    assert result.success is True, result.error
    props = {p["name"]: p["dataType"][0] for p in wv.classes[cls]["properties"]}
    assert (props["ratio"], props["r4"], props["flag"], props["amt"]) == (
        "number", "number", "boolean", "text")
    stored = _stored(wv, cls)
    assert stored["1"]["ratio"] == 1.5 and stored["2"]["ratio"] == 1e-300
    assert stored["2"]["r4"] == 3.4e38 and stored["1"]["flag"] is True
    assert stored["1"]["amt"] == BIG
    assert "hold every source value of 3 row(s)" in str(result.reconciliation.get("message"))


def test_document_proof_compares_numbers_exactly_at_any_width():
    """R9/D352: the proof's numeric spelling never rounds to 28 digits or raises."""
    from decimal import Decimal

    from services.pgvector_document_verify import _canon, compare_documents

    a = Decimal("1234567890123456789012345678901.5")
    b = Decimal("1234567890123456789012345678901.4")
    assert _canon(a) != _canon(b)
    assert _canon(3.4e38) == _canon(Decimal("340000000000000000000000000000000000000"))
    assert _canon(Decimal("1000.00")) == _canon(1000)
    proof = compare_documents([{"x": str(a)}], [{"x": b}], [("x", "x")], numeric={"x"})
    assert proof["passed"] is False and proof["differing_fields"] == ["x"]


def test_d293_gates_grade_weaviate_number_as_float64():
    """R8/R3: the plan's gates read Weaviate ``number`` as float64, not Oracle NUMBER."""
    from services.type_coercion import is_lossy_coercion, is_precision_collapse_coercion

    for grade in (is_lossy_coercion, is_precision_collapse_coercion):
        assert not grade("DOUBLE PRECISION", "number", dest_db="weaviate", dest_table_exists=False)
        assert not grade("REAL", "number", dest_db="weaviate", dest_table_exists=False)
        assert grade("NUMERIC(20,6)", "number", dest_db="weaviate", dest_table_exists=False)
    # Oracle's NUMBER is unchanged: a float into it is still graded lossy.
    assert is_lossy_coercion("DOUBLE PRECISION", "NUMBER", dest_db="oracle")
