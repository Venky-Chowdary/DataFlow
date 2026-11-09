"""D265 / D230 - Oracle NUMBER(18,6) into Weaviate, and job progress during the write.

QA: Oracle ``NUMBER(18,6)`` -> Weaviate. Reproduced live (gvenzl/oracle-free
23 + semitechnologies/weaviate 1.25.4):

* new class: Oracle's ``ID`` folds to ``id`` and was declared as a class
  property; Weaviate refused the class (``'id' is a reserved property name``)
  and the run failed before any row.
* existing class whose ``amount`` is ``number`` (float64): preflight passed,
  the exact decimal text was refused per object, and the NULL-amount row
  landed before strict stopped the run (a partial class to clean up).
* the refusal text ``the given value is '99999999999.999999'`` was masked to
  ``99*******99.999999`` by the PII phone pattern.
* D230: a blocking source wait re-labels the job ``phase="reading"`` after the
  write started, and the per-batch progress write never set the phase back,
  so get_job reported reading while rows were committed.

Fakes parse the REST body the way the server does; the live cases run when
Weaviate is on 127.0.0.1:8080 (and PostgreSQL for the engine case).
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import connectors.weaviate_writer as weaviate_writer  # noqa: E402
from tests.test_r13_precision_vector_decimal_exact_text import (  # noqa: E402
    BIG,
    WeaviateFake,
    _Resp,
    _body,
    pg_table,  # noqa: F401 - fixture
)

BASE = "http://127.0.0.1:8080"
ORA_HEADERS = ["ID", "AMOUNT", "NAME"]
ORA_TYPES = {"ID": "NUMBER(10,0)", "AMOUNT": "NUMBER(18,6)", "NAME": "VARCHAR2(20)"}
ORA_ROWS = [["1", "1.500000", "a"], ["2", BIG, "b"], ["3", "-0.000001", "c"], ["4", "__DF_SQL_NULL__", "d"]]


class StrictWeaviateFake(WeaviateFake):
    """Refuses what the real server refuses: reserved property names, text into number."""

    def post(self, url, data=None, headers=None, timeout=None, json=None):
        body = _body(data, json)
        if url.endswith("/v1/schema"):
            for p in body.get("properties", []):
                if p["name"] in {"id", "_id", "_additional"}:
                    return _Resp(422, {"error": [{"message": f"'{p['name']}' is a reserved property name"}]})
        if url.endswith("/v1/batch/objects"):
            out = []
            for o in body.get("objects", []):
                cls = self.classes.get(o["class"], {"properties": []})
                numeric = {p["name"] for p in cls["properties"] if p["dataType"] == ["number"]}
                bad = [k for k, v in o.get("properties", {}).items() if k in numeric and isinstance(v, str)]
                if bad:
                    out.append({"id": o["id"], "result": {"errors": {"error": [{"message": f"invalid number property '{bad[0]}'"}]}}})
                    continue
                self._store(o["class"], o)
                out.append({"id": o["id"], "class": o["class"], "result": {"status": "SUCCESS"}})
            self.batch_bodies.append(data if isinstance(data, str) else data.decode())
            return _Resp(200, out)
        return super().post(url, data=data, headers=headers, timeout=timeout, json=json)


def _kwargs(cls: str, **over) -> dict:
    kw = dict(host="127.0.0.1", port=8080, database="", username="", password="", schema="",
              connection_string="", ssl=False, table_name=cls, headers=ORA_HEADERS, data_rows=ORA_ROWS,
              mappings=[{"source": h, "target": h, "target_type": "text"} for h in ORA_HEADERS],
              column_types=ORA_TYPES, embedding_model="hash/32", error_policy="strict",
              destination_pk_columns=["ID"])
    kw.update(over)
    return kw


def _number_class(fake: WeaviateFake, cls: str) -> None:
    fake.classes[cls] = {"class": cls, "properties": [
        {"name": "content", "dataType": ["text"]}, {"name": "source_id", "dataType": ["text"]},
        {"name": "chunk_index", "dataType": ["int"]}, {"name": "amount", "dataType": ["number"]},
        {"name": "name", "dataType": ["text"]}]}
    fake.objects[cls] = {}


def test_oracle_upper_case_id_is_identity_not_a_declared_property(monkeypatch):
    fake = StrictWeaviateFake()
    monkeypatch.setattr(weaviate_writer, "_requests_session", lambda: fake)
    result = weaviate_writer.write_mapped_rows(**_kwargs("D265OraNew"))
    assert result.ok, result.error
    props = {p["name"]: p["dataType"] for p in fake.classes["D265OraNew"]["properties"]}
    assert "id" not in props and props["amount"] == ["text"], props
    stored = {o["properties"]["source_id"]: o["properties"] for o in fake.objects["D265OraNew"].values()}
    assert stored["2"]["amount"] == BIG and "amount" not in stored["4"]


def test_decimal_onto_existing_number_property_is_refused_before_any_write(monkeypatch):
    fake = StrictWeaviateFake()
    _number_class(fake, "D265OraNum")
    monkeypatch.setattr(weaviate_writer, "_requests_session", lambda: fake)
    result = weaviate_writer.write_mapped_rows(**_kwargs("D265OraNum"))
    assert not result.ok
    assert "AMOUNT NUMBER(18,6) -> AMOUNT number" in result.error and "nothing was written" in result.error
    assert fake.batch_bodies == [] and fake.objects["D265OraNum"] == {}  # no partial class to clean up
    assert [d["column"] for d in result.rejected_details] == ["AMOUNT"]


def test_integer_onto_existing_number_property_is_not_refused():
    assert weaviate_writer.weaviate_decimal_into_number_columns(
        [{"source": "QTY", "target": "QTY"}, {"source": "AMOUNT", "target": "AMOUNT"}],
        {"QTY": "NUMBER(10,0)", "AMOUNT": "NUMBER(18,6)"},
        {"qty": "FLOAT", "amount": "TEXT"},
    ) == []


def test_decimal_in_a_refusal_message_is_not_masked_as_a_phone():
    from services.pii_guard import _redact_text

    msg = f"invalid number property 'amount': requires a float, the given value is '{BIG}'"
    assert _redact_text(msg) == msg
    assert "415-555-1234" not in _redact_text("call 415-555-1234 now")
    assert "4155551234" not in _redact_text("phone 4155551234.")


# ------------------------------------------------------------ live Weaviate
def _weaviate_up() -> bool:
    import requests

    try:
        return requests.get(f"{BASE}/v1/meta", timeout=2).status_code == 200
    except requests.RequestException:
        return False


@pytest.mark.parametrize("existing_number", [False, True])
def test_live_weaviate_oracle_shaped_numeric(existing_number):
    if not _weaviate_up():
        pytest.skip("Weaviate not reachable on 127.0.0.1:8080")
    import requests

    cls = "D265Live" + uuid.uuid4().hex[:8]
    if existing_number:
        requests.post(f"{BASE}/v1/schema", json={"class": cls, "vectorizer": "none", "properties": [
            {"name": "content", "dataType": ["text"]}, {"name": "source_id", "dataType": ["text"]},
            {"name": "chunk_index", "dataType": ["int"]}, {"name": "amount", "dataType": ["number"]},
            {"name": "name", "dataType": ["text"]}]}, timeout=10).raise_for_status()
    try:
        result = weaviate_writer.write_mapped_rows(**_kwargs(cls))
        objs = requests.get(f"{BASE}/v1/objects", params={"class": cls, "limit": 20}, timeout=10).json()
        stored = {o["properties"]["source_id"]: o["properties"] for o in objs.get("objects") or []}
        if existing_number:
            assert not result.ok and "nothing was written" in result.error
            assert stored == {}
        else:
            assert result.ok, result.error
            schema = requests.get(f"{BASE}/v1/schema/{cls}", timeout=10).json()
            props = {p["name"]: p["dataType"] for p in schema["properties"]}
            assert props["amount"] == ["text"] and "iD" not in props
            assert stored["2"]["amount"] == BIG and stored["3"]["amount"] == "-0.000001"
    finally:
        requests.delete(f"{BASE}/v1/schema/{cls}", timeout=10)


# ------------------------------------------------------------ D230 progress
class _RecordingStore:
    def __init__(self):
        self.jobs: dict[str, dict] = {}
        self.log: list[dict] = []

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def update_job_fields(self, job_id, fields=None, **_k):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True

    def update_job_status(self, job_id, status, **kw):
        self.jobs.setdefault(job_id, {}).update(kw)
        self.jobs[job_id]["status"] = status
        self.log.append({"status": status, **dict(self.jobs[job_id])})
        return True

    def list_jobs(self, limit=50):
        return list(self.jobs.values())

    def create_transfer_job(self, data):
        job_id = data.get("job_id") or uuid.uuid4().hex[:24]
        self.jobs[job_id] = dict(data)
        return job_id

    def __getattr__(self, _name):
        return lambda *a, **k: None


def test_get_job_phase_is_writing_once_rows_are_committed(pg_table, monkeypatch):  # noqa: F811
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    pg, table = pg_table
    store = _RecordingStore()
    fake = WeaviateFake()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(weaviate_writer, "_requests_session", lambda: fake)
    src = EndpointConfig(kind="database", format="postgresql", host=pg["host"], port=pg["port"],
                         database=pg["database"], username=pg["username"], password=pg["password"],
                         schema="public", table=table)
    dst = EndpointConfig(kind="database", format="weaviate", host="127.0.0.1", port=1,
                         table="D230Progress", extra={"embedding_model": "hash/32"})
    req = TransferRequest(source=src, destination=dst, sync_mode="full_refresh_overwrite",
                          skip_preflight=True, validation_mode="strict")
    res = UniversalTransferEngine().execute_tracked(req, "job" + uuid.uuid4().hex[:8])
    assert res.success, res.error
    committed = [s for s in store.log if s["status"] == "running" and int(s.get("records_processed") or 0) > 0]
    assert committed, json.dumps(store.log, default=str)[:2000]
    first = committed[0]
    # get_job reads phase/message/records_processed straight off this document.
    assert first["phase"] == "writing", first
    assert first["message"].startswith("Writing batch") and first["records_processed"] == 4
