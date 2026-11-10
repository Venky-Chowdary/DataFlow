"""D285 - Qdrant payload decimals into a new Weaviate class; a failed run leaves nothing.

QA (Nov 3, Qdrant 30 points -> new Weaviate class, overwrite strict): the
payload amount 99999999999.999999 was stored as the number 100000000000, the
job failed on its own value check and reported 'the partial batch could not be
removed'.

On this code the value side is already exact (round 13: the class declares a
decimal ``text`` and the payload carries the exact JSON text). What still
reproduced is the residue: any strict failure after Weaviate accepted part of
a batch left those objects in the class the run created. The undo had no
existence probe for Weaviate (so it could not tell "created by this run" from
"empty before") and no Weaviate branch to remove objects, so it reported
'could not be removed' and kept them.

No Qdrant/Weaviate server here: REST fakes parse the bodies the way the
servers do; the engine runs in process (memory job store).
"""

from __future__ import annotations

import os
import uuid

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("DATAFLOW_EMBEDDING_MODEL", "hash/32")

import connectors.qdrant_writer as qdrant_writer  # noqa: E402
import connectors.weaviate_writer as weaviate_writer  # noqa: E402
from tests.test_r13_precision_vector_decimal_exact_text import (  # noqa: E402
    BIG,
    QdrantFake,
    WeaviateFake,
    _body,
    _Resp,
)

CLS = "D285QdWv"
FP_CLASS = "DfVectorCollections"


class _RawResp(_Resp):
    def __init__(self, status: int, text: str):
        self.status_code = status
        self.text = text
        self.content = text.encode()


class QdrantSource(QdrantFake):
    """Scroll answers the exact JSON text a Qdrant server sends."""

    def __init__(self, amounts: dict[int, str]):
        super().__init__()
        points = ",".join(
            f'{{"id": {i}, "payload": {{"id": {i}, "amount": {amt}, "code": "c{i}"}}}}'
            for i, amt in amounts.items()
        )
        self.raw = f'{{"result": {{"points": [{points}], "next_page_offset": null}}, "status": "ok"}}'
        self.collections["src"] = {"params": {}}
        self.points["src"] = {str(i): {"id": i} for i in amounts}

    def post(self, url, data=None, headers=None, timeout=None, json=None):
        if url.endswith("/points/scroll"):
            return _RawResp(200, self.raw)
        return super().post(url, data=data, headers=headers, timeout=timeout, json=json)


class TypedWeaviate(WeaviateFake):
    """Refuses a value whose JSON type is not the property's, as the server does."""

    def __init__(self, refuse_source_ids: frozenset[str] = frozenset()):
        super().__init__()
        self.refuse = refuse_source_ids

    def post(self, url, data=None, headers=None, timeout=None, json=None):
        body = _body(data, json)
        if url.endswith("/v1/batch/objects"):
            out = []
            for o in body.get("objects", []):
                cls = self.classes.get(o["class"], {"properties": []})
                kinds = {p["name"]: p["dataType"][0] for p in cls["properties"]}
                props = o.get("properties", {})
                bad = [
                    k for k, v in props.items()
                    if v is not None and (
                        (kinds.get(k) == "text" and not isinstance(v, str))
                        or (kinds.get(k) == "number" and isinstance(v, str))
                    )
                ]
                if bad or props.get("source_id") in self.refuse:
                    msg = f"invalid property '{bad[0]}'" if bad else "refused by test"
                    out.append({"id": o["id"], "result": {"errors": {"error": [{"message": msg}]}}})
                    continue
                self._store(o["class"], o)
                out.append({"id": o["id"], "class": o["class"], "result": {"status": "SUCCESS"}})
            self.batch_bodies.append(data if isinstance(data, str) else data.decode())
            return _Resp(200, out)
        return super().post(url, data=data, headers=headers, timeout=timeout, json=json)

    def delete(self, url, headers=None, timeout=None, data=None, json=None):
        super().delete(url, headers=headers, timeout=timeout, data=data, json=json)
        return _Resp(200, {})  # the server answers 200 on a class or object delete


def _amounts() -> dict[int, str]:
    edge = {2: BIG, 3: "-0.000001", 4: "1e-300", 30: "null"}
    return {i: edge.get(i, f"{i}.5") for i in range(1, 31)}


def _run(monkeypatch, qd, wv, *, sync_mode="full_refresh_overwrite"):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    class _Mongo(_FakeMongo):
        def update_job_fields(self, *a, **k):
            return None

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    monkeypatch.setattr(qdrant_writer, "_requests_session", lambda: qd)
    monkeypatch.setattr(weaviate_writer, "_requests_session", lambda: wv)
    src = EndpointConfig(kind="database", format="qdrant", host="127.0.0.1", port=6333, table="src")
    dst = EndpointConfig(kind="database", format="weaviate", host="127.0.0.1", port=8080,
                         table=CLS, extra={"embedding_model": "hash/32"})
    req = TransferRequest(source=src, destination=dst, sync_mode=sync_mode,
                          validation_mode="strict")
    return UniversalTransferEngine().execute_tracked(req, "job" + uuid.uuid4().hex[:8])


def test_failed_strict_run_into_a_new_class_removes_the_class_it_created(monkeypatch):
    wv = TypedWeaviate(refuse_source_ids=frozenset({"7"}))
    res = _run(monkeypatch, QdrantSource(_amounts()), wv)
    assert not res.success
    assert "rejected 1 batch object" in str(res.error)
    # Weaviate had accepted 29 objects; the class this run created is gone,
    # with the embedding fingerprint the run stamped for it.
    assert CLS not in wv.classes and not wv.objects.get(CLS)
    assert not wv.objects.get(FP_CLASS)


def test_destination_table_exists_answers_for_weaviate(monkeypatch):
    from services.dest_precount import destination_table_exists

    wv = TypedWeaviate()
    wv.classes[CLS] = {"class": CLS, "properties": []}
    monkeypatch.setattr(weaviate_writer, "_requests_session", lambda: wv)
    cfg = {"type": "weaviate", "host": "127.0.0.1", "port": 8080}
    assert destination_table_exists("weaviate", cfg, schema="", table_name=CLS) is True
    assert destination_table_exists("weaviate", cfg, schema="", table_name="d285Missing") is False


def test_qdrant_payload_decimals_land_as_exact_text_in_a_new_class(monkeypatch):
    wv = TypedWeaviate()
    res = _run(monkeypatch, QdrantSource(_amounts()), wv)
    assert res.success, res.error
    props = {p["name"]: p["dataType"] for p in wv.classes[CLS]["properties"]}
    assert props["amount"] == ["text"], props
    stored = {o["properties"]["source_id"]: o["properties"] for o in wv.objects[CLS].values()}
    assert len(stored) == 30
    assert stored["2"]["amount"] == BIG
    assert stored["3"]["amount"] == "-0.000001"
    assert stored["4"]["amount"] == "1e-300"
    assert "amount" not in stored["30"]  # JSON null stays absent, never 0


def test_decimal_into_an_existing_number_property_is_blocked_before_any_write(monkeypatch):
    wv = TypedWeaviate()
    wv.classes[CLS] = {"class": CLS, "properties": [
        {"name": n, "dataType": [d]} for n, d in (
            ("content", "text"), ("source_id", "text"), ("chunk_index", "int"),
            ("amount", "number"), ("code", "text"))]}
    wv.objects[CLS] = {}
    res = _run(monkeypatch, QdrantSource(_amounts()), wv, sync_mode="append")
    assert not res.success
    assert "amount" in str(res.error)
    assert wv.batch_bodies == [] and wv.objects[CLS] == {}
