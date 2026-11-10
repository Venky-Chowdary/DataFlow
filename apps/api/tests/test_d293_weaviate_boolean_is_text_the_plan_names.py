"""D293 - a BOOLEAN into a new Weaviate class: the plan says text, the writer sent a bool.

QA (Kafka 30 JSON messages, ``flag`` boolean -> new Weaviate class, strict):
the plan mapped BOOLEAN -> TEXT and the class declared ``flag: text``, but
the batch carried the JSON boolean, so Weaviate refused 29 of 30 objects
('invalid text property flag ... not a string, but bool') and the all-NULL
row was left in the class.

Root cause: ``weaviate_mapped_property_types`` declares every non-integer
carrier ``text`` (``DDL_TYPES["weaviate"]``), while the record preparation
keeps a source boolean as a Python bool (and a list of strings as a list);
``build_weaviate_objects`` sent them as they were. The Gate-8 read-back then
compared the source ``True`` with the stored ``"true"`` byte for byte.

Fix: a property the class declares ``text`` (the class this run creates, or
the live class) gets the canonical text of the cell (``true``/``false``,
compact JSON), and Gate-8 compares a source-declared BOOLEAN field as a
boolean on both sides. The residue part is D285 (the undo drops a class a
failed run created).

No Weaviate server here: the fake refuses a value whose JSON type is not
the property's, as the server does.
"""

from __future__ import annotations

import json
import os
import uuid

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("DATAFLOW_EMBEDDING_MODEL", "hash/32")

import connectors.weaviate_writer as weaviate_writer  # noqa: E402
from tests.test_d285_qdrant_to_weaviate_decimal_and_failed_run_residue import (  # noqa: E402
    CLS,
    QdrantSource,
    TypedWeaviate,
    _run,
)

NULL = "__DF_SQL_NULL__"
HEADERS = ["id", "flag", "ratio", "d", "u", "doc", "tags"]
TYPES = {"id": "INTEGER", "flag": "BOOLEAN", "ratio": "DOUBLE PRECISION", "d": "DATE",
         "u": "UUID", "doc": "JSON", "tags": "ARRAY"}
ROWS = [
    ["1", "true", "1.5", "2024-01-02", "00000000-0000-0000-0000-000000000001", '{"a":1}', '["x","y"]'],
    ["2", "false", "1e-300", "2024-02-29", "00000000-0000-0000-0000-000000000002", "[1,2]", '["z"]'],
    ["3", NULL, NULL, NULL, NULL, NULL, NULL],
]


def _write(fake, cls, monkeypatch):
    monkeypatch.setattr(weaviate_writer, "_requests_session", lambda: fake)
    return weaviate_writer.write_mapped_rows(
        host="127.0.0.1", port=8080, database="", username="", password="", schema="",
        connection_string="", ssl=False, table_name=cls, headers=HEADERS, data_rows=ROWS,
        mappings=[{"source": h, "target": h, "target_type": "text"} for h in HEADERS],
        column_types=TYPES, embedding_model="hash/32", error_policy="strict",
        destination_pk_columns=["id"],
    )


def _stored(fake, cls):
    return {o["properties"]["source_id"]: o["properties"] for o in fake.objects[cls].values()}


def test_new_class_stores_every_planned_text_column_as_its_exact_text(monkeypatch):
    fake = TypedWeaviate()
    result = _write(fake, "D293New", monkeypatch)
    assert result.ok, result.error
    assert result.rows_written == 3
    props = {p["name"]: p["dataType"] for p in fake.classes["D293New"]["properties"]}
    assert {k: props[k] for k in HEADERS[1:]} == {k: ["text"] for k in HEADERS[1:]}
    stored = _stored(fake, "D293New")
    assert stored["1"]["flag"] == "true" and stored["2"]["flag"] == "false"
    assert stored["1"]["ratio"] == "1.5" and stored["2"]["ratio"] == "1e-300"
    assert stored["1"]["d"] == "2024-01-02"
    assert stored["1"]["u"] == "00000000-0000-0000-0000-000000000001"
    assert json.loads(stored["1"]["doc"]) == {"a": 1} and json.loads(stored["2"]["doc"]) == [1, 2]
    assert json.loads(stored["1"]["tags"]) == ["x", "y"]
    assert all(isinstance(stored[s][k], str) for s in ("1", "2") for k in HEADERS[1:])
    assert not set(HEADERS[1:]) & set(stored["3"])  # SQL NULL omitted, never "" / "null"


def test_existing_class_follows_its_live_property_types(monkeypatch):
    fake = TypedWeaviate()
    fake.classes["D293Live"] = {"class": "D293Live", "properties": [
        {"name": n, "dataType": [d]} for n, d in (
            ("content", "text"), ("source_id", "text"), ("chunk_index", "int"),
            ("flag", "boolean"), ("ratio", "text"), ("d", "text"), ("u", "text"),
            ("doc", "text"), ("tags", "text"))]}
    fake.objects["D293Live"] = {}

    result = _write(fake, "D293Live", monkeypatch)
    assert result.ok, result.error
    stored = _stored(fake, "D293Live")
    assert stored["1"]["flag"] is True and stored["2"]["flag"] is False  # boolean property keeps a bool
    assert stored["1"]["tags"] == '["x","y"]'


def test_kafka_shaped_boolean_source_lands_30_of_30_and_passes_gate8(monkeypatch):
    """The QA shape: a JSON boolean per message, the last message all NULL."""
    qd = QdrantSource({})
    points = []
    for i in range(1, 31):
        flag = None if i == 30 else bool(i % 2)
        points.append({"id": i, "payload": {"id": i, "flag": flag, "code": None if i == 30 else f"c{i}"}})
    qd.raw = json.dumps({"result": {"points": points, "next_page_offset": None}, "status": "ok"})
    qd.points["src"] = {str(i): {"id": i} for i in range(1, 31)}
    wv = TypedWeaviate()
    res = _run(monkeypatch, qd, wv)
    assert res.success, res.error
    props = {p["name"]: p["dataType"] for p in wv.classes[CLS]["properties"]}
    assert props["flag"] == ["text"]
    stored = _stored(wv, CLS)
    assert len(stored) == 30
    assert stored["1"]["flag"] == "true" and stored["2"]["flag"] == "false"
    assert "flag" not in stored["30"]


def test_gate8_compares_a_source_boolean_with_its_stored_text():
    from services.pgvector_document_verify import boolean_keys, compare_documents

    mappings = [{"source": "flag", "target": "flag", "target_type": "text"}]
    assert boolean_keys(mappings, {"flag": "BOOLEAN"}) == {"flag"}
    assert boolean_keys(mappings) == set()
    proof = compare_documents(
        [{"flag": True}, {"flag": False}, {"flag": None}],
        [{"flag": "true"}, {"flag": "false"}, {"flag": None}],
        [("flag", "flag")],
        boolean=boolean_keys(mappings, {"flag": "BOOLEAN"}),
    )
    assert proof["passed"], proof
    wrong = compare_documents(
        [{"flag": True}], [{"flag": "false"}], [("flag", "flag")], boolean={"flag"}
    )
    assert not wrong["passed"]


def test_build_objects_leaves_non_text_properties_alone():
    rows = [{"id": "1", "source_id": "1", "chunk_index": 0, "content": "x",
             "embedding": [0.1, 0.2], "metadata": {"flag": True, "n": 5}}]
    objects, rejected = weaviate_writer.build_weaviate_objects(
        rows, class_name="C", dimension=2, text_properties={"flag"})
    assert not rejected
    assert objects[0]["properties"]["flag"] == "true" and objects[0]["properties"]["n"] == 5
    objects, _ = weaviate_writer.build_weaviate_objects(rows, class_name="C", dimension=2)
    assert objects[0]["properties"]["flag"] is True
    assert str(uuid.UUID(objects[0]["id"]))
