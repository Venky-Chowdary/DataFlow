"""D355 - ``mask_pii`` on a JSON column masks the document, and a leak fails the run.

QA round 39 (prod afd88cd3): Timescale (PostgreSQL) -> MongoDB with
``column_transforms {doc: mask_pii}`` alone (6 of 6 rows unmasked) and inside
a 6-step pipeline: the documents landed in plain text and the job was green.

Root cause (not Mongo-specific; PostgreSQL destinations wrote the same
values): the transform masked a JSON document only where a PII pattern
(email, 10-digit phone, dashed SSN, luhn-valid card, IPv4) matched inside its
text. Names, addresses, dates of birth, short phone numbers, unformatted SSNs
and luhn-invalid card numbers stayed in clear, so a document holding none of
the patterns landed whole; a JSON number the card pattern matched became
unquoted text (invalid JSON). Gate-8 and the G-1 proof both compare against
the transform's own output, so they agreed with a transform that protected
nothing.

Fixed: a document is masked value by value, as each value would be masked in
a scalar column (keys, nesting, booleans and nulls keep the document's shape);
and a post-write protection proof fails the run when the destination still
holds a source value in clear in a protected column.
"""

from __future__ import annotations

import json
import socket
import uuid
from types import SimpleNamespace

import pytest

from services.column_transform_chain import compile_column_chains
from services.shape_engine import ShapeEngine
from services.shape_models import ShapeRecipe
from services.transform_engine import apply_transform
from tests.test_d190_d194_d197_declared_string_types_e2e import (
    PG,
    _drop,
    _pg,
    _run,
    _wait_job,
    transfer_client,  # noqa: F401 - fixture
)

pymongo = pytest.importorskip("pymongo")

pytestmark = pytest.mark.timeout(600)

#: QA-like documents: realistic PII, most of which no PII pattern recognises.
DOCS = [
    {
        "customer": {"first_name": "Maria", "last_name": "Garcia", "email": "maria.garcia@example.com"},
        "phone": "555-0101",
        "ssn": "123456789",
        "card": "1234 5678 9012 3456",
        "address": {"line1": "742 Evergreen Terrace", "city": "Springfield", "zip": "49007"},
        "tags": ["vip", "newsletter"],
        "active": True,
        "manager": None,
    },
    {
        "name": "Bob Stone",
        "dob": "1985-03-14",
        "contacts": [{"type": "home", "phone": "+44 20 7946 0958"}, {"type": "work", "email": "bob@corp.example.org"}],
        "notes": "Prefers calls after 5pm",
    },
    {"name": "Chen Wei", "passport": "E12345678", "card_number": "4000 1234 5678 9010", "visits": 12},
    {"patient": {"name": "Ana Lima", "mrn": "MRN-00451", "diagnosis": "asthma"}, "ip": "10.1.2.3"},
    {"name": "Olu Ade", "ssn": "078-05-1120", "salary": 85000.50, "children": ["Tobi", "Kemi"]},
    {"employee_id": "E-9921", "full_name": "Sam Lee", "home": {"street": "1 Main St", "unit": "4B"}},
]

SIX_STEPS = ["trim", "strip_controls", "normalize_unicode", "mask_pii", "trim", "default:{}"]


def _leaves(node):
    if isinstance(node, dict):
        for v in node.values():
            yield from _leaves(v)
    elif isinstance(node, list):
        for v in node:
            yield from _leaves(v)
    elif node is not None and not isinstance(node, bool):
        yield str(node)


def _shape(node):
    """Keys and nesting, with every value replaced by its kind."""
    if isinstance(node, dict):
        return {k: _shape(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_shape(v) for v in node]
    return "flag" if isinstance(node, bool) or node is None else "value"


def _flags(node):
    if isinstance(node, dict):
        for v in node.values():
            yield from _flags(v)
    elif isinstance(node, list):
        for v in node:
            yield from _flags(v)
    elif node is None or isinstance(node, bool):
        yield node


def _assert_masked(doc: dict, masked) -> None:
    masked = json.loads(masked) if isinstance(masked, str) else masked
    assert _shape(masked) == _shape(doc)
    clear = set(_leaves(doc))
    leaked = [v for v in _leaves(masked) if v in clear]
    assert not leaked, leaked
    # Booleans and nulls carry no value to hide: the document keeps them.
    assert list(_flags(masked)) == list(_flags(doc))


# ----------------------------------------------------------------- transform


@pytest.mark.parametrize("doc", DOCS, ids=[f"doc{i}" for i in range(len(DOCS))])
def test_mask_pii_masks_every_value_of_a_json_document(doc):
    out, err = apply_transform(json.dumps(doc), "mask_pii")
    assert err is None
    _assert_masked(doc, out)


def test_a_native_document_is_masked_like_its_json_text():
    for doc in DOCS:
        assert apply_transform(doc, "mask_pii") == apply_transform(json.dumps(doc), "mask_pii")


def test_values_keep_the_scalar_masks_shape():
    out = json.loads(apply_transform(json.dumps(DOCS[0]), "mask_pii")[0])
    scalar = {v: apply_transform(v, "mask_pii")[0] for v in ("maria.garcia@example.com", "Springfield")}
    assert out["customer"]["email"] == scalar["maria.garcia@example.com"] == "m***********@*******.com"
    assert out["address"]["city"] == scalar["Springfield"] == "Sp*******ld"


def test_a_json_number_is_masked_to_valid_json_not_unquoted_text():
    out, err = apply_transform('{"card": 4111111111111111, "phone": 5551234567}', "mask_pii")
    assert err is None
    parsed = json.loads(out)  # base wrote {"card": 411111…1111, ...}: not JSON
    assert parsed == {"card": "411111…1111", "phone": "55******67"}


def test_a_scalar_no_pattern_changes_is_still_masked():
    # Luhn-invalid 16 digits: the card pattern declines, the value was returned as is.
    assert apply_transform("1234 5678 9012 3456", "mask_pii")[0] == "1234 5…3456"
    assert apply_transform("1234567890123456", "mask_pii")[0] == "123456…3456"
    # Unchanged scalar semantics.
    assert apply_transform("a@b.co", "mask_pii")[0] == "a*@***.co"
    assert apply_transform("Alice Johnson", "mask_pii")[0] == "Alice …nson"
    assert apply_transform("", "mask_pii")[0] == ""
    assert apply_transform(None, "mask_pii")[0] is None


def test_text_that_only_looks_like_json_is_masked_as_text():
    assert apply_transform("[draft] notes about Bob", "mask_pii")[0] == "[draft… Bob"


@pytest.mark.parametrize("chain", [["mask_pii"], SIX_STEPS])
def test_a_pipeline_masks_the_document_like_the_single_step(chain):
    payload, refusal = compile_column_chains({"doc": chain}, ["id", "doc"])
    assert refusal == ""
    engine = ShapeEngine(ShapeRecipe.parse(payload, source_columns=["id", "doc"]))
    rows = engine.apply_batch([{"id": i, "doc": json.dumps(d)} for i, d in enumerate(DOCS)])
    for doc, row in zip(DOCS, rows):
        _assert_masked(doc, row["doc"])
        assert row["doc"] == apply_transform(json.dumps(doc), "mask_pii")[0]


# --------------------------------------------------------------------- proof


def test_protected_columns_come_from_map_transforms_and_pipeline_steps():
    from src.transfer.protection_proof import protected_columns

    maps = [
        {"source": "id", "target": "id", "transform": "none"},
        {"source": "doc", "target": "document", "transform": "json"},
        {"source": "email", "target": "email", "transform": "sha256"},  # alias of hash_pii
        {"source": "note", "target": "note", "transform": "redact"},
    ]
    recipe = {"steps": [
        {"op": "trim", "column": "doc"},
        {"op": "mask_pii", "column": "doc"},
        {"op": "mask_pii", "column": "derived_only"},  # not a mapped source column
        {"op": "redact", "column": "id", "enabled": False},
    ]}
    assert sorted(protected_columns(maps, recipe)) == [
        ("doc", "document", "mask_pii"),
        ("email", "email", "hash_pii"),
        ("note", "note", "redact"),
    ]
    assert protected_columns([{"source": "a", "target": "a", "transform": "trim"}], None) == []


def _proof(monkeypatch, src_rows, dest_rows, *, sync_mode="full_refresh_overwrite", **summary):
    import services.reconciliation as recon_mod
    import src.transfer.reconcile_step as rs
    from src.transfer.protection_proof import protection_proof

    monkeypatch.setattr(rs, "_proof_source_batches", lambda **_k: iter([src_rows]))
    monkeypatch.setattr(recon_mod, "read_target_sample", lambda *_a, **_k: dest_rows)
    endpoint = SimpleNamespace(format="mongodb", kind="database", table="t", collection="t",
                               host="127.0.0.1", port=27017, database="d", schema="")
    source = SimpleNamespace(format="postgresql", kind="database", table="t", schema="public",
                             host="127.0.0.1", port=5432, database="d")
    monkeypatch.setattr("src.transfer.adapters.resolve_connector_config",
                        lambda ep: {"type": ep.format, "database": "d"})
    return protection_proof(
        endpoint=endpoint,
        source_endpoint=source,
        dest_summary={"sync_mode": sync_mode, "table": "t", **summary},
        mappings=[{"source": "id", "target": "id", "transform": "none"},
                  {"source": "doc", "target": "doc", "transform": "mask_pii"}],
        shape_recipe=None,
    )


def test_proof_fails_a_destination_document_still_holding_a_clear_value(monkeypatch):
    from src.transfer.protection_proof import apply_protection_proof

    src = [{"id": i, "doc": json.dumps(d)} for i, d in enumerate(DOCS)]
    leaked_doc = json.loads(apply_transform(json.dumps(DOCS[1]), "mask_pii")[0])
    leaked_doc["contacts"][0]["phone"] = "+44 20 7946 0958"  # one value left in clear
    dest = [{"id": 0, "doc": apply_transform(json.dumps(DOCS[0]), "mask_pii")[0]},
            {"id": 1, "doc": leaked_doc},  # a native BSON document
            {"id": 2, "doc": json.dumps(DOCS[2])}]  # the whole document in clear
    proof = _proof(monkeypatch, src, dest)
    assert proof["status"] == "failed"
    assert proof["leaked_columns"] == ["doc"]
    assert "doc (2 rows)" in proof["reason"]
    assert "7946" not in json.dumps(proof) and "Chen" not in json.dumps(proof)

    recon = {"passed": True, "verification_verdict": "verified",
             "message": "Row fidelity verified — source and target checksums match (3 rows)"}
    apply_protection_proof(recon, proof)
    assert recon["passed"] is False and recon["verification_verdict"] == "failed"
    assert recon["message"].startswith("Protection proof failed:")
    assert "checksums match" not in recon["message"]


def test_proof_verifies_a_masked_destination(monkeypatch):
    src = [{"id": i, "doc": json.dumps(d)} for i, d in enumerate(DOCS)]
    dest = [{"id": i, "doc": apply_transform(json.dumps(d), "mask_pii")[0]} for i, d in enumerate(DOCS)]
    proof = _proof(monkeypatch, src, dest)
    assert proof["status"] == "verified", proof
    assert proof["complete"] is True and proof["destination_rows_checked"] == len(DOCS)


def test_proof_does_not_judge_rows_another_run_wrote(monkeypatch):
    src = [{"id": 0, "doc": json.dumps(DOCS[0])}]
    proof = _proof(monkeypatch, src, [{"id": 0, "doc": json.dumps(DOCS[0])}],
                   sync_mode="incremental_append", dest_table_existed_before=True)
    assert proof["status"] == "skipped"
    first_load = _proof(monkeypatch, src, [{"id": 0, "doc": json.dumps(DOCS[0])}],
                        sync_mode="incremental_append", dest_table_existed_before=False)
    assert first_load["status"] == "failed"


def test_proof_that_cannot_read_is_unverifiable_never_verified(monkeypatch):
    import src.transfer.reconcile_step as rs

    def _boom(**_k):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(rs, "_proof_source_batches", _boom)
    monkeypatch.setattr("src.transfer.adapters.resolve_connector_config",
                        lambda ep: {"type": ep.format, "database": "d"})
    from src.transfer.protection_proof import protection_proof

    out = protection_proof(
        endpoint=SimpleNamespace(format="mongodb", kind="database", table="t", collection="t"),
        source_endpoint=SimpleNamespace(format="postgresql", kind="database", table="t"),
        dest_summary={"sync_mode": "full_refresh_overwrite"},
        mappings=[{"source": "doc", "target": "doc", "transform": "mask_pii"}],
        shape_recipe=None,
    )
    assert out["status"] == "unverifiable"


# ---------------------------------------------------------------- live routes


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.fixture
def pg_source_table():
    conn = _pg()
    table = "d355_" + uuid.uuid4().hex[:8]
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE public."{table}" (id integer PRIMARY KEY, doc jsonb)')
            for i, doc in enumerate(DOCS, 1):
                cur.execute(f'INSERT INTO public."{table}" VALUES (%s, %s)', (i, json.dumps(doc)))
        yield table
    finally:
        conn.close()
        _drop(table)


@pytest.fixture
def mongo_db():
    if not _up(27017):
        pytest.skip("MongoDB not reachable on :27017")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    db = client["dataflow_test"]
    created: list[str] = []
    try:
        yield db, created
    finally:
        for name in created:
            db[name].drop()
        client.close()


def _connectors() -> tuple[str, str]:
    from services.connector_store import create_connector

    src = create_connector({
        "name": "d355-pg-" + uuid.uuid4().hex[:6], "type": "postgresql", "role": "source",
        "host": PG["host"], "port": PG["port"], "database": PG["dbname"],
        "username": PG["user"], "password": PG["password"], "schema": "public",
        "ssl": False, "workspace_id": "",
    })
    dst = create_connector({
        "name": "d355-mongo-" + uuid.uuid4().hex[:6], "type": "mongodb", "role": "destination",
        "host": "127.0.0.1", "port": 27017, "database": "dataflow_test",
        "ssl": False, "workspace_id": "",
    })
    return str(src.id), str(dst.id)


def _stage(table: str, collection: str, transform) -> dict:
    src, dst = _connectors()
    return dict(
        source_connector_id=src,
        source_table=table,
        dest_connector_id=dst,
        dest_table=collection,
        sync_mode="full_refresh_overwrite",
        column_transforms={"doc": transform},
        pii_acknowledgement={"approved_by": "qa@example.com", "reason": "d355"},
    )


def _dest_docs(db, collection: str) -> dict[int, dict]:
    return {d["id"]: d["doc"] for d in db[collection].find({}, {"_id": 0})}


@pytest.mark.parametrize("transform", ["mask_pii", SIX_STEPS], ids=["single_step", "six_step_pipeline"])
def test_qa_route_postgres_jsonb_to_mongo_lands_masked(
    transfer_client, pg_source_table, mongo_db, transform  # noqa: F811
):
    db, created = mongo_db
    collection = pg_source_table + "_m"
    created.append(collection)
    _plan, job = _run(transfer_client, **_stage(pg_source_table, collection, transform))
    docs = _dest_docs(db, collection)
    assert sorted(docs) == list(range(1, len(DOCS) + 1))
    for i, doc in enumerate(DOCS, 1):
        assert isinstance(docs[i], dict), docs[i]  # still a BSON document, not text
        _assert_masked(doc, docs[i])
    proof = (job.get("reconciliation") or {}).get("protection_proof") or {}
    assert proof.get("status") == "verified", proof
    assert proof.get("columns") == ["doc"]


def test_qa_route_fails_when_the_destination_holds_a_clear_value(
    transfer_client, pg_source_table, mongo_db, monkeypatch  # noqa: F811
):
    """A protection that writes the clear document (as base did for documents
    no pattern matched) must not leave the job green."""
    import services.transform_engine as te
    from src.ai.copilot.transfer_tools import start_transfer

    monkeypatch.setattr(te, "mask_pii_cell", lambda raw, text: text, raising=False)
    monkeypatch.setattr(te, "pii_mask", lambda text: text, raising=False)
    db, created = mongo_db
    collection = pg_source_table + "_leak"
    created.append(collection)
    staged = start_transfer(validation_mode="strict", **_stage(pg_source_table, collection, "mask_pii"))
    assert staged.success, staged.error
    confirmed = transfer_client.post(
        "/api/v1/copilot/confirm", json={"ack_id": staged.output["ack_id"], "reason": "d355"}
    )
    assert confirmed.status_code == 200, confirmed.text
    job = _wait_job(str(confirmed.json().get("job_id") or ""))
    assert job.get("status") == "failed", {k: job.get(k) for k in ("status", "message")}
    message = str(job.get("error") or job.get("message") or "")
    assert "Protection proof failed" in message and "doc (6 rows)" in message, message
    # The failure names the column, never the values it found.
    assert "Garcia" not in message and "Stone" not in message
    proof = (job.get("reconciliation") or {}).get("protection_proof") or {}
    assert proof.get("leaked_columns") == ["doc"]
