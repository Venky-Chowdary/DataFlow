"""D397: redact / hash_pii on a JSON column into MongoDB writes a valid BSON
value, decided by the transform, never by what the text looks like.

QA: MySQL ``payload JSON`` -> Mongo with ``payload: redact`` refused all 21
rows ("MongoDB JSON refused <redacted> (refuse silent string invent)"); the
Snowflake VARIANT hash_pii route quarantined 9 of 10.

PR #433 (6bbe210cd) made ``bind_mongo_json_document`` wrap any cell that
*looked* like a protection token (the literal ``<redacted>`` or any 32-hex
string) into an invented document ``{"_datawrap_redacted": ...}`` /
``{"_datawrap_hash": ...}``. On the live route every run then failed its
read-back ("source='\"<redacted>\"' dest='{\"_datawrap_redacted\":...}'"), and
an untransformed JSON cell holding a bare MD5 would have been relabelled as a
PII hash.

Rule applied: the redact token and the hash_pii digest are text. Into a JSON
field they are stored as a JSON string value (RFC 8259 allows a string as a
JSON text; the SQL destinations already bind them as a JSON string literal,
and the read-back expects exactly that). The decision is the mapping's
transform, shared by the coercion probe (preflight) and the writer (R8). Any
other non-JSON text still refuses before Execute.

Live MySQL / PostgreSQL sources -> live MongoDB.
"""

from __future__ import annotations

import json
import re
import uuid

import pytest

# Build Settings before ``transfer_client`` blanks DATAFLOW_SEED_DEMO.
import services.config  # noqa: F401
from tests import value_fidelity_route as r
from tests.test_d190_d194_d197_declared_string_types_e2e import (
    _run,
    transfer_client,  # noqa: F401 - fixture
)
from tests.test_d355_mask_pii_json_document_mongo import mongo_db  # noqa: F401

pytestmark = pytest.mark.timeout(300)


def test_writer_binds_a_protection_token_as_a_json_string_by_transform():
    from connectors.mongodb_writer import bind_mongo_json_document, bind_mongo_json_field

    digest = "553cd7" + "ab" * 13
    assert bind_mongo_json_field("<redacted>", "redact") == "<redacted>"
    assert bind_mongo_json_field(digest, "hash_pii") == digest
    assert bind_mongo_json_field('{"a": 1}', "") == {"a": 1}
    # No transform: text is judged as JSON, whatever it looks like.
    for text in ("<redacted>", digest, "not json"):
        with pytest.raises(ValueError, match="refuse silent string invent"):
            bind_mongo_json_document(text)
        with pytest.raises(ValueError, match="refuse silent string invent"):
            bind_mongo_json_field(text, "")


def test_preflight_passes_the_tokens_into_a_mongo_json_field():
    """Validate binds the cell through the writer's own function (R8)."""
    from services.coercion_probe import analyze_coercion

    for transform in ("redact", "hash_pii"):
        report = analyze_coercion(
            mappings=[{"source": "payload", "target": "payload", "transform": transform,
                       "source_type": "JSON", "target_type": "OBJECT"}],
            sample_rows=[{"payload": '{"email": "a@example.com"}'}, {"payload": '{"n": 2}'}],
            source_types={"payload": "JSON"},
            dest_types={"payload": "OBJECT"},
            dest_db_type="mongodb",
            validation_mode="strict",
        )
        column = report["columns"][0]
        assert column["failed"] == 0 and column["severity"] == "ok", (transform, column)


def _seed(kind: str, table: str) -> None:
    jt = "JSON" if kind == "mysql" else "JSONB"
    r.execute(kind, f"CREATE TABLE {r.quote(kind, table)} (id INT PRIMARY KEY, payload {jt}, note VARCHAR(20))")
    for i in range(1, 6):
        payload = None if i == 3 else json.dumps({"email": f"u{i}@example.com", "n": i})
        r.execute(
            kind,
            f"INSERT INTO {r.quote(kind, table)} VALUES (%s, %s, %s)",  # nosec B608
            (i, payload, f"n{i}"),
        )


@pytest.mark.parametrize("transform", ["redact", "hash_pii"])
@pytest.mark.parametrize("src_kind", ["mysql", "postgresql"])
def test_protected_json_lands_in_mongo_and_is_verified(
    transfer_client, mongo_db, transform, src_kind  # noqa: F811
):
    from services.connector_store import create_connector

    db, created = mongo_db
    table = f"p2d397_{uuid.uuid4().hex[:8]}"
    collection = table + "_m"
    created.append(collection)
    _seed(src_kind, table)
    try:
        src = create_connector({
            "name": "p2-src-" + table, "role": "source", "workspace_id": "", "ssl": False,
            **r.dest_config(src_kind),
        })
        dst = create_connector({
            "name": "p2-mongo-" + table, "type": "mongodb", "role": "destination",
            "host": "127.0.0.1", "port": 27017, "database": "dataflow_test",
            "ssl": False, "workspace_id": "",
        })
        _plan, job = _run(
            transfer_client,
            source_connector_id=src.id, source_table=table,
            dest_connector_id=dst.id, dest_table=collection,
            sync_mode="full_refresh_overwrite",
            column_transforms={"payload": transform},
            pii_acknowledgement={"approved_by": "qa@example.com", "reason": "d397"},
        )
        docs = {d["id"]: d for d in db[collection].find({}, {"_id": 0})}
        assert sorted(docs) == [1, 2, 3, 4, 5]
        assert docs[3]["payload"] is None
        for i in (1, 2, 4, 5):
            value = docs[i]["payload"]
            assert isinstance(value, str), value
            assert "example.com" not in value
            if transform == "redact":
                assert value == "<redacted>"
            else:
                assert re.fullmatch(r"[0-9a-f]{32}", value), value
            assert docs[i]["note"] == f"n{i}"
        proof = (job.get("reconciliation") or {}).get("protection_proof") or {}
        assert proof.get("status") == "verified", proof
    finally:
        r.drop(src_kind, table)
