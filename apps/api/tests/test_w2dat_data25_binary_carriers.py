"""DATA-25: binary round-trips as binary, or is labelled where it cannot.

Before the fix a MongoDB BinData field that the sampled documents carried
sparsely was typed from its stringified sample ('AP8B') and PostgreSQL got
the base64 text in a TEXT column. Redis stores each row as a JSON text
value, so bytes land base64-encoded there; that is now labelled "stored as
base64 text" on the Map mapping and in the Trust Report (no raw-bytes mode).
"""

from __future__ import annotations

import pytest

from tests import w2dat_live as live


def _mongo():
    if not live.reachable("127.0.0.1", 27017):
        pytest.skip("MongoDB is not reachable")
    import pymongo

    return pymongo.MongoClient("127.0.0.1", 27017, serverSelectionTimeoutMS=5000)


def test_sparse_mongo_binary_lands_as_bytea():
    live.need_pg()
    from bson.binary import Binary

    client = _mongo()
    coll, table = f"w2dat_d25m_{live.uid()}", f"w2dat_d25p_{live.uid()}"
    docs = [{"_id": i, "n": i, "addr": {"city": "x"}} for i in range(1, 5)]
    docs.append({"_id": 5, "n": 5, "addr": {}, "bin": Binary(b"\x00\xff\x01")})
    client["dataflow"][coll].insert_many(docs)
    try:
        res = live.run(live.ep_mongo(coll), live.ep_pg(table), key="_id")
        assert res.success, res.error
        types = dict(live.pg(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table,)))
        assert types["bin"] == "bytea"
        got = live.pg(f'SELECT encode(bin, \'hex\') FROM "{table}" WHERE bin IS NOT NULL')
        assert got == [("00ff01",)]
    finally:
        client["dataflow"].drop_collection(coll)
        client.close()
        live.drop_pg(table)


def test_redis_base64_is_labelled_in_the_trust_report():
    from services.parity_report import build_parity_report, public_report

    job = {
        "_id": "job-25",
        "status": "completed",
        "reconciliation": {"passed": True, "assurance_level": "full_checksum",
                           "source_rows": 3, "target_rows": 3},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {
            "destination": {"format": "redis"},
            "mappings": [{"source": "id", "target": "id", "source_type": "INTEGER"},
                         {"source": "blob", "target": "blob", "source_type": "BYTEA"}],
        },
    }
    report = public_report(build_parity_report(job, reread=False))
    assert report["representation_notes"] == [
        "blob: stored as base64 text (Redis values are text, so the bytes are base64-encoded)"]
    assert "blob: stored as base64 text" in report["confidence"]["statement"]


def test_map_labels_the_redis_mapping():
    from services.mapping_pipeline import run_mapping_pipeline

    out = run_mapping_pipeline(
        source_columns=["id", "blob"], target_columns=[],
        source_schemas=[{"name": "id", "inferred_type": "INTEGER", "samples": ["1"]},
                        {"name": "blob", "inferred_type": "BYTEA", "samples": ["AP8Q"]}],
        target_schemas=None, file_format="postgresql", confidence_threshold=0.5,
        source_samples={"id": ["1"], "blob": ["AP8Q"]}, validation_mode="strict", use_llm=False,
        destination_db_type="redis", source_db_type="postgresql", destination_table_exists=False,
        source_types_authoritative=True,
    )
    blob = next(m for m in out["mappings"] if m["source"] == "blob")
    assert "stored as base64 text" in blob["reasoning"]
    other = next(m for m in out["mappings"] if m["source"] == "id")
    assert "base64" not in other["reasoning"]
