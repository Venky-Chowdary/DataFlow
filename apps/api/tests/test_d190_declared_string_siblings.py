"""D190 / D194 siblings: every self-describing source keeps a declared string.

The end-to-end routes (Mongo -> PG, Kafka -> PG) are in
``test_d190_d194_d197_declared_string_types_e2e``. No DynamoDB is reachable on
this workstation, so its introspection is driven with the reader faked at the
boto boundary. CSV declares nothing and keeps value inference — guarded here so
the fix does not leak into files.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

_TRICKY = ["20240101", "0012", "t", "f", "1700000000", "1700000000000"]


@pytest.mark.parametrize("value", _TRICKY)
def test_kafka_json_string_is_declared_text(value):
    from services.schema_introspect import _kafka_value_to_logical

    assert _kafka_value_to_logical(value) == "VARCHAR"
    # JSON's own scalars keep their type.
    assert _kafka_value_to_logical(True) == "BOOLEAN"
    assert _kafka_value_to_logical(20240101) == "INTEGER"


def test_kafka_iso_instant_in_a_json_string_is_still_an_instant():
    """JSON has no temporal type; an ISO literal is not bare digits."""
    from services.schema_introspect import _kafka_value_to_logical

    assert _kafka_value_to_logical("2024-01-02T03:04:05Z") == "TIMESTAMPTZ"


@pytest.mark.parametrize("value", [*_TRICKY, "", " "])
@pytest.mark.parametrize("key", ["created_at", "order_date", "is_active", "zip", "k"])
def test_bson_digit_and_flag_strings_are_text(value, key):
    from services.schema_introspect import _sample_logical_type

    assert _sample_logical_type(value, key) == "TEXT"


def test_bson_iso_instant_string_keeps_its_recognised_carrier():
    """Prior QA contract (test_mongo_bson_numeric_authority): an ISO literal is
    not bare digits, so it is still an instant."""
    from services.schema_introspect import _sample_logical_type

    assert _sample_logical_type("2024-01-02T03:04:05Z", "created_at") == "TIMESTAMPTZ"


@pytest.mark.parametrize("key", ["12", "0012", "20240101"])
def test_kafka_key_declared_varchar_survives_a_thin_sample(key):
    """One message keyed '12' must not profile ``_kafka_key`` INTEGER."""
    from services.mapping_pipeline import run_mapping_pipeline

    cols = ["_kafka_key", "v"]
    samples = {"_kafka_key": [key], "v": [key]}
    out = run_mapping_pipeline(
        cols, [],
        source_schemas=[
            {"name": c, "inferred_type": "VARCHAR", "samples": samples[c]} for c in cols
        ],
        source_samples=samples,
        destination_db_type="postgresql",
        source_db_type="kafka",
        destination_table_exists=False,
        use_llm=False,
    )
    by = {m["source"]: m for m in out["mappings"]}
    for c in cols:
        assert by[c]["source_type"].upper() == "VARCHAR", by[c]
        assert by[c]["target_type"].upper() in {"TEXT", "VARCHAR"}, by[c]


def test_preflight_keeps_declared_kafka_key_text_into_a_text_column():
    from services.preflight_service import run_file_preflight

    pf = run_file_preflight(
        columns=["_kafka_key"],
        column_types={"_kafka_key": "VARCHAR"},
        row_count=1,
        mappings=[{"source": "_kafka_key", "target": "_kafka_key", "confidence": 0.99}],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="kafka",
        sync_mode="incremental_append",
        sample_rows=[{"_kafka_key": "12"}],
        destination_column_types={"_kafka_key": "TEXT"},
        destination_table_exists=True,
        destination_db_type="postgresql",
    )
    blockers = " ".join(str(b) for b in pf.get("blockers") or [])
    assert "_kafka_key" not in blockers, blockers


def test_dynamodb_s_attributes_keep_their_declared_string(monkeypatch):
    from connectors import dynamodb_reader
    from services.schema_introspect import _introspect_dynamodb

    names = ["pk", "order_date", "code", "flag", "seen_at", "qty"]
    declared = {
        "pk": "VARCHAR", "order_date": "VARCHAR", "code": "VARCHAR",
        "flag": "VARCHAR", "seen_at": "VARCHAR", "qty": "INTEGER",
    }
    rows = [
        ["20240101", "20240101", "0012", "t", "2024-01-02T03:04:05Z", "1"],
        ["1700000000", "20231231", "0007", "f", "2024-01-03T03:04:05Z", "2"],
        ["0012", dynamodb_reader.DDB_NULL_SENTINEL, "", "t", "2024-01-04T03:04:05Z", "3"],
    ]
    monkeypatch.setattr(dynamodb_reader, "describe_table_schema", lambda _c, _t: (list(names), dict(declared)))
    monkeypatch.setattr(
        dynamodb_reader, "describe_table_key_contract_or_empty",
        lambda _c, _t: ([{"name": "pk", "key_type": "HASH"}], {}),
    )
    monkeypatch.setattr(dynamodb_reader, "list_tables", lambda _c: ["t"])
    monkeypatch.setattr(dynamodb_reader, "estimate_item_count", lambda _c, _t: 3)
    monkeypatch.setattr(
        dynamodb_reader, "read_table_batch",
        lambda **_k: (
            SimpleNamespace(headers=list(names), rows=rows, meta={"native_types": dict(declared)}),
            None,
        ),
    )

    info = _introspect_dynamodb(table="t", host="us-east-1")
    assert info["ok"], info
    types = {c["name"]: c["inferred_type"] for c in info["columns"]}
    for col in ("pk", "order_date", "code", "flag"):
        assert types[col] == "VARCHAR", types
    assert types["qty"] == "INTEGER"
    # An ISO instant in an S attribute is still recognised (DynamoDB has no
    # temporal type); bare digits are not.
    assert types["seen_at"].startswith("TIMESTAMP"), types


def test_map_keeps_declared_strings_on_self_describing_sources():
    from services.mapping_pipeline import run_mapping_pipeline

    cols = ["order_date", "created_at", "is_active", "code"]
    samples = {
        "order_date": ["20240101", "20231231"],
        "created_at": ["1700000000", "1700000001"],
        "is_active": ["t", "f"],
        "code": ["0012", "0007"],
    }
    for engine, declared in (("mongodb", "TEXT"), ("kafka", "VARCHAR"), ("dynamodb", "VARCHAR")):
        out = run_mapping_pipeline(
            cols, [],
            source_schemas=[{"name": c, "inferred_type": declared, "samples": samples[c]} for c in cols],
            source_samples=samples,
            destination_db_type="postgresql",
            source_db_type=engine,
            destination_table_exists=False,
            use_llm=False,
        )
        by = {m["source"]: m for m in out["mappings"]}
        for c in cols:
            assert by[c]["source_type"].upper() in {"TEXT", "VARCHAR"}, (engine, by[c])
            assert by[c]["target_type"].upper() in {"TEXT", "VARCHAR"}, (engine, by[c])
            assert by[c]["fidelity"] == "preserve", (engine, by[c])


def test_csv_value_inference_is_unchanged():
    """Files declare nothing: CSV keeps inferring from values (not this defect)."""
    from services.schema_inference import infer_column

    assert infer_column(["t", "f"], field_name="is_active")["logical_type"] == "BOOLEAN"
    assert infer_column(["20240101", "20231231"], field_name="order_date")["logical_type"] == "DATE"
