"""Specialty and cloud connector defects from the live QA round.

Oracle table samples must not emit LIMIT (ORA-03047). A driver-declared
NUMBER(10,2) must not shrink to the sample envelope. Non-SQL sources sample
through their readers. Kafka dest COUNT must not report 0 when topic metadata
is simply unloaded. Redis and DynamoDB preserving wires are not per-column
risk contracts. Vector destinations fall back to the existing TF-IDF embedder
when sentence-transformers is absent, and say so on the row. TimescaleDB
destination schema uses the Postgres catalog. Azurite gets a blob API version
it accepts.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from services.decimal_observe import cursor_declared_numeric_types
from services.dest_precount import _kafka_partitions_after_refresh
from services.dialect_profiles import append_result_limit, sample_select_sql
from services.type_system import is_lossy_coercion


def test_oracle_sample_and_query_window_do_not_emit_limit() -> None:
    sample = sample_select_sql("oracle", '"ORDERS"', 25)
    assert "LIMIT" not in sample.upper()
    assert "FETCH FIRST 25 ROWS ONLY" in sample

    bounded = append_result_limit("oracle", "SELECT * FROM ORDERS", 50)
    assert "LIMIT" not in bounded.upper()
    assert bounded.endswith("FETCH FIRST 50 ROWS ONLY")

    # A hand-written extract that already has a window is left alone.
    custom = "SELECT * FROM ORDERS FETCH FIRST 10 ROWS ONLY"
    assert append_result_limit("oracle", custom, 50) == custom

    top = append_result_limit("sqlserver", "SELECT * FROM ORDERS", 10)
    assert top.startswith("SELECT TOP 10 ")
    assert "LIMIT" not in top.upper()

    assert "LIMIT 25" in sample_select_sql("postgresql", '"orders"', 25)


def test_cursor_precision_wins_over_a_narrow_sample() -> None:
    headers = ["AMOUNT", "NOTE"]
    # PEP 249: name, type, display, internal, precision, scale, null_ok
    description = (
        ("AMOUNT", object(), None, None, 10, 2, True),
        ("NOTE", object(), None, None, None, None, True),
    )
    declared = cursor_declared_numeric_types(headers, description)
    assert declared == {"AMOUNT": "DECIMAL(10,2)"}

    # Oracle unconstrained NUMBER: precision 0, scale -127. Not a size.
    unconstrained = cursor_declared_numeric_types(
        ["N"],
        (("N", object(), None, None, 0, -127, True),),
    )
    assert unconstrained == {}

    from services.procedure_source import _overlay_declared_numerics

    merged = _overlay_declared_numerics(
        headers,
        description,
        {"AMOUNT": "DECIMAL(5,2)", "NOTE": "VARCHAR"},
    )
    assert merged["AMOUNT"] == "DECIMAL(10,2)"
    assert merged["NOTE"] == "VARCHAR"


class _ColdKafka:
    """partitions_for_topic is None until topics() loads metadata."""

    def __init__(self) -> None:
        self.refreshed = False

    def partitions_for_topic(self, topic: str):
        if not self.refreshed:
            return None
        if topic == "orders":
            return {0, 1}
        return None

    def topics(self):
        self.refreshed = True
        return {"orders", "other"}


class _MissingKafka:
    def partitions_for_topic(self, topic: str):
        return None

    def topics(self):
        return {"other"}


def test_kafka_count_refreshes_metadata_before_reporting_zero() -> None:
    cold = _ColdKafka()
    assert _kafka_partitions_after_refresh(cold, "orders") == {0, 1}
    assert cold.refreshed is True

    missing = _MissingKafka()
    assert _kafka_partitions_after_refresh(missing, "orders") == set()


def test_redis_and_dynamodb_preserving_wires_do_not_need_a_contract() -> None:
    assert is_lossy_coercion("UUID", "string", dest_db="redis") is False
    assert is_lossy_coercion("UUID", "S", dest_db="dynamodb") is False
    assert is_lossy_coercion("DOUBLE PRECISION", "N", dest_db="dynamodb") is False
    assert is_lossy_coercion("TIMESTAMPTZ", "S", dest_db="dynamodb") is False
    # A sized decimal on Dynamo is still a clamp, not the unbounded N wire.
    assert is_lossy_coercion(
        "DOUBLE PRECISION", "DECIMAL(10,2)", dest_db="dynamodb"
    ) is True


def test_dynamodb_temporal_wire_is_offset_text() -> None:
    from connectors.dynamodb_writer import _to_dynamo_value

    value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    text = _to_dynamo_value(value, "TIMESTAMPTZ")
    assert isinstance(text, str)
    assert "+05:30" in text


def test_timescaledb_destination_schema_uses_postgres(monkeypatch) -> None:
    called: dict[str, str] = {}

    def fake(**kwargs):
        called["db"] = "postgresql"
        called["table"] = str(kwargs.get("table") or "")
        return {
            "ok": True,
            "columns": [{"name": "id", "inferred_type": "INTEGER", "nullable": False}],
            "tables": ["readings"],
        }

    monkeypatch.setattr("services.schema_introspect._introspect_postgresql", fake)
    from services.schema_introspect import introspect_schema

    info = introspect_schema(
        "timescaledb",
        host="ts.internal",
        port=5432,
        database="metrics",
        username="u",
        password="p",
        table="readings",
    )
    assert called["table"] == "readings"
    assert info.get("ok") is True
    assert info["columns"][0]["name"] == "id"


def test_azurite_pins_a_blob_api_version_real_azure_does_not(monkeypatch) -> None:
    captured: list[dict] = []

    class _Client:
        def __init__(self, *args, **kwargs):
            captured.append(dict(kwargs))

        @classmethod
        def from_connection_string(cls, conn, **kwargs):
            captured.append(dict(kwargs))
            return cls()

    monkeypatch.setattr("azure.storage.blob.BlobServiceClient", _Client)
    from connectors.adls_common import blob_service_client

    blob_service_client(
        {"host": "127.0.0.1", "port": 10000, "username": "devstoreaccount1", "password": "k"}
    )
    blob_service_client(
        {"host": "acct.blob.core.windows.net", "port": 443, "username": "acct", "password": "k"}
    )
    assert captured[0].get("api_version") == "2021-12-02"
    assert "api_version" not in captured[1]


def test_non_sql_sample_uses_the_reader_not_limit_sql(monkeypatch) -> None:
    from src.ai.copilot.query_tools import _sample_batch_source

    class _Probe:
        headers = ["id", "qty"]
        rows = [[1, 3], [2, 4]]
        meta = {"native_types": {"id": "integer", "qty": "integer"}}

    def _read(*args, **kwargs):
        assert args[0] == "elasticsearch"
        assert "LIMIT" not in str(args)
        return _Probe()

    monkeypatch.setattr("src.transfer.stream._read_batch", _read)
    rows, columns, schema = _sample_batch_source(
        {"type": "elasticsearch", "host": "es.internal", "port": 9200},
        "orders",
        10,
    )
    assert columns == ["id", "qty"]
    assert rows == [{"id": 1, "qty": 3}, {"id": 2, "qty": 4}]
    assert schema["qty"] == "integer"


def test_vector_destination_falls_back_when_sentence_transformers_is_missing(monkeypatch) -> None:
    import sys

    from services.vectorization import _get_embedder, vectorize_records

    _get_embedder.cache_clear()
    monkeypatch.setitem(sys.modules, "sentence_transformers", None)
    import src.ai.rag.embedding_service as embedding_service

    monkeypatch.setattr(embedding_service, "_embedding_service", None)
    rows = vectorize_records(
        [{"id": "1", "content": "postgres row about orders"}],
        content_column="content",
        model="sentence-transformers/all-MiniLM-L6-v2",
        skip_chunking=True,
    )
    assert rows
    assert rows[0]["embedding"]
    assert len(rows[0]["embedding"]) == 384
    assert rows[0]["metadata"].get("_df_embedding_backend") == "tfidf_fallback"
    _get_embedder.cache_clear()
