"""D136 / D148 / D156: BSON and Elasticsearch ``date`` is a UTC millisecond instant.

* D136 — a relational timestamp could not load into MongoDB without a risk
  contract, the verdict naming the carrier ``DATE``.
* D148 — a zone declared for a naive DATETIME (``source_timezone``) still left
  the mapping at 0.70 under the G4 floor: the create-new risk stamp judged a
  declared ``DATETIME(6)`` without its samples, while G3 (with them) cleared it.
* D156 — SQL Server DATETIMEOFFSET → MongoDB stayed blocked after signing the
  contract: Gate-8 fingerprinted the microsecond source at six digits against
  the millisecond read-back, so the contracted narrowing failed reconcile.

One model: the carrier is ``TIMESTAMPTZ(3)``; a zoned (or zone-declared) source
with whole-millisecond values is preserved; sub-millisecond values are a
contracted narrowing, graded like every other fractional-second narrowing, that
a signed contract clears end to end.
"""

from __future__ import annotations

import datetime as dt
import os
import socket
import uuid

import pytest

from services.document_instant import instant_date_carrier
from services.reconciliation import fingerprint_for_reconcile
from services.type_coercion import assess_create_new_type_risk

DOC_ENGINES = ("mongodb", "elasticsearch")


@pytest.mark.parametrize("engine", DOC_ENGINES)
def test_document_date_is_a_millisecond_instant(engine):
    assert instant_date_carrier(engine, "date") == "TIMESTAMPTZ(3)"


@pytest.mark.parametrize("engine", DOC_ENGINES)
def test_read_back_digest_matches_a_contracted_microsecond_source(engine):
    stored = fingerprint_for_reconcile(
        dt.datetime(2024, 1, 2, 1, 4, 5, 123000), ddl_type="date", engine=engine
    )
    # Reader cells arrive as ISO text; the store floors to the millisecond.
    for source in ("2024-01-02T01:04:05.123456", "2024-01-02T01:04:05.123999"):
        assert fingerprint_for_reconcile(source, ddl_type="date", engine=engine) == stored
    # A real difference still differs.
    assert (
        fingerprint_for_reconcile("2024-01-02T01:04:05.124", ddl_type="date", engine=engine)
        != stored
    )


@pytest.mark.parametrize("engine", DOC_ENGINES)
def test_create_new_risk_is_judged_on_the_samples(engine):
    whole = ["2024-01-02 03:04:05", "2024-01-02 03:04:05.120000"]
    assert assess_create_new_type_risk(
        "TIMESTAMPTZ(6)", "date", destination_db_type=engine, samples=whole
    ) == []
    kinds = {
        r["kind"]
        for r in assess_create_new_type_risk(
            "TIMESTAMPTZ(6)", "date", destination_db_type=engine,
            samples=["2024-01-02 03:04:05.123456"],
        )
    }
    assert "precision_collapse" in kinds


@pytest.mark.parametrize("engine", DOC_ENGINES)
def test_zone_declared_datetime6_clears_the_mapping_floor(engine):
    from services.mapping_pipeline import run_mapping_pipeline
    from services.source_engine_scope import bind_source_engine

    samples = ["2024-01-02 03:04:05", "2024-05-06 07:08:09"]
    with bind_source_engine("mysql"):
        result = run_mapping_pipeline(
            source_columns=["created_at"],
            target_columns=[],
            # What declare_zone_on_schema stamps for DATETIME(6) + a zone.
            source_schemas=[{"name": "created_at", "inferred_type": "TIMESTAMPTZ(6)", "samples": samples}],
            target_schemas=None,
            file_format="mysql",
            confidence_threshold=0.85,
            source_samples={"created_at": samples},
            validation_mode="strict",
            use_llm=False,
            destination_db_type=engine,
            source_db_type="mysql",
            destination_table_exists=False,
            source_types_authoritative=True,
        )
    (mapping,) = [m for m in result["mappings"] if m["source"] == "created_at"]
    assert mapping["confidence"] >= 0.85, mapping
    assert not mapping.get("create_new_risks")


def test_verdict_names_the_bson_instant_not_a_calendar_date():
    from services.root_cause_engine import _column_type_path_label

    label = _column_type_path_label("ts", {"ts": "TIMESTAMP_NTZ → DATE"}, "mongodb")
    assert "BSON instant" in label
    assert _column_type_path_label("d", {"d": "TIMESTAMP → DATE"}, "postgresql") == "d TIMESTAMP → DATE"


# ── live: SQL Server / MySQL → MongoDB through the engine, preflight ON ──────


def _reachable(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.fixture
def isolated(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import src.transfer.engine as engine_mod

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _source(engine: str, coldef: str, literal: str):
    """Create a uuid table with two rows; return (endpoint, drop callable)."""
    from src.transfer.models import EndpointConfig

    table = "d136_" + uuid.uuid4().hex[:10]
    if engine == "mysql":
        pymysql = pytest.importorskip("pymysql")
        password = os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD") or os.environ.get("MYSQL_PASSWORD")
        if not password or not _reachable(3306):
            pytest.skip("live MySQL not configured")
        conn = pymysql.connect(
            host="127.0.0.1", user="dataflow", password=password, database="dataflow", autocommit=True
        )
        endpoint = EndpointConfig(
            kind="database", format="mysql", host="127.0.0.1", port=3306,
            database="dataflow", username="dataflow", password=password, table=table,
        )
    else:
        pyodbc = pytest.importorskip("pyodbc")
        password = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD")
        if not password or not _reachable(1433):
            pytest.skip("live SQL Server not configured")
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
            f"UID=sa;PWD={password};TrustServerCertificate=yes",
            autocommit=True,
        )
        endpoint = EndpointConfig(
            kind="database", format="sqlserver", host="127.0.0.1", port=1433,
            database="dataflow", username="sa", password=password, schema="dbo", table=table,
            extra={"trust_server_certificate": True},
        )
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, created_at {coldef})")
    cur.execute(f"INSERT INTO {table} VALUES (1, {literal}), (2, {literal})")

    def drop():
        cur.execute(f"DROP TABLE {table}")
        conn.close()

    return endpoint, drop


def _run(fake, source, **kwargs):
    pymongo = pytest.importorskip("pymongo")
    if not _reachable(27017):
        pytest.skip("live MongoDB not reachable")
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    collection = source.table
    request = TransferRequest(
        source=source,
        destination=EndpointConfig(
            kind="database", format="mongodb", host="127.0.0.1", port=27017,
            database="dataflow_test", collection=collection,
        ),
        sync_mode="full_refresh_overwrite",
        **kwargs,
    )
    job_id = "d136" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    try:
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        docs = list(client["dataflow_test"][collection].find({}, {"_id": 0}).sort("id", 1))
    finally:
        client["dataflow_test"][collection].drop()
        client.close()
    return result, docs


def test_sqlserver_datetimeoffset_whole_ms_loads_without_contract(isolated):
    source, drop = _source("mssql", "DATETIMEOFFSET", "'2024-01-02 03:04:05.123 +02:00'")
    try:
        result, docs = _run(isolated, source)
    finally:
        drop()
    assert result.success, result.error
    assert [d["created_at"] for d in docs] == [dt.datetime(2024, 1, 2, 1, 4, 5, 123000)] * 2


def test_mysql_datetime6_with_declared_zone_loads(isolated):
    source, drop = _source("mysql", "DATETIME(6)", "'2024-01-02 03:04:05'")
    try:
        result, docs = _run(isolated, source, source_timezone="UTC")
    finally:
        drop()
    assert result.success, result.error
    assert [d["created_at"] for d in docs] == [dt.datetime(2024, 1, 2, 3, 4, 5)] * 2


def test_signed_contract_clears_sqlserver_microsecond_narrowing(isolated):
    from services.migration_risk_contract import create_migration_risk_contract

    source, drop = _source("mssql", "DATETIMEOFFSET", "'2024-01-02 03:04:05.1234567 +02:00'")
    try:
        unsigned, _ = _run(isolated, source)
        assert not unsigned.success
        assert "BSON instant" in unsigned.error

        contract = create_migration_risk_contract(
            column="created_at", source_type="DATETIMEOFFSET", destination_type="date",
            approved_by="risk-approver@example.com", reason="Accept millisecond truncation",
            execution_policy="CAST_AND_CONTINUE", expected_precision_loss=True,
        ).to_dict()
        signed, docs = _run(
            isolated,
            source,
            mappings=[
                {"source": "id", "target": "id", "confidence": 1.0},
                {
                    "source": "created_at", "target": "created_at", "confidence": 1.0,
                    "risk_acknowledged": True, "risk_contract": contract,
                },
            ],
            triggered_by="risk-approver@example.com",
        )
    finally:
        drop()
    assert signed.success, signed.error
    assert [d["created_at"] for d in docs] == [dt.datetime(2024, 1, 2, 1, 4, 5, 123000)] * 2
