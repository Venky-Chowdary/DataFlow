"""D1 live proof: Postgres → MinIO run 1 then run 2, independent reread.

A passing unit test does not close D1. This talks to compose Postgres and
MinIO, transfers a declared ``decimal(12,2)``, then reads the object back on a
boto3 client the transfer engine never received — and asks the product's own
destination probe / Map whether run 2 refuses the shape it just wrote.
"""

from __future__ import annotations

import json
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.mapping_pipeline import run_mapping_pipeline
from src.transfer.engine import UniversalTransferEngine
from src.transfer.endpoint_intelligence import introspect_endpoint
from src.transfer.models import EndpointConfig, TransferRequest


def _require_pg_minio() -> None:
    from tests.typed_fidelity_helpers import require_ports

    require_ports(5432, 9000)


def _pg_connect():
    import psycopg2

    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )


def _s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="dataflow",
        aws_secret_access_key="dataflowsecret",
        region_name="us-east-1",
    )


def test_d1_postgres_minio_repeated_run_live() -> None:
    _require_pg_minio()
    pytest.importorskip("psycopg2")
    pytest.importorskip("boto3")

    suffix = uuid.uuid4().hex[:10]
    src_table = f"d1_src_{suffix}"
    bucket = f"d1-minio-{suffix}"
    key = f"exports/{src_table}.json"

    s3 = _s3_client()
    s3.create_bucket(Bucket=bucket)

    conn = _pg_connect()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE {src_table} ("
        "id integer PRIMARY KEY, "
        "amount decimal(12,2) NOT NULL, "
        "note varchar(64), "
        "created_on date"
        ")"
    )
    cur.execute(
        f"INSERT INTO {src_table} (id, amount, note, created_on) VALUES "
        "(1, 12.50, 'alpha', DATE '2026-08-30'), "
        "(2, 100.00, 'beta', DATE '2026-08-31')"
    )
    conn.close()

    source = EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=src_table,
    )
    destination = EndpointConfig(
        kind="database",
        format="s3",
        host="localhost",
        port=9000,
        database=bucket,
        table=key,
        username="dataflow",
        password="dataflowsecret",
    )

    def _run(job_tag: str):
        request = TransferRequest(
            source=source,
            destination=destination,
            sync_mode="full_refresh_overwrite",
            stream_contracts=[
                {
                    "name": "d1",
                    "sync_mode": "full_refresh_overwrite",
                    "primary_key": "id",
                    "selected": True,
                }
            ],
            skip_preflight=True,
        )
        return UniversalTransferEngine().execute_tracked(request, job_tag)

    first = _run(uuid.uuid4().hex[:24])
    assert first.success is True, first.error
    assert first.records_transferred == 2
    accounting = dict(first.row_accounting or {})
    assert int(accounting.get("rejected_rows") or 0) == 0

    # Independent destination reread — a client the engine never received.
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    landed = json.loads(body.decode("utf-8"))
    if isinstance(landed, dict) and "data" in landed:
        landed = landed["data"]
    assert isinstance(landed, list)
    assert len(landed) == 2
    amounts = sorted(Decimal(str(row["amount"])) for row in landed)
    assert amounts == [Decimal("12.50"), Decimal("100.00")]

    probe = introspect_endpoint(destination)
    schema = dict(probe.get("schema") or {})
    authority = dict(probe.get("schema_authority") or {})
    assert probe.get("table_exists") is True
    assert schema, probe
    # The probe may still *measure* DECIMAL(2,2); that measurement is a profile.
    if "amount" in authority:
        assert authority["amount"] == "sampled"
    mapped = run_mapping_pipeline(
        ["id", "amount", "note", "created_on"],
        list(schema.keys()),
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1"]},
            {"name": "amount", "inferred_type": "DECIMAL(12,2)", "samples": ["12.50"]},
            {"name": "note", "inferred_type": "VARCHAR(64)", "samples": ["alpha"]},
            {"name": "created_on", "inferred_type": "DATE", "samples": ["2026-08-30"]},
        ],
        target_schemas=[
            {"name": name, "inferred_type": carrier} for name, carrier in schema.items()
        ],
        use_llm=False,
        source_db_type="postgresql",
        destination_db_type="s3",
        destination_table_exists=True,
        source_types_authoritative=True,
        target_type_authority=authority or None,
    )
    by_source = {str(m.get("source")): m for m in mapped.get("mappings") or []}
    amount_row = by_source["amount"]
    assert amount_row.get("fidelity") != "lossy_cast", amount_row
    assert amount_row.get("type_narrowing") is not True, amount_row
    assert float(amount_row.get("confidence") or 0) >= 0.85, amount_row

    second = _run(uuid.uuid4().hex[:24])
    assert second.success is True, second.error
    assert second.records_transferred == 2
    accounting2 = dict(second.row_accounting or {})
    assert int(accounting2.get("rejected_rows") or 0) == 0

    body2 = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    landed2 = json.loads(body2.decode("utf-8"))
    if isinstance(landed2, dict) and "data" in landed2:
        landed2 = landed2["data"]
    assert len(landed2) == 2
    amounts2 = sorted(Decimal(str(row["amount"])) for row in landed2)
    assert amounts2 == [Decimal("12.50"), Decimal("100.00")]

    evidence = {
        "date": "2026-09-01",
        "pytest_nodeid": "tests/test_d1_postgres_minio_repeated_run_live.py::test_d1_postgres_minio_repeated_run_live",
        "route": "postgresql → s3/minio",
        "source_table": src_table,
        "bucket": bucket,
        "key": key,
        "run1": {
            "success": bool(first.success),
            "records_transferred": first.records_transferred,
            "rejected_rows": int(accounting.get("rejected_rows") or 0),
            "error": first.error,
        },
        "independent_reread_run1": landed,
        "dest_probe": {
            "table_exists": probe.get("table_exists"),
            "schema": schema,
            "schema_authority": authority,
        },
        "map_amount": {
            "fidelity": amount_row.get("fidelity"),
            "type_narrowing": amount_row.get("type_narrowing"),
            "confidence": amount_row.get("confidence"),
            "target_type": amount_row.get("target_type"),
            "target_type_origin": amount_row.get("target_type_origin"),
            "source_type": amount_row.get("source_type"),
        },
        "run2": {
            "success": bool(second.success),
            "records_transferred": second.records_transferred,
            "rejected_rows": int(accounting2.get("rejected_rows") or 0),
            "error": second.error,
        },
        "independent_reread_amounts": [str(a) for a in amounts2],
    }
    out = Path("/opt/cursor/artifacts/d1_postgres_minio_live_evidence.json")
    if out.parent.is_dir():
        out.write_text(json.dumps(evidence, indent=2, default=str) + "\n")
