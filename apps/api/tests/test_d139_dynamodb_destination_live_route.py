"""D139: DynamoDB is advertised as a transfer-live destination — it must load.

The destination probe passed only the ``database`` field (often the region) to
``test_dynamodb``, so a destination whose table sat in ``table`` failed with
"Table name is required": existence unknown, G2 blocked, and every mapping was
reported as a fidelity collapse needing a risk contract. Proven here through
the engine with preflight ON against moto.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

pytest.importorskip("moto")
boto3 = pytest.importorskip("boto3")

from moto import mock_aws  # noqa: E402


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    with mock_aws():
        yield boto3.client("dynamodb", region_name="us-east-1")


@pytest.fixture
def isolated(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import src.transfer.engine as engine_mod

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _dest(table: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database", format="dynamodb", host="us-east-1", table=table,
        username="test", password="test", region="us-east-1",
    )


def _source(tmp_path):
    from src.transfer.models import EndpointConfig

    path = os.path.join(str(tmp_path), "src.db")
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, amount REAL, qty INTEGER)")
    conn.executemany("INSERT INTO t VALUES (?,?,?,?)", [(1, "a", 1.5, 3), (2, "b", 2.25, 4)])
    conn.commit()
    conn.close()
    return EndpointConfig(kind="database", format="sqlite", database=path, table="t")


def _run(fake, request):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "d139" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_destination_probe_reads_the_table_field(aws):
    from src.transfer.endpoint_intelligence import introspect_endpoint

    aws.create_table(
        TableName="d139_probe",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
        BillingMode="PAY_PER_REQUEST",
    )
    endpoint = _dest("d139_probe")
    endpoint.extra = {"introspect_purpose": "destination", "introspect_strict": True}
    info = introspect_endpoint(endpoint)
    assert info["connected"] is True, info.get("message")
    assert info.get("table_exists") is True


@pytest.mark.parametrize("existing", [True, False], ids=["existing_table", "create_new"])
def test_sql_rows_load_into_dynamodb_with_preflight(aws, isolated, tmp_path, existing):
    from src.transfer.models import TransferRequest

    table = "d139_" + uuid.uuid4().hex[:8]
    if existing:
        aws.create_table(
            TableName=table,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
            BillingMode="PAY_PER_REQUEST",
        )
    result = _run(
        isolated,
        TransferRequest(source=_source(tmp_path), destination=_dest(table), sync_mode="full_refresh_append"),
    )
    assert result.success, result.error
    items = sorted(aws.scan(TableName=table)["Items"], key=lambda i: i["id"]["N"])
    assert items == [
        {"id": {"N": "1"}, "name": {"S": "a"}, "amount": {"N": "1.5"}, "qty": {"N": "3"}},
        {"id": {"N": "2"}, "name": {"S": "b"}, "amount": {"N": "2.25"}, "qty": {"N": "4"}},
    ]
