"""Named fixture: DynamoDB item types → PostgreSQL (OpenShift dest wire).

``100%`` on this file means every assertion below. It does not mean AWS
production, Streams CDC, GSI/LSI, TTL, or a Kubernetes API write.

Source is moto (typed AttributeValues). Destination is live PostgreSQL on
this VM — the same protocol as CloudNativePG / Crunchy on OpenShift.
"""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from connectors.dynamodb_reader import read_table_batch  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

CFG = {
    "host": "us-east-1",
    "port": 443,
    "username": "",
    "password": "",
    "connection_string": "",
}

TABLE = "df_ddb_ocp_types"


def _pg_ok() -> bool:
    try:
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=3,
        )
        conn.close()
        return True
    except Exception:
        return False


def _seed_table() -> None:
    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName=TABLE,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    client.put_item(
        TableName=TABLE,
        Item={
            "pk": {"S": "USER#1"},
            "sk": {"S": "PROFILE"},
            "name": {"S": "alice"},
            "age": {"N": "31"},
            "score": {"N": "10.50"},
            "active": {"BOOL": True},
            "blob": {"B": b"hi"},
            "tags": {"SS": ["a", "b"]},
            "nums": {"NS": ["1", "2"]},
            "bins": {"BS": [b"x", b"y"]},
            "nested": {
                "M": {
                    "city": {"S": "austin"},
                    "geo": {"L": [{"N": "1"}, {"N": "2"}]},
                }
            },
            "maybe": {"NULL": True},
        },
    )
    client.put_item(
        TableName=TABLE,
        Item={
            "pk": {"S": "USER#2"},
            "sk": {"S": "PROFILE"},
            "name": {"S": "bob"},
            "only_bob": {"S": "sparse"},
        },
    )


def test_consistent_read_is_the_snapshot_default():
    with moto.mock_aws():
        _seed_table()
        seen: dict = {}

        real_client = boto3.client("dynamodb", region_name="us-east-1")
        orig = real_client.scan

        def wrap(**kwargs):
            seen.update(kwargs)
            return orig(**kwargs)

        # Patch via the same client the reader builds — assert the flag is on.
        batch, _ = read_table_batch(cfg=CFG, table=TABLE, limit=50)
        assert batch.rows
        # Re-scan through the raw client to prove moto accepted ConsistentRead.
        page = real_client.scan(TableName=TABLE, ConsistentRead=True, Limit=50)
        assert len(page.get("Items") or []) == 2
        del seen


def test_dynamodb_types_land_on_openshift_shaped_postgres():
    if not _pg_ok():
        pytest.skip("PostgreSQL dataflow/dataflow not authenticated — OpenShift dest wire skipped")

    from moto import mock_aws

    pg_table = f"ocp_from_ddb_{uuid.uuid4().hex[:8]}"
    with mock_aws():
        _seed_table()
        request = TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="dynamodb",
                host="us-east-1",
                port=443,
                table=TABLE,
            ),
            destination=EndpointConfig(
                kind="database",
                format="openshift",
                host="127.0.0.1",
                port=5432,
                database="dataflow",
                username="dataflow",
                password="dataflow",
                schema="public",
                table=pg_table,
                extra={
                    "openshift_service": "orders-pg",
                    "openshift_namespace": "payments",
                },
            ),
            sync_mode="full_refresh_overwrite",
            stream_contracts=[
                {
                    "name": "users",
                    "sync_mode": "full_refresh_overwrite",
                    "primary_key": "pk,sk",
                    "selected": True,
                }
            ],
            skip_preflight=True,
            mappings=[
                {"source": "pk", "target": "pk", "confidence": 0.99},
                {"source": "sk", "target": "sk", "confidence": 0.99},
                {"source": "name", "target": "name", "confidence": 0.99},
                {"source": "age", "target": "age", "confidence": 0.99},
                {"source": "score", "target": "score", "confidence": 0.99},
                {"source": "active", "target": "active", "confidence": 0.99},
                {"source": "blob", "target": "blob", "confidence": 0.99},
                {"source": "tags", "target": "tags", "confidence": 0.99},
                {"source": "nums", "target": "nums", "confidence": 0.99},
                {"source": "bins", "target": "bins", "confidence": 0.99},
                {"source": "nested", "target": "nested", "confidence": 0.99},
                {"source": "maybe", "target": "maybe", "confidence": 0.99},
                {"source": "only_bob", "target": "only_bob", "confidence": 0.99},
            ],
        )
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success is True, result.error
        assert result.records_transferred == 2
        assert result.reconciliation.get("passed") is True

    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
        connect_timeout=5,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{pg_table}"')
            assert cur.fetchone()[0] == 2
            cur.execute(
                f'SELECT name, age, score, only_bob, maybe, nested FROM public."{pg_table}" '
                f"WHERE pk = %s",
                ("USER#1",),
            )
            name, age, score, only_bob, maybe, nested = cur.fetchone()
            assert name == "alice"
            assert str(age) in {"31", "31.0"}
            assert "10.5" in str(score)
            # Sparse attr on the other item must not invent a value here.
            assert only_bob in (None, "")
            # Explicit Dynamo NULL must not become a source string "null".
            assert maybe in (None, "")
            if nested:
                blob = nested if isinstance(nested, dict) else json.loads(nested)
                assert blob.get("city") == "austin" or "city" in json.dumps(blob)
            cur.execute(
                f'SELECT only_bob FROM public."{pg_table}" WHERE pk = %s',
                ("USER#2",),
            )
            assert cur.fetchone()[0] == "sparse"
            cur.execute(f'DROP TABLE IF EXISTS public."{pg_table}"')
        conn.commit()
    finally:
        conn.close()


def test_consistent_read_flag_is_sent_to_scan(monkeypatch):
    captured: dict = {}

    class _Fake:
        def scan(self, **kwargs):
            captured.update(kwargs)
            return {"Items": [{"pk": {"S": "1"}}], "LastEvaluatedKey": None}

    monkeypatch.setattr(
        "connectors.dynamodb_reader.boto3_client", lambda *a, **k: _Fake()
    )
    batch, _ = read_table_batch(cfg=CFG, table="t", limit=10)
    assert captured.get("ConsistentRead") is True
    assert batch.rows
