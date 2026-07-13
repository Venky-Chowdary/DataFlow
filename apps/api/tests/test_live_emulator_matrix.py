"""Live end-to-end smoke tests against local Docker emulators.

Tests skip automatically when a container port is unreachable, so CI without
the emulator stack can still pass. Running these locally (the DataFlow dev box
has the containers) exercises the live connector surface.
"""

from __future__ import annotations

import socket
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import write_destination_database
from src.transfer.models import EndpointConfig


RECORDS = [
    {"id": "1", "amount": "1000.00"},
    {"id": "2", "amount": "2000.50"},
]
COLUMNS = ["id", "amount"]
SCHEMA = {"id": "INTEGER", "amount": "DECIMAL"}
MAPPINGS = [
    {"source": "id", "target": "id"},
    {"source": "amount", "target": "amount"},
]


AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)


def _is_reachable(host: str, port: int) -> bool:
    if not host or not port:
        return False
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


CASES = [
    pytest.param(
        EndpointConfig(
            kind="database",
            format="s3",
            host="localhost",
            port=9000,
            database="dataflow",
            username="dataflow",
            password="dataflowsecret",
            table="payments_s3.json",
        ),
        id="s3-minio",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="gcs",
            host="localhost",
            port=4443,
            database="dataflow-test",
            table="payments_gcs.json",
        ),
        id="gcs-fake-gcs-server",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="adls",
            host="localhost",
            port=10000,
            database="test",
            username="devstoreaccount1",
            password=AZURITE_KEY,
            table="payments_adls.json",
        ),
        id="adls-azurite",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="bigquery",
            host="127.0.0.1",
            port=9050,
            database="dataflow-test",
            schema="dataflow",
            table="payments_bigquery",
        ),
        id="bigquery-emulator",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="dynamodb",
            host="localhost",
            port=8000,
            database="test",
            table="payments_dynamodb",
        ),
        id="dynamodb-local",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="redis",
            host="localhost",
            port=6379,
            database="0",
            table="payments_redis",
        ),
        id="redis",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="elasticsearch",
            host="localhost",
            port=9200,
            database="dataflow",
            table="payments_es",
        ),
        id="elasticsearch",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="mongodb",
            host="localhost",
            port=27017,
            database="dataflow",
            table="payments_mongodb",
        ),
        id="mongodb",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="presto",
            host="localhost",
            port=8082,
            database="memory",
            schema="default",
            table="payments_presto",
        ),
        id="presto",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="trino",
            host="localhost",
            port=8081,
            database="memory",
            schema="default",
            username="test",
            table="payments_trino",
        ),
        id="trino",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="clickhouse",
            host="localhost",
            port=9002,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            table="payments_clickhouse",
        ),
        id="clickhouse",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="tidb",
            host="localhost",
            port=4000,
            database="test",
            username="root",
            table="payments_tidb",
        ),
        id="tidb",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="mariadb",
            host="localhost",
            port=3307,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            table="payments_mariadb",
        ),
        id="mariadb",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="citus",
            host="localhost",
            port=5435,
            database="dataflow",
            username="dataflow",
            password="dataflowsecret",
            schema="public",
            table="payments_citus",
        ),
        id="citus",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="timescaledb",
            host="localhost",
            port=5434,
            database="dataflow",
            username="dataflow",
            password="dataflowsecret",
            schema="public",
            table="payments_timescaledb",
        ),
        id="timescaledb",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="materialize",
            host="localhost",
            port=6875,
            database="materialize",
            username="materialize",
            table="payments_materialize",
        ),
        id="materialize",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="risingwave",
            host="localhost",
            port=4566,
            database="dev",
            username="root",
            table="payments_risingwave",
        ),
        id="risingwave",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="questdb",
            host="localhost",
            port=8812,
            database="qdb",
            username="admin",
            password="quest",
            schema="public",
            table="payments_questdb",
        ),
        id="questdb",
    ),
    pytest.param(
        EndpointConfig(
            kind="database",
            format="cockroachdb",
            host="localhost",
            port=26257,
            database="defaultdb",
            username="root",
            schema="public",
            table="payments_cockroachdb",
        ),
        id="cockroachdb",
    ),
]


@pytest.mark.parametrize("endpoint", CASES)
def test_write_destination_database_local_emulator(endpoint: EndpointConfig):
    if not _is_reachable(endpoint.host, endpoint.port):
        pytest.skip(f"{endpoint.format} emulator not reachable on {endpoint.host}:{endpoint.port}")

    rows, ddl_log, summary = write_destination_database(
        endpoint,
        RECORDS,
        COLUMNS,
        SCHEMA,
        MAPPINGS,
    )
    assert rows == 2, f"{endpoint.format}: expected 2 rows, got {rows} (summary={summary})"
    assert summary.get("error") is None, f"{endpoint.format}: {summary.get('error')}"
