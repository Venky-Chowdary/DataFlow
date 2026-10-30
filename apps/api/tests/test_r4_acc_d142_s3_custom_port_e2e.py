"""QA round 4, D142: an S3 custom endpoint must keep its port on the data read.

QA: "an S3 custom endpoint drops the port on data read" (reproduced at 16:05
ET on ac612203); asked for a test with MinIO on a non-default port.

MinIO is not obtainable on this workstation (no image; Docker Hub, quay.io and
dl.min.io are refused by the egress proxy). This test therefore runs the S3
wire protocol from ``moto``'s HTTP server on a non-default port, through the
real boto3 client the product uses. Set ``DATAFLOW_TEST_MINIO_ENDPOINT``
(e.g. ``http://127.0.0.1:19000``) plus ``DATAFLOW_TEST_MINIO_KEY`` /
``DATAFLOW_TEST_MINIO_SECRET`` to run it against a real MinIO instead.

Nothing listens on 80, 443 or 9000 here, so a read that loses the port fails
to connect instead of passing by accident. Every connector spelling a user can
type is driven through MCP ``test_connector``, ``sample_connector_object`` and
``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job into a new PostgreSQL
table, with a cell-by-cell check (NULL, empty, unicode included).
"""
from __future__ import annotations

import csv
import io
import os
import uuid

import pytest

from test_d190_d194_d197_declared_string_types_e2e import (  # noqa: F401
    _drop,
    _pg,
    _pg_connector,
    _pg_rows,
    _run,
    transfer_client,
)

_ROWS = [
    ("1", "alpha", "10.5"),
    ("2", "", "0"),
    ("3", "Zoë ✓ 中文", "-3.25"),
    ("4", "  padded  ", ""),
]


@pytest.fixture(scope="module")
def s3_endpoint():
    boto3 = pytest.importorskip("boto3")
    external = os.environ.get("DATAFLOW_TEST_MINIO_ENDPOINT", "").strip()
    key = os.environ.get("DATAFLOW_TEST_MINIO_KEY", "test")
    secret = os.environ.get("DATAFLOW_TEST_MINIO_SECRET", "test")
    server = None
    if external:
        endpoint = external
    else:
        moto_server = pytest.importorskip("moto.server")
        server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
        server.start()
        host, port = server.get_host_and_port()
        assert int(port) not in (80, 443, 9000)
        endpoint = f"http://{host}:{port}"
    client = boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=key,
        aws_secret_access_key=secret, region_name="us-east-1",
    )
    bucket = "r4d142-" + uuid.uuid4().hex[:8]
    client.create_bucket(Bucket=bucket)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["id", "label", "amount"])
    w.writerows(_ROWS)
    client.put_object(Bucket=bucket, Key="d142/data.csv", Body=buf.getvalue().encode("utf-8"))
    try:
        yield endpoint, bucket, key, secret
    finally:
        if server is not None:
            server.stop()


def _shapes(endpoint: str):
    from urllib.parse import urlparse

    u = urlparse(endpoint)
    port = int(u.port)
    return {
        # The connector form's defaults (a region in Host, 443 in Port) plus
        # the real address in the Endpoint URL field.
        "endpoint_url_field": {"host": "us-east-1", "port": 443, "endpoint_url": endpoint},
        "host_and_port_fields": {"host": u.hostname, "port": port},
        "url_in_host_default_port": {"host": endpoint, "port": 443},
        "host_with_port_default_port": {"host": f"localhost:{port}", "port": 9000},
        "localhost_name": {"host": "localhost", "port": port},
    }


_SHAPE_IDS = list(_shapes("http://127.0.0.1:1"))


def _s3_connector(fields: dict, bucket: str, key: str, secret: str) -> str:
    from services.connector_store import create_connector

    spec = {
        "name": "r4d142-" + uuid.uuid4().hex[:6], "type": "s3", "role": "source",
        "database": bucket, "username": key, "password": secret, "ssl": False,
        "extra": {"path_style": True},
        "workspace_id": "", **fields,
    }
    if "endpoint_url" in fields:
        spec["extra"] = {**spec["extra"], "endpoint_url": fields["endpoint_url"]}
    return str(create_connector(spec).id)


@pytest.mark.parametrize("shape", _SHAPE_IDS)
def test_s3_custom_port_test_sample_and_transfer(transfer_client, s3_endpoint, shape):
    from src.ai.copilot.tools import get_pilot_tools

    endpoint, bucket, key, secret = s3_endpoint
    _pg().close()
    source_id = _s3_connector(_shapes(endpoint)[shape], bucket, key, secret)
    tools = get_pilot_tools()

    probe = tools.execute("test_connector", {"connector_id": source_id})
    assert probe.success, probe.error
    assert (probe.output or {}).get("ok", (probe.output or {}).get("success")) is not False, probe.output

    sample = tools.execute(
        "sample_connector_object", {"connector_id": source_id, "table": "d142/data.csv"}
    )
    assert sample.success, sample.error

    table = "r4d142_" + uuid.uuid4().hex[:8]
    _drop(table)
    try:
        _run(
            transfer_client,
            source_connector_id=source_id,
            source_table="d142/data.csv",
            dest_connector_id=_pg_connector("r4d142"),
            dest_table=table,
            sync_mode="full_refresh_overwrite",
        )
        got = _pg_rows(table, ["id", "label", "amount"], "id")
        norm = [
            (str(r[0]), "" if r[1] is None else str(r[1]), "" if r[2] is None else str(r[2]))
            for r in got
        ]

        def _num(text):
            return "" if text in ("", "None") else float(text)

        assert [(a, b, _num(c)) for a, b, c in norm] == [
            (a, b, _num(c)) for a, b, c in _ROWS
        ], got
    finally:
        _drop(table)
