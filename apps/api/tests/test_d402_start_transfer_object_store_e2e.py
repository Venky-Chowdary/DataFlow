"""D402 — the QA route: start_transfer → Confirm → job, S3 CSV into PostgreSQL.

QA's S3 CSV (id 3 twice, ``column_transforms`` on name) with
``full_refresh_overwrite`` + ``dedupe_key=id``. On the user's own path
(plan, confirm, the job thread) the plan must disclose the collapse, the
table must hold one row per key (the last copy, after the transform) and the
job's proof must say so: "transform proof unverifiable — 31 rows re-read but
30 landed" is not an honest verdict on a correct collapse (R9).

S3-compatible connectors (wasabi, cloudflare_r2, ...) read through the S3
reader; their plan refused the same dedupe_key as "a table" (D367's false
refusal on another connector).
"""

from __future__ import annotations

import uuid

import pytest

from tests.conftest import pg_test_cfg
from tests.test_d402_dedupe_every_file_route_e2e import _BUCKET, _csv_bytes, _expected
from tests.test_upsert_infile_duplicate_start_transfer_e2e import (  # noqa: F401
    _wait_job,
    transfer_client,
)


def _connectors(endpoint: str, ctype: str) -> tuple[str, str]:
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:6]
    src = create_connector({
        "name": f"d402-{ctype}-{token}", "type": ctype, "role": "source",
        "host": "us-east-1", "port": int(endpoint.rsplit(":", 1)[1]),
        "database": _BUCKET, "username": "test", "password": "test", "schema": "",
        "ssl": False, "endpoint_url": endpoint, "path_style": True,
    })
    pg = pg_test_cfg()
    dst = create_connector({
        "name": f"d402-pg-{token}", "type": "postgresql", "role": "destination",
        "host": pg["host"], "port": pg["port"], "database": pg["database"],
        "username": pg["username"], "password": pg["password"], "schema": "public",
        "ssl": False,
    })
    return src.id, dst.id


@pytest.mark.parametrize("ctype", ["s3", "wasabi"])
def test_s3_csv_overwrite_with_dedupe_key_lands_one_row_per_key(
    transfer_client, local_object_store, ctype
) -> None:
    if not local_object_store:
        pytest.fail("moto is required for the S3 start_transfer route")
    import boto3
    import psycopg2

    key = f"d402/{uuid.uuid4().hex[:6]}/dups.csv"
    boto3.client(
        "s3", endpoint_url=local_object_store, aws_access_key_id="test",
        aws_secret_access_key="test", region_name="us-east-1",
    ).put_object(Bucket=_BUCKET, Key=key, Body=_csv_bytes().replace(b"NAME_", b"name_"))
    source_id, dest_id = _connectors(local_object_store, ctype)
    table = f"d402_st_{ctype}_{uuid.uuid4().hex[:6]}"

    from src.ai.copilot.transfer_tools import start_transfer

    staged = start_transfer(
        source_connector_id=source_id, source_table=key,
        dest_connector_id=dest_id, dest_table=table,
        sync_mode="full_refresh_overwrite", dedupe_key="id",
        column_transforms={"name": ["trim", "upper"]},
        schema_policy="propagate_columns", validation_mode="strict",
    )
    assert staged.success, staged.error
    plan = staged.output.get("plan") or {}
    assert plan.get("in_file_dedupe") == "keep_last", sorted(plan)
    assert "last row" in str(plan.get("in_file_dedupe_note") or "")

    confirmed = transfer_client.post(
        "/api/v1/copilot/confirm", json={"ack_id": staged.output["ack_id"], "reason": "d402"}
    )
    assert confirmed.status_code == 200, confirmed.text
    job = _wait_job(str(confirmed.json().get("job_id") or ""))
    message = str(job.get("message") or "")
    assert job.get("status") == "completed", (job.get("status"), job.get("error"), message)
    assert "unverifiable" not in message and "unverified" not in message, message

    pg = pg_test_cfg()
    conn = psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["username"], password=pg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, name FROM public."{table}" ORDER BY id, name')
            rows = [(int(r[0]), str(r[1])) for r in cur.fetchall()]
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()
    assert rows == _expected(), [r for r in rows if r[0] == 3]
