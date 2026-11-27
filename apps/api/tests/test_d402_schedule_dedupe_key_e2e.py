"""D402 on schedules — a scheduled run does what its plan said about dedupe_key.

``create_schedule(dedupe_key='id')`` on an S3 CSV overwrite planned keep_last
("Overwrite keeps the last row of each id") but saved ``in_file_dedupe=block``
with ``primary_key='id'``: every run became an upsert-policy overwrite that
refused the file's repeated key. The plan and the run disagreed (R8), and the
REST create/PATCH had no way to store a dedupe key at all.

Each leg saves the schedule through its real route (MCP create_schedule →
Confirm; REST POST with server-side planning; REST PATCH), then runs it the
way the runner does: ``build_schedule_request`` from the stored schedule and
the saved connectors, through ``UniversalTransferEngine.execute_tracked``.
The S3 source holds id 3 twice; the table must hold 30 keys with the LAST
copy of id 3, under a run that is not an upsert.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_d402_dedupe_every_file_route_e2e import _BUCKET, _csv_bytes, _expected
from tests.test_d402_start_transfer_object_store_e2e import _connectors
from tests.test_upsert_infile_duplicate_start_transfer_e2e import (  # noqa: F401
    _require_postgres,
    transfer_client,
)


def _stage(endpoint: str) -> str:
    import boto3

    if not endpoint:
        pytest.fail("moto is required for the S3 schedule route")
    key = f"d402s/{uuid.uuid4().hex[:6]}/dups.csv"
    boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id="test",
        aws_secret_access_key="test", region_name="us-east-1",
    ).put_object(Bucket=_BUCKET, Key=key, Body=_csv_bytes())
    return key


def _run(schedule_id: str) -> tuple[dict, list[tuple]]:
    """One beat, built from the stored schedule exactly as the runner builds it."""
    from services.mongodb_service import get_mongodb_service
    from services.schedule_runner import _resolve_connector_for_schedule, build_schedule_request
    from services.schedule_store import get_schedule
    from src.transfer.engine import UniversalTransferEngine

    sched = get_schedule(schedule_id)
    request = build_schedule_request(
        sched,
        _resolve_connector_for_schedule(sched.source_connector_id, sched),
        _resolve_connector_for_schedule(sched.dest_connector_id, sched),
        triggered_by="schedule",
    )
    job_id = uuid.uuid4().hex[:24]
    get_mongodb_service().update_job_status(job_id, "pending", transfer_request={})
    UniversalTransferEngine().execute_tracked(request, job_id)
    job = get_mongodb_service().get_job(job_id) or {}
    pg = _require_postgres()
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f'public."{sched.dest_table}"',))
            rows: list[tuple] = []
            if cur.fetchone()[0] is not None:
                cur.execute(f'SELECT id, name FROM public."{sched.dest_table}" ORDER BY id, name')
                rows = [(int(r[0]), str(r[1])) for r in cur.fetchall()]
    finally:
        pg.close()
    return job, rows


def _drop(table: str) -> None:
    pg = _require_postgres()
    try:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        pg.close()


def _assert_landed_last_copy(job: dict, rows: list[tuple]) -> None:
    message = str(job.get("message") or job.get("error") or "")
    assert job.get("status") == "completed", (job.get("status"), message)
    assert rows == _expected(), [r for r in rows if r[0] == 3]
    assert "unverif" not in message.lower(), message


def test_mcp_create_schedule_stores_and_honours_dedupe_key(
    transfer_client, local_object_store
) -> None:
    from services.schedule_store import get_schedule
    from src.ai.copilot.schedule_tools import create_schedule

    key = _stage(local_object_store)
    source_id, dest_id = _connectors(local_object_store, "s3")
    table = f"d402_sched_mcp_{uuid.uuid4().hex[:6]}"
    staged = create_schedule(
        source_connector_id=source_id, source_table=key,
        dest_connector_id=dest_id, dest_table=table, cadence="daily at 03:17 UTC",
        sync_mode="full_refresh_overwrite", dedupe_key="id",
        schema_policy="propagate_columns", validation_mode="strict",
        name=f"d402-{uuid.uuid4().hex[:6]}",
    )
    assert staged.success, staged.error
    assert staged.output["plan"].get("in_file_dedupe") == "keep_last"
    assert staged.output["preview"].get("in_file_dedupe") == "keep_last", staged.output["preview"]
    confirmed = transfer_client.post(
        "/api/v1/copilot/confirm", json={"ack_id": staged.output["ack_id"], "reason": "d402"}
    )
    assert confirmed.status_code == 200, confirmed.text
    sched = get_schedule(confirmed.json()["schedule_id"])
    try:
        assert (sched.in_file_dedupe, sched.dedupe_key) == ("keep_last", "id")
        # The key names copies to drop, not an identity to upsert on (R6).
        assert sched.duplicate_policy != "upsert" and not sched.primary_key, (
            sched.duplicate_policy, sched.primary_key,
        )
        job, rows = _run(sched.id)
        _assert_landed_last_copy(job, rows)
    finally:
        _drop(table)


def test_mcp_create_schedule_refuses_dedupe_key_with_block(
    transfer_client, local_object_store
) -> None:
    from src.ai.copilot.schedule_tools import create_schedule

    key = _stage(local_object_store)
    source_id, dest_id = _connectors(local_object_store, "s3")
    staged = create_schedule(
        source_connector_id=source_id, source_table=key,
        dest_connector_id=dest_id, dest_table=f"d402_sched_x_{uuid.uuid4().hex[:6]}",
        cadence="daily at 03:17 UTC", sync_mode="full_refresh_overwrite", dedupe_key="id",
        in_file_dedupe="block", schema_policy="propagate_columns", validation_mode="strict",
    )
    assert not staged.success
    assert "dedupe_key" in str(staged.error) and "block" in str(staged.error), staged.error


def test_rest_create_and_patch_store_and_honour_dedupe_key(
    transfer_client, local_object_store
) -> None:
    key = _stage(local_object_store)
    source_id, dest_id = _connectors(local_object_store, "s3")
    table = f"d402_sched_rest_{uuid.uuid4().hex[:6]}"
    body = {
        "name": f"d402-rest-{uuid.uuid4().hex[:6]}", "source_connector_id": source_id,
        "source_table": key, "dest_connector_id": dest_id, "dest_table": table,
        "interval": "daily", "sync_mode": "full_refresh_overwrite",
        "schema_policy": "propagate_columns", "validation_mode": "strict",
        "plan_mappings": True, "dedupe_key": "id",
    }
    try:
        created = transfer_client.post("/api/v1/schedules/", json=body)
        assert created.status_code == 201, created.text
        saved = created.json()
        assert (saved.get("in_file_dedupe"), saved.get("dedupe_key")) == ("keep_last", "id"), saved
        job, rows = _run(saved["id"])
        _assert_landed_last_copy(job, rows)

        # Edit: dropping keep_last while the dedupe key stays would write both
        # copies of id 3 on an overwrite — refused, the stored schedule unchanged.
        bad = transfer_client.patch(f"/api/v1/schedules/{saved['id']}", json={"in_file_dedupe": "block"})
        assert bad.status_code == 400, bad.text
        assert "dedupe_key" in bad.text
        # Edit: the first run's table is keyed on id, so a dedupe on (id, name)
        # would land id 3 twice into it on the next beat — refused, unchanged.
        refused = transfer_client.patch(
            f"/api/v1/schedules/{saved['id']}", json={"dedupe_key": "id,name"}
        )
        assert refused.status_code == 400, refused.text
        assert "keyed on" in refused.text and "(id)" in refused.text, refused.text
        assert transfer_client.get(f"/api/v1/schedules/{saved['id']}").json()["dedupe_key"] == "id"
        # With that table gone the edit is saved, and the stream key it bound follows.
        _drop(table)
        moved = transfer_client.patch(
            f"/api/v1/schedules/{saved['id']}", json={"dedupe_key": "id,name"}
        )
        assert moved.status_code == 200, moved.text
        assert moved.json().get("dedupe_key") == "id,name", moved.json()
        assert moved.json()["stream_contracts"][0]["primary_key"] == ["id", "name"], moved.json()
        job, rows = _run(saved["id"])
        assert job.get("status") == "completed", (job.get("status"), job.get("message"))
        # (id, name) pairs are unique, so both copies of id 3 are distinct keys now.
        assert sorted(r[0] for r in rows).count(3) == 2, rows
    finally:
        _drop(table)


def test_rest_patch_adds_dedupe_key_to_a_schedule_saved_without_one(
    transfer_client, local_object_store
) -> None:
    key = _stage(local_object_store)
    source_id, dest_id = _connectors(local_object_store, "s3")
    table = f"d402_sched_edit_{uuid.uuid4().hex[:6]}"
    base = {
        "name": f"d402-edit-{uuid.uuid4().hex[:6]}", "source_connector_id": source_id,
        "source_table": key, "dest_connector_id": dest_id, "dest_table": table,
        "interval": "daily", "sync_mode": "full_refresh_overwrite",
        "schema_policy": "propagate_columns", "validation_mode": "strict",
        "mappings": [
            {"source": c, "target": c, "confidence": 1.0} for c in ("id", "name", "note", "amount")
        ],
    }
    try:
        created = transfer_client.post("/api/v1/schedules/", json=base)
        assert created.status_code == 201, created.text
        sid = created.json()["id"]
        patched = transfer_client.patch(
            f"/api/v1/schedules/{sid}", json={"dedupe_key": "id", "in_file_dedupe": "keep_last"}
        )
        assert patched.status_code == 200, patched.text
        assert (patched.json().get("in_file_dedupe"), patched.json().get("dedupe_key")) == ("keep_last", "id")
        job, rows = _run(sid)
        _assert_landed_last_copy(job, rows)
    finally:
        _drop(table)


def test_rest_upsert_schedule_with_dedupe_key_collapses_like_a_one_off_upsert(
    transfer_client, local_object_store
) -> None:
    """An upsert plan with dedupe_key collapses the file's repeated keys (last wins)."""
    key = _stage(local_object_store)
    source_id, dest_id = _connectors(local_object_store, "s3")
    table = f"d402_sched_up_{uuid.uuid4().hex[:6]}"
    body = {
        "name": f"d402-up-{uuid.uuid4().hex[:6]}", "source_connector_id": source_id,
        "source_table": key, "dest_connector_id": dest_id, "dest_table": table,
        "interval": "daily", "sync_mode": "upsert", "schema_policy": "propagate_columns",
        "validation_mode": "strict", "plan_mappings": True, "dedupe_key": "id",
    }
    try:
        created = transfer_client.post("/api/v1/schedules/", json=body)
        assert created.status_code == 201, created.text
        saved = created.json()
        assert (saved["in_file_dedupe"], saved["dedupe_key"], saved["primary_key"]) == (
            "keep_last", "id", "id"), saved
        job, rows = _run(saved["id"])
        _assert_landed_last_copy(job, rows)
        # The upsert identity and the dedupe key move together or not at all:
        # the table is keyed on id, so a dedupe on name is refused.
        refused = transfer_client.patch(f"/api/v1/schedules/{saved['id']}", json={"dedupe_key": "name"})
        assert refused.status_code == 400, refused.text
        kept = transfer_client.get(f"/api/v1/schedules/{saved['id']}").json()
        assert (kept["dedupe_key"], kept["primary_key"]) == ("id", "id"), kept
        _drop(table)
        moved = transfer_client.patch(f"/api/v1/schedules/{saved['id']}", json={"dedupe_key": "name"})
        assert moved.status_code == 200, moved.text
        out = moved.json()
        contract_key = out["stream_contracts"][0]["primary_key"]
        assert (out["dedupe_key"], out["primary_key"]) == ("name", "name"), out
        assert contract_key in ("name", ["name"]), out["stream_contracts"]
    finally:
        _drop(table)
