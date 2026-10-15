"""Round 14 regressions.

R11-001: an overwrite replay that writes zero rows must not replace a landed
S3 object. R14-001: a second Run now names the job the first call already
bound. get_job exposes rerun_of and parent_job_id. A cast plan reports one
column type. Lenient stays lenient. compare_datasets resolves an S3 key.
"""

from __future__ import annotations

import services.schedule_runner as runner
import services.schedule_store as store


def test_overwrite_replay_does_not_replace_a_landed_s3_object():
    """298 rows became 0 when a failed overwrite replay promoted a header."""
    from connectors.s3_writer import write_mapped_rows

    lines = ["id,name"] + [f"{i},n{i}" for i in range(1, 299)]
    live = ("\n".join(lines) + "\n").encode()
    key = "QA_R14/pii3_copy.csv"
    store_obj = {key: live}
    puts: list[str] = []

    class _S3:
        def head_bucket(self, **_kwargs):
            return {}

        def head_object(self, *, Bucket, Key):
            if Key not in store_obj:
                from botocore.exceptions import ClientError

                raise ClientError(
                    {"Error": {"Code": "404", "Message": "missing"}}, "HeadObject"
                )
            return {"ContentLength": len(store_obj[Key])}

        def put_object(self, *, Bucket, Key, Body, ContentType):
            puts.append(Key)
            store_obj[Key] = bytes(Body)

        def copy_object(self, **kwargs):
            source = kwargs["CopySource"]["Key"]
            store_obj[kwargs["Key"]] = store_obj[source]

        def delete_object(self, *, Bucket, Key):
            store_obj.pop(Key, None)

    from unittest.mock import patch

    with patch("connectors.s3_writer.boto3_client", return_value=_S3()):
        result = write_mapped_rows(
            host="",
            port=0,
            database="qa",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name=key,
            headers=["id", "name"],
            data_rows=[],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
            ],
            column_types={"id": "INTEGER", "name": "TEXT"},
            sync_mode="full_refresh_overwrite",
            protect_landed_object=True,
            create_table=False,
        )
    assert result.ok is False
    assert "left untouched" in (result.error or "")
    assert store_obj[key] == live
    assert key not in puts


def _isolate(tmp_path, monkeypatch):
    import services.connector_store as connectors

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(store, "_job_is_live", lambda _job_id: True)
    monkeypatch.setattr(connectors, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connectors, "_use_mongo", lambda: False)
    monkeypatch.setattr(connectors, "_resolve_backend", lambda: "file")
    monkeypatch.setattr(connectors, "_backend_choice", "file")


def test_claim_write_carries_the_job_id(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    sched = store.create_schedule({
        "name": "QA_R14",
        "source_connector_id": "src",
        "source_table": "s",
        "dest_connector_id": "dst",
        "dest_table": "d",
        "cron": "*/5 * * * *",
        "timezone": "UTC",
        "sync_mode": "full_refresh_overwrite",
        "enabled": True,
        "mappings": [{"source": "id", "target": "id", "confidence": 1.0}],
    })
    claimed = store.mark_schedule_running(sched.id, "inst", "job-6ad00409")
    assert claimed is not None
    assert claimed.running_job_id == "job-6ad00409"
    assert store.mark_schedule_running(sched.id, "other", "job-other") is None
    holder = store.last_claim_refusal(sched.id)
    assert holder is not None
    assert holder["job_id"] == "job-6ad00409"
    text = store.describe_claim_holder(holder)
    assert "job-6ad00409" in text
    assert "no job bound yet" not in text


def test_second_run_now_names_the_job_already_bound(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    sched = store.create_schedule({
        "name": "QA_R14_run",
        "source_connector_id": "src",
        "source_table": "s",
        "dest_connector_id": "dst",
        "dest_table": "d",
        "cron": "*/5 * * * *",
        "timezone": "UTC",
        "sync_mode": "full_refresh_overwrite",
        "enabled": True,
        "mappings": [{"source": "id", "target": "id", "confidence": 1.0}],
    })

    def _bind(schedule_id, *_args, **_kwargs):
        store.set_running_job(schedule_id, "job-6ad00409")
        return "job-6ad00409"

    monkeypatch.setattr(runner, "_dispatch_transfer", _bind)
    first = runner._run_schedule(sched.id, manual=True)
    assert first == "job-6ad00409"
    assert store.get_schedule(sched.id).running_job_id == "job-6ad00409"
    try:
        runner._run_schedule(sched.id, manual=True)
    except runner.ScheduleStartError as exc:
        text = str(exc)
        status = exc.http_status
    else:
        raise AssertionError("second Run now started another job")
    assert status == 409
    assert "job-6ad00409" in text
    assert "no job bound yet" not in text
    assert store.get_schedule(sched.id).running_job_id == "job-6ad00409"


def test_get_job_exposes_rerun_of_and_parent_job_id():
    from src.ai.copilot.job_reads import job_lineage
    from src.transfer.models import sanitize_job_for_api

    job = {
        "status": "completed",
        "transfer_request": {
            "rerun_of": "job-parent-rerun",
            "parent_job_id": "job-parent-sync",
        },
    }
    lineage = job_lineage(job)
    assert lineage["rerun_of"] == "job-parent-rerun"
    assert lineage["parent_job_id"] == "job-parent-sync"
    safe = sanitize_job_for_api(job)
    assert safe["rerun_of"] == "job-parent-rerun"
    assert safe["parent_job_id"] == "job-parent-sync"


def test_cast_plan_reports_one_column_type():
    """A live INTEGER and a DECIMAL stamp must not both be the plan's cast."""
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["qty"],
        column_types={"qty": "TEXT"},
        row_count=1,
        mappings=[{
            "source": "qty",
            "target": "qty",
            "transform": "none",
            "target_type": "DECIMAL",
            "confidence": 1.0,
        }],
        sample_rows=[{"qty": "4"}],
        destination_connected=True,
        destination_column_types={"qty": "INTEGER"},
        destination_table_exists=False,
        destination_db_type="postgresql",
        destination_can_create=True,
        destination_can_write=True,
        skip_population_fit=True,
        sync_mode="full_refresh_append",
    )
    planned = result["plan"]["mappings"][0]
    assert str(planned["target_type"]).upper() == "INTEGER"
    assert planned["transform"] == "integer"
    assert str(result["plan"]["destination_column_types"]["qty"]).upper() == "INTEGER"


def test_s3_key_resolves_to_the_catalog_file():
    from services.dataset_resolve import pick_dataset
    from src.ai.training.universal_data_feeder import UniversalSchema

    catalog = [
        UniversalSchema(
            name="pii3.csv",
            source="upload",
            columns=["email"],
            samples={"email": ["a@b.c"]},
            row_count=3,
            file_type="csv",
            path="s3://qa/QA_R4/pii3.csv",
        ),
        UniversalSchema(
            name="Financial Services",
            source="industry",
            columns=["amount"],
            samples={},
            row_count=0,
            industry="finance",
        ),
    ]
    chosen = pick_dataset(catalog, "QA_R4/pii3.csv")
    assert chosen is not None
    assert chosen.name == "pii3.csv"
