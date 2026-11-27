"""D402 review — the dedupe collapse and its post-write proof use ONE key equality.

keep_last collapsed by exact text; the post-write check grouped by the
destination's collation. On MariaDB's ``utf8mb4_general_ci`` (PAD SPACE)
``'a'``/``'A'`` and ``'b'``/``'b '`` are one key each, so a correct exact
collapse was failed after the table had already been replaced.

Now the ambiguity is found before any write, and the operator chooses
(``dedupe_key_equality``): ``destination`` collapses by the destination's
rules, ``exact`` keeps both spellings and the proof groups by exact bytes.
Unchosen, the run is refused with nothing written.

Live MariaDB (``DATAFLOW_MYSQL_TEST_DSN``, default the CI login on :3306),
through ``execute_tracked`` on two routes: a CSV upload (file stream) and an
S3 object (buffered read). Also here: a CSV export whose collapse escaped is
never green (file destinations read the export back), the NULL-key and
unique-key rules of the post-write count on live PostgreSQL, and the billed
warehouse rule.
"""

from __future__ import annotations

import os
import uuid
from urllib.parse import urlparse

import pytest

from tests.test_upsert_infile_duplicate_start_transfer_e2e import transfer_client  # noqa: F401

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.conftest import pg_test_cfg  # noqa: E402

_BUCKET = "dataflow-matrix"
CSV = b"code,v\na,1\nA,2\nb,3\nb ,4\nc,5\nc,6\n"


def _mariadb() -> dict:
    u = urlparse(os.environ.get("DATAFLOW_MYSQL_TEST_DSN")
                 or "mysql://dataflow:dataflow@127.0.0.1:3306/dataflow")
    return {"host": u.hostname or "127.0.0.1", "port": u.port or 3306,
            "database": (u.path or "/dataflow").lstrip("/"),
            "username": u.username or "dataflow", "password": u.password or ""}


def _sql(statement: str) -> list[tuple]:
    import pymysql

    cfg = _mariadb()
    try:
        conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
                               password=cfg["password"], database=cfg["database"])
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"MariaDB is required for the D402 key-equality e2e: {exc}")
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
            rows = [tuple(r) for r in cur.fetchall()] if cur.description else []
        conn.commit()
        return rows
    finally:
        conn.close()


def _exists(table: str) -> bool:
    return bool(_sql(f"SHOW TABLES LIKE '{table}'"))


@pytest.fixture()
def jobs(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _source(route: str, extra: dict, s3_endpoint: str) -> dict:
    if route == "upload":
        return {"source": EndpointConfig(kind="file", format="csv", extra=dict(extra)),
                "source_filename": "codes.csv", "source_content": CSV}
    import boto3

    if not s3_endpoint:
        pytest.fail("moto is required for the S3 route")
    key = f"d402eq/{uuid.uuid4().hex[:6]}/codes.csv"
    boto3.client("s3", endpoint_url=s3_endpoint, aws_access_key_id="test",
                 aws_secret_access_key="test", region_name="us-east-1",
                 ).put_object(Bucket=_BUCKET, Key=key, Body=CSV)
    return {"source": EndpointConfig.from_dict("database", {
        "kind": "database", "format": "s3", "host": "us-east-1",
        "port": int(s3_endpoint.rsplit(":", 1)[1]), "database": _BUCKET, "table": key,
        "username": "test", "password": "test", "ssl": False,
        "extra": {**extra, "endpoint_url": s3_endpoint, "path_style": True}})}


def _run(jobs, route: str, table: str, choice: str, s3_endpoint: str, *,
         destination: EndpointConfig | None = None) -> tuple[object, dict]:
    extra = {"in_file_dedupe": "keep_last", "dedupe_key": "code",
             **({"dedupe_key_equality": choice} if choice else {})}
    dest = destination or EndpointConfig.from_dict("database", {
        "kind": "database", "format": "mysql", **_mariadb(), "table": table, "extra": dict(extra)})
    request = TransferRequest(
        destination=dest, sync_mode="full_refresh_overwrite",
        schema_policy="propagate_columns", validation_mode="strict",
        **_source(route, extra, s3_endpoint),
    )
    job_id = uuid.uuid4().hex[:24]
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    return result, dict(jobs.get_job(job_id) or {})


@pytest.mark.parametrize("route", ["upload", "s3"])
def test_unchosen_equality_refuses_before_any_write(jobs, local_object_store, route) -> None:
    table = f"d402eq_{uuid.uuid4().hex[:8]}"
    try:
        result, job = _run(jobs, route, table, "", local_object_store)
        text = f"{result.error} {job.get('message')}"
        assert result.success is False and job.get("status") == "failed", text
        assert "'a' = 'A'" in text and "'b' = 'b '" in text, text
        assert "dedupe_key_equality=destination" in text and "dedupe_key_equality=exact" in text, text
        assert not _exists(table), "the refused run created or wrote the table"
    finally:
        _sql(f"DROP TABLE IF EXISTS `{table}`")


@pytest.mark.parametrize("route", ["upload", "s3"])
def test_exact_keeps_both_spellings_and_its_proof_groups_by_exact_bytes(
    jobs, local_object_store, route
) -> None:
    table = f"d402eq_{uuid.uuid4().hex[:8]}"
    try:
        result, job = _run(jobs, route, table, "exact", local_object_store)
        message = str(job.get("message") or result.error or "")
        assert result.success is True and job.get("status") == "completed", message
        assert "dedupe" not in message.lower() and "unverif" not in message.lower(), message
        rows = sorted(_sql(f"SELECT CAST(code AS BINARY), v FROM `{table}`"))
        assert rows == sorted([(b"A", 2), (b"a", 1), (b"b", 3), (b"b ", 4), (b"c", 6)]), rows
    finally:
        _sql(f"DROP TABLE IF EXISTS `{table}`")


@pytest.mark.parametrize("route", ["upload", "s3"])
def test_destination_collapses_by_its_rules_last_row_wins(jobs, local_object_store, route) -> None:
    table = f"d402eq_{uuid.uuid4().hex[:8]}"
    try:
        result, job = _run(jobs, route, table, "destination", local_object_store)
        message = str(job.get("message") or result.error or "")
        assert result.success is True and job.get("status") == "completed", message
        assert "dedupe" not in message.lower() and "unverif" not in message.lower(), message
        rows = sorted(_sql(f"SELECT CAST(code AS BINARY), v FROM `{table}`"))
        assert rows == [(b"A", 2), (b"b ", 4), (b"c", 6)], rows
    finally:
        _sql(f"DROP TABLE IF EXISTS `{table}`")


def test_exact_is_refused_when_a_unique_key_would_reject_the_second_spelling(
    jobs, local_object_store
) -> None:
    table = f"d402eq_{uuid.uuid4().hex[:8]}"
    _sql(f"CREATE TABLE `{table}` (code VARCHAR(20) COLLATE utf8mb4_general_ci PRIMARY KEY, v INT)")
    _sql(f"INSERT INTO `{table}` VALUES ('kept', 9)")
    try:
        result, job = _run(jobs, "s3", table, "exact", local_object_store)
        assert result.success is False and job.get("status") == "failed", job.get("message")
        assert _sql(f"SELECT code, v FROM `{table}`") == [("kept", 9)]
    finally:
        _sql(f"DROP TABLE IF EXISTS `{table}`")


def test_a_csv_export_whose_collapse_escaped_is_never_green(jobs, tmp_path, monkeypatch) -> None:
    import services.duplicate_policy as dp

    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    monkeypatch.setattr(dp, "keep_last_reads_whole", lambda _request: False)
    monkeypatch.setattr(dp, "keep_last_records", lambda _request, records: records)
    monkeypatch.setattr(dp, "iter_keep_last", lambda batches, _keys, **_kw: batches)
    extra = {"in_file_dedupe": "keep_last", "dedupe_key": "code"}
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv", extra=dict(extra)),
        source_filename="codes.csv", source_content=b"code,v\nx,1\ny,2\nx,3\n",
        destination=EndpointConfig(kind="file_export", format="csv", extra=dict(extra)),
        sync_mode="full_refresh_overwrite", validation_mode="strict", skip_preflight=True,
    )
    job_id = uuid.uuid4().hex[:24]
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    job = jobs.get_job(job_id) or {}
    recon = job.get("reconciliation") or getattr(result, "reconciliation", None) or {}
    text = f"{result.error} {job.get('message')} {recon.get('message')}"
    assert not (result.success and job.get("status") == "completed" and recon.get("passed")), text
    assert "more than once" in text, text


def _pg():
    import psycopg2

    cfg = pg_test_cfg()
    conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                            user=cfg["username"], password=cfg["password"])
    conn.autocommit = True
    return cfg, conn


def test_post_write_count_leaves_null_keys_out_and_skips_a_covering_unique_key() -> None:
    from services.dedupe_postwrite import count_duplicate_keys

    cfg, conn = _pg()
    table = f"d402nk_{uuid.uuid4().hex[:8]}"
    keyed = f"{table}_pk"
    dest = {"type": "postgresql", "host": cfg["host"], "port": cfg["port"],
            "database": cfg["database"], "username": cfg["username"], "password": cfg["password"]}
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{table}" (id INT, v TEXT)')
            cur.execute(f"INSERT INTO \"{table}\" VALUES (1,'a'),(2,'b'),(NULL,'c'),(NULL,'d')")
            cur.execute(f'CREATE TABLE "{keyed}" (id INT PRIMARY KEY, v TEXT)')
        # Two NULL keys are two rows with no key, not one key held twice.
        assert count_duplicate_keys("postgresql", dest, schema="public", table=table,
                                    columns=["id"], exact=True) == 0
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO \"{table}\" VALUES (2,'b2')")
        assert count_duplicate_keys("postgresql", dest, schema="public", table=table,
                                    columns=["id"], exact=True) == 1
        # A primary key on the column already proves uniqueness: no scan.
        assert count_duplicate_keys("postgresql", dest, schema="public", table=keyed,
                                    columns=["id"], exact=True) is None
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'DROP TABLE IF EXISTS "{keyed}"')
        conn.close()


@pytest.mark.parametrize("warehouse", ["snowflake", "bigquery", "redshift", "databricks"])
def test_billed_warehouses_are_not_scanned_unasked(warehouse) -> None:
    from services.dedupe_postwrite import apply_infile_dedupe_check, infile_dedupe_check

    summary = {"dedupe_key_targets": ["id"]}
    check = infile_dedupe_check(summary, db_type=warehouse, cfg={}, schema="s", table_name="t",
                                whole_table=True, sql_dialect=warehouse)
    assert check["status"] == "unverified" and "billed warehouse" in check["reason"], check
    stamped = {"passed": True, "message": "Row fidelity verified"}
    apply_infile_dedupe_check(stamped, check)
    assert stamped["verification_verdict"] == "partial" and stamped["message"].startswith("Dedupe unverified")


def test_start_transfer_offers_and_honours_the_destination_choice(
    transfer_client, local_object_store
) -> None:
    """The remedy the refusal names is reachable on the MCP tool the operator uses."""
    import boto3

    from services.connector_store import create_connector
    from src.ai.copilot.transfer_tools import start_transfer
    from tests.test_upsert_infile_duplicate_start_transfer_e2e import _wait_job

    key = f"d402eq/{uuid.uuid4().hex[:6]}/codes.csv"
    boto3.client("s3", endpoint_url=local_object_store, aws_access_key_id="test",
                 aws_secret_access_key="test", region_name="us-east-1",
                 ).put_object(Bucket=_BUCKET, Key=key, Body=CSV)
    tag = uuid.uuid4().hex[:6]
    src = create_connector({
        "name": f"d402eq-s3-{tag}", "type": "s3", "role": "source", "host": "us-east-1",
        "port": int(local_object_store.rsplit(":", 1)[1]), "database": _BUCKET,
        "username": "test", "password": "test", "schema": "", "ssl": False,
        "endpoint_url": local_object_store, "path_style": True,
    })
    dst = create_connector({"name": f"d402eq-mdb-{tag}", "type": "mysql", "role": "destination",
                            **_mariadb(), "ssl": False})
    table = f"d402eq_st_{tag}"
    try:
        staged = start_transfer(
            source_connector_id=src.id, source_table=key, dest_connector_id=dst.id,
            dest_table=table, sync_mode="full_refresh_overwrite", dedupe_key="code",
            dedupe_key_equality="destination", schema_policy="propagate_columns",
            validation_mode="strict",
        )
        assert staged.success, staged.error
        assert staged.output["plan"].get("dedupe_key_equality") == "destination"
        confirmed = transfer_client.post(
            "/api/v1/copilot/confirm", json={"ack_id": staged.output["ack_id"], "reason": "d402"})
        assert confirmed.status_code == 200, confirmed.text
        job = _wait_job(str(confirmed.json().get("job_id") or ""))
        assert job.get("status") == "completed", (job.get("status"), job.get("message"))
        # start_transfer keys the table it creates on code with a case-sensitive
        # PAD SPACE collation (utf8mb4_bin, D205): 'a'/'A' stay two keys, 'b'/'b '
        # are one, and the last row of that key wins.
        rows = sorted(_sql(f"SELECT CAST(code AS BINARY), v FROM `{table}`"))
        assert rows == [(b"A", 2), (b"a", 1), (b"b ", 4), (b"c", 6)], rows
    finally:
        _sql(f"DROP TABLE IF EXISTS `{table}`")


def test_a_schedule_carries_the_equality_choice_to_every_run() -> None:
    from services.schedule_runner import build_schedule_request
    from services.schedule_store import PipelineSchedule

    sched = PipelineSchedule.from_dict({
        "id": "s1", "name": "n", "source_connector_id": "", "source_table": "codes.csv",
        "dest_connector_id": "", "dest_table": "t", "interval": "daily",
        "in_file_dedupe": "keep_last", "dedupe_key": "code", "dedupe_key_equality": "exact",
        "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in ("code", "v")],
    })
    request = build_schedule_request(sched, {"type": "s3", "database": "b"}, {"type": "mysql"})
    for side in (request.source, request.destination):
        assert (side.extra or {}).get("dedupe_key_equality") == "exact", side.extra
