"""Round 13 regressions.

R11-001: a failed replay, including overwrite, must not replace a landed object.
SSRF: 172.16/12 and 192.168/16 are refused before a socket or libpq dial.
R7-001: a Postgres NULL is not an empty CSV field, and the internal fingerprint
marker is not what an operator reads. R12-001: the refusal sentence states the
reason. R12-002: ``*/7`` is anchored to the previous run.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


def test_overwrite_replay_does_not_replace_a_landed_s3_object():
    """298 rows became 0 when a failed replay promoted a header (R11-001)."""
    from connectors.s3_writer import write_mapped_rows

    lines = ["id,name"] + [f"{i},n{i}" for i in range(1, 11)]
    live = ("\n".join(lines) + "\n").encode()
    key = "QA_R13/pii3_copy.csv"
    store = {key: live}
    puts: list[str] = []

    class _S3:
        def head_bucket(self, **_kwargs):
            return {}

        def head_object(self, *, Bucket, Key):
            if Key not in store:
                from botocore.exceptions import ClientError

                raise ClientError(
                    {"Error": {"Code": "404", "Message": "missing"}}, "HeadObject"
                )
            return {"ContentLength": len(store[Key])}

        def put_object(self, *, Bucket, Key, Body, ContentType):
            puts.append(Key)
            store[Key] = bytes(Body)

        def copy_object(self, **kwargs):
            source = kwargs["CopySource"]["Key"]
            store[kwargs["Key"]] = store[source]

        def delete_object(self, *, Bucket, Key):
            store.pop(Key, None)

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
    assert store[key] == live
    assert key not in puts


def test_commit_local_object_failure_leaves_the_live_key(tmp_path):
    from services.copy_s3_common import commit_local_object

    live = b"id\n" + b"\n".join(f"{i}".encode() for i in range(298)) + b"\n"
    store = {"QA_R13/pii3_copy.csv": live}
    path = tmp_path / "next.csv"
    path.write_bytes(b"id\n")

    class _S3:
        def upload_file(self, filename, bucket, key):
            store[key] = path.read_bytes()

        def head_object(self, *, Bucket, Key):
            if Key not in store:
                raise KeyError(Key)
            return {"ContentLength": len(store[Key])}

        def copy_object(self, **_kwargs):
            raise RuntimeError("commit failed")

        def delete_object(self, *, Bucket, Key):
            if Key != "QA_R13/pii3_copy.csv":
                store.pop(Key, None)

    from unittest.mock import patch

    with patch("services.copy_s3_common.s3_client", return_value=_S3()):
        with pytest.raises(RuntimeError, match="commit failed"):
            commit_local_object({"database": "qa"}, "QA_R13/pii3_copy.csv", str(path))
    assert store["QA_R13/pii3_copy.csv"] == live


def test_postgres_and_mysql_private_ranges_do_not_dial(monkeypatch):
    from services.egress_guard import EgressRefused

    dials: list[str] = []

    def _refuse_pg(**_kwargs):
        dials.append("postgres")
        raise AssertionError("postgres dialled")

    def _refuse_my(**_kwargs):
        dials.append("mysql")
        raise AssertionError("mysql dialled")

    monkeypatch.setattr("psycopg2.connect", _refuse_pg)
    monkeypatch.setattr("pymysql.connect", _refuse_my)
    from connectors.mysql_conn import get_connection as mysql_connect
    from connectors.postgresql_conn import get_connection as pg_connect

    with pytest.raises(EgressRefused, match="172.16/12"):
        pg_connect(
            host="172.16.9.9",
            port=5432,
            database="db",
            username="u",
            password="p",
            connection_string="",
            ssl=False,
        )
    with pytest.raises(EgressRefused, match="192.168/16"):
        mysql_connect(
            host="192.168.4.4",
            port=3306,
            database="db",
            username="u",
            password="p",
            connection_string="",
            ssl=False,
        )
    assert dials == []


def test_connect_rechecks_a_literal_private_address():
    import socket
    import time

    from services.egress_guard import EgressRefused, pinned_egress

    started = time.perf_counter()
    with pytest.raises(EgressRefused, match="192.168/16"):
        with pinned_egress(allowlist=()):
            sock = socket.socket()
            try:
                sock.connect(("192.168.9.9", 9))
            finally:
                sock.close()
    assert time.perf_counter() - started < 1.0


def test_pg_copy_null_token_is_not_an_empty_field():
    from services.copy_pg_s3 import _pg_csv_export_expr

    # The COPY clause is built next to this expression. D115/C04: the operator
    # CSV carries NULL as an unquoted empty field and a real '' as a quoted
    # "" (PostgreSQL's CSV writer does this natively with NULL ''), so the
    # internal __DF_SQL_NULL__ wire sentinel must never reach the file.
    import inspect

    from services import copy_pg_s3

    source = inspect.getsource(copy_pg_s3.copy_postgres_to_s3)
    assert "DELIMITER {delim}, NULL '')" in source
    assert "__DF_SQL_NULL__" not in source
    assert "name" in _pg_csv_export_expr("name", "text", "name")


def test_refused_append_rerun_detail_states_the_reason():
    from services.execution_engine_contract import (
        decide_job_rerun,
        rerun_refusal_detail,
    )

    decision = decide_job_rerun(
        status="completed",
        sync_mode="full_refresh_append",
        rows_committed=12,
    )
    detail = rerun_refusal_detail(decision["reason"])
    for field in ("error", "message", "reason"):
        assert "12 row" in detail[field]
        assert "force=true" in detail[field] or field == "reason"
    assert "force=true" in detail["error"]
    assert "force=true" in detail["message"]


def test_lenient_s3_to_s3_preflight_is_not_blocked_on_inferred_types():
    """Inferred DECIMAL→INTEGER on a file copy is not a cast (R8-003)."""
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["amount"],
        column_types={"amount": "DECIMAL"},
        row_count=2,
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "confidence": 0.99,
                "target_type": "INTEGER",
            }
        ],
        destination_connected=True,
        sample_rows=[{"amount": "10.50"}, {"amount": "2"}],
        destination_column_types={"amount": "INTEGER"},
        destination_table_exists=True,
        destination_can_write=True,
        destination_db_type="s3",
        source_kind="database",
        source_format="s3",
        source_config={"type": "s3"},
        validation_mode="lenient",
        skip_population_fit=True,
    )
    g3 = next(g for g in result["gates"] if g["id"] == "g3_schema_contract")
    assert g3["status"] != "block"
    assert (g3.get("details") or {}).get("rule_id") == "g3_schema_contract.object_store_copy"
    decision = (result.get("proof_bundle") or {}).get("transfer_decision") or {}
    assert decision.get("decision") != "block", decision
    assert "g3_schema_contract" not in str(result.get("blockers") or "")


def test_star_slash_seven_is_anchored_to_the_previous_run():
    from services.schedule_store import canonicalize_cadence, compute_next_run

    interval, cron = canonicalize_cadence("every 7 minutes", "")
    assert interval == "every_7m"
    assert cron == ""
    anchored_interval, anchored_cron = canonicalize_cadence("daily", "*/7 * * * *")
    assert anchored_interval == "every_7m"
    assert anchored_cron == ""
    kept_interval, kept_cron = canonicalize_cadence("cron", "*/5 * * * *")
    assert kept_cron == "*/5 * * * *"
    assert kept_interval == "cron"

    start = datetime(2026, 1, 1, 10, 56, tzinfo=timezone.utc)
    rolled = datetime.fromisoformat(
        compute_next_run("daily", start, cron="*/7 * * * *")
    )
    assert rolled - start == timedelta(minutes=7)
