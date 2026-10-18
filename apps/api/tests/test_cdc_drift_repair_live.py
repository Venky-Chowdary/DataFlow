"""Live exactly-once CDC drift-repair proof for Oracle and SQL Server."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import uuid
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from connectors.cdc_eos_sa import sa_dest_watermark_view  # noqa: E402
from connectors.generic_sql import get_connection  # noqa: E402
from connectors.lsn_guards import (  # noqa: E402
    extract_cdc_commit_position,
)
from connectors.postgresql_conn import get_connection as pg_connection  # noqa: E402
from connectors.sqlserver_cdc_native import (  # noqa: E402
    SqlServerNativeCdc,
    _hex_to_lsn,
    decode_mssql_cdc_token,
    encode_mssql_cdc_token,
)
from cdc_sqlserver_live_helpers import (  # noqa: E402
    sqlserver_server_now,
    wait_for_sqlserver_capture_job_healthy,
)
from services.brand_env import getenv_brand  # noqa: E402
from services.cdc_exactly_once import WATERMARK_TABLE, batch_lsn  # noqa: E402
from services import cdc_lease_store  # noqa: E402
from services.audit_log import list_audit_events  # noqa: E402
from services.cdc_drift_repair import (  # noqa: E402
    CdcDriftRequiresFencedRepair,
    _build_drift_evidence,
)
from services.cdc_lease import (  # noqa: E402
    acquire_lease,
    mssql_cdc_resource,
    oracle_cdc_resource,
    release_lease,
)
from services.mongodb_service import (  # noqa: E402
    MemoryMongoDBService,
    get_mongodb_service,
)
from services.sync_cursor import clear_watermark, get_watermark  # noqa: E402
from src.ai.copilot import ack_ledger, lifecycle_tools  # noqa: E402
from src.transfer.adapters import resolve_connector_config  # noqa: E402
from src.transfer.connector_capabilities import resolve_driver_type  # noqa: E402
from src.transfer.cdc_transfer import _single_stream_cursor_key  # noqa: E402
import src.transfer.engine as transfer_engine  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import (  # noqa: E402
    EndpointConfig,
    TransferRequest,
    transfer_request_to_dict,
)
from src.main import app  # noqa: E402
from src.routers import copilot_router  # noqa: E402
from test_cdc_oracle_logminer_transfer_e2e import (  # noqa: E402
    _oracle_cfg,
    _oracle_logminer_ready,
)
from test_cdc_oracle_logminer_txn_live import _pg_cfg, _pg_ready  # noqa: E402
from test_cdc_sqlserver_native_transfer_e2e import (  # noqa: E402
    _sqlserver_native_ready,
    _wait_for_cdc_events,
)
from tests.test_cdc_exactly_once_live_engines import ENGINES  # noqa: E402




@pytest.mark.parametrize(
    "route",
    [
        pytest.param(
            "oracle",
            marks=pytest.mark.skipif(
                not _oracle_logminer_ready() or not _pg_ready(),
                reason="Oracle LogMiner and live PostgreSQL environment are required",
            ),
            id="oracle-logminer-to-postgresql",
        ),
        pytest.param(
            "sqlserver",
            marks=pytest.mark.skipif(
                (getenv_brand("SQLSERVER_ENABLE", "") or "").strip().lower()
                not in {"1", "true", "yes", "on"}
                or not _sqlserver_native_ready()
                or not _pg_ready(),
                reason="SQL Server native CDC and live PostgreSQL environment are required",
            ),
            id="sqlserver-cdc-to-postgresql",
        ),
    ],
)
def test_cdc_drift_repair_live(route: str, tmp_path: Path, monkeypatch, caplog) -> None:
    mongo = get_mongodb_service()
    if not isinstance(mongo, MemoryMongoDBService):
        pytest.skip("The live drift proof requires DATAFLOW_JOB_STORE=memory")

    run_id = uuid.uuid4().hex[:10]
    workspace_id = f"workspace-m6-drift-{route}-{run_id}"
    job_id = f"m6-live-drift-{route}-{run_id}"
    actor = "m6-drift-live-operator@example.test"
    pg_cfg = _pg_cfg()
    source_table = (
        f"M6DRIFT_{run_id.upper()}" if route == "oracle" else f"cdc_drift_{run_id}"
    )
    dest_table = f"cdc_drift_live_{run_id}"
    if route == "oracle":
        raw_source_cfg = _oracle_cfg()
        source_schema = str(raw_source_cfg["schema"])
        source_pk = "ID"
        source_value = "V"
        source_endpoint = EndpointConfig(
            kind="database",
            format="oracle",
            host=str(raw_source_cfg["host"]),
            port=int(raw_source_cfg["port"]),
            database=str(raw_source_cfg["database"]),
            schema=source_schema,
            table=source_table,
            username=str(raw_source_cfg["username"]),
            password=str(raw_source_cfg["password"]),
            connection_string=str(raw_source_cfg.get("connection_string") or ""),
            ssl=bool(raw_source_cfg.get("ssl", False)),
            extra={
                key: value
                for key, value in raw_source_cfg.items()
                if key
                not in {
                    "db_type",
                    "host",
                    "port",
                    "database",
                    "schema",
                    "username",
                    "password",
                    "connection_string",
                    "ssl",
                }
            },
        )
        source_connect_cfg = dict(raw_source_cfg)
        source_connect_cfg["type"] = "oracle"
    else:
        raw_source_cfg = dict(ENGINES["sqlserver"])
        source_schema = "dbo"
        source_pk = "id"
        source_value = "v"
        source_connect_cfg = {
            "host": raw_source_cfg["host"],
            "port": raw_source_cfg["port"],
            "database": raw_source_cfg["database"],
            "username": raw_source_cfg["username"],
            "password": raw_source_cfg["password"],
            "connection_string": "",
            "ssl": False,
            "db_type": "sqlserver",
        }
        source_endpoint = EndpointConfig(
            kind="database",
            format="sqlserver",
            host=str(raw_source_cfg["host"]),
            port=int(raw_source_cfg["port"]),
            database=str(raw_source_cfg["database"]),
            schema=source_schema,
            table=source_table,
            username=str(raw_source_cfg["username"]),
            password=str(raw_source_cfg["password"]),
            extra={"cdc_row_filter": "all update old"},
        )
    destination_endpoint = EndpointConfig(
        kind="database",
        format="postgresql",
        host=str(pg_cfg["host"]),
        port=int(pg_cfg["port"]),
        database=str(pg_cfg["database"]),
        schema="public",
        table=dest_table,
        username=str(pg_cfg["username"]),
        password=str(pg_cfg["password"]),
    )
    mapping = [
        {"source": source_pk, "target": "id", "confidence": 1.0},
        {"source": source_value, "target": "v", "confidence": 1.0},
    ]
    column_types = {source_pk: "INTEGER", source_value: "VARCHAR(200)"}
    contracts = [
        {
            "name": source_table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": source_pk,
            "cursor_semantics": "cdc_position",
            "sync_mode": "cdc",
        }
    ]
    request = TransferRequest(
        source=source_endpoint,
        destination=destination_endpoint,
        mappings=mapping,
        column_types=column_types,
        skip_preflight=True,
        sync_mode="cdc",
        validation_mode="balanced",
        workspace_id=workspace_id,
        stream_contracts=contracts,
        delivery_guarantee="exactly_once",
    )
    request.delivery_pinned = True
    mongo.create_transfer_job(
        {
            "_id": job_id,
            "workspace_id": workspace_id,
            "status": "pending",
            "transfer_request": transfer_request_to_dict(request),
        }
    )
    source_reader = None
    source_table_created = False
    held_lease = None
    capture_job_may_be_stopped = False
    original_lease_store = cdc_lease_store.get_store()
    redis_url = (
        getenv_brand("CDC_LEASE_REDIS_URL")
        or getenv_brand("REDIS_URL")
        or "redis://127.0.0.1:6379/15"
    )
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_BACKEND", "redis")
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_REDIS_URL", redis_url)
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_REDIS_PREFIX", f"m6-drift-live:{run_id}:")
    redis_store = cdc_lease_store.configure_store(
        backend="redis",
        url=redis_url,
        prefix=f"m6-drift-live:{run_id}:",
    )
    assert redis_store.name == "redis"

    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "pilot-acks.json"))
    monkeypatch.setattr(ack_ledger, "_ledger", None)
    monkeypatch.setattr(
        copilot_router,
        "_caller",
        lambda _request: ("admin", actor),
    )
    client = TestClient(app)
    engine = UniversalTransferEngine()
    ids = [int(run_id[:7], 16) + offset for offset in range(3)]
    base_values = [f"{route}-snapshot-{offset}" for offset in range(3)]
    streamed_values = [f"{route}-streamed-{offset}" for offset in range(3)]
    damaged_values = [f"damage-sentinel-{route}-{run_id}-{offset}" for offset in range(3)]
    cursor_key = ""
    source_connection = None
    try:
        if route == "oracle":
            source_connection = get_connection(**source_connect_cfg)
            table_ref = f'"{source_schema}"."{source_table}"'
            with source_connection as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"CREATE TABLE {table_ref} "
                        "(ID NUMBER(10) PRIMARY KEY, V VARCHAR2(200))"
                    )
                    source_table_created = True
                    cur.execute(
                        f"ALTER TABLE {table_ref} "
                        "ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                    )
                    for row_id, value in zip(ids, base_values):
                        cur.execute(
                            f"INSERT INTO {table_ref} (ID, V) VALUES (:id, :value)",
                            {"id": row_id, "value": value},
                        )
                conn.commit()
            source_table_created = True
        else:
            source_reader = SqlServerNativeCdc(
                source_connect_cfg,
                table=source_table,
                primary_key=source_pk,
                schema=source_schema,
                cursor_key=f"{job_id}:source",
                row_filter="all update old",
            )
            with source_reader._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"CREATE TABLE dbo.[{source_table}] "
                        "(id INT NOT NULL PRIMARY KEY, v NVARCHAR(200) NOT NULL)"
                    )
                    source_table_created = True
                    cur.execute(
                        """
                        IF NOT EXISTS (
                            SELECT 1 FROM sys.databases
                            WHERE name = DB_NAME() AND is_cdc_enabled = 1
                        )
                            EXEC sys.sp_cdc_enable_db;
                        """
                    )
                    cur.execute(
                        f"""
                        EXEC sys.sp_cdc_enable_table
                            @source_schema = N'dbo',
                            @source_name = N'{source_table}',
                            @role_name = NULL,
                            @supports_net_changes = 0;
                        """
                    )
                    for row_id, value in zip(ids, base_values):
                        cur.execute(
                            f"INSERT INTO dbo.[{source_table}] (id, v) VALUES (%s, %s)",
                            (row_id, value),
                        )
                conn.commit()
            source_table_created = True
            with source_reader._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM cdc.change_tables
                        WHERE source_object_id <> OBJECT_ID(%s)
                        """,
                        (f"{source_schema}.{source_table}",),
                    )
                    other_cdc_enabled_tables = int(cur.fetchone()[0] or 0)
            assert other_cdc_enabled_tables >= 1, (
                "SQL Server drift proof requires another CDC-enabled table "
                "in the database"
            )

        resolved_source_cfg = resolve_connector_config(
            source_endpoint, workspace_id=workspace_id
        )
        resolved_dest_cfg = resolve_connector_config(
            destination_endpoint, workspace_id=workspace_id
        )
        cursor_key = _single_stream_cursor_key(
            src_type=resolve_driver_type(source_endpoint.format),
            src_cfg=resolved_source_cfg,
            table_name=source_table,
            dest_type=resolve_driver_type(destination_endpoint.format),
            dest_cfg=resolved_dest_cfg,
            dest_table=dest_table,
            stream_name=source_table,
        )

        result = engine.execute_tracked(request, job_id, resume=False)
        assert result.success, result.error or result.error_details
        assert result.destination_summary.get("cdc_delivery") == "exactly_once"
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v FROM public."{dest_table}" ORDER BY id'
                )
                snapshot_rows = cur.fetchall()
        assert [int(row[0]) for row in snapshot_rows] == ids
        assert [str(row[1]) for row in snapshot_rows] == base_values

        if route == "oracle":
            with get_connection(**source_connect_cfg) as conn:
                with conn.cursor() as cur:
                    for row_id, value in zip(ids, streamed_values):
                        cur.execute(
                            f"UPDATE {table_ref} SET V = :value WHERE ID = :id",
                            {"value": value, "id": row_id},
                        )
                conn.commit()
        else:
            with source_reader._conn() as conn:
                with conn.cursor() as cur:
                    for row_id, value in zip(ids, streamed_values):
                        cur.execute(
                            f"UPDATE dbo.[{source_table}] SET v = %s WHERE id = %s",
                            (value, row_id),
                        )
                conn.commit()
            source_reader.force_cdc_scan()
            initial_watermark = (
                result.destination_summary.get("cdc") or {}
            ).get("watermark")
            handoff_lsn = decode_mssql_cdc_token(initial_watermark)["lsn"]
            assert handoff_lsn, result.destination_summary
            _wait_for_cdc_events(
                source_reader,
                handoff_lsn,
                {(4, row_id) for row_id in ids},
            )

        result = engine.execute_tracked(request, job_id, resume=True)
        assert result.success, result.error or result.error_details
        assert result.destination_summary.get("cdc_delivery") == "exactly_once"
        before_view = sa_dest_watermark_view(
            resolved_dest_cfg, cursor_key, "postgresql"
        )
        token = get_watermark(cursor_key)
        assert token
        p_eos = batch_lsn(token)
        assert p_eos
        assert p_eos == before_view.committed_lsn
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v, "_df_lsn" FROM public."{dest_table}" ORDER BY id'
                )
                clean_rows = cur.fetchall()
                for row_id, sentinel in zip(ids, damaged_values):
                    cur.execute(
                        f'UPDATE public."{dest_table}" SET v = %s WHERE id = %s',
                        (sentinel, row_id),
                    )
            conn.commit()
        damaged_before = [
            (int(row[0]), str(row[1]), str(row[2] or "")) for row in clean_rows
        ]
        assert [row[2] for row in damaged_before] == [p_eos] * 3
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v, "_df_lsn" FROM public."{dest_table}" ORDER BY id'
                )
                damaged_rows = cur.fetchall()
        assert [str(row[1]) for row in damaged_rows] == damaged_values
        assert [str(row[2] or "") for row in damaged_rows] == [
            row[2] for row in damaged_before
        ]

        before_refusal = {
            "eos": (
                before_view.apply_seq,
                before_view.committed_lsn,
                before_view.apply_checksum,
                before_view.fence_epoch,
            ),
            "sync": get_watermark(cursor_key),
            "destination": [
                (int(row[0]), str(row[1]), str(row[2] or ""))
                for row in damaged_rows
            ],
        }
        caplog.clear()
        with patch.object(
            transfer_engine,
            "_fail_runtime_job",
            wraps=transfer_engine._fail_runtime_job,
        ) as failure_mapper:
            refusal = engine.execute_tracked(request, job_id, resume=True)
            assert any(
                len(call.args) > 2
                and isinstance(call.args[2], CdcDriftRequiresFencedRepair)
                for call in failure_mapper.call_args_list
            )
        assert not refusal.success
        assert (
            refusal.error_details.get("code")
            == "cdc_drift_requires_fenced_repair"
        ), refusal.error_details
        saved_job = mongo.get_job(job_id) or {}
        evidence = (saved_job.get("extras") or {}).get("cdc_drift_repair") or {}
        assert isinstance(evidence, dict)
        assert evidence.get("drift_count") == 3
        assert len(evidence.get("keys") or []) == 3
        assert evidence.get("capture_pending") is False
        assert evidence.get("capture_pending_reason") is None
        head_position = evidence.get("capture_head_position")
        reader_position = evidence.get("reader_position")
        assert head_position and reader_position, evidence
        from connectors.lsn_guards import compare_lsn, lsn_family

        reader_family = lsn_family(reader_position)
        assert reader_family is not None, evidence
        assert reader_family == lsn_family(head_position), evidence
        assert compare_lsn(reader_position, head_position) >= 0, evidence
        assert extract_cdc_commit_position(evidence.get("committed_position")) == p_eos
        assert str(evidence.get("cursor_key") or "") == cursor_key
        assert saved_job.get("cdc_lease_cursor_key") == cursor_key
        rendered_evidence = json.dumps(evidence, sort_keys=True, default=str)
        for sentinel in [*damaged_values, *base_values, *streamed_values]:
            assert sentinel not in rendered_evidence
            assert sentinel not in json.dumps(
                refusal.ddl_executed, sort_keys=True, default=str
            )
            assert sentinel not in caplog.text
        after_first_refusal_view = sa_dest_watermark_view(
            resolved_dest_cfg, cursor_key, "postgresql"
        )
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v, "_df_lsn" FROM public."{dest_table}" ORDER BY id'
                )
                after_first_rows = cur.fetchall()
        after_first_refusal = {
            "eos": (
                after_first_refusal_view.apply_seq,
                after_first_refusal_view.committed_lsn,
                after_first_refusal_view.apply_checksum,
                after_first_refusal_view.fence_epoch,
            ),
            "sync": get_watermark(cursor_key),
            "destination": [
                (int(row[0]), str(row[1]), str(row[2] or ""))
                for row in after_first_rows
            ],
        }
        assert after_first_refusal == before_refusal
        assert before_refusal["eos"][1] == p_eos

        repeated_refusal = engine.execute_tracked(request, job_id, resume=True)
        assert not repeated_refusal.success
        assert (
            repeated_refusal.error_details.get("code")
            == "cdc_drift_requires_fenced_repair"
        ), repeated_refusal.error_details
        repeated_job = mongo.get_job(job_id) or {}
        repeated_evidence = (
            (repeated_job.get("extras") or {}).get("cdc_drift_repair") or {}
        )
        assert [item["key_id"] for item in repeated_evidence["keys"]] == [
            item["key_id"] for item in evidence["keys"]
        ]
        repeated_view = sa_dest_watermark_view(
            resolved_dest_cfg, cursor_key, "postgresql"
        )
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v, "_df_lsn" FROM public."{dest_table}" ORDER BY id'
                )
                repeated_rows = cur.fetchall()
        assert (
            repeated_view.apply_seq,
            repeated_view.committed_lsn,
            repeated_view.apply_checksum,
            repeated_view.fence_epoch,
            get_watermark(cursor_key),
            [
                (int(row[0]), str(row[1]), str(row[2] or ""))
                for row in repeated_rows
            ],
        ) == (
            *before_refusal["eos"],
            before_refusal["sync"],
            before_refusal["destination"],
        )

        if route == "oracle":
            lease_resource = oracle_cdc_resource(
                source_schema,
                source_table,
                mode="logminer",
                host=str(resolved_source_cfg.get("host") or ""),
            )
        else:
            lease_resource = mssql_cdc_resource(
                str(resolved_source_cfg.get("database") or "master"),
                source_schema,
                source_table,
                mode="cdc",
            )
        lease_resource = str(evidence.get("cdc_lease_resource") or lease_resource)
        assert lease_resource
        held_lease = acquire_lease(
            cursor_key,
            resource=lease_resource,
            holder_id=f"live-other-holder-{run_id}",
            min_generation=int(before_view.fence_epoch or 0),
            meta={"purpose": "live drift repair lease conflict proof"},
        )
        staged = lifecycle_tools._job_tool(
            "repair_cdc_drift",
            job_id=job_id,
            selector="",
            key_ids=[],
        )
        assert staged.success, staged.error
        lease_conflict = client.post(
            "/api/v1/copilot/confirm",
            headers={"X-Workspace-Id": workspace_id},
            json={"ack_id": staged.output["ack_id"]},
        )
        assert lease_conflict.status_code == 409, lease_conflict.text
        assert "stop the stream first" in lease_conflict.text.lower()
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v, "_df_lsn" FROM public."{dest_table}" ORDER BY id'
                )
                lease_conflict_rows = cur.fetchall()
        assert [
            (int(row[0]), str(row[1]), str(row[2] or ""))
            for row in lease_conflict_rows
        ] == before_refusal["destination"]
        conflict_view = sa_dest_watermark_view(
            resolved_dest_cfg, cursor_key, "postgresql"
        )
        assert (
            conflict_view.apply_seq,
            conflict_view.committed_lsn,
            conflict_view.apply_checksum,
            conflict_view.fence_epoch,
            get_watermark(cursor_key),
        ) == (*before_refusal["eos"], before_refusal["sync"])
        assert release_lease(
            cursor_key,
            holder_id=held_lease.holder_id,
            generation=held_lease.generation,
        )
        held_lease = None

        from services.cdc_resume_tokens import unwrap_resume_token
        from connectors.oracle_logminer import encode_logminer_token

        token_data = unwrap_resume_token(token)
        assert isinstance(token_data, dict), type(token_data).__name__
        if route == "oracle":
            bumped_token = encode_logminer_token(
                int(token_data["scn"]) + 1000,
                table=str(token_data.get("table") or source_table),
                phase="streaming",
                rs_id=str(token_data.get("rs_id") or ""),
                ssn=int(token_data.get("ssn") or 0),
                scn_complete=bool(token_data.get("scn_complete")),
            )
        else:
            state = decode_mssql_cdc_token(token)
            prior_bytes = _hex_to_lsn(state.get("lsn"))
            assert prior_bytes, state
            newer_bytes = (int.from_bytes(prior_bytes, "big") + 1).to_bytes(
                len(prior_bytes), "big"
            )
            bumped_token = encode_mssql_cdc_token(
                newer_bytes,
                table=str(state.get("table") or source_table),
                phase="streaming",
                seqval=str(state.get("seqval") or ""),
                capture_instance=str(state.get("capture_instance") or ""),
            )
        newer_lsn = batch_lsn(bumped_token)
        assert newer_lsn
        assert compare_lsn(newer_lsn, p_eos) > 0
        newer_id = ids[-1]
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'UPDATE public."{dest_table}" SET "_df_lsn" = %s WHERE id = %s',
                    (newer_lsn, newer_id),
                )
            conn.commit()

        audit_before = list_audit_events(
            limit=1000, workspace_id=workspace_id, actor=actor
        )
        staged = lifecycle_tools._job_tool(
            "repair_cdc_drift",
            job_id=job_id,
            selector="",
            key_ids=[],
        )
        assert staged.success, staged.error
        approved = client.post(
            "/api/v1/copilot/confirm",
            headers={"X-Workspace-Id": workspace_id},
            json={"ack_id": staged.output["ack_id"]},
        )
        assert approved.status_code == 200, approved.text
        first_repair = approved.json()
        assert first_repair.get("verified") == 2, first_repair
        first_outcomes = first_repair.get("outcomes") or {}
        assert first_outcomes.get("repaired") == 2, first_repair
        assert first_outcomes.get("refused_newer_event") == 1, first_repair
        repaired_job = mongo.get_job(job_id) or {}
        repaired_evidence = (
            (repaired_job.get("extras") or {}).get("cdc_drift_repair") or {}
        )
        assert repaired_evidence.get("resolved") is False
        last_repair = repaired_evidence.get("last_repair") or {}
        assert last_repair.get("verified") == 2
        last_outcomes = last_repair.get("outcomes") or {}
        assert last_outcomes.get("repaired") == 2
        assert last_outcomes.get("refused_newer_event") == 1
        first_repair_view = sa_dest_watermark_view(
            resolved_dest_cfg, cursor_key, "postgresql"
        )
        assert (
            first_repair_view.apply_seq,
            first_repair_view.committed_lsn,
            first_repair_view.apply_checksum,
        ) == before_refusal["eos"][:3]
        assert first_repair_view.fence_epoch > before_refusal["eos"][3]
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v, "_df_lsn" FROM public."{dest_table}" ORDER BY id'
                )
                first_repair_rows = cur.fetchall()
        for row in first_repair_rows:
            row_id, value, row_lsn = int(row[0]), str(row[1]), str(row[2] or "")
            if row_id == newer_id:
                assert value == damaged_values[-1]
                assert compare_lsn(row_lsn, p_eos) > 0
                assert row_lsn == newer_lsn
            else:
                assert value == streamed_values[ids.index(row_id)]
                assert row_lsn == p_eos
        audit_after = list_audit_events(
            limit=1000, workspace_id=workspace_id, actor=actor
        )
        new_audit = [
            item
            for item in audit_after
            if item.get("resource") == job_id
            and item.get("action")
            in {"cdc_drift_repair_started", "cdc_drift_repair_completed"}
        ]
        assert len(new_audit) - len(
            [
                item
                for item in audit_before
                if item.get("resource") == job_id
                and item.get("action")
                in {"cdc_drift_repair_started", "cdc_drift_repair_completed"}
            ]
        ) >= 2, new_audit

        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'UPDATE public."{dest_table}" SET "_df_lsn" = %s WHERE id = %s',
                    (p_eos, newer_id),
                )
            conn.commit()
        from services.cdc_value_digest import _scan_table

        source_key_rows = _scan_table(
            resolve_driver_type(source_endpoint.format),
            resolved_source_cfg,
            source_table,
            [source_pk],
        )
        assert source_key_rows is not None
        newer_source_row = next(
            row
            for row in source_key_rows
            if int(
                next(
                    value
                    for field, value in row.items()
                    if str(field).casefold() == source_pk.casefold()
                )
            )
            == newer_id
        )
        newer_key_id = _build_drift_evidence(
            cursor_key=cursor_key,
            source_table=source_table,
            dest_table=dest_table,
            pk_fields=[source_pk],
            committed_position=evidence["committed_position"],
            rows=[newer_source_row],
        )["keys"][0]["key_id"]
        current_evidence = (
            (mongo.get_job(job_id) or {}).get("extras") or {}
        ).get("cdc_drift_repair") or {}
        assert newer_key_id in {
            item.get("key_id") for item in current_evidence.get("keys", [])
        }, {
            "computed_key_id": newer_key_id,
            "evidence_key_ids": [
                item.get("key_id") for item in current_evidence.get("keys", [])
            ],
            "newer_source_row": newer_source_row,
            "source_pk": source_pk,
            "source_table": source_table,
            "cursor_key": cursor_key,
        }
        staged = lifecycle_tools._job_tool(
            "repair_cdc_drift",
            job_id=job_id,
            selector="",
            key_ids=[newer_key_id],
        )
        assert staged.success, staged.error
        resolved = client.post(
            "/api/v1/copilot/confirm",
            headers={"X-Workspace-Id": workspace_id},
            json={"ack_id": staged.output["ack_id"]},
        )
        assert resolved.status_code == 200, resolved.text
        second_repair = resolved.json()
        assert second_repair.get("verified") == 1, second_repair
        assert (second_repair.get("outcomes") or {}).get("repaired") == 1
        final_job = mongo.get_job(job_id) or {}
        final_evidence = (final_job.get("extras") or {}).get("cdc_drift_repair") or {}
        assert final_evidence.get("resolved") is True
        final_summary = final_evidence.get("last_repair") or {}
        assert final_summary.get("verified") == 1
        assert (final_summary.get("outcomes") or {}).get("repaired") == 1
        final_repair_view = sa_dest_watermark_view(
            resolved_dest_cfg, cursor_key, "postgresql"
        )
        assert (
            final_repair_view.apply_seq,
            final_repair_view.committed_lsn,
            final_repair_view.apply_checksum,
        ) == before_refusal["eos"][:3]
        assert final_repair_view.fence_epoch > first_repair_view.fence_epoch
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v, "_df_lsn" FROM public."{dest_table}" ORDER BY id'
                )
                fully_repaired_rows = cur.fetchall()
        assert [str(row[1]) for row in fully_repaired_rows] == streamed_values
        assert all(str(row[2] or "") == p_eos for row in fully_repaired_rows)

        result = engine.execute_tracked(request, job_id, resume=True)
        assert result.success, result.error or result.error_details
        updated_value = f"{route}-after-repair-{run_id}"
        changed_id = ids[0]
        if route == "oracle":
            with get_connection(**source_connect_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE {table_ref} SET V = :value WHERE ID = :id",
                        {"value": updated_value, "id": changed_id},
                    )
                conn.commit()
        else:
            force_scan_calls = []
            original_force_scan = SqlServerNativeCdc.force_cdc_scan

            def record_force_scan(reader):
                force_scan_calls.append(reader)
                return original_force_scan(reader)

            monkeypatch.setattr(
                SqlServerNativeCdc, "force_cdc_scan", record_force_scan
            )
            capture_job_may_be_stopped = True
            with source_reader._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("EXEC sys.sp_cdc_stop_job 'capture'")
                conn.commit()
            with source_reader._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE dbo.[{source_table}] SET v = %s WHERE id = %s",
                        (updated_value, changed_id),
                    )
                conn.commit()
        result = engine.execute_tracked(request, job_id, resume=True)
        assert result.success, result.error or result.error_details
        if route == "sqlserver":
            assert force_scan_calls
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v, "_df_lsn" FROM public."{dest_table}" ORDER BY id'
                )
                final_destination_rows = cur.fetchall()
        changed_destination = next(
            row for row in final_destination_rows if int(row[0]) == changed_id
        )
        assert str(changed_destination[1]) == updated_value
        assert compare_lsn(str(changed_destination[2]), p_eos) > 0

        if route == "oracle":
            with get_connection(**source_connect_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT ID, V FROM {table_ref} ORDER BY ID"
                    )
                    final_source_rows = cur.fetchall()
        else:
            with source_reader._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT id, v FROM dbo.[{source_table}] ORDER BY id"
                    )
                    final_source_rows = cur.fetchall()
        source_projection = [
            [int(row[0]), str(row[1])] for row in final_source_rows
        ]
        destination_projection = [
            [int(row[0]), str(row[1])] for row in final_destination_rows
        ]
        assert len(destination_projection) == len(source_projection) == 3
        source_md5 = hashlib.md5(
            json.dumps(source_projection, separators=(",", ":")).encode(),
            usedforsecurity=False,
        ).hexdigest()
        destination_md5 = hashlib.md5(
            json.dumps(destination_projection, separators=(",", ":")).encode(),
            usedforsecurity=False,
        ).hexdigest()
        assert destination_md5 == source_md5
        assert (
            (mongo.get_job(job_id) or {}).get("status") == "completed"
        )
    finally:
        try:
            if route == "sqlserver" and capture_job_may_be_stopped:
                since = sqlserver_server_now(source_reader)
                with source_reader._conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("EXEC sys.sp_cdc_start_job 'capture'")
                    conn.commit()
                wait_for_sqlserver_capture_job_healthy(source_reader, since=since)
        finally:
            if held_lease is not None:
                release_lease(
                    cursor_key,
                    holder_id=held_lease.holder_id,
                    generation=held_lease.generation,
                )
            try:
                if route == "oracle" and source_table_created:
                    with get_connection(**source_connect_cfg) as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                f'DROP TABLE "{source_schema}"."{source_table}" PURGE'
                            )
                        conn.commit()
                elif route == "sqlserver" and source_table_created:
                    with source_reader._conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                f"""
                                EXEC sys.sp_cdc_disable_table
                                    @source_schema = N'dbo',
                                    @source_name = N'{source_table}',
                                    @capture_instance = N'all';
                                DROP TABLE IF EXISTS dbo.[{source_table}];
                                """
                            )
                        conn.commit()
            except Exception:
                pass
            try:
                with pg_connection(**pg_cfg) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'DROP TABLE IF EXISTS public."{dest_table}"'
                        )
                        if cursor_key:
                            cur.execute(
                                f'DELETE FROM "{WATERMARK_TABLE}" WHERE stream_key = %s',
                                (cursor_key,),
                            )
                    conn.commit()
            except Exception:
                pass
            if cursor_key:
                clear_watermark(cursor_key)
            if source_connection is not None:
                try:
                    source_connection.close()
                except Exception:
                    pass
            if source_reader is not None:
                try:
                    source_reader.close()
                except Exception:
                    pass
            cdc_lease_store.reset_store(original_lease_store)


@pytest.mark.parametrize(
    "route",
    [
        pytest.param(
            "oracle",
            marks=pytest.mark.skipif(
                not _oracle_logminer_ready() or not _pg_ready(),
                reason="Oracle LogMiner and live PostgreSQL environment are required",
            ),
            id="oracle-logminer-to-postgresql",
        ),
        pytest.param(
            "sqlserver",
            marks=pytest.mark.skipif(
                (getenv_brand("SQLSERVER_ENABLE", "") or "").strip().lower()
                not in {"1", "true", "yes", "on"}
                or not _sqlserver_native_ready()
                or not _pg_ready(),
                reason="SQL Server native CDC and live PostgreSQL environment are required",
            ),
            id="sqlserver-cdc-to-postgresql",
        ),
    ],
)
def test_snapshot_insert_resume_eos_live(
    route: str, monkeypatch
) -> None:
    mongo = get_mongodb_service()
    if not isinstance(mongo, MemoryMongoDBService):
        pytest.skip("The live snapshot proof requires DATAFLOW_JOB_STORE=memory")

    run_id = uuid.uuid4().hex[:10]
    workspace_id = f"workspace-eossnap-{route}-{run_id}"
    job_id = f"eossnap-{route}-{run_id}"
    pg_cfg = _pg_cfg()
    source_table = (
        f"EOSSNAP_{run_id.upper()}" if route == "oracle" else f"eossnap_{run_id}"
    )
    dest_table = f"eossnap_live_{run_id}"
    if route == "oracle":
        raw_source_cfg = _oracle_cfg()
        source_schema = str(raw_source_cfg["schema"])
        source_pk, source_value = "ID", "V"
        source_endpoint = EndpointConfig(
            kind="database",
            format="oracle",
            host=str(raw_source_cfg["host"]),
            port=int(raw_source_cfg["port"]),
            database=str(raw_source_cfg["database"]),
            schema=source_schema,
            table=source_table,
            username=str(raw_source_cfg["username"]),
            password=str(raw_source_cfg["password"]),
            connection_string=str(raw_source_cfg.get("connection_string") or ""),
            ssl=bool(raw_source_cfg.get("ssl", False)),
            extra={
                key: value
                for key, value in raw_source_cfg.items()
                if key
                not in {
                    "db_type",
                    "host",
                    "port",
                    "database",
                    "schema",
                    "username",
                    "password",
                    "connection_string",
                    "ssl",
                }
            },
        )
        source_connect_cfg = dict(raw_source_cfg)
        source_connect_cfg["type"] = "oracle"
    else:
        raw_source_cfg = dict(ENGINES["sqlserver"])
        source_schema = "dbo"
        source_pk, source_value = "id", "v"
        source_connect_cfg = {
            "host": raw_source_cfg["host"],
            "port": raw_source_cfg["port"],
            "database": raw_source_cfg["database"],
            "username": raw_source_cfg["username"],
            "password": raw_source_cfg["password"],
            "connection_string": "",
            "ssl": False,
            "db_type": "sqlserver",
        }
        source_endpoint = EndpointConfig(
            kind="database",
            format="sqlserver",
            host=str(raw_source_cfg["host"]),
            port=int(raw_source_cfg["port"]),
            database=str(raw_source_cfg["database"]),
            schema=source_schema,
            table=source_table,
            username=str(raw_source_cfg["username"]),
            password=str(raw_source_cfg["password"]),
            extra={"cdc_row_filter": "all update old"},
        )
    destination_endpoint = EndpointConfig(
        kind="database",
        format="postgresql",
        host=str(pg_cfg["host"]),
        port=int(pg_cfg["port"]),
        database=str(pg_cfg["database"]),
        schema="public",
        table=dest_table,
        username=str(pg_cfg["username"]),
        password=str(pg_cfg["password"]),
    )
    mapping = [
        {"source": source_pk, "target": "id", "confidence": 1.0},
        {"source": source_value, "target": "v", "confidence": 1.0},
    ]
    request = TransferRequest(
        source=source_endpoint,
        destination=destination_endpoint,
        mappings=mapping,
        column_types={source_pk: "INTEGER", source_value: "VARCHAR(200)"},
        skip_preflight=True,
        sync_mode="cdc",
        validation_mode="balanced",
        workspace_id=workspace_id,
        stream_contracts=[
            {
                "name": source_table,
                "selected": True,
                "snapshot_mode": "initial",
                "primary_key": source_pk,
                "cursor_semantics": "cdc_position",
                "sync_mode": "cdc",
            }
        ],
        delivery_guarantee="exactly_once",
    )
    request.delivery_pinned = True
    mongo.create_transfer_job(
        {
            "_id": job_id,
            "workspace_id": workspace_id,
            "status": "pending",
            "transfer_request": transfer_request_to_dict(request),
        }
    )
    monkeypatch.delenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", raising=False)
    original_lease_store = cdc_lease_store.get_store()
    redis_url = (
        getenv_brand("CDC_LEASE_REDIS_URL")
        or getenv_brand("REDIS_URL")
        or "redis://127.0.0.1:6379/15"
    )
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_BACKEND", "redis")
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_REDIS_URL", redis_url)
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_REDIS_PREFIX", f"eossnap:{run_id}:")
    assert cdc_lease_store.configure_store(
        backend="redis", url=redis_url, prefix=f"eossnap:{run_id}:"
    ).name == "redis"

    ids = list(range(1, 101))
    inserted_ids = list(range(101, 151))
    values = {row_id: f"value-{row_id}" for row_id in ids + inserted_ids}
    source_reader = None
    source_table_created = False
    cursor_key = ""
    try:
        if route == "oracle":
            table_ref = f'"{source_schema}"."{source_table}"'
            with get_connection(**source_connect_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"CREATE TABLE {table_ref} "
                        "(ID NUMBER(10) PRIMARY KEY, V VARCHAR2(200))"
                    )
                    source_table_created = True
                    cur.execute(
                        f"ALTER TABLE {table_ref} "
                        "ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                    )
                    cur.executemany(
                        f"INSERT INTO {table_ref} (ID, V) VALUES (:id, :value)",
                        [{"id": row_id, "value": values[row_id]} for row_id in ids],
                    )
                conn.commit()
        else:
            source_reader = SqlServerNativeCdc(
                source_connect_cfg,
                table=source_table,
                primary_key=source_pk,
                schema=source_schema,
                cursor_key=f"{job_id}:source",
                row_filter="all update old",
            )
            with source_reader._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"CREATE TABLE dbo.[{source_table}] "
                        "(id INT NOT NULL PRIMARY KEY, v NVARCHAR(200) NOT NULL)"
                    )
                    source_table_created = True
                    cur.execute(
                        """
                        IF NOT EXISTS (
                            SELECT 1 FROM sys.databases
                            WHERE name = DB_NAME() AND is_cdc_enabled = 1
                        )
                            EXEC sys.sp_cdc_enable_db;
                        """
                    )
                    cur.execute(
                        f"""
                        EXEC sys.sp_cdc_enable_table
                            @source_schema = N'dbo',
                            @source_name = N'{source_table}',
                            @role_name = NULL,
                            @supports_net_changes = 0;
                        """
                    )
                    cur.executemany(
                        f"INSERT INTO dbo.[{source_table}] (id, v) VALUES (%s, %s)",
                        [(row_id, values[row_id]) for row_id in ids],
                    )
                conn.commit()

        resolved_source_cfg = resolve_connector_config(
            source_endpoint, workspace_id=workspace_id
        )
        resolved_dest_cfg = resolve_connector_config(
            destination_endpoint, workspace_id=workspace_id
        )
        cursor_key = _single_stream_cursor_key(
            src_type=resolve_driver_type(source_endpoint.format),
            src_cfg=resolved_source_cfg,
            table_name=source_table,
            dest_type=resolve_driver_type(destination_endpoint.format),
            dest_cfg=resolved_dest_cfg,
            dest_table=dest_table,
            stream_name=source_table,
        )
        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, job_id, resume=False)
        assert result.success, result.error or result.error_details
        assert result.destination_summary.get("cdc_delivery") == "exactly_once"

        if route == "oracle":
            with get_connection(**source_connect_cfg) as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        f"INSERT INTO {table_ref} (ID, V) VALUES (:id, :value)",
                        [
                            {"id": row_id, "value": values[row_id]}
                            for row_id in inserted_ids
                        ],
                    )
                conn.commit()
        else:
            with source_reader._conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        f"INSERT INTO dbo.[{source_table}] (id, v) VALUES (%s, %s)",
                        [(row_id, values[row_id]) for row_id in inserted_ids],
                    )
                conn.commit()
            source_reader.force_cdc_scan()
            initial_watermark = (
                result.destination_summary.get("cdc") or {}
            ).get("watermark")
            handoff_lsn = decode_mssql_cdc_token(initial_watermark)["lsn"]
            assert handoff_lsn, result.destination_summary
            _wait_for_cdc_events(
                source_reader,
                handoff_lsn,
                {(2, row_id) for row_id in inserted_ids},
            )

        result = engine.execute_tracked(request, job_id, resume=True)
        assert result.success, result.error or result.error_details
        assert result.destination_summary.get("cdc_delivery") == "exactly_once"
        with pg_connection(**pg_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT id, v FROM public."{dest_table}" ORDER BY id')
                destination_rows = cur.fetchall()
        if route == "oracle":
            with get_connection(**source_connect_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT ID, V FROM {table_ref} ORDER BY ID")
                    source_rows = cur.fetchall()
        else:
            with source_reader._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT id, v FROM dbo.[{source_table}] ORDER BY id"
                    )
                    source_rows = cur.fetchall()
        source_projection = [[int(row[0]), str(row[1])] for row in source_rows]
        destination_projection = [
            [int(row[0]), str(row[1])] for row in destination_rows
        ]
        assert len(source_projection) == len(destination_projection) == 150
        source_md5 = hashlib.md5(
            json.dumps(source_projection, separators=(",", ":")).encode(),
            usedforsecurity=False,
        ).hexdigest()
        destination_md5 = hashlib.md5(
            json.dumps(destination_projection, separators=(",", ":")).encode(),
            usedforsecurity=False,
        ).hexdigest()
        assert destination_md5 == source_md5
    finally:
        try:
            if route == "oracle" and source_table_created:
                with get_connection(**source_connect_cfg) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'DROP TABLE "{source_schema}"."{source_table}" PURGE'
                        )
                    conn.commit()
            elif route == "sqlserver" and source_table_created:
                with source_reader._conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            EXEC sys.sp_cdc_disable_table
                                @source_schema = N'dbo',
                                @source_name = N'{source_table}',
                                @capture_instance = N'all';
                            DROP TABLE IF EXISTS dbo.[{source_table}];
                            """
                        )
                    conn.commit()
        except Exception:
            pass
        try:
            with pg_connection(**pg_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(f'DROP TABLE IF EXISTS public."{dest_table}"')
                    if cursor_key:
                        cur.execute(
                            f'DELETE FROM "{WATERMARK_TABLE}" WHERE stream_key = %s',
                            (cursor_key,),
                        )
                conn.commit()
        except Exception:
            pass
        if cursor_key:
            clear_watermark(cursor_key)
        if source_reader is not None:
            source_reader.close()
        cdc_lease_store.reset_store(original_lease_store)
