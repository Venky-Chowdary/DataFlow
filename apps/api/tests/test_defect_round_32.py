"""Regressions for the open CDC, queue, and connector defects."""

from __future__ import annotations

from datetime import datetime

import pytest

from connectors.aws_common import resolve_endpoint_url
from connectors.sql_temporal import coerce_sql_temporal, input_has_timezone
from connectors.writer_common import normalize_temporal_cells
from services.cdc_catchup import release_finished_cdc_slot
from services.cdc_exactly_once import DestWmView, ExactlyOnceRouteError, plan_open_session
from services.cdc_value_digest import compare_row_sets
from services.reconciliation import reconcile
from services.reconcile_coverage import CDC_SOURCE_IMAGE_VALUES


def test_second_cdc_open_keeps_the_dest_fence() -> None:
    dest = DestWmView(committed_lsn="0/40", fence_epoch=1)
    opened = plan_open_session(dest=dest, incoming_fence=0, job_resume={"lsn": "0/40"})
    assert opened.fence_epoch == 1
    assert opened.fence_raised is False
    from services.cdc_exactly_once import assert_writer_fence

    with pytest.raises(ExactlyOnceRouteError):
        assert_writer_fence(0, 1)


def test_pgoutput_short_offset_binds_as_mysql_naive_utc() -> None:
    assert input_has_timezone("2024-06-01 12:00:00+00") is True
    assert input_has_timezone("2024-06-01 12:00:00+05") is True
    assert input_has_timezone("2024-06-01") is False
    utc = coerce_sql_temporal(
        "2024-06-01 12:00:00+00", "TIMESTAMPTZ", engine="mysql"
    )
    shifted = coerce_sql_temporal(
        "2024-06-01 12:00:00+05", "TIMESTAMP(6)", engine="mysql"
    )
    assert utc == datetime(2024, 6, 1, 12, 0, tzinfo=None)
    assert utc.tzinfo is None
    assert shifted == datetime(2024, 6, 1, 7, 0)
    assert shifted.tzinfo is None
    rows = normalize_temporal_cells(
        [("2024-06-01 12:00:00+00",)],
        ["TIMESTAMPTZ"],
        engine="mysql",
    )
    assert isinstance(rows[0][0], datetime)
    assert rows[0][0].tzinfo is None
    assert rows[0][0].hour == 12


def test_minio_host_and_port_become_an_endpoint() -> None:
    url = resolve_endpoint_url({"host": "minio", "port": 9000})
    assert url == "http://minio:9000"
    assert resolve_endpoint_url({"host": "us-east-1", "port": 443}) == ""
    assert resolve_endpoint_url({"host": "us-east-1"}) == ""


def test_failed_one_shot_drops_the_slot_after_the_worker_closes(monkeypatch) -> None:
    dropped: list[str] = []

    def _drop(_cfg, *, slot_name, publication_name):
        dropped.append(slot_name)
        return {"slot": "dropped", "slot_name": slot_name, "publication": "dropped"}

    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *a, **k: False
    )
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture", _drop
    )
    monkeypatch.setattr(
        "services.sync_cursor.clear_watermark", lambda _key: {"cleared": True}
    )
    out = release_finished_cdc_slot(
        {
            "cdc_slot_name": "df_orders_slot",
            "cursor_key": "pg:qa:orders→mysql:qa:orders:stream",
        },
        reason="failed",
        source_cfg={"type": "postgresql", "database": "qa"},
        job_id="job-1292",
        worker_closed=True,
    )
    assert out["released"] is True
    assert dropped == ["df_orders_slot"]


def test_cdc_value_proof_fails_when_an_update_is_missing() -> None:
    columns = ["id", "qty"]
    source = [{"id": 1, "qty": 3}, {"id": 2, "qty": 1}]
    dest = [{"id": 1, "qty": 1}, {"id": 2, "qty": 1}, {"id": 9, "qty": 9}]
    proof = compare_row_sets(source, dest, columns, engine="mysql")
    assert proof.missing == 1
    assert proof.matched is False
    report = reconcile(
        source_rows=2,
        target_rows=3,
        source_checksum=proof.source_digest,
        target_checksum=proof.dest_digest,
        checksum_scope=CDC_SOURCE_IMAGE_VALUES,
    )
    assert report.passed is False
    assert report.assurance_level == CDC_SOURCE_IMAGE_VALUES
    assert "full_checksum" not in (report.assurance_level or "")


def test_cdc_value_proof_allows_dest_extras() -> None:
    columns = ["id", "qty"]
    source = [{"id": 1, "qty": 3}]
    dest = [{"id": 1, "qty": 3}, {"id": 9, "qty": 1}]
    proof = compare_row_sets(source, dest, columns, engine="postgresql")
    assert proof.missing == 0
    assert proof.matched is True
    report = reconcile(
        source_rows=1,
        target_rows=2,
        source_checksum=proof.source_digest,
        target_checksum=proof.dest_digest,
        checksum_scope=CDC_SOURCE_IMAGE_VALUES,
    )
    assert report.passed is True
    assert report.checksum_match is True
    assert report.assurance_level == CDC_SOURCE_IMAGE_VALUES


def test_bigquery_draft_requires_the_service_account_json() -> None:
    from src.ai.copilot.connector_create import build_connector_draft, draft_is_complete

    draft = build_connector_draft(
        "",
        {
            "type": "bigquery",
            "database": "qa-project",
            "service_account": '{"type":"service_account","project_id":"qa-project"}',
        },
    )
    ok, missing = draft_is_complete(draft)
    assert ok, missing
    assert draft["service_account"].startswith("{")
    bare = build_connector_draft("", {"type": "bigquery", "database": "qa-project"})
    ok, missing = draft_is_complete(bare)
    assert ok is False
    assert "service_account" in missing


def test_object_store_plan_keeps_profiled_decimal() -> None:
    from src.ai.copilot.schema_tools import _normalize_columns

    columns = _normalize_columns(
        {
            "columns": ["id", "price"],
            "schema": "exports",
            "column_types": {"id": "BIGINT", "price": "DECIMAL(12,2)"},
        }
    )
    by_name = {c["name"]: c["inferred_type"] for c in columns}
    assert by_name["price"] == "DECIMAL(12,2)"
    assert by_name["id"] == "BIGINT"


def test_pgvector_create_new_keeps_numeric() -> None:
    from services.type_system import ddl_type

    assert ddl_type("pgvector", "DECIMAL(10,2)") == "NUMERIC(10,2)"
    assert ddl_type("pgvector", "INTEGER") == "BIGINT"
    assert "TEXT" not in ddl_type("pgvector", "BOOLEAN")


def test_timescaledb_privilege_probe_uses_postgres() -> None:
    from services.destination_privilege_probe import _normalize_engine

    assert _normalize_engine("timescaledb") == "postgresql"
    assert _normalize_engine("timescale") == "postgresql"


def test_risk_acceptance_signs_only_a_lossy_mapping() -> None:
    from services.migration_risk_contract import mapping_has_clearing_risk_contract
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    mappings = [
        {
            "source": "price",
            "target": "price",
            "source_type": "DECIMAL(12,2)",
            "target_type": "VARCHAR(8)",
            "fidelity": "lossy_cast",
        },
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "fidelity": "exact",
        },
    ]
    with pytest.raises(ValueError, match="Nothing was signed"):
        _sign_required_risk_contracts(
            mappings,
            {"approved_by": "qa"},
            table="orders",
        )
    signed = _sign_required_risk_contracts(
        mappings,
        {
            "approved_by": "qa.operator",
            "reason": "Redis string sink is the accepted carrier",
            "execution_policy": "QUARANTINE_ROW",
        },
        table="orders",
    )
    assert mapping_has_clearing_risk_contract(signed[0]) is True
    assert signed[1].get("risk_contract") is None


def test_short_offset_classifies_as_timestamptz() -> None:
    from services.schema_inference import _classify_value

    assert _classify_value("2024-06-01 12:00:00+00") == "TIMESTAMPTZ"
    assert _classify_value("2024-06-01 12:00:00+05") == "TIMESTAMPTZ"


def test_csv_wire_keeps_timezone_offset() -> None:
    from datetime import timezone

    from connectors.sql_temporal import format_wire_value

    aware = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
    wire = format_wire_value(aware, "TIMESTAMPTZ", engine="postgresql")
    assert wire is not None
    assert "+00:00" in wire


def test_overwrite_heap_does_not_block_duplicate_keys() -> None:
    from services.data_integrity import _check_duplicate_keys

    mappings = [{"source": "id", "target": "id"}]
    rows = [{"id": 1}, {"id": 1}]
    heap = _check_duplicate_keys(
        mappings,
        rows,
        sync_mode="full_refresh_overwrite",
        primary_key="id",
        destination_pk_columns=[],
    )
    assert heap["blocks_transfer"] is False
    keyed = _check_duplicate_keys(
        mappings,
        rows,
        sync_mode="full_refresh_overwrite",
        primary_key="id",
        destination_pk_columns=["id"],
    )
    assert keyed["blocks_transfer"] is True


def test_legacy_ssh_is_registered_only_for_the_last_attempt() -> None:
    """Paramiko 5 dropped group14-sha1 and ssh-rsa. The last attempt adds them.

    The class preferred lists stay modern, group1-sha1 is never offered, and
    a fresh transport does not advertise the legacy names.
    """
    import socket

    import paramiko

    from connectors.sftp_common import _enable_legacy_ssh, _register_legacy_ssh

    preferred_kex = tuple(paramiko.Transport._preferred_kex)
    preferred_keys = tuple(paramiko.Transport._preferred_keys)
    _register_legacy_ssh(paramiko)

    assert "diffie-hellman-group14-sha1" in paramiko.Transport._kex_info
    assert "diffie-hellman-group1-sha1" not in paramiko.Transport._kex_info
    assert paramiko.Transport._key_info.get("ssh-rsa") is paramiko.rsakey.RSAKey
    assert "ssh-rsa" in paramiko.rsakey.RSAKey.HASHES
    assert tuple(paramiko.Transport._preferred_kex) == preferred_kex
    assert tuple(paramiko.Transport._preferred_keys) == preferred_keys
    assert "diffie-hellman-group14-sha1" not in preferred_kex
    assert "ssh-rsa" not in preferred_keys

    left, right = socket.socketpair()
    modern = legacy = None
    try:
        modern = paramiko.Transport(left)
        legacy = paramiko.Transport(right)
        _enable_legacy_ssh(legacy)
        assert "diffie-hellman-group14-sha1" not in modern.get_security_options().kex
        assert "ssh-rsa" not in modern.get_security_options().key_types
        assert "diffie-hellman-group14-sha1" in legacy.get_security_options().kex
        assert "ssh-rsa" in legacy.get_security_options().key_types
        assert "diffie-hellman-group1-sha1" not in legacy.get_security_options().kex
        assert tuple(paramiko.Transport._preferred_kex) == preferred_kex
    finally:
        for transport in (modern, legacy):
            if transport is not None:
                transport.close()
        left.close()
        right.close()


def test_adls_emulator_pins_version_and_real_azure_does_not(monkeypatch) -> None:
    from connectors.adls import test_adls
    from connectors.adls_common import (
        _emulator_endpoint,
        api_version_rejected,
        blob_service_client,
    )

    assert _emulator_endpoint({"host": "127.0.0.1", "port": 10000}) is True
    assert _emulator_endpoint(
        {"connection_string": "AccountName=devstoreaccount1;BlobEndpoint=http://azurite:10000/devstoreaccount1"}
    ) is True
    assert _emulator_endpoint(
        {"host": "prodacct.blob.core.windows.net", "port": 443, "username": "prodacct"}
    ) is False
    assert api_version_rejected(RuntimeError("The specified API version is invalid x-ms-version"))
    assert api_version_rejected(RuntimeError("AuthenticationFailed account key")) is False

    captured: list[dict] = []

    class _Client:
        def __init__(self, account_url, credential=None, **kwargs):
            captured.append({"url": account_url, **kwargs})

        @staticmethod
        def from_connection_string(_cs, **kwargs):
            captured.append(dict(kwargs))
            return object()

    monkeypatch.setattr("azure.storage.blob.BlobServiceClient", _Client)
    blob_service_client(
        {
            "connection_string": (
                "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
                "AccountKey=abc;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
            )
        }
    )
    blob_service_client(
        {
            "username": "prodacct",
            "password": "key",
            "host": "prodacct.blob.core.windows.net",
            "port": 443,
        }
    )
    blob_service_client(
        {
            "username": "prodacct",
            "password": "key",
            "host": "prodacct.blob.core.windows.net",
            "api_version": "2025-01-05",
        }
    )
    assert captured[0]["api_version"] == "2021-12-02"
    assert "api_version" not in captured[1]
    assert captured[2]["api_version"] == "2025-01-05"

    probes: list[str | None] = []

    class _Probe:
        def list_containers(self):
            version = probes[-1]
            if version != "2021-12-02":
                raise RuntimeError("x-ms-version is not supported")
            return []

    def _factory(cfg):
        probes.append(cfg.get("api_version"))
        return _Probe()

    monkeypatch.setattr("connectors.adls.blob_service_client", _factory)
    result = test_adls(
        host="tunnel.example",
        port=443,
        database="",
        username="devstoreaccount1",
        password="key",
        schema="",
        connection_string="",
        ssl=False,
    )
    assert result.ok is True
    assert probes == [None, "2021-12-02"]


def test_adls_container_listing_falls_back_to_public_protocol_with_limit() -> None:
    from connectors.adls_common import list_service_containers

    consumed: list[str] = []

    class _ProtocolClient:
        def list_containers(self):
            for name in ("first", "second", "third"):
                consumed.append(name)
                yield name

    result = list_service_containers(_ProtocolClient(), maxresults=2)

    assert result == ["first", "second"]
    assert consumed == ["first", "second"]


def test_adls_container_listing_uses_generated_segment_without_include() -> None:
    from connectors.adls_common import list_service_containers

    calls: list[dict] = []

    class _Service:
        def list_containers_segment(self, *, include, maxresults):
            calls.append({"include": include, "maxresults": maxresults})
            return "page"

    class _InternalClient:
        service = _Service()

    class _SdkClient:
        _client = _InternalClient()

    assert list_service_containers(_SdkClient(), maxresults=3) == "page"
    assert calls == [{"include": None, "maxresults": 3}]


def test_object_store_plan_types_are_not_authoritative() -> None:
    from src.ai.copilot.transfer_tools import _plan_source_types_authoritative

    assert (
        _plan_source_types_authoritative(
            {"kind": "object_store", "type": "s3"},
            {"db_type": "s3"},
            None,
        )
        is False
    )
    assert (
        _plan_source_types_authoritative(
            {"kind": "database", "type": "postgresql"},
            {"db_type": "postgresql"},
            None,
        )
        is True
    )
    assert (
        _plan_source_types_authoritative(
            {"kind": "database", "type": "postgresql"},
            {"db_type": "postgresql"},
            {"name": "extract_orders"},
        )
        is False
    )


def test_pgvector_create_adds_numeric_column_and_skips_text() -> None:
    from psycopg2 import sql

    from connectors.pgvector_writer import (
        _exec_schema_table,
        _pgvector_extra_cells,
        _pgvector_typed_extras,
    )

    extras = _pgvector_typed_extras(
        [
            {"source": "price", "target": "price", "target_type": "DECIMAL(10,2)"},
            {"source": "title", "target": "title", "target_type": "TEXT"},
            {"source": "payload", "target": "payload", "target_type": "JSON"},
            {"source": "id", "target": "id", "target_type": "BIGINT"},
            {"source": "flag", "target": "active", "target_type": "BOOLEAN"},
        ],
        {},
    )
    assert extras == [("price", "NUMERIC(10,2)"), ("active", "BOOLEAN")]
    assert _pgvector_extra_cells({"Price": "10.50", "ACTIVE": False}, extras) == [
        "10.50",
        False,
    ]

    class _Cur:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        def execute(self, query, params=None) -> None:
            self.calls.append((query, params))

    cur = _Cur()
    _exec_schema_table(cur, "public", "chunks", 32, extras)
    alters = [
        call
        for call in cur.calls
        if isinstance(call[0], sql.Composed)
        and any(
            getattr(part, "_wrapped", None) == " ADD COLUMN IF NOT EXISTS "
            for part in call[0].seq
        )
    ]
    assert len(alters) == 2
    tokens = [
        part._wrapped
        for _query, _params in alters
        for part in _query.seq
        if isinstance(part, sql.SQL) and part._wrapped not in {" ", " ADD COLUMN IF NOT EXISTS "}
    ]
    assert "NUMERIC(10,2)" in tokens
    assert "BOOLEAN" in tokens


def test_in_batch_upsert_collapse_is_not_a_rejected_row() -> None:
    """Two images of one key in one CDC/upsert bundle write the latest image.

    The earlier image used to fall out of the accepted count and show up as
    one quarantined duplicate on a table whose keys are unique.
    """
    from connectors.sql_write_materialize import SqlMappedBundle, finish_sql_mapped_bundle
    from connectors.writer_common import _rejected_row_count

    bundle = SqlMappedBundle(
        start=0,
        mapped_rows=[("1", "old"), ("1", "new")],
        transform_errors=[],
        rejected_details=[],
        accepted_source_rows=[1, 2],
        headers=["id", "v"],
        source_row_count=2,
    )
    finished = finish_sql_mapped_bundle(
        bundle,
        target_cols=["id", "v"],
        target_types=["TEXT", "TEXT"],
        policy="quarantine",
        dialect_label="PostgreSQL",
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert finished.collapsed_duplicate_rows == 1
    assert len(finished.dense_rows) == 1
    assert finished.dense_rows[0][1] == "new"
    assert finished.rejected_details == []
    assert (
        _rejected_row_count(
            [["1", "old"], ["1", "new"]],
            [()] * len(finished.dense_rows),
            finished.rejected_details,
            "quarantine",
            source_row_count=2,
            collapsed_duplicates=finished.collapsed_duplicate_rows,
        )
        == 0
    )
    insert = finish_sql_mapped_bundle(
        bundle,
        target_cols=["id", "v"],
        target_types=["TEXT", "TEXT"],
        policy="quarantine",
        dialect_label="PostgreSQL",
        write_mode="insert",
        conflict_columns=["id"],
    )
    assert insert.collapsed_duplicate_rows == 0
    assert len(insert.dense_rows) == 2


def test_missing_identity_column_does_not_audit_another_column(monkeypatch) -> None:
    from services.data_quality import run_integrity_audit

    monkeypatch.setattr("services.column_case.header_index", lambda *_a, **_k: None)
    report = run_integrity_audit(
        headers=["status", "id"],
        rows=[["open", "1"], ["open", "2"]],
        primary_key="id",
        sync_mode="upsert",
        dest_kind="postgresql",
    )
    assert report.issues == []
    assert any("not in this batch" in w for w in report.warnings)


def test_sqlserver_nvarchar_max_holds_unicode_varchar_does_not() -> None:
    from services.encoding_capacity import classify_capacity, quarantine_unfit_encoding
    from services.schema_introspect import _sqlserver_to_logical

    national = _sqlserver_to_logical("nvarchar")
    unbounded = _sqlserver_to_logical("nvarchar(max)")
    assert national == "NVARCHAR"
    assert unbounded == "NVARCHAR(MAX)"
    assert classify_capacity("sqlserver", national).form == "utf16"
    assert classify_capacity("sqlserver", unbounded).form == "utf16"
    assert classify_capacity("sqlserver", "VARCHAR(MAX)").form == "cp1252"
    names = [("山田太郎",), ("Łukasz",)]
    kept_national = quarantine_unfit_encoding(
        names, ["name"], [unbounded], [], "quarantine", dest_db="sqlserver"
    )
    assert kept_national == names
    rejected: list[dict] = []
    kept_varchar = quarantine_unfit_encoding(
        names, ["name"], ["VARCHAR(100)"], rejected, "quarantine", dest_db="sqlserver"
    )
    assert kept_varchar == []
    assert any("U+90CE" in str(d.get("reason")) for d in rejected)
    assert any("U+0141" in str(d.get("reason")) for d in rejected)


def test_pg_timestamptz_mysql_create_new_is_datetime6_and_binds() -> None:
    from connectors.cdc_eos_sa import _coerce_eos_row
    from connectors.sql_temporal import coerce_sql_temporal
    from connectors.writer_common import quarantine_unfit_temporals
    from services.source_engine_scope import bind_source_engine
    from services.type_system import (
        ddl_type,
        is_lossy_coercion,
        is_timezone_polarity_loss,
        materialize_dest_ddl,
    )

    wire = "2026-10-08 00:26:39.458238+00"
    early = "1900-01-01 00:00:00+00"
    late = "2100-01-01 00:00:00+00"
    with bind_source_engine("postgresql"):
        assert ddl_type("mysql", "TIMESTAMPTZ") == "DATETIME(6)"
    with bind_source_engine("mysql"):
        assert ddl_type("mysql", "TIMESTAMPTZ") == "TIMESTAMP(6)"
    assert is_timezone_polarity_loss(
        "TIMESTAMPTZ", "DATETIME(6)", dest_db="mysql"
    ) is False
    assert is_lossy_coercion("TIMESTAMPTZ", "DATETIME(6)", dest_db="mysql") is False
    for raw in (wire, early, late):
        bound = coerce_sql_temporal(raw, "DATETIME(6)", engine="mysql")
        assert isinstance(bound, datetime) and bound.tzinfo is None
    with pytest.raises(ValueError):
        coerce_sql_temporal(early, "TIMESTAMP(6)", engine="mysql")
    held = quarantine_unfit_temporals(
        [(wire,), (early,)],
        ["updated_at"],
        ["DATETIME(6)"],
        [],
        "quarantine",
        dest_db="mysql",
    )
    assert len(held) == 2
    row = _coerce_eos_row(
        {"updated_at": wire, "note": wire},
        {"updated_at": "TIMESTAMPTZ", "note": "TEXT"},
        "mysql",
    )
    assert row["updated_at"] == datetime(2026, 10, 8, 0, 26, 39, 458238)
    assert row["updated_at"].tzinfo is None
    assert row["note"] == wire
    assert materialize_dest_ddl("mysql", "TEXT").upper() in {"TEXT", "LONGTEXT", "MEDIUMTEXT"}
