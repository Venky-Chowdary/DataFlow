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
