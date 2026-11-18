"""D396, D393, D320, D301, D397.

Unit proofs. A passing test is not a live retest of the original QA pair.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest


def test_mysql_bit8_binds_the_integer_not_the_digit_string() -> None:
    """PostgreSQL BIT(8) '10101010' is eight characters. MySQL BIT(8) is one byte."""
    from connectors.sql_bind import normalize_sql_bind_value

    assert normalize_sql_bind_value("10101010", "BIT(8)", engine="mysql") == 0b10101010
    assert normalize_sql_bind_value("1010", "BIT(4)", engine="mariadb") == 0b1010
    assert normalize_sql_bind_value("1010", "BIT(4)", engine="postgresql") == "1010"
    assert normalize_sql_bind_value(None, "BIT(8)", engine="mysql") is None
    assert normalize_sql_bind_value("10101010", "BIT VARYING(8)", engine="postgresql") == "10101010"
    with pytest.raises(ValueError):
        normalize_sql_bind_value("hello", "BIT(8)", engine="mysql")


def test_elasticsearch_keyless_overwrite_refuses_before_any_client() -> None:
    from connectors.elasticsearch_writer import write_mapped_rows

    def boom(*_a, **_k):
        raise AssertionError("Elasticsearch client must not open when there is no document id")

    with patch("connectors.elasticsearch_writer._client", side_effect=boom):
        result = write_mapped_rows(
            host="localhost",
            port=9200,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["order_id", "sku"],
            data_rows=[["o-1", "sku-9"]],
            mappings=[
                {"source": "order_id", "target": "order_id"},
                {"source": "sku", "target": "sku"},
            ],
            column_types={"order_id": "TEXT", "sku": "TEXT"},
            create_table=True,
            conflict_columns=[],
            error_policy="fail",
        )
    assert result.ok is False
    assert result.rows_written == 0
    assert "lack document identity" in (result.error or "")
    assert "Advanced" in (result.error or "")


def test_elasticsearch_preflight_refuses_a_keyless_overwrite() -> None:
    from preflight.gates import gate_g6_target_ddl
    from preflight.models import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        PreflightContext,
        SourceConfig,
        TransferPlan,
    )

    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            columns=[ColumnSchema(name="order_id", inferred_type="TEXT")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="elasticsearch",
            connected=True,
            can_write=True,
            can_create_table=True,
            table_exists=False,
        ),
        mappings=[ColumnMapping(source="order_id", target="order_id", confidence=0.99)],
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    result = gate_g6_target_ddl(PreflightContext(plan=plan))
    assert result.status.value == "block", result
    assert "lack document identity" in result.message
    assert result.details.get("upsert_key") == ""


def test_elasticsearch_preflight_accepts_a_mapped_id() -> None:
    from preflight.gates import gate_g6_target_ddl
    from preflight.models import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        PreflightContext,
        SourceConfig,
        TransferPlan,
    )

    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            columns=[ColumnSchema(name="id", inferred_type="TEXT")],
            row_count_estimate=1,
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="elasticsearch",
            connected=True,
            can_write=True,
            can_create_table=True,
            table_exists=False,
        ),
        mappings=[ColumnMapping(source="id", target="id", confidence=0.99)],
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        ddl_compatible=True,
    )
    result = gate_g6_target_ddl(PreflightContext(plan=plan))
    assert result.status.value != "block" or "lack document identity" not in result.message


def test_mongo_json_redact_and_hash_are_documents_other_text_refuses() -> None:
    from connectors.mongodb_writer import bind_mongo_json_document

    assert bind_mongo_json_document("<redacted>") == {"_datawrap_redacted": "<redacted>"}
    digest = "553cd7" + ("ab" * 13)
    assert len(digest) == 32
    assert bind_mongo_json_document(digest) == {"_datawrap_hash": digest}
    assert bind_mongo_json_document('{"a": 1}') == {"a": 1}
    with pytest.raises(ValueError, match="refuse silent string invent"):
        bind_mongo_json_document("not json")


def test_keyless_incremental_append_of_a_changed_key_is_not_a_block(tmp_path: Path, monkeypatch) -> None:
    """A heap plus a cursor delta stores one new version. It does not fail the run."""
    from services.preflight_service import run_file_preflight
    from services.sync_cursor import IncrementalReadScope

    db_path = tmp_path / "heap.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE jobs (id TEXT, name TEXT, updated_at TEXT)")
    conn.execute("INSERT INTO jobs VALUES (?, ?, ?)", ("a", "A", "1"))
    conn.commit()
    conn.close()

    def _scope(**_kwargs: Any) -> IncrementalReadScope:
        return IncrementalReadScope(cursor_column="updated_at", watermark="1")

    monkeypatch.setattr("services.preflight_service._resolve_read_scope", _scope)
    result = run_file_preflight(
        columns=["id", "name", "updated_at"],
        column_types={"id": "VARCHAR", "name": "VARCHAR", "updated_at": "VARCHAR"},
        row_count=1,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "name", "target": "name", "confidence": 0.99},
            {"source": "updated_at", "target": "updated_at", "confidence": 0.99},
        ],
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        source_connected=True,
        source_kind="file",
        source_format="csv",
        sync_mode="incremental_append",
        sample_rows=[{"id": "a", "name": "A2", "updated_at": "2"}],
        destination_db_type="sqlite",
        destination_table="jobs",
        destination_table_exists=True,
        destination_pk_columns=[],
        destination_config={"type": "sqlite", "connection_string": f"sqlite:///{db_path}"},
        destination_column_types={"id": "TEXT", "name": "TEXT", "updated_at": "TEXT"},
        validation_mode="strict",
        contract_primary_key="id",
        stream_contracts=[{
            "name": "jobs",
            "sync_mode": "incremental_append",
            "primary_key": "id",
            "cursor_field": "updated_at",
            "cursor_semantics": "modification_timestamp",
            "selected": True,
        }],
    )
    g6 = {g["id"]: g for g in result["gates"]}["g6_target_ddl"]
    assert g6["status"] != "block", g6
    assert result["passed"] is True
    assert "new version" in g6["message"]


def test_enforced_incremental_append_still_blocks(tmp_path: Path) -> None:
    from services.preflight_service import run_file_preflight

    db_path = tmp_path / "keyed.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO jobs VALUES (?, ?)", ("a", "A"))
    conn.commit()
    conn.close()
    result = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "VARCHAR", "name": "VARCHAR"},
        row_count=1,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "name", "target": "name", "confidence": 0.99},
        ],
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        source_connected=True,
        source_kind="file",
        source_format="csv",
        sync_mode="incremental_append",
        sample_rows=[{"id": "a", "name": "A2"}],
        destination_db_type="sqlite",
        destination_table="jobs",
        destination_table_exists=True,
        destination_pk_columns=["id"],
        destination_config={"type": "sqlite", "connection_string": f"sqlite:///{db_path}"},
        destination_column_types={"id": "TEXT", "name": "TEXT"},
        validation_mode="strict",
        contract_primary_key="id",
    )
    g6 = {g["id"]: g for g in result["gates"]}["g6_target_ddl"]
    assert g6["status"] == "block", g6
    assert result["passed"] is False


def _parity_job(*, pk: list[str], source_rows: int, target_rows: int, before: int) -> dict:
    return {
        "status": "completed",
        "sync_mode": "incremental_append",
        "reconciliation": {
            "source_rows": source_rows,
            "target_rows": target_rows,
            "target_rows_before": before,
            "passed": True,
            "sample_compare": {"rows_compared": target_rows, "_sample": None},
        },
        "destination_summary": {
            "sync_mode": "incremental_append",
            "primary_key_columns": pk,
            "table": "t",
            "type": "sqlserver",
        },
        "transfer_request": {
            "sync_mode": "incremental_append",
            "mappings": [
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
            ],
            "destination": {"format": "sqlserver", "table": "t"},
        },
    }


def test_in_place_merge_is_not_a_missing_row() -> None:
    """31 rows present, 2 keys updated in place: no unexplained missing row."""
    from services.parity_evidence import extract_evidence

    evidence = extract_evidence(_parity_job(pk=["id"], source_rows=2, target_rows=31, before=31))
    assert evidence
    missing = [
        o for o in evidence[0].observations
        if o.kind == "missing_in_destination" and not (o.cause or {}).get("explained")
    ]
    assert missing == []
    merged = [
        o for o in evidence[0].observations
        if (o.cause or {}).get("code") == "merged_in_place"
    ]
    assert merged and merged[0].rows == 2
    assert "No previous version was kept" in merged[0].cause["detail"]


def test_keyless_incremental_append_that_did_not_grow_is_still_missing() -> None:
    from services.parity_evidence import extract_evidence

    evidence = extract_evidence(_parity_job(pk=[], source_rows=2, target_rows=30, before=30))
    missing = [
        o for o in evidence[0].observations
        if o.kind == "missing_in_destination" and not (o.cause or {}).get("explained")
    ]
    assert missing and sum(o.rows for o in missing) == 2
    assert not any((o.cause or {}).get("code") == "merged_in_place" for o in evidence[0].observations)
