"""D392, D403, D404, D298, D312 — unit proofs, not a live retest.

D392: a SQL Server CDC poll that only saw DML must not open a mapping review
whose ddl is the capture-instance name. A real ADD COLUMN does, and names
the column. An acknowledged review is not replaced by a new one.

D403: ``2024-11-03T05:37:00Z`` and ``2024-11-03 05:37:00+00:00`` are one instant
on a MySQL text carrier of a DATETIMEOFFSET. A different instant still fails
and the drift message names the key. NULL stays NULL.

D404: Kafka incremental_append with no declared cursor does not invent
``updated_at``. A payload cursor that cannot be paged is refused by the plan.

D298: a missing DynamoDB table is creatable from mapped columns and a HASH key,
and refused before Confirm when neither exists.

D312: host ``api.notion.com`` calls ``/v1/search``. A base that already has
``/v1`` is not doubled.
"""
from __future__ import annotations

import sys
import types
from urllib.parse import urljoin

from connectors.sqlserver_cdc_native import SqlServerNativeCdc
from services.cdc_drift_repair import cdc_drift_operator_message
from services.cdc_value_digest import compare_row_sets, stamp_cdc_instant_carriers
from src.ai.copilot.transfer_tools import _identity_stream_contract
from src.transfer.engine_kafka import kafka_unpageable_cursor


class _Cursor:
    def __init__(self, columns, ddl):
        self.columns = columns
        self.ddl = ddl
        self.mode = "cols"

    def execute(self, sql, params=None):
        self.mode = "ddl" if "ddl_history" in str(sql) else "cols"

    def fetchall(self):
        if self.mode == "ddl":
            # cdc.ddl_history answers (ddl_command, ddl_lsn); the first fake
            # returned the command alone. Faithful-catalog cases live in
            # tests/test_p2_d392_sqlserver_ddl_history.py.
            return [(row[0], (i + 1).to_bytes(10, "big")) for i, row in enumerate(self.ddl)]
        return self.columns


def _reader():
    return SqlServerNativeCdc(
        {"host": "h", "database": "db", "connector_id": "src-ss"},
        table="orders",
        primary_key="id",
        schema="dbo",
        capture_instance="dbo_orders",
    )


def _history(monkeypatch):
    stored: dict[tuple[str, str], list[dict]] = {}

    def list_history(source_key, table):
        return list(stored.get((source_key, table), []))

    def record_ddl(source_key, table, ddl, offset, schema_snapshot):
        entry = {
            "version": len(stored.get((source_key, table), [])) + 1,
            "ddl": ddl,
            "schema_snapshot": schema_snapshot,
        }
        stored.setdefault((source_key, table), []).append(entry)
        return entry

    import services.cdc_schema_history as history

    monkeypatch.setattr(history, "list_history", list_history)
    monkeypatch.setattr(history, "record_ddl", record_ddl)
    return stored


def test_dml_only_capture_does_not_open_a_review_and_an_added_column_does(monkeypatch, tmp_path):
    import services.cdc_mapping_review as rev

    path = tmp_path / "reviews.jsonl"
    monkeypatch.setattr(rev, "STORE_PATH", path)
    monkeypatch.setenv("CDC_MAPPING_REVIEW_STORE", str(path))
    stored = _history(monkeypatch)
    reader = _reader()
    cols = [("id", "int", 1), ("name", "nvarchar", 2)]
    reader._maybe_record_capture_schema(_Cursor(cols, []), offset="0a")
    assert rev.list_reviews(source_key=reader.source_key, status="open") == []
    assert stored[(reader.source_key, "dbo.orders")][0]["ddl"] == "cdc.capture_instance=dbo_orders"

    # A new process: the column list is already the baseline. DML still does not review.
    again = _reader()
    again._maybe_record_capture_schema(_Cursor(cols, []), offset="0b")
    assert rev.list_reviews(source_key=reader.source_key, status="open") == []

    added = _reader()
    added._maybe_record_capture_schema(
        _Cursor([*cols, ("note", "nvarchar", 3)], [("ALTER TABLE dbo.orders ADD note nvarchar(20)",)]),
        offset="0c",
    )
    reviews = rev.list_reviews(source_key=reader.source_key, status="open")
    assert len(reviews) == 1
    assert reviews[0]["column_names"] == ["note"]
    assert "note" in reviews[0]["ddl"]

    assert rev.acknowledge_review(reviews[0]["id"])["status"] == "acknowledged"
    # The same column set must not open another review, so this id cannot block again.
    follow = _reader()
    follow._maybe_record_capture_schema(
        _Cursor([*cols, ("note", "nvarchar", 3)], []),
        offset="0d",
    )
    assert rev.list_reviews(source_key=reader.source_key, status="open") == []
    assert rev.open_review_for_source(reader.source_key, "orders") is None


def test_capture_instance_switch_still_reviews(monkeypatch):
    recorded = {}

    def flag(**kwargs):
        recorded.update(kwargs)
        return kwargs

    import services.cdc_mapping_review as rev
    import services.cdc_schema_history as history

    monkeypatch.setattr(history, "list_history", lambda *_a, **_k: [])
    monkeypatch.setattr(
        history, "record_ddl", lambda *a, **k: {"version": 1, "ddl": k.get("ddl", a[2] if len(a) > 2 else "")}
    )
    monkeypatch.setattr(rev, "flag_mapping_review", flag)
    reader = _reader()
    reader._maybe_record_capture_schema(
        _Cursor([("id", "int", 1), ("extra", "int", 2)], []),
        ddl="cdc.capture_instance switch dbo_orders_v1->dbo_orders_v2",
        force=True,
    )
    assert recorded["ddl"] == "cdc.capture_instance switch dbo_orders_v1->dbo_orders_v2"
    assert "extra" in recorded["column_names"]


def test_mysql_text_carrier_compares_the_instant_and_a_real_difference_names_the_key():
    mappings = [{
        "source": "tstz",
        "target": "tstz",
        "source_type": "datetimeoffset",
        "target_type": "VARCHAR(64)",
    }]
    types = stamp_cdc_instant_carriers({"tstz": "VARCHAR(64)", "id": "INT"}, mappings)
    same = compare_row_sets(
        [{"id": 1, "tstz": "2024-11-03 05:37:00+00:00"}],
        [{"id": 1, "tstz": "2024-11-03T05:37:00Z"}],
        ["id", "tstz"],
        engine="mysql",
        dest_types=types,
    )
    assert same.missing == 0
    nulls = compare_row_sets(
        [{"id": 2, "tstz": None}],
        [{"id": 2, "tstz": None}],
        ["id", "tstz"],
        engine="mysql",
        dest_types=types,
    )
    assert nulls.missing == 0
    shifted = compare_row_sets(
        [{"id": 3, "tstz": "2024-11-03 05:37:00+00:00"}],
        [{"id": 3, "tstz": "2024-11-03T06:37:00Z"}],
        ["id", "tstz"],
        engine="mysql",
        dest_types=types,
    )
    assert shifted.missing == 1
    message = cdc_drift_operator_message({
        "drift_count": shifted.missing,
        "dest_table": "mx_ss_my",
        "pk_fields": ["id"],
        "keys": [{"id": "3"}],
    })
    assert "id" in message
    assert message.startswith("CDC verification found 1 row")


def _stub_requests() -> None:
    """The plan gate imports the HTTP reader. This machine has no requests."""
    if "requests" in sys.modules and hasattr(sys.modules["requests"], "Session"):
        return
    module = sys.modules.get("requests") or types.ModuleType("requests")
    module.Session = type("Session", (), {})  # type: ignore[attr-defined]
    sys.modules["requests"] = module


def test_kafka_does_not_invent_updated_at_and_refuses_a_column_cursor_at_plan():
    _stub_requests()
    contracts, error = _identity_stream_contract(
        mode="incremental_append",
        source_table="orders",
        source_columns=["id", "updated_at", "note"],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "updated_at", "target": "updated_at"},
            {"source": "note", "target": "note"},
        ],
        operator_key="",
        catalog_key="",
        source_type="kafka",
    )
    assert error == ""
    assert contracts == [] or "cursor_field" not in contracts[0]

    from services.preflight_cursor_gate import build_sync_contract_gate

    blocked = build_sync_contract_gate(
        [{"name": "orders", "selected": True, "cursor_field": "updated_at",
          "cursor_semantics": "modification_timestamp"}],
        sync="incremental_append",
        validation="balanced",
        dest="postgresql",
        src="kafka",
        kind="database",
        source_columns=["id", "updated_at"],
        pass_status="pass",
        block_status="block",
    )
    assert blocked["status"] == "block"
    assert "updated_at" in blocked["message"]
    assert "offset is the cursor" in blocked["message"]

    offset = build_sync_contract_gate(
        [{"name": "orders", "selected": True, "cursor_field": "_kafka_offset",
          "cursor_semantics": "monotonic_sequence"}],
        sync="incremental_append",
        validation="balanced",
        dest="postgresql",
        src="kafka",
        kind="database",
        source_columns=["id", "_kafka_offset"],
        pass_status="pass",
        block_status="block",
    )
    assert offset["status"] == "pass"
    assert kafka_unpageable_cursor("kafka", "") == ""
    assert kafka_unpageable_cursor("postgresql", "updated_at") == ""


def test_dynamodb_create_new_uses_the_hash_key_or_refuses():
    from connectors.dynamodb_writer import dynamodb_create_refusal
    from connectors.writer_common import resolve_target_columns

    pending = [{
        "source": "id",
        "target": "id",
        "source_type": "INTEGER",
        "assignment_strategy": "pending_dest_schema",
    }]
    cols, _types = resolve_target_columns(
        pending, {"id": "INTEGER"}, table_exists=False, dest_db="dynamodb",
    )
    assert cols == ["id"]
    assert dynamodb_create_refusal(pending) == ""
    refused = dynamodb_create_refusal([])
    assert "no columns were provided" in refused
    assert "HASH key" in refused
    no_key = dynamodb_create_refusal([{"source": "note", "target": "note"}])
    assert no_key
    assert "invent a key" in no_key


def test_notion_host_keeps_the_v1_path_and_does_not_double_it():
    # This environment has no requests package. The URL rule does not call it.
    _stub_requests()
    from connectors.sdk.notion import NotionSource

    origin = NotionSource({"api_key": "ntn-synthetic", "base_url": "https://api.notion.com"})
    assert origin._base_url == "https://api.notion.com/v1/"
    assert urljoin(origin._base_url, "search") == "https://api.notion.com/v1/search"

    slashed = NotionSource({"api_key": "ntn-synthetic", "base_url": "https://api.notion.com/"})
    assert slashed._base_url == "https://api.notion.com/v1/"

    already = NotionSource({"api_key": "ntn-synthetic", "base_url": "https://api.notion.com/v1"})
    assert already._base_url == "https://api.notion.com/v1/"
    assert "/v1/v1" not in already._base_url

    already_slashed = NotionSource({
        "api_key": "ntn-synthetic", "base_url": "https://api.notion.com/v1/",
    })
    assert already_slashed._base_url == "https://api.notion.com/v1/"
