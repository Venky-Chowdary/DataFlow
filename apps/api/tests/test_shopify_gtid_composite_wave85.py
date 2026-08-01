"""Wave 85: Shopify metafield polarity + GTID watermark close + composite snapshot deletes.

Research anchors
----------------
- Shopify Admin metafield types
  (https://shopify.dev/docs/apps/build/metafields/list-of-data-types):
  ``list.*`` are JSON arrays; ``money``/measurement/``rich_text_field``/``link``
  are JSON objects — never invent opaque VARCHAR for structured polarity.
- Debezium DBZ-3577: read-only incremental snapshots close GTID windows via
  executed-set containment (not lexicographic string order).
- Debezium DDD-3: composite PK tombstones must emit the joined key, not
  ``row.get([\"a\",\"b\"])`` which silently loses deletes.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_shopify_metafield_list_json_money_measurement_reference():
    from connectors.saas_write_carriers import shopify_metafield_type_to_carrier

    assert shopify_metafield_type_to_carrier("list.boolean") == "ARRAY<BOOLEAN>"
    assert shopify_metafield_type_to_carrier("list.number_integer") == "ARRAY<BIGINT>"
    assert shopify_metafield_type_to_carrier("list.number_decimal") == "ARRAY<DECIMAL(22,9)>"
    assert shopify_metafield_type_to_carrier("list.date") == "ARRAY<DATE>"
    assert shopify_metafield_type_to_carrier("list.date_time") == "ARRAY<TIMESTAMPTZ>"
    assert shopify_metafield_type_to_carrier("list.single_line_text_field") == "ARRAY<TEXT>"
    assert shopify_metafield_type_to_carrier("list.money") == "ARRAY<JSON>"
    assert shopify_metafield_type_to_carrier("list.weight") == "ARRAY<JSON>"

    assert shopify_metafield_type_to_carrier("json") == "JSON"
    assert shopify_metafield_type_to_carrier("money") == "JSON"
    assert shopify_metafield_type_to_carrier("rating") == "JSON"
    assert shopify_metafield_type_to_carrier("rich_text_field") == "JSON"
    assert shopify_metafield_type_to_carrier("link") == "JSON"
    assert shopify_metafield_type_to_carrier("weight") == "JSON"
    assert shopify_metafield_type_to_carrier("dimension") == "JSON"
    assert shopify_metafield_type_to_carrier("volume") == "JSON"
    assert shopify_metafield_type_to_carrier("temperature") == "JSON"
    assert shopify_metafield_type_to_carrier("product_reference") == "VARCHAR(255)"
    assert shopify_metafield_type_to_carrier("file_reference") == "VARCHAR(255)"
    # Still preserve scalar polarity for basics.
    assert shopify_metafield_type_to_carrier("boolean") == "BOOLEAN"
    assert shopify_metafield_type_to_carrier("number_decimal") == "DECIMAL(22,9)"


def test_shopify_live_types_merge_list_metafield():
    from connectors.saas_write_carriers import shopify_live_types_for_columns

    live = shopify_live_types_for_columns(
        "products",
        ["title", "custom.tags", "custom.cost"],
        metafield_defs=[
            {
                "namespace": "custom",
                "key": "tags",
                "type": "list.single_line_text_field",
                "validations": [],
            },
            {
                "namespace": "custom",
                "key": "cost",
                "type": "money",
                "validations": [],
            },
        ],
    )
    assert live["title"] == "VARCHAR(255)"
    assert live["custom.tags"] == "ARRAY<TEXT>"
    assert live["custom.cost"] == "JSON"


def test_gtid_watermark_window_closed_dbz3577():
    from connectors.writer_common import gtid_watermark_window_closed

    low = "uuid-a:1-40"
    high = "uuid-a:1-100,uuid-b:1-5"
    assert gtid_watermark_window_closed(low=low, high=high) is True
    assert gtid_watermark_window_closed(low=low, high="uuid-a:1-30") is False
    assert (
        gtid_watermark_window_closed(
            low=low, high=high, event_gtid="uuid-a:50"
        )
        is True
    )
    assert (
        gtid_watermark_window_closed(
            low=low, high=high, event_gtid="uuid-a:101"
        )
        is False
    )
    # Empty low is contained by any high (window can close).
    assert gtid_watermark_window_closed(low="", high=high) is True
    # Never invent closed from empty high when low is non-empty.
    assert gtid_watermark_window_closed(low=low, high="") is False


def test_composite_pk_signal_roundtrip_and_runner_deletes():
    from services.cdc_incremental_runner import interleave_incremental_snapshot
    from services.cdc_incremental_snapshot import (
        SnapshotSignal,
        request_incremental_snapshot,
    )
    from services.cdc_snapshot_window import _PK_SEP
    import services.cdc_incremental_runner as runner_mod

    sig = SnapshotSignal.from_dict(
        {
            "id": "snap_test",
            "source_key": "src:mysql",
            "table": "orders",
            "primary_key": ["tenant_id", "id"],
            "status": "pending",
        }
    )
    assert sig.primary_key == ["tenant_id", "id"]

    requested = request_incremental_snapshot(
        "src:wave85",
        "line_items",
        primary_key=["tenant_id", "sku"],
        chunk_size=10,
    )
    assert requested.primary_key == ["tenant_id", "sku"]

    # Runner must emit joined composite tombstone keys (not None from list.get).
    claimed = SnapshotSignal(
        id="snap_run",
        source_key="src:wave85-run",
        table="orders",
        status="running",
        primary_key=["tenant_id", "id"],
        chunk_size=10,
        gtid_low="uuid-a:1-1",
        gtid_high="uuid-a:1-10",
    )

    def fetch_chunk(_sig):
        return (
            [
                {"tenant_id": "t1", "id": "1", "v": "a"},
                {"tenant_id": "t1", "id": "2", "v": "b"},
            ],
            f"t1{_PK_SEP}2",
            True,
        )

    def stream_during(_sig):
        return [
            {
                "op": "d",
                "pk": f"t1{_PK_SEP}1",
                "row": {"tenant_id": "t1", "id": "1"},
            }
        ]

    with (
        patch.object(runner_mod, "claim_next_signal", return_value=claimed),
        patch.object(runner_mod, "mark_chunk", return_value=claimed),
        patch.object(runner_mod, "complete_signal", return_value=claimed),
        patch.object(runner_mod, "update_signal", return_value=claimed),
    ):
        batches = list(
            interleave_incremental_snapshot(
                "src:wave85-run",
                table="orders",
                fetch_chunk=fetch_chunk,
                stream_events_during_chunk=stream_during,
                max_chunks_per_poll=1,
            )
        )

    assert len(batches) == 1
    batch = batches[0]
    assert any(r.get("id") == "2" for r in batch.inserts)
    assert not any(r.get("id") == "1" and not r.get("__deleted") for r in batch.inserts)
    assert f"t1{_PK_SEP}1" in batch.deletes
    # GTID watermarks surface on resume_token for operator proof.
    win = (batch.resume_token or {}).get("snapshot_window") or {}
    assert win.get("gtid_low") == "uuid-a:1-1"
    assert win.get("gtid_high") == "uuid-a:1-10"


def test_shopify_reverse_etl_planner_registered():
    from services.reverse_etl import plan_activation, supported_activation_kinds

    plan = plan_activation(
        destination_kind="shopify",
        object_name="customers",
        primary_key="id",
    )
    assert plan.destination_kind == "shopify"
    assert plan.batch_size == 50
    assert plan.primary_key == ["id"]
    assert any("Metafield" in n for n in plan.notes)
    assert "shopify" in supported_activation_kinds()
