"""PG/SQLite → Redis staging blocked by rc-fidelity-collapse on every column.

A Redis key prefix with no keys is reported ``table_exists=True`` on purpose
(prefixes are logical namespaces). With zero sampled fields the mapper then
left every column ``pending_dest_schema`` with an empty target type, and the
coercion check blocked each one as "pending Studio/Map stamp". For a sampled
(schemaless) destination an empty namespace has no schema to wait for: fields
are created on write, typed from the destination type map like create-new.
"""

from __future__ import annotations

import socket
import sqlite3
import uuid

import pytest


def _redis_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 6379), timeout=1):
            return True
    except OSError:
        return False


def _map(dest_db: str, exists):
    from services.semantic_mapper import map_columns

    return map_columns(
        ["id", "name", "qty"],
        [],
        source_schemas=[
            {"name": "id", "inferred_type": "INT4", "samples": ["1"]},
            {"name": "name", "inferred_type": "TEXT", "samples": ["n1"]},
            {"name": "qty", "inferred_type": "INT4", "samples": ["1"]},
        ],
        destination_db_type=dest_db,
        destination_table_exists=exists,
        source_db_type="postgresql",
    )


@pytest.mark.parametrize("dest_db", ["redis", "mongodb"])
def test_empty_schemaless_namespace_maps_as_fields_created_on_write(dest_db):
    rows = _map(dest_db, True)
    assert [r["assignment_strategy"] for r in rows] == ["identity_passthrough"] * 3, rows
    assert all(r["create_new"] and r["target_type"] for r in rows), rows


def test_existing_sql_table_with_unread_columns_still_waits_for_its_schema():
    rows = _map("postgresql", True)
    assert {r["assignment_strategy"] for r in rows} == {"pending_dest_schema"}
    assert all(r["target_type"] == "" for r in rows)


@pytest.mark.skipif(not _redis_up(), reason="Redis not reachable on localhost:6379")
def test_sqlite_to_empty_redis_prefix_validates_without_fidelity_collapse(tmp_path):
    from services import connector_store
    from src.ai.copilot.tools import DataPilotTools

    db = tmp_path / "src.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
        conn.executemany(
            "INSERT INTO items VALUES (?, ?, ?)", [(i, f"n{i}", i * 2) for i in range(1, 11)]
        )
    tag = uuid.uuid4().hex[:8]
    src = connector_store.create_connector(
        {"name": f"rd-src-{tag}", "type": "sqlite", "role": "both", "database": str(db)}
    )
    dst = connector_store.create_connector(
        {"name": f"rd-dst-{tag}", "type": "redis", "role": "both", "host": "localhost",
         "port": 6379, "database": "0", "ssl": False}
    )
    try:
        res = DataPilotTools().execute("start_transfer", {
            "source_connector_id": src.id, "source_table": "items",
            "dest_connector_id": dst.id, "dest_table": f"rdfc_{tag}",
            "sync_mode": "full_refresh_overwrite",
        })
        assert "fidelity-collapse" not in (res.error or ""), res.error
        assert res.success, res.error
        plan = (res.output or {}).get("plan") or {}
        convs = {c["source_column"]: c for c in plan.get("type_conversions") or []}
        assert set(convs) == {"id", "name", "qty"}, convs
        assert all(
            c["to_type"] and c["fidelity"] != "dest_type_unread" for c in convs.values()
        ), convs
    finally:
        connector_store.delete_connector(src.id)
        connector_store.delete_connector(dst.id)


# ── What run 1 writes and what run 2 reads back ────────────────────────────


@pytest.mark.parametrize(
    "src_type, raw, expected",
    [("INT4", "1", 1), ("BIGINT", "9007199254740993", 9007199254740993), ("BOOLEAN", "true", True)],
)
def test_create_new_redis_carrier_keeps_json_number_and_boolean(src_type, raw, expected):
    """The writer stores a JSON document, so a number stays a JSON number.

    The type map stamped every logical type ``string``; the writer then left
    ``"1"`` a JSON string on run 1, while run 2 (sampled ``BIGINT``) wrote ``1``.
    """
    from connectors.redis_writer import _normalize_redis_typed_doc
    from services.decision_kernel import InventContext, invent_dest_type
    from services.type_system import is_lossy_coercion

    carrier = invent_dest_type(src_type, dest_db="redis", context=InventContext.CREATE_NEW)
    assert not is_lossy_coercion(src_type, carrier, dest_db="redis", dest_table_exists=False)
    doc = _normalize_redis_typed_doc({"c": raw}, ["c"], [carrier])
    assert doc["c"] == expected and type(doc["c"]) is type(expected), (carrier, doc)


@pytest.mark.parametrize("src_type", ["DECIMAL(10,2)", "TIMESTAMP", "DATE", "UUID"])
def test_redis_text_carriers_stay_exact_text(src_type):
    """JSON has no decimal/date/uuid type — exact text, never a float or a guess."""
    from services.decision_kernel import InventContext, invent_dest_type

    assert invent_dest_type(src_type, dest_db="redis", context=InventContext.CREATE_NEW) == "string"


def test_rerun_sampled_naive_timestamp_is_not_a_narrowing():
    """Run 2 samples the ISO text run 1 wrote; it is the same naive instant."""
    from services.type_system import is_lossy_coercion

    assert not is_lossy_coercion("TIMESTAMP_NTZ", "TIMESTAMP", dest_db="redis")
    assert not is_lossy_coercion("TIMESTAMP", "TIMESTAMP", dest_db="redis")


def test_redis_reader_envelope_is_not_a_destination_field_overwrite_empties():
    from services.preflight_service import _apply_overwrite_emptied_gate

    out: dict = {"gates": []}
    _apply_overwrite_emptied_gate(
        out,
        sync_mode="full_refresh_overwrite",
        destination_table_exists=True,
        live_dest_columns=["redis_key", "redis_type", "id", "name"],
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        regenerated=[],
        schema_policy="manual_review",
        acknowledged=False,
        dest_kind="redis",
        validation_mode="balanced",
    )
    assert not out["gates"] and not out.get("warnings"), out


def test_rerun_sampled_decimal_text_scores_as_the_carrier_run1_wrote():
    """Run 1 stores DECIMAL(10,2) as exact JSON text; run 2 profiles bare DECIMAL.

    A sampled profile is never a ceiling, so the exact-name pair must score
    against the widened carrier, not fall below the Map floor as "lossy".
    """
    from services.semantic_mapper import map_columns

    rows = map_columns(
        ["price"],
        ["price"],
        source_schemas=[{"name": "price", "inferred_type": "DECIMAL(10,2)", "samples": ["1.25", "12.50"]}],
        target_schemas=[{"name": "price", "inferred_type": "DECIMAL", "samples": ["1.25", "12.50"]}],
        destination_db_type="redis",
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    assert rows[0]["target"] == "price" and rows[0]["confidence"] >= 0.75, rows[0]
    # The row names the carrier the write uses (exact JSON text), so Validate's
    # conversion contract does not read bare DECIMAL as invented precision.
    assert rows[0]["target_type"] == "string" and not rows[0].get("requires_review"), rows[0]


@pytest.mark.parametrize("sampled", ["VARCHAR", "string"])
def test_rerun_redis_text_carrier_is_the_sanctioned_carrier_not_a_conflict(sampled):
    """Run 2's price is the exact text run 1 wrote — the map's own Redis carrier.

    Map quality scored it ``weak_or_conflicted`` (0.63) because it asked the
    SQL DDL materializer, which echoes DECIMAL(10,2), instead of the carrier
    the destination type map gives the write.
    """
    from services.mapping_quality import classify_mapping_confidence

    cls = classify_mapping_confidence(
        {"source": "price", "target": "price", "source_type": "DECIMAL(10,2)",
         "target_type": sampled, "transform": "none", "conversion_class": "lossless"},
        source_profile={"samples": ["1.25", "12.50", "3.75"]},
        destination_db_type="redis",
    )
    assert cls["confidence_class"] != "weak_or_conflicted", cls
