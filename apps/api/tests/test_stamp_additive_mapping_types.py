"""Decision Kernel additive Map stamps — Excel→PG partial Studio cliff."""

from __future__ import annotations

from services.decision_kernel import stamp_additive_mapping_types


def test_stamp_additive_invents_create_new_under_backfill():
    mappings, unstamped = stamp_additive_mapping_types(
        [
            {
                "source": "Change_from_Previous_Year",
                "target": "Change_from_Previous_Year",
                "source_type": "DOUBLE",
                "confidence": 0.9,
            }
        ],
        dest_db="postgresql",
        live_dest_types={"Country": "TEXT", "Year": "INTEGER"},
        source_types={"Change_from_Previous_Year": "DOUBLE"},
        backfill_new_fields=True,
    )
    assert unstamped == []
    row = mappings[0]
    assert row.get("create_new") is True
    stamp = str(row.get("target_type") or "").upper()
    assert stamp
    assert "VARCHAR" not in stamp or "DOUBLE" in stamp or "FLOAT" in stamp or "NUMERIC" in stamp or "DOUBLE PRECISION" in stamp
    # Prefer numeric invent over bare text for DOUBLE source.
    assert any(tok in stamp for tok in ("DOUBLE", "FLOAT", "NUMERIC", "REAL", "DECIMAL"))


def test_stamp_additive_replaces_source_identity_bootstrap():
    """FE source-as-destType must not skip Kernel CREATE_NEW invent."""
    mappings, unstamped = stamp_additive_mapping_types(
        [
            {
                "source": "uid",
                "target": "uid",
                "source_type": "UUID",
                "target_type": "UUID",  # Map bootstrap echo — not Kernel invent
                "create_new": True,
                "assignment_strategy": "create_compatible_new",
            }
        ],
        dest_db="bigquery",
        live_dest_types={},
        source_types={"uid": "UUID"},
        backfill_new_fields=True,
    )
    assert unstamped == []
    stamp = str(mappings[0].get("target_type") or "").upper()
    assert stamp
    # BigQuery create-new UUID invent is STRING (not native UUID).
    assert "UUID" not in stamp or stamp == "STRING"
    assert stamp in {"STRING", "TEXT", "VARCHAR"} or "STRING" in stamp


def test_stamp_additive_binds_live_carrier_without_invent():
    mappings, unstamped = stamp_additive_mapping_types(
        [
            {
                "source": "Year",
                "target": "Year",
                "source_type": "INTEGER",
            }
        ],
        dest_db="postgresql",
        live_dest_types={"Year": "BIGINT"},
        backfill_new_fields=True,
    )
    assert unstamped == []
    assert mappings[0]["target_type"] == "BIGINT"


def test_stamp_additive_refuses_pending_dest_schema():
    mappings, unstamped = stamp_additive_mapping_types(
        [
            {
                "source": "orphan",
                "target": "orphan",
                "source_type": "DOUBLE",
                "target_type": "DOUBLE PRECISION",  # invented residue must clear
                "create_new": True,
                "assignment_strategy": "pending_dest_schema",
            }
        ],
        dest_db="postgresql",
        live_dest_types={"id": "INTEGER"},
        backfill_new_fields=True,
    )
    assert not str(mappings[0].get("target_type") or "").strip()
    assert mappings[0].get("create_new") is False
    assert unstamped == []  # pending is not "unstamped additive" — honesty leave empty


def test_stamp_additive_live_carrier_overrides_map_invent():
    mappings, unstamped = stamp_additive_mapping_types(
        [
            {
                "source": "Year",
                "target": "Year",
                "source_type": "INTEGER",
                "target_type": "TEXT",  # Map invent must not beat live BIGINT
                "create_new": True,
                "assignment_strategy": "create_compatible_new",
            }
        ],
        dest_db="postgresql",
        live_dest_types={"Year": "BIGINT"},
        backfill_new_fields=True,
    )
    assert unstamped == []
    assert mappings[0]["target_type"] == "BIGINT"
    assert mappings[0].get("create_new") is False


def test_stamp_additive_without_backfill_leaves_blank_not_unstamped():
    """No invent authority → blank stamp, not a g6 invent-failure (Property 2)."""
    mappings, unstamped = stamp_additive_mapping_types(
        [
            {
                "source": "note",
                "target": "note",
                "source_type": "TEXT",
            }
        ],
        dest_db="postgresql",
        live_dest_types={"id": "INTEGER"},
        backfill_new_fields=False,
        dest_table_exists=True,
    )
    assert unstamped == []
    assert not str(mappings[0].get("target_type") or "").strip()


def test_stamp_additive_create_table_invents_without_backfill():
    """Missing dest object invents CREATE TABLE stamps (legitimate happy path)."""
    mappings, unstamped = stamp_additive_mapping_types(
        [
            {"source": "id", "target": "id", "source_type": "BIGINT"},
            {"source": "nm", "target": "nm", "source_type": "TEXT"},
        ],
        dest_db="postgresql",
        live_dest_types={},
        backfill_new_fields=False,
        dest_table_exists=False,
    )
    assert unstamped == []
    assert mappings[0]["target_type"]
    assert mappings[1]["target_type"]
    assert mappings[0].get("create_new") is True


def test_stamp_additive_invent_refused_lists_unstamped(monkeypatch):
    """Invent-required but refused → unstamped (gate BLOCK path)."""
    from services.decision_kernel import invent as invent_mod
    from services.decision_kernel.invent import InventRefused, InventContext

    def _boom(*_a, **_k):
        raise InventRefused("forced", context=InventContext.CREATE_NEW)

    monkeypatch.setattr(invent_mod, "invent_dest_type", _boom)
    mappings, unstamped = stamp_additive_mapping_types(
        [
            {
                "source": "x",
                "target": "x",
                "source_type": "TEXT",
                "create_new": True,
                "assignment_strategy": "create_compatible_new",
            }
        ],
        dest_db="postgresql",
        live_dest_types={"id": "INTEGER"},
        backfill_new_fields=True,
        dest_table_exists=True,
    )
    assert "x" in unstamped
    assert not str(mappings[0].get("target_type") or "").strip()


def test_preflight_stamps_additive_before_execute_refuse():
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["id", "Change_from_Previous_Year"],
        column_types={"id": "INTEGER", "Change_from_Previous_Year": "DOUBLE"},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95, "target_type": "INTEGER"},
            {
                "source": "Change_from_Previous_Year",
                "target": "Change_from_Previous_Year",
                "confidence": 0.9,
                "create_new": True,
                "assignment_strategy": "create_compatible_new",
                # Intentionally blank — Kernel must stamp under backfill.
            },
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        sync_mode="full_refresh_append",
        sample_rows=[
            {"id": 1, "Change_from_Previous_Year": 1.5},
            {"id": 2, "Change_from_Previous_Year": -0.25},
        ],
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_column_types={"id": "INTEGER"},
        destination_can_create=True,
        destination_can_write=True,
        validation_mode="strict",
        backfill_new_fields=True,
        schema_policy="propagate_columns",
    )
    stamped = {
        r["target"]: r["target_type"]
        for r in (result.get("stamped_mappings") or [])
        if r.get("target")
    }
    assert "Change_from_Previous_Year" in stamped
    assert str(stamped["Change_from_Previous_Year"]).strip()
    assert not any(b.get("id") == "g6_additive_stamp" for b in (result.get("blockers") or []))


def test_kernel_invent_refreshes_when_source_type_is_profiled_later():
    """Provisional TEXT stamp must not outlive the profiled source type.

    Validate stamps once before profiling (every file column looks like TEXT →
    VARCHAR) and again after. The stale VARCHAR used to create the destination
    column *and* then be reported as a DECIMAL→VARCHAR fidelity collapse — a
    blocker the product inflicted on itself on every file → warehouse route.
    """
    first, _ = stamp_additive_mapping_types(
        [{"source": "amount", "target": "amount", "confidence": 0.99}],
        dest_db="snowflake",
        live_dest_types={},
        source_types={"amount": "TEXT"},
        dest_table_exists=False,
    )
    assert first[0]["target_type"].upper().startswith(("VARCHAR", "TEXT", "STRING"))

    second, _ = stamp_additive_mapping_types(
        first,
        dest_db="snowflake",
        live_dest_types={},
        source_types={"amount": "DECIMAL(9,4)"},
        dest_table_exists=False,
    )
    assert "NUMBER(9,4)" in second[0]["target_type"].upper().replace(" ", "")


def test_operator_stamp_is_never_refreshed_by_profiling():
    rows, _ = stamp_additive_mapping_types(
        [
            {
                "source": "amount",
                "target": "amount",
                "target_type": "VARCHAR(64)",
                "create_new": True,
            }
        ],
        dest_db="snowflake",
        live_dest_types={},
        source_types={"amount": "DECIMAL(9,4)"},
        dest_table_exists=False,
    )
    assert rows[0]["target_type"] == "VARCHAR(64)"
