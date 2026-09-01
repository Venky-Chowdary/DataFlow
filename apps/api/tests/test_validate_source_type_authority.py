"""Validate must score mappings against the schema Execute will read.

Validate used to trust the ``column_types`` the browser posted while Execute
re-derived them from the live connector. A stale Map stamp (Mongo ``_id`` sent
as ``VARCHAR``) therefore passed every gate and then hard-failed at write on the
live ``OBJECTID``. Live introspection is now the authority at Validate too, so
the block happens before the operator is told the transfer is ready.
"""

from __future__ import annotations

from services.source_schema_authority import (
    reconcile_source_types,
    restamp_mapping_source_types,
)


def test_live_types_win_and_drift_is_reported():
    types, drift = reconcile_source_types(
        {"_id": "VARCHAR", "views": "INTEGER"},
        {"_id": "OBJECTID", "views": "INTEGER"},
    )
    assert types["_id"] == "OBJECTID"
    assert drift == [{"column": "_id", "declared": "VARCHAR", "live": "OBJECTID"}]


def test_declared_columns_absent_from_live_schema_are_kept():
    types, drift = reconcile_source_types({"extra": "DECIMAL(9,2)"}, {})
    assert types == {"extra": "DECIMAL(9,2)"}
    assert drift == []


def test_same_logical_refinement_is_not_drift():
    _types, drift = reconcile_source_types({"code": "VARCHAR"}, {"code": "VARCHAR(24)"})
    assert drift == []


def test_mapping_source_type_is_restamped_for_artifact_parity():
    rows = restamp_mapping_source_types(
        [{"source": "_id", "target": "id", "source_type": "VARCHAR"}],
        {"_id": "OBJECTID"},
    )
    assert rows[0]["source_type"] == "OBJECTID"


def test_restamp_finds_folded_oracle_catalog_keys():
    rows = restamp_mapping_source_types(
        [{"source": "amount", "target": "amount", "source_type": "VARCHAR"}],
        {"AMOUNT": "DECIMAL(18,2)"},
    )
    assert rows[0]["source_type"] == "DECIMAL(18,2)"


def test_stale_map_stamp_blocks_at_validate_not_at_write():
    """The pasted Mongo→Postgres symptom, as a gate assertion."""
    from services.preflight_service import run_file_preflight

    stale_map = [
        {
            "source": "_id",
            "target": "id",
            "source_type": "VARCHAR",
            "target_type": "TEXT",
            "confidence": 0.95,
            "create_new": True,
        }
    ]
    live_types, drift = reconcile_source_types({"_id": "VARCHAR"}, {"_id": "OBJECTID"})
    assert drift, "live introspection must be treated as authoritative"

    result = run_file_preflight(
        columns=["_id"],
        column_types=live_types,
        row_count=1,
        mappings=restamp_mapping_source_types(stale_map, live_types),
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="mongodb",
        sync_mode="full_refresh_append",
        sample_rows=[{"_id": "6991173f8d64fcf16f3a0805"}],
        destination_db_type="postgresql",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
    )
    assert not result["passed"], "Validate must refuse what Execute would refuse"
