"""DEF-C-025: an overwrite must say which destination values it will empty.

QA dropped ``note`` from the source and ran a full-refresh overwrite under
``manual_review``: the destination kept ``note`` but all 20 values came back
NULL with no review step. The live table and the mapping already show it, so
Validate (and Execute's own preflight) name it without any schema history.
"""

from __future__ import annotations

import pytest

from services.overwrite_keep import overwrite_emptied_columns
from services.preflight_service import run_file_preflight

_TYPES = {"id": "INTEGER", "name": "VARCHAR(50)", "amt": "NUMERIC(10,2)"}
_ROWS = [{"id": str(i), "name": f"n{i}", "amt": "1.25"} for i in range(20)]


def _overwrite(
    dest_db: str,
    *,
    dest_types: dict[str, str] | None = None,
    policy: str = "manual_review",
    ack: bool = False,
    sync_mode: str = "full_refresh_overwrite",
    identity: list[str] | None = None,
) -> dict:
    cols = list(_TYPES)
    return run_file_preflight(
        columns=cols,
        column_types=_TYPES,
        row_count=len(_ROWS),
        mappings=[{"source": c, "target": c, "confidence": 1.0} for c in cols],
        sample_rows=_ROWS,
        destination_connected=True,
        source_kind="database",
        source_format="postgresql",
        destination_table_exists=True,
        destination_can_write=True,
        destination_can_create=True,
        destination_column_types=dest_types or {**_TYPES, "note": "VARCHAR(100)"},
        destination_identity_columns=identity,
        destination_db_type=dest_db,
        destination_table="t",
        sync_mode=sync_mode,
        schema_policy=policy,
        schema_drift_acknowledged=ack,
    )


def _gate(result: dict) -> dict | None:
    return next((g for g in result["gates"] if g["id"] == "overwrite_emptied_columns"), None)


def _warning(result: dict) -> dict | None:
    return next(
        (w for w in result.get("warnings") or [] if isinstance(w, dict)
         and w.get("id") == "overwrite_emptied_columns"),
        None,
    )


@pytest.mark.parametrize("dest_db", ["mysql", "postgresql", "sqlserver", "snowflake"])
def test_manual_review_pauses_before_an_overwrite_empties_a_column(dest_db):
    result = _overwrite(dest_db)
    gate = _gate(result)
    assert gate is not None and gate["status"] == "block"
    assert gate["details"]["columns"] == ["note"]
    assert "note" in gate["message"] and "lost" in gate["message"]
    assert result["passed"] is False
    assert any(b.get("id") == "overwrite_emptied_columns" for b in result["blockers"])


def test_acknowledged_overwrite_proceeds_and_still_names_the_loss():
    result = _overwrite("mysql", ack=True)
    assert _gate(result) is None
    assert result["passed"] is True, [b.get("id") for b in result["blockers"]]
    assert _warning(result)["details"]["columns"] == ["note"]


def test_a_propagating_policy_warns_instead_of_pausing():
    result = _overwrite("postgresql", policy="propagate_all")
    assert _gate(result) is None
    assert _warning(result) is not None


def test_append_and_full_mapping_are_not_flagged():
    assert _gate(_overwrite("mysql", sync_mode="full_refresh_append")) is None
    assert _gate(_overwrite("mysql", dest_types=dict(_TYPES))) is None


@pytest.mark.parametrize("dest_db", ["hubspot", "salesforce", "kafka"])
def test_upserting_sinks_keep_unwritten_properties(dest_db):
    assert _gate(_overwrite(dest_db)) is None
    assert _warning(_overwrite(dest_db)) is None


def test_engine_and_identity_columns_are_regenerated_not_emptied():
    assert overwrite_emptied_columns(
        ["id", "_df_lsn", "note"], [{"source": "id", "target": "ID"}]
    ) == ["note"]
    result = _overwrite(
        "sqlserver",
        dest_types={**_TYPES, "row_id": "BIGINT"},
        identity=["row_id"],
    )
    assert _gate(result) is None
