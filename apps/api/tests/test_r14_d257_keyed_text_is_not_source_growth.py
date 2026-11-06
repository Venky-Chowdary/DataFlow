"""D257 follow-up: a MySQL key carrier DataFlow sized for TEXT is not source growth.

MySQL cannot index TEXT, so a TEXT key lands as the widest full-value
VARCHAR the 3072-byte key budget allows (VARCHAR(768) utf8mb4). Run 2 of a
PostgreSQL TEXT key into that table proposed "cust_code VARCHAR(768) -> TEXT"
and paused for an ALTER the writer would never run (E2-023). The proposal now
asks the writer's keyed rule; a non-key VARCHAR(n) -> TEXT stays an
approvable widen.
"""

from __future__ import annotations

from services.schema_drift import dest_key_groups, detect_schema_drift, propose_dest_widens

_LIVE = {"cust_code": "VARCHAR(768) COLLATE utf8mb4_bin", "note": "VARCHAR(20)",
         "region": "VARCHAR(16)", "code": "VARCHAR(100)",
         "nopad": "VARCHAR(768) COLLATE utf8mb4_0900_bin",
         "mid": "NVARCHAR(450) COLLATE Latin1_General_100_BIN2", "mnote": "NVARCHAR(50)"}


def _map(col: str) -> dict:
    return {"source": col, "target": col, "source_type": "TEXT",
            "target_type": _LIVE[col], "target_type_origin": "destination_catalog"}


def _props(cols, groups, dest="mysql", src="TEXT"):
    return {
        p["column"]: p["new_type"]
        for p in propose_dest_widens(
            [_map(c) for c in cols], source_schema={c: src for c in cols},
            target_schema=_LIVE, dest_db=dest, key_groups=groups,
        )
    }


def test_text_onto_the_key_carrier_dataflow_sized_is_not_a_widen():
    groups = dest_key_groups(["cust_code"], None, "cust_code")
    assert groups == [["cust_code"]]
    assert _props(["cust_code", "note"], groups) == {"note": "TEXT"}
    # A no-pad / binary collation on the carrier is still that carrier (D205).
    assert _props(["nopad"], [["nopad"]]) == {}


def test_sqlserver_full_value_key_carrier_is_not_a_widen():
    # 900-byte key / 2 bytes per NVARCHAR character = NVARCHAR(450).
    for src in ("TEXT", "NVARCHAR(MAX)"):
        assert _props(["mid", "mnote"], [["mid"]], dest="sqlserver", src=src) == {
            "mnote": "TEXT" if src == "TEXT" else "NVARCHAR(MAX)"
        }


def test_live_contract_lane_does_not_pause_the_keyed_column():
    drift = detect_schema_drift(
        source_columns=["cust_code"], source_schema={"cust_code": "TEXT"},
        target_columns=["cust_code"], target_schema={"cust_code": _LIVE["cust_code"]},
        mappings=[_map("cust_code")], destination_db_type="mysql", table_exists=True,
        dest_key_groups=[["cust_code"]],
    )
    additive = (drift.get("schema_evolution") or {}).get("additive") or []
    assert not [a for a in additive if a.get("kind") == "widen_type"]


def test_a_key_with_budget_left_is_offered_the_budgeted_varchar_not_text():
    # (region VARCHAR(16), code VARCHAR(100)): 3072 - 64 bytes leaves 752 chars.
    assert _props(["code"], [["region", "code"]]) == {"code": "VARCHAR(752)"}


def test_unkeyed_and_non_mysql_columns_keep_the_plain_widen():
    assert _props(["code"], None) == {"code": "TEXT"}
    assert _props(["cust_code"], [["cust_code"]], dest="postgresql") == {"cust_code": "TEXT"}


def test_a_keyed_varchar_growth_is_capped_at_the_key_budget():
    """VARCHAR(2000) cannot be a MySQL key (8000 bytes > 3072): the widen is VARCHAR(768)."""
    from connectors.schema_drift import mysql_keyed_widen_carrier

    live = {"id": "VARCHAR(200) CHARACTER SET UTF8MB4", "region": "VARCHAR(16)"}
    assert mysql_keyed_widen_carrier([["id"]], "id", "VARCHAR(2000) CHARACTER SET UTF8MB4", live) == (
        "VARCHAR(768) CHARACTER SET UTF8MB4", False)
    assert mysql_keyed_widen_carrier([["id"]], "id", "VARCHAR(300)", live) == ("VARCHAR(300)", False)
    assert mysql_keyed_widen_carrier([["region", "id"]], "id", "VARCHAR(2000)", live) == (
        "VARCHAR(752)", False)
    assert mysql_keyed_widen_carrier([["id"]], "id", "JSON", live)[1] is True


def test_plain_indexes_and_unique_keys_are_one_key_group_source():
    """The writer reads every index; the proposal reads the same set from the catalog."""
    indexes_meta = {"status": "ok", "items": [
        {"name": "ix_code", "columns": [{"name": "code"}], "unique": False},
        {"name": "ix_expr", "columns": [], "expression": "lower(code)"},
    ]}
    groups = dest_key_groups(["cust_code"], [{"columns": ["note"]}], "cust_code", indexes_meta)
    assert groups == [["cust_code"], ["note"], ["code"]]
    assert _props(["code"], groups) == {"code": "VARCHAR(768)"}
