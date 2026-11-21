"""Review of the Oracle ``''`` gate (REVIEW_ORACLE.md): one test per finding.

The gate must decide alike on every Validate entry point (REST, Studio, MCP,
Execute) and never pass what Execute then refuses. A SQLite source stands in
for the SQL source here (it has a catalog, NOT NULL, a primary key and the
push-down); the Oracle legs run in oracle-live.
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from services import oracle_empty_not_null as oe


@pytest.fixture
def sqlite_source(tmp_path, monkeypatch):
    """``t``: text PK ``id`` ('' in one row), ``note TEXT NOT NULL`` ('' twice), nullable ``memo``."""
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    db = tmp_path / "src.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL, note TEXT NOT NULL, memo TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?, ?, ?)",
        [("a", "", None), ("b", "x", "keep"), ("", "", "keep"), ("c", "  ", "keep")],
    )
    conn.commit()
    conn.close()
    return {"type": "sqlite", "database": str(db)}


_MAPS = [{"source": c, "target": c, "confidence": 0.99, "target_type": "TEXT"} for c in ("id", "note", "memo")]


def _validate(cfg, **kw):
    """``run_file_preflight`` with the arguments Studio's plan preflight passes: no nullability."""
    from services.preflight_service import run_file_preflight

    args = dict(
        columns=["id", "note", "memo"],
        column_types={"id": "TEXT", "note": "TEXT", "memo": "TEXT"},
        row_count=4,
        mappings=[dict(m) for m in _MAPS],
        destination_connected=True,
        sample_rows=[{"id": "b", "note": "x", "memo": "keep"}],
        destination_table_exists=False,
        destination_can_create=True,
        destination_db_type="oracle",
        source_kind="database",
        source_format="sqlite",
        source_config=cfg,
        source_table="t",
        destination_table="DEST",
        sync_mode="full_refresh_overwrite",
    )
    args.update(kw)
    return run_file_preflight(**args)


def _gate(result):
    return next((g for g in result.get("gates") or [] if g.get("id") == oe.GATE_ID), None)


def _blocker(result):
    return next((b for b in result.get("blockers") or [] if oe.GATE_ID in (b.get("id"), b.get("gate"))), None)


# ---------------------------------------------------------------- finding 1

def test_f1_studio_validate_blocks_without_being_handed_the_source_nullability(sqlite_source):
    result = _validate(sqlite_source)
    assert result["passed"] is False
    gate = _gate(result)
    assert gate and gate["status"] == "block", result.get("gates")
    assert "note (NOT NULL from the source) would receive 2 empty string(s)" in gate["message"]


def test_f1_rest_validate_blocks_a_new_oracle_table(sqlite_source, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    import importlib

    router_module = importlib.import_module("src.routers.preflight_router")
    monkeypatch.setattr(
        router_module, "inspect_destination_for_preflight",
        lambda **_kw: {"connected": True, "table_exists": False, "db_type": "oracle",
                       "can_create_table": True, "can_write": True},
    )
    from src.main import app

    with TestClient(app) as client:
        resp = client.post("/api/v1/preflight/run", json={
            "columns": ["id", "note", "memo"],
            "column_types": {"id": "TEXT", "note": "TEXT", "memo": "TEXT"},
            "row_count": 4,
            "mappings": _MAPS,
            "sample_rows": [{"id": "b", "note": "x", "memo": "keep"}],
            "source_kind": "database", "source_type": "sqlite",
            "source_config": sqlite_source, "source_table": "t",
            "dest_kind": "database", "dest_type": "oracle", "dest_host": "127.0.0.1",
            "dest_table": "DEST",
        })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    blockers = [b for b in body.get("blockers") or [] if oe.GATE_ID in (b.get("id"), b.get("gate"))]
    assert blockers, body.get("blockers")
    assert "2 empty string(s)" in blockers[0]["message"]


# ---------------------------------------------------------------- finding 3

def test_f3_a_source_error_mid_walk_is_could_not_check_not_a_crash():
    def broken_walk(cols):
        yield {"note": "x"}
        raise ConnectionError("server closed the connection")

    gate, blockers = oe.preflight_gate(
        destination_db_type="oracle", mappings=[dict(m) for m in _MAPS],
        column_nullability={"note": False}, destination_table_exists=False,
        destination_column_nullability={}, column_types={}, key_columns=[],
        sample_rows=[], walk=broken_walk,
    )
    assert gate["status"] == "block" and blockers
    assert "could not rule out empty strings in note" in gate["message"]
    assert "server closed the connection" in gate["message"]


def test_f3_the_walk_reads_the_fit_scans_scope(sqlite_source, monkeypatch):
    """A row filter that excludes every '' row: the run is not blocked."""
    from services import preflight_population

    seen: dict = {}
    real = preflight_population._iter_table_population_for_preflight

    def spy(**kw):
        seen.update(kw)
        return real(**kw)

    monkeypatch.setattr(preflight_population, "_iter_table_population_for_preflight", spy)
    result = _validate(
        sqlite_source, mappings=[dict(m) for m in _MAPS if m["source"] == "note"],
        source_filter={"column": "memo", "op": "eq", "value": "keep"},
    )
    # The whole table holds 2 '' notes; the filtered rows hold 1 ('' with id '').
    assert seen.get("source_filter") == {"column": "memo", "op": "eq", "value": "keep"}
    assert "would receive 1 empty string(s)" in _gate(result)["message"]
    clean = _validate(
        sqlite_source, mappings=[dict(m) for m in _MAPS if m["source"] == "note"],
        source_filter={"column": "id", "op": "eq", "value": "b"},
    )
    assert _gate(clean) is None


# ---------------------------------------------------------------- finding 4

def test_f4_a_preview_only_source_blocks_a_create_new_not_null_column():
    gate, blockers = oe.preflight_gate(
        destination_db_type="oracle", mappings=[dict(m) for m in _MAPS],
        column_nullability={"note": False}, destination_table_exists=False,
        destination_column_nullability={}, column_types={}, key_columns=[],
        sample_rows=[{"note": "x"}], walk=lambda cols: None,
    )
    assert gate["status"] == "block"
    assert "could not rule out empty strings in note, because only a preview" in gate["message"]
    assert any(a["id"] == oe.OPTION for a in blockers[0]["suggested_actions"])


def test_f4_a_scan_that_stops_blocks_instead_of_warning(monkeypatch):
    monkeypatch.setattr(oe, "SCAN_MAX_ROWS", 2)
    rows = [{"note": "x"}] * 3 + [{"note": ""}]
    targets = oe.candidates(
        destination_db_type="oracle", mappings=[dict(m) for m in _MAPS],
        column_nullability={"note": False}, destination_table_exists=False,
        destination_column_nullability={}, key_columns=[],
    )
    measured = oe.measure(targets, walk=lambda cols: iter(rows))
    assert measured[0]["exact"] is False and measured[0]["why"] == "rows"
    gate, _ = oe.build_gate(measured, mappings=_MAPS)
    assert gate["status"] == "block" and "the scan stopped after 2 rows" in gate["message"]


def test_f4_the_push_down_counts_the_whole_source_exactly(sqlite_source):
    counts = {
        c: oe.sql_empty_probe(source_connector_id="", source_config=sqlite_source,
                              source_table="t", column=c)
        for c in ("note", "id", "memo")
    }
    # '  ' is not the empty string; NULL is not either.
    assert counts == {"note": {"state": "found", "count": 2}, "id": {"state": "found", "count": 1},
                      "memo": {"state": "zero", "count": 0}}
    # A count past any walk bound is still exact: the walk is never asked.
    targets = oe.candidates(
        destination_db_type="oracle", mappings=[dict(m) for m in _MAPS],
        column_nullability={"note": False}, destination_table_exists=False,
        destination_column_nullability={}, key_columns=[],
    )
    measured = oe.measure(targets, probe=lambda col, now: {"state": "found", "count": 7}, whole_scope=True,
                          walk=lambda cols: pytest.fail("walked"))
    assert measured[0]["count"] == 7 and measured[0]["exact"] and measured[0]["how"] == "pushdown"


# ---------------------------------------------------------------- finding 5

def test_f5_one_source_into_two_targets_is_decided_per_mapping():
    maps = [{"source": "note", "target": "a", "target_type": "TEXT"},
            {"source": "note", "target": "b", "target_type": "TEXT"}]
    only_b = oe.sign_empty_as_null(
        maps, {"option": oe.OPTION, "columns": ["b"], "approved_by": "qa", "reason": "r"}, table="T",
    )
    assert oe.approved_contract(only_b[0]) is None and oe.approved_contract(only_b[1])
    gate, blockers = oe.preflight_gate(
        destination_db_type="oracle", mappings=only_b, column_nullability={"note": False},
        destination_table_exists=False, destination_column_nullability={}, column_types={},
        key_columns=[], sample_rows=[], walk=lambda cols: iter([{"note": ""}, {"note": ""}]),
        destination_table="T",
    )
    assert gate["status"] == "block"
    assert [c["target"] for c in gate["details"]["columns"]] == ["a"]
    assert [c["target"] for c in gate["details"]["approved"]] == ["b"]
    assert "a (NOT NULL from the source) would receive 2 empty string(s)" in gate["message"]


# ---------------------------------------------------------------- finding 6

def test_f6_the_source_primary_key_is_never_offered_as_null_able(sqlite_source):
    result = _validate(sqlite_source, column_nullability={"id": False, "note": False, "memo": True})
    gate = _gate(result)
    by_target = {c["target"]: c for c in gate["details"]["columns"]}
    assert by_target["id"]["approvable"] is False and by_target["note"]["approvable"] is True
    option = next(a for a in _blocker(result)["suggested_actions"] if a["id"] == oe.OPTION)
    assert option["risk_acceptance"]["columns"] == ["note"]
    assert "id (key column) would receive 1 empty string(s)" in gate["message"]


# ---------------------------------------------------------------- finding 7

def test_f7_gate8_never_calls_a_table_wide_null_count_explained(monkeypatch):
    signed = oe.sign_empty_as_null(
        [{"source": "note", "target": "note"}],
        {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"}, table="T",
    )
    scopes = []

    def fake(endpoint, table, columns, scope=None):
        scopes.append(getattr(scope, "kind", None))
        return {"note": 9}

    monkeypatch.setattr(oe, "destination_null_rows", fake)
    # An append into a table that held rows, no key list: the run's rows cannot be named.
    out = oe.explain_empty_as_null(
        {"passed": True, "message": "ok"}, endpoint=SimpleNamespace(format="oracle", table="T"),
        mappings=signed, records=[], dest_summary={"sync_mode": "append", "table": "T"},
        rows_written=3, validation_mode="strict",
    )
    assert scopes == [None]
    assert "explained_changes" not in out
    assert out["unscoped_null_cells"][0]["table_wide_null_cells"] == 9
    assert "NULL cells in this column: 9 (table-wide)" in out["message"]
    assert "not counted as explained" in out["message"]
    # The run's keys name its rows: counted there, and explained.
    scopes.clear()
    out = oe.explain_empty_as_null(
        {"passed": True, "message": ""}, endpoint=SimpleNamespace(format="oracle", table="T"),
        mappings=signed, records=[],
        dest_summary={"sync_mode": "append", "table": "T", "primary_key_columns": ["ID"],
                      "written_ids": [1, 2, 3]},
        rows_written=3,
    )
    assert scopes == ["run_keys"] and out["explained_changes"][0]["rows"] == 9


def test_f7_the_run_records_are_counted_as_written_after_the_transform():
    signed = oe.sign_empty_as_null(
        [{"source": "note", "target": "note", "transform": "trim"}],
        {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"}, table="T",
    )
    out = oe.explain_empty_as_null(
        {"passed": True, "message": ""}, endpoint=SimpleNamespace(format="oracle", table="T"),
        mappings=signed, records=[{"note": "   "}, {"note": ""}, {"note": "x"}],
    )
    assert out["explained_changes"][0]["rows"] == 2


def test_f7_destination_count_is_limited_to_the_run_keys(tmp_path, monkeypatch):
    from services.null_marker_probe import ProbeScope
    from src.transfer.models import EndpointConfig

    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    db = tmp_path / "dest.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INT, note TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, None), (2, None), (3, "x"), (4, None)])
    conn.commit()
    conn.close()
    endpoint = EndpointConfig(kind="database", format="sqlite", database=str(db), table="t")
    assert oe.destination_null_rows(endpoint, "t", ["note"]) == {"note": 3}
    scope = ProbeScope("run_keys", "id", ("1", "3"))
    assert oe.destination_null_rows(endpoint, "t", ["note"], scope=scope) == {"note": 1}


# ---------------------------------------------------------------- finding 9

def _signed_note(table="ORDERS"):
    return oe.sign_empty_as_null(
        [{"source": "note", "target": "note"}, {"source": "other", "target": "other"}],
        {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"}, table=table,
    )


def test_f9_a_contract_pasted_onto_another_column_or_table_is_not_an_approval(monkeypatch):
    note, other = _signed_note()
    assert oe.approved_contract(note, table="ORDERS")
    pasted = {**other, oe.CONTRACT_KEY: note[oe.CONTRACT_KEY]}
    assert oe.approved_contract(pasted, table="ORDERS") is None
    retargeted = {**note, "target": "NOTE_2"}
    assert oe.approved_contract(retargeted, table="ORDERS") is None
    assert oe.approved_contract(note, table="APP.CUSTOMERS") is None
    assert oe.approved_contract(note, table="app.orders")  # same table, schema-qualified
    monkeypatch.setattr(oe, "current_workspace", lambda: "ws-a")
    bound = oe.sign_empty_as_null(
        [{"source": "note", "target": "note"}],
        {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"}, table="ORDERS",
    )[0]
    assert oe.approved_contract(bound, table="ORDERS")
    monkeypatch.setattr(oe, "current_workspace", lambda: "ws-b")
    assert oe.approved_contract(bound, table="ORDERS") is None


def test_f9_the_relaxed_column_keeps_no_stale_carried_not_null_item():
    from services.schema_fidelity import resolve_create_fidelity_plan

    catalog = {"dialect": "postgresql", "columns": ["id", "note"], "primary_key": ["id"],
               "nullable": {"id": False, "note": False}}
    signed = oe.sign_empty_as_null(
        [{"source": "id", "target": "id"}, {"source": "note", "target": "note"}],
        {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"}, table="T",
    )
    plan = resolve_create_fidelity_plan(
        source_schema_catalog=catalog, mappings=signed, target_columns=["id", "note"],
        target_types=["NUMBER(19)", "CLOB"], dest_dialect="oracle", dest_table="T",
    )
    note_items = [i for i in plan.report.items if i.aspect == "not_null" and i.name == "note"]
    assert [i.status for i in note_items] == ["relaxed_by_contract"]
    # Signed for another table: nothing is relaxed.
    other = resolve_create_fidelity_plan(
        source_schema_catalog=catalog, mappings=signed, target_columns=["id", "note"],
        target_types=["NUMBER(19)", "CLOB"], dest_dialect="oracle", dest_table="OTHER",
    )
    assert "note" in other.not_null_columns
