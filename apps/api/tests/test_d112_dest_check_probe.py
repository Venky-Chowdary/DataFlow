"""D112 — destination CHECK constraints are probed against source rows at
Validate, naming the offenders before Confirm.

Pre-fix: ``run_file_preflight`` had no ``destination_check_constraints``
parameter and ``services.dest_check_probe`` did not exist — the first
violation surfaced post-Confirm as a raw driver error.
"""

from __future__ import annotations

import pytest

from services.dest_check_probe import probe_dest_check_constraints


def _map(cols):
    return [{"source": c, "target": c} for c in cols]


def test_probe_names_the_offending_row():
    report = probe_dest_check_constraints(
        rows=[
            {"id": 17, "amt": -5.25},
            {"id": 23, "amt": 100.00},
        ],
        mappings=_map(["id", "amt"]),
        check_constraints=[
            {"name": "hq2207_cd_amt_check", "sqltext": "CHECK ((amt >= 0))"}
        ],
    )
    assert report["ran"] is True
    assert report["evaluated"] == 1
    assert report["violating_count"] == 1
    assert report["violations"][0]["row_index"] == 0
    assert report["violations"][0]["constraint"] == "hq2207_cd_amt_check"
    assert report["violations"][0]["values"] == {"amt": -5.25}


def test_sql_three_valued_logic_null_is_not_a_violation():
    report = probe_dest_check_constraints(
        rows=[{"amt": None}, {"amt": -1}],
        mappings=_map(["amt"]),
        check_constraints=[{"sqltext": "amt >= 0"}],
    )
    # NULL -> UNKNOWN, which CHECK tolerates; only the -1 row violates.
    assert report["violating_count"] == 1


def test_nonportable_predicate_is_reported_not_evaluated():
    report = probe_dest_check_constraints(
        rows=[{"note": "x"}],
        mappings=_map(["note"]),
        check_constraints=[{"sqltext": "char_length(note) > 0"}],
    )
    assert report["ran"] is True
    assert report["evaluated"] == 0
    assert report["not_evaluated"] == 1
    assert report["violating_count"] == 0
    assert "not proven" in report["note"].lower() or "not evaluable" in report["note"].lower()


def test_in_between_isnotnull_predicates():
    report = probe_dest_check_constraints(
        rows=[
            {"code": "zz", "amt": 5, "note": "ok"},
            {"code": "aa", "amt": 50, "note": None},
        ],
        mappings=_map(["code", "amt", "note"]),
        check_constraints=[
            {"sqltext": "code IN ('aa','bb')"},
            {"sqltext": "amt BETWEEN 0 AND 10"},
            {"sqltext": "note IS NOT NULL"},
        ],
    )
    # row 0: code 'zz' not in list -> violate; amt ok; note ok
    # row 1: code ok; amt 50 outside -> violate; note NULL -> violate
    assert report["violating_count"] == 3


def test_run_file_preflight_surfaces_dest_check_finding():
    from services.preflight_service import run_file_preflight

    out = run_file_preflight(
        columns=["id", "amt"],
        column_types={"id": "integer", "amt": "numeric"},
        row_count=2,
        mappings=[{"source": "id", "target": "id"}, {"source": "amt", "target": "amt"}],
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        destination_connected=True,
        destination_column_types={"id": "integer", "amt": "numeric(12,2)"},
        destination_table_exists=True,
        sample_rows=[{"id": 1, "amt": 3.5}, {"id": 2, "amt": -9.0}],
        destination_check_constraints=[
            {"name": "amt_nonneg", "sqltext": "amt >= 0"}
        ],
    )
    probe = out.get("dest_check_probe") or {}
    assert probe.get("ran") is True
    assert probe.get("violating_count") == 1
    codes = {f.get("code") for f in out.get("constraint_findings") or []}
    assert "dest_check_violation" in codes
